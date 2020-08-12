#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/spdk_drive_interface.hpp"
#include <folly/Exception.h>
#include <fds/obj_allocator.hpp>
#include <fds/utils.hpp>
#include <filesystem>
extern "C" {
#include <spdk/env.h>
#include <spdk/thread.h>
#include <spdk/string.h>
#ifdef SPDK_DRIVE_USE_URING
#include <spdk/module/bdev/uring/bdev_uring.h>
#else
#include <spdk/module/bdev/aio/bdev_aio.h>
#endif
}
namespace iomgr {

static io_thread_t _non_io_thread = std::make_shared< io_thread >();

SpdkDriveInterface::SpdkDriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb) {
    // m_my_msg_modid = iomanager.register_msg_module(bind_this(handle_msg, 1));
    m_my_msg_modid = iomanager.register_msg_module([this](iomgr_msg* msg) { handle_msg(msg); });
}

static void bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev* bdev, void* event_ctx) {}

io_device_ptr SpdkDriveInterface::open_dev(const std::string& devname, [[maybe_unused]] int oflags) {
    if (iomanager.am_i_worker_reactor()) {
        DEBUG_ASSERT(iomanager.am_i_tight_loop_reactor(), "Opening SPDK drive on non-spdk system");
        return _open_dev(devname);
    } else {
        // Issue the opendev on any one of the tight loop reactor
        io_device_ptr ret;
        iomanager.run_on(thread_regex::least_busy_worker,
                         [this, &ret, devname](io_thread_addr_t taddr) { ret = _open_dev(devname); },
                         true /* wait_for_completion */);
        return ret;
    }
}

io_device_ptr SpdkDriveInterface::_open_dev(const std::string& devname) {
    std::string bdevname = devname;

    // Check if the device is a file, if so create a bdev out of the file and open that bdev. This is meant only for
    // a testing and docker environment
    if (std::filesystem::is_regular_file(std::filesystem::status(devname))) {
        LOGINFO("Opening {} as an SPDK drive, creating a bdev out of the file, performance will be impacted", devname);
        bdevname += std::string("_bdev");

#ifdef SPDK_DRIVE_USE_URING
        auto ret_bdev = create_uring_bdev(bdevname.c_str(), devname.c_str(), 512u);
        if (ret_bdev == nullptr) {
            folly::throwSystemError(fmt::format("Unable to open the device={} to create bdev error", bdevname));
        }
#else
        int ret = create_aio_bdev(bdevname.c_str(), devname.c_str(), 512u);
        if (ret != 0) {
            folly::throwSystemError(fmt::format("Unable to open the device={} to create bdev error={}", bdevname, ret));
        }
#endif
    }

    struct spdk_bdev_desc* desc = NULL;
    auto rc = spdk_bdev_open_ext(bdevname.c_str(), true, bdev_event_cb, NULL, &desc);
    if (rc != 0) { folly::throwSystemError(fmt::format("Unable to open the device={} error={}", bdevname, rc)); }

    auto iodev = std::make_shared< IODevice >();
    iodev->dev = backing_dev_t(desc);
    iodev->thread_scope = thread_regex::all_worker;
    iodev->pri = 9;
    iodev->io_interface = this;
    iodev->devname = bdevname;

    add_io_device(iodev, true /* wait_to_add */);
    LOGINFOMOD(iomgr, "Device {} opened successfully", bdevname);
    return iodev;
}

void SpdkDriveInterface::close_dev(const io_device_ptr& iodev) {
    IOInterface::close_dev(iodev);
    // TODO: Close the bdev device
    iodev->clear();
}

void SpdkDriveInterface::init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) {
    auto dctx = new SpdkDriveDeviceContext();
    iodev->m_thread_local_ctx[thr->thread_idx] = (void*)dctx;
    dctx->channel = spdk_bdev_get_io_channel(iodev->bdev_desc());
    if (dctx->channel == NULL) {
        folly::throwSystemError(fmt::format("Unable to get io channel for bdev={}", spdk_bdev_get_name(iodev->bdev())));
    }
}

void SpdkDriveInterface::clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) {
    auto dctx = (SpdkDriveDeviceContext*)iodev->m_thread_local_ctx[thr->thread_idx];
    if (dctx->channel != NULL) { spdk_put_io_channel(dctx->channel); }
    delete (dctx);
}

static spdk_io_channel* get_io_channel(IODevice* iodev) {
    auto tidx = iomanager.this_reactor()->select_thread()->thread_idx;
    auto dctx = (SpdkDriveDeviceContext*)iodev->m_thread_local_ctx[tidx];
    return dctx->channel;
}

static void complete_io(SpdkIocb* iocb) {
    // As of now we complete the batch as soon as 1 io is completed. We can potentially do batching based on if async
    // thread is completed or not
    auto& ecb = iocb->iface->get_end_of_batch_cb();
    if (ecb) { ecb(1); }

    sisl::ObjectAllocator< SpdkIocb >::deallocate(iocb);
}

static void process_completions(struct spdk_bdev_io* bdev_io, bool success, void* ctx) {
    SpdkIocb* iocb = (SpdkIocb*)ctx;
    LOGTRACEMOD(iomgr, "Received completion on bdev = {}", (void*)iocb->iodev->bdev_desc());
    spdk_bdev_free_io(bdev_io);

    if (success) {
        iocb->result = 0;
        LOGTRACEMOD(iomgr, "bdev io completed successfully. io info: {}", iocb->to_string());
    } else {
        // COUNTER_INCREMENT(m_metrics, completion_errors, 1);
        iocb->result = -1;
    }

    auto& cb = iocb->comp_cb ? iocb->comp_cb : iocb->iface->get_completion_cb();
    cb(*iocb->result, (uint8_t*)iocb->user_cookie);

    if (iocb->owner_thread == nullptr) {
        // If the iocb has been issued by this thread, we need to complete io, else that different thread will do so
        complete_io(iocb);
    }
}

static void submit_io(void* b) {
    SpdkIocb* iocb = (SpdkIocb*)b;
    int rc = 0;
    if (iocb->op_type == SpdkDriveOpType::READ) {
        if (iocb->user_data) {
            rc = spdk_bdev_read(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->user_data, iocb->offset,
                                iocb->size, process_completions, (void*)iocb);
        } else {
            rc = spdk_bdev_readv(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->iovs, iocb->iovcnt,
                                 iocb->offset, iocb->size, process_completions, (void*)iocb);
        }
    } else if (iocb->op_type == SpdkDriveOpType::WRITE) {
        if (iocb->user_data) {
            rc = spdk_bdev_write(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->user_data, iocb->offset,
                                 iocb->size, process_completions, (void*)iocb);
        } else {
            rc = spdk_bdev_writev(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->iovs, iocb->iovcnt,
                                  iocb->offset, iocb->size, process_completions, (void*)iocb);
        }
    } else if (iocb->op_type == SpdkDriveOpType::UNMAP) {
        rc = spdk_bdev_unmap(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->offset, iocb->size,
                             process_completions, (void*)iocb);
    } else {
        LOGDFATAL("Invalid operation type {}", iocb->op_type);
        return;
    }

    if (rc != 0) {
        if (rc == -ENOMEM) {
            LOGDEBUGMOD(iomgr, "Bdev is lacking memory to do IO right away, queueing it\n");
            iocb->copy_iovs();
            spdk_bdev_queue_io_wait(iocb->iodev->bdev(), get_io_channel(iocb->iodev), &iocb->io_wait_entry);
        }
    }
}

inline bool SpdkDriveInterface::try_submit_io(SpdkIocb* iocb, bool part_of_batch) {
    bool ret = true;
    if (iomanager.am_i_tight_loop_reactor()) {
        submit_io(iocb);
    } else if (iomanager.am_i_io_reactor()) {
        do_async_in_tloop_thread(iocb, part_of_batch);
    } else {
        ret = false;
    }
    return ret;
}

void SpdkDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, cookie);
    iocb->user_data = (char*)data;
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) {
        auto ret = do_sync_io(iocb);
        if (m_comp_cb) m_comp_cb((ret != size), cookie);
    }
}

void SpdkDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                    bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, cookie);
    iocb->user_data = (char*)data;
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) {
        auto ret = do_sync_io(iocb);
        if (m_comp_cb) m_comp_cb((ret != size), cookie);
    }
}

void SpdkDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, cookie);

    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) {
        auto ret = do_sync_io(iocb);
        if (m_comp_cb) m_comp_cb((ret != size), cookie);
    }
}

void SpdkDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, cookie);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(std::move(iocb), part_of_batch)) {
        auto ret = do_sync_io(iocb);
        if (m_comp_cb) m_comp_cb((ret != size), cookie);
    }
}

void SpdkDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::UNMAP, size, offset, cookie);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) {
        auto ret = do_sync_io(iocb);
        if (m_comp_cb) m_comp_cb((ret != size), cookie);
    }
}

ssize_t SpdkDriveInterface::sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, nullptr);
    iocb->user_data = (char*)data;
    return do_sync_io(iocb);
}

ssize_t SpdkDriveInterface::sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, nullptr);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    return do_sync_io(iocb);
}

ssize_t SpdkDriveInterface::sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, nullptr);
    iocb->user_data = (char*)data;
    return do_sync_io(iocb);
}

ssize_t SpdkDriveInterface::sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, nullptr);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    return do_sync_io(iocb);
}

ssize_t SpdkDriveInterface::do_sync_io(SpdkIocb* iocb) {
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->owner_thread = _non_io_thread;
    iocb->copy_iovs();
    iocb->comp_cb = [&](int64_t res, uint8_t* cookie) {
        std::unique_lock< std::mutex > lk(m_sync_cv_mutex);
        iocb->result = res;
        m_sync_cv.notify_all();
    };

    auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
    iomanager.multicast_msg(thread_regex::least_busy_worker, msg);

    {
        std::unique_lock< std::mutex > lk(m_sync_cv_mutex);
        m_sync_cv.wait(lk, [&]() { return iocb->result; });
    }

    auto ret = (*iocb->result == 0) ? iocb->size : 0;
    sisl::ObjectAllocator< SpdkIocb >::deallocate(iocb);
    return ret;
}

void SpdkDriveInterface::do_async_in_tloop_thread(SpdkIocb* iocb, bool part_of_batch) {
    assert(iomanager.am_i_io_reactor()); // We have to run reactor otherwise async response will not be handled.
    iocb->owner_thread = iomanager.iothread_self(); // TODO: This makes a shared_ptr copy, see if we can avoid it
    iocb->copy_iovs();
    iocb->comp_cb = [this, iocb, part_of_batch](int64_t res, uint8_t* cookie) {
        static thread_local std::vector< SpdkIocb* > comp_iocbs;
        if (part_of_batch) {
            comp_iocbs.emplace_back(iocb);
            if (comp_iocbs.size() == SPDK_BATCH_IO_NUM) {
                // send batch io done msg to spdk thread;
                iocb->result = res;
                auto reply = iomgr_msg::create(spdk_msg_type::ASYNC_BATCH_IO_DONE, m_my_msg_modid,
                                               (uint8_t*)&(comp_iocbs[0]), sizeof(SpdkIocb*) * SPDK_BATCH_IO_NUM);
                iomanager.send_msg(iocb->owner_thread, reply);
                comp_iocbs.clear();
            }
            // not reaching threashold yet, keep waiting other batch io comp_cb;
        } else {
            // not batch io, go ahead to send msg;
            iocb->result = res;
            auto reply =
                iomgr_msg::create(spdk_msg_type::ASYNC_IO_DONE, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
            iomanager.send_msg(iocb->owner_thread, reply);
        }
    };

    static thread_local std::vector< SpdkIocb* > batch_io;
    if (part_of_batch && batch_io.size() < SPDK_BATCH_IO_NUM) {
        batch_io.emplace_back(iocb);
        if (batch_io.size() == SPDK_BATCH_IO_NUM) {
            auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_BATCH_IO, m_my_msg_modid, (uint8_t*)&(batch_io[0]),
                                         sizeof(SpdkIocb*) * SPDK_BATCH_IO_NUM);
            iomanager.multicast_msg(thread_regex::least_busy_worker, msg);
        }
        batch_io.clear();
    } else {
        auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
        iomanager.multicast_msg(thread_regex::least_busy_worker, msg);
    }
} // namespace iomgr

void SpdkDriveInterface::handle_msg(iomgr_msg* msg) {
    switch (msg->m_type) {
    case spdk_msg_type::QUEUE_IO: {
        auto iocb = (SpdkIocb*)msg->data_buf().bytes;
        submit_io((void*)iocb);
        break;
    }

    case spdk_msg_type::QUEUE_BATCH_IO: {
        auto iocb = (SpdkIocb**)msg->data_buf().bytes;
        for (size_t i = 0; i < SPDK_BATCH_IO_NUM; ++i) {
            submit_io((void*)iocb[i]);
        }
        break;
    }

    case spdk_msg_type::ASYNC_IO_DONE: {
        auto iocb = (SpdkIocb*)msg->data_buf().bytes;
        if (m_comp_cb) m_comp_cb(*iocb->result, (uint8_t*)iocb->user_cookie);
        complete_io(iocb);
        break;
    }

    case spdk_msg_type::ASYNC_BATCH_IO_DONE: {
        auto iocb = (SpdkIocb**)msg->data_buf().bytes;
        for (size_t i = 0; i < SPDK_BATCH_IO_NUM; ++i) {
            if (m_comp_cb) m_comp_cb(*iocb[i]->result, (uint8_t*)iocb[i]->user_cookie);
            complete_io(iocb[i]);
        }
        break;
    }
    }
}

size_t SpdkDriveInterface::get_size(IODevice* iodev) {
    return spdk_bdev_get_num_blocks(iodev->bdev()) * spdk_bdev_get_block_size(iodev->bdev());
}
} // namespace iomgr

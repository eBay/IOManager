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
#include <spdk/module/bdev/nvme/bdev_nvme.h>
#include <spdk/nvme.h>
}
namespace iomgr {

static io_thread_t _non_io_thread = std::make_shared< io_thread >();

SpdkDriveInterface::SpdkDriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb) {
    // m_my_msg_modid = iomanager.register_msg_module(bind_this(handle_msg, 1));
    m_my_msg_modid = iomanager.register_msg_module([this](iomgr_msg* msg) { handle_msg(msg); });
}

static void bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev* bdev, void* event_ctx) {}

struct creat_ctx {
    std::string address;
    iomgr_drive_type addr_type;
    std::error_condition err;
    const char* names[128];
    std::mutex lock;
    std::condition_variable cv;
    bool done{false};
    std::string bdev_name;
};

static void create_fs_bdev(std::shared_ptr< creat_ctx > ctx) {
    {
        auto lg = std::lock_guard< std::mutex >(ctx->lock);
        LOGINFO("Opening {} as an SPDK drive, creating a bdev out of the file, performance will be impacted",
                ctx->address);
        auto const bdev_name = ctx->address + std::string("_bdev");

#ifdef SPDK_DRIVE_USE_URING
        auto ret_bdev = create_uring_bdev(bdev_name.c_str(), ctx->address.c_str(), 512u);
        if (ret_bdev == nullptr) {
            folly::throwSystemError(fmt::format("Unable to open the device={} to create bdev error", bdev_name));
        }
#else
        int ret = create_aio_bdev(bdev_name.c_str(), ctx->address.c_str(), 512u);
        if (ret != 0) {
            folly::throwSystemError(
                fmt::format("Unable to open the device={} to create bdev error={}", bdev_name, ret));
        }
#endif
        ctx->bdev_name = bdev_name;
        ctx->done = true;
    }
    ctx->cv.notify_all();
}

static void create_bdev_done(void* cb_ctx, size_t bdev_cnt, int rc) {
    auto context = static_cast< creat_ctx* >(cb_ctx);
    {
        auto lg = std::lock_guard< std::mutex >(context->lock);
        LOGDEBUGMOD(iomgr, "Volume setup for {} received: [rc={}, cnt={}]", context->address, rc, bdev_cnt);
        if (0 < bdev_cnt) {
            context->bdev_name = std::string(context->names[--bdev_cnt]);
            LOGDEBUGMOD(iomgr, "Created BDev: [{}]", context->bdev_name);
        }
        context->done = true;
    }
    context->cv.notify_all();
}

static void create_nvme_bdev(std::shared_ptr< creat_ctx > ctx) {
    auto address_c = ctx->address.c_str();
    spdk_nvme_transport_id trid;
    memset(&trid, 0, sizeof(trid));
    trid.trtype = SPDK_NVME_TRANSPORT_PCIE;

    do {
        auto rc = spdk_nvme_transport_id_parse(&trid, address_c);
        if (rc < 0) {
            LOGERROR("Failed to parse given str: {}", address_c);
            continue;
        }

        if (trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
            struct spdk_pci_addr pci_addr;
            if (spdk_pci_addr_parse(&pci_addr, trid.traddr) < 0) {
                LOGERROR("Invalid traddr={}", address_c);
                continue;
            }
            spdk_pci_addr_fmt(trid.traddr, sizeof(trid.traddr), &pci_addr);
        } else {
            if (trid.subnqn[0] == '\0') { snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN); }
        }

        /* Enumerate all of the controllers */
        spdk_nvme_host_id hostid{};
        uint32_t count{128};
        if (rc = bdev_nvme_create(&trid, &hostid, "iomgr", ctx->names, count, nullptr, 0, create_bdev_done,
                                  (void*)ctx.get());
            0 != rc) {
            LOGERROR("Failed createing NVMe BDEV from {}", trid.traddr);
        }
        // Good exit path, wait for create_bdev_done!
        return;
    } while (false);
    // Failed!
    {
        auto lg = std::lock_guard< std::mutex >(ctx->lock);
        ctx->err = std::make_error_condition(std::errc::io_error);
        ctx->done = true;
    }
    ctx->cv.notify_all();
}

static void _creat_dev(std::shared_ptr< creat_ctx > ctx) {
    // Check if the device is a file, if so create a bdev out of the file and open that bdev. This is meant only for
    // a testing and docker environment
    try {
        switch (ctx->addr_type) {
        case iomgr_drive_type::raw_nvme:
            create_nvme_bdev(ctx);
            break;

        case iomgr_drive_type::file:
        case iomgr_drive_type::block:
            create_fs_bdev(ctx);
            break;

        case iomgr_drive_type::spdk_bdev:
            // Already a bdev, set the ctx as done
            ctx->bdev_name = ctx->address;
            ctx->done = true;
            break;

        default:
            LOGMSG_ASSERT(0, "Unsupported device={} of type={} being opened", ctx->address, enum_name(ctx->addr_type));
            ctx->err = std::make_error_condition(std::errc::bad_file_descriptor);
        }
    } catch (std::exception& e) {
        if (!ctx->err) {
            LOGMSG_ASSERT(0, "Error in creating bdev for device={} of type={}, Exception={}", ctx->address,
                          enum_name(ctx->addr_type), e.what());
            ctx->err = std::make_error_condition(std::errc::bad_file_descriptor);
        }
    }
}

io_device_ptr SpdkDriveInterface::open_dev(const std::string& devname, iomgr_drive_type drive_type,
                                           [[maybe_unused]] int oflags) {
    io_device_ptr ret{nullptr};
    m_opened_device.withWLock([this, &devname, &ret, &drive_type](auto& m) {
        auto it = m.find(devname);
        if ((it == m.end()) || (it->second == nullptr)) {
            ret = _real_open_dev(devname, drive_type);
            m.insert(std::make_pair<>(devname, ret));
        } else {
            ret = it->second;
        }
    });
    return ret;
}

io_device_ptr SpdkDriveInterface::_real_open_dev(const std::string& devname, iomgr_drive_type drive_type) {
    io_device_ptr ret{nullptr};
    RELEASE_ASSERT(!iomanager.am_i_worker_reactor(),
                   "We cannot open the device from a worker reactor thread unless its a bdev");

    // First create the bdev
    auto create_ctx = std::make_shared< creat_ctx >();
    create_ctx->address = devname;
    create_ctx->addr_type = (drive_type == iomgr_drive_type::unknown) ? get_drive_type(devname) : drive_type;
    {
        iomanager.run_on(
            thread_regex::least_busy_worker, [create_ctx](io_thread_addr_t taddr) { _creat_dev(create_ctx); },
            false /* wait_for_completion */);
        auto ul = std::unique_lock< std::mutex >(create_ctx->lock);
        create_ctx->cv.wait(ul, [create_ctx] { return create_ctx->done; });
    }

    if (!create_ctx->bdev_name.empty() && !create_ctx->err) {
        // Issue the opendev on any one of the tight loop reactor
        iomanager.run_on(
            thread_regex::least_busy_worker,
            [this, &ret, bdev_name = create_ctx->bdev_name](io_thread_addr_t taddr) {
                ret = _open_dev_in_worker(bdev_name);
            },
            true /* wait_for_completion */);
    }
    return ret;
}

io_device_ptr SpdkDriveInterface::_open_dev_in_worker(const std::string& bdev_name) {
    struct spdk_bdev_desc* desc = NULL;
    auto rc = spdk_bdev_open_ext(bdev_name.c_str(), true, bdev_event_cb, NULL, &desc);
    if (rc != 0) { folly::throwSystemError(fmt::format("Unable to open the device={} error={}", bdev_name, rc)); }

    auto iodev = std::make_shared< IODevice >();
    iodev->dev = backing_dev_t(desc);
    iodev->thread_scope = thread_regex::all_tloop;
    iodev->pri = 9;
    iodev->io_interface = this;
    iodev->devname = bdev_name;

    add_io_device(iodev, true /* wait_to_add */);
    LOGINFOMOD(iomgr, "Device {} opened successfully", bdev_name);
    return iodev;
}

void SpdkDriveInterface::close_dev(const io_device_ptr& iodev) {
    IOInterface::close_dev(iodev);
    m_opened_device.wlock()->erase(iodev->devname);

    // TODO: Close the bdev device
    iodev->clear();
}

iomgr_drive_type SpdkDriveInterface::get_drive_type(const std::string& devname) const {
    iomgr_drive_type type = DriveInterface::get_drive_type(devname);
    if (type != iomgr_drive_type::unknown) { return type; }

    /* Lets find out if it is a nvme transport */
    spdk_nvme_transport_id trid;
    memset(&trid, 0, sizeof(trid));
    auto devname_c = devname.c_str();

    auto rc = spdk_nvme_transport_id_parse(&trid, devname_c);
    if ((rc == 0) && (trid.trtype == SPDK_NVME_TRANSPORT_PCIE)) { return iomgr_drive_type::raw_nvme; }

    auto bdev = spdk_bdev_get_by_name(devname_c);
    if (bdev) { return iomgr_drive_type::spdk_bdev; }

    return iomgr_drive_type::unknown;
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
    // As of now we complete the batch as soon as 1 io is completed. We can potentially do batching based on if
    // async thread is completed or not
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
        COUNTER_INCREMENT(iocb->iface->get_metrics(), completion_errors, 1);
        iocb->result = -1;
    }

    auto& cb = iocb->comp_cb ? iocb->comp_cb : iocb->iface->get_completion_cb();
    cb(*iocb->result, (uint8_t*)iocb->user_cookie);

    if (iocb->owner_thread == nullptr) {
        // If the iocb has been issued by this thread, we need to complete io, else that different thread
        // will do so
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
            iocb->copy_iovs();
            rc = spdk_bdev_readv(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->iovs, iocb->iovcnt,
                                 iocb->offset, iocb->size, process_completions, (void*)iocb);
        }
    } else if (iocb->op_type == SpdkDriveOpType::WRITE) {
        if (iocb->user_data) {
            rc = spdk_bdev_write(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->user_data, iocb->offset,
                                 iocb->size, process_completions, (void*)iocb);
        } else {
            iocb->copy_iovs();
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
            COUNTER_INCREMENT(iocb->iface->get_metrics(), queued_ios_for_memory_pressure, 1);
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
        COUNTER_INCREMENT(m_metrics, num_async_io_non_spdk_thread, 1);
        do_async_in_tloop_thread(iocb, part_of_batch);
    } else {
        COUNTER_INCREMENT(m_metrics, force_sync_io_non_spdk_thread, 1);
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
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                    bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, cookie);
    iocb->user_data = (char*)data;
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, cookie);

    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, cookie);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(std::move(iocb), part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::UNMAP, size, offset, cookie);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

ssize_t SpdkDriveInterface::sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, nullptr);
    iocb->user_data = (char*)data;
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, nullptr);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, nullptr);
    iocb->user_data = (char*)data;
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, nullptr);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::do_sync_io(SpdkIocb* iocb, const io_interface_comp_cb_t& comp_cb) {
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
    if (comp_cb) comp_cb(*iocb->result, (uint8_t*)iocb->user_cookie);
    complete_io(iocb);

    return ret;
}

void SpdkDriveInterface::do_async_in_tloop_thread(SpdkIocb* iocb, bool part_of_batch) {
    static thread_local SpdkBatchIocb* s_batch_info_ptr = nullptr;

    assert(iomanager.am_i_io_reactor()); // We have to run reactor otherwise async response will not be handled.
    iocb->owner_thread = iomanager.iothread_self(); // TODO: This makes a shared_ptr copy, see if we can avoid it
    iocb->copy_iovs();
    iocb->comp_cb = [this, iocb](int64_t res, uint8_t* cookie) {
        iocb->result = res;
        if (!iocb->batch_info_ptr) {
            auto reply =
                iomgr_msg::create(spdk_msg_type::ASYNC_IO_DONE, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
            iomanager.send_msg(iocb->owner_thread, reply);
        } else {
            ++(iocb->batch_info_ptr->num_io_comp);

            // batch io completion count should never exceed nuber of batch io;
            assert(iocb->batch_info_ptr->num_io_comp <= iocb->batch_info_ptr->batch_io->size());

            // re-use the batch info ptr which contains all the batch iocbs to send/create
            // async_batch_io_done msg;
            if (iocb->batch_info_ptr->num_io_comp == iocb->batch_info_ptr->batch_io->size()) {
                auto reply = iomgr_msg::create(spdk_msg_type::ASYNC_BATCH_IO_DONE, m_my_msg_modid,
                                               (uint8_t*)iocb->batch_info_ptr, sizeof(SpdkBatchIocb*));
                iomanager.send_msg(iocb->owner_thread, reply);
            }

            // if there is any batched io doesn't get all the iocb completion, we will see memory leak in
            // the end reported becuase batch io will not freed by spdk thread due to missing async io done
            // msg;
        }
    };

    if (!part_of_batch) {
        // we don't have a use-case for same user thread to issue part_of_batch to both true and false for now.
        // if we support same user-thread to send both, we should not use iocb->batch_info_ptr to check whether it is
        // batch io in comp_cb (line: 470);
        assert(iocb->batch_info_ptr == nullptr);
        auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
        iomanager.multicast_msg(thread_regex::least_busy_worker, msg);
    } else {
        if (s_batch_info_ptr == nullptr) { s_batch_info_ptr = new SpdkBatchIocb(); }

        iocb->batch_info_ptr = s_batch_info_ptr;
        s_batch_info_ptr->batch_io->push_back(iocb);

        // send this batch
        if (s_batch_info_ptr->batch_io->size() == SPDK_BATCH_IO_NUM) {
            auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_BATCH_IO, m_my_msg_modid, (uint8_t*)s_batch_info_ptr,
                                         sizeof(SpdkBatchIocb*));
            iomanager.multicast_msg(thread_regex::least_busy_worker, msg);

            // reset batch info ptr, memory will be freed by spdk thread after batch io completes;
            s_batch_info_ptr = nullptr;
        }
    }
}

void SpdkDriveInterface::handle_msg(iomgr_msg* msg) {
    switch (msg->m_type) {
    case spdk_msg_type::QUEUE_IO: {
        auto iocb = (SpdkIocb*)msg->data_buf().bytes;
        submit_io((void*)iocb);
        break;
    }

    case spdk_msg_type::QUEUE_BATCH_IO: {
        auto batch_info = (SpdkBatchIocb*)msg->data_buf().bytes;
        for (auto& iocb : *(batch_info->batch_io)) {
            submit_io((void*)iocb);
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
        auto batch_info = (SpdkBatchIocb*)(msg->data_buf().bytes);
        for (auto& iocb : *(batch_info->batch_io)) {
            if (m_comp_cb) { m_comp_cb(*iocb->result, (uint8_t*)iocb->user_cookie); }
            complete_io(iocb);
        }

        // now return memory to vector pool
        delete batch_info;
        break;
    }
    }
}

size_t SpdkDriveInterface::get_size(IODevice* iodev) {
    return spdk_bdev_get_num_blocks(iodev->bdev()) * spdk_bdev_get_block_size(iodev->bdev());
}

drive_attributes SpdkDriveInterface::get_attributes(const io_device_ptr& dev) const {
    assert(dev->is_spdk_dev());
    struct spdk_bdev* g_bdev = dev->bdev();
    drive_attributes attr;

    auto blk_size = spdk_bdev_get_block_size(g_bdev);

    /* Try to get the atomic physical page size from controller */
    attr.atomic_phys_page_size = 0;
    if (strncmp(g_bdev->product_name, "NVMe disk", sizeof("NVMe disk")) == 0) {
        struct nvme_bdev* n_bdev = (struct nvme_bdev*)g_bdev->ctxt;

        // Get the namespace data if available
        const struct spdk_nvme_ns_data* nsdata = spdk_nvme_ns_get_data(n_bdev->nvme_ns->ns);
        if (nsdata->nsfeat.ns_atomic_write_unit) {
            attr.atomic_phys_page_size = nsdata->nawupf * blk_size;
        } else {
            const struct spdk_nvme_ctrlr_data* cdata = spdk_nvme_ctrlr_get_data(n_bdev->nvme_bdev_ctrlr->ctrlr);
            attr.atomic_phys_page_size = cdata->awupf * blk_size;
        }
    }

    // TODO: At the moment we would not be able to support drive whose atomic phys page size < 4K. Need a way
    // to ensure that or fail the startup.
    if (attr.atomic_phys_page_size < 4096) {
        attr.atomic_phys_page_size = std::max(blk_size * spdk_bdev_get_acwu(dev->bdev()), 4096u);
    }

    // TODO-1 At the moment we are hard coding this - there is no sure shot way of getting this correctly. Our
    // performance metrics indicate this is the good phys page size. In NVMe 1.4 spec, there is a way to get the page
    // size optimal for NVMe viz. Namespace Preferred Write Granularity (NPWG) and Namespace Preferred Write Alignment
    // (NPWA).
    // TODO-2 Introduce attr.optimal_delete_boundary which can obtained from attribute - Namespace Preferred Deallocate
    // Granularity (NPDG):
    attr.phys_page_size = 4096u;

    // We want alignment to be mininum 512. TODO: Can we let it be 1 byte too??
    attr.align_size = std::max(spdk_bdev_get_buf_align(g_bdev), 512ul);

    return attr;
}

drive_attributes SpdkDriveInterface::get_attributes(const std::string& devname, const iomgr_drive_type drive_type) {
    return get_attributes(open_dev(devname, drive_type, 0));
}

} // namespace iomgr

#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/spdk_drive_interface.hpp"
#include <spdk/env.h>
#include <spdk/thread.h>
#include <spdk/string.h>
#include <folly/Exception.h>
#include <fds/obj_allocator.hpp>
#include <fds/utils.hpp>

using namespace std;
namespace iomgr {

SpdkDriveInterface::SpdkDriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb) {
    // m_my_msg_modid = iomanager.register_msg_module(bind_this(handle_msg, 1));
    m_my_msg_modid = iomanager.register_msg_module([this](iomgr_msg* msg) { handle_msg(msg); });
}

io_device_ptr SpdkDriveInterface::open_dev(const std::string& devname, [[maybe_unused]] int oflags) {
    struct spdk_bdev_desc* desc = NULL;
    auto rc = spdk_bdev_open_ext(devname.c_str(), true, NULL, NULL, &desc);
    if (rc != 0) { folly::throwSystemError(fmt::format("Unable to open the device={} error={}", devname, rc)); }

    auto iodev = std::make_shared< io_device_t >();
    iodev->dev = backing_dev_t(desc);
    iodev->is_global = true;
    iodev->pri = 9;
    iodev->io_interface = this;
    iodev->device_ctx = (void*)calloc(sizeof(SpdkDriveDeviceContext), 1);

    LOGINFOMOD(iomgr, "Device {} opened successfully", devname);
    return iodev;
}

void SpdkDriveInterface::on_io_thread_start([[maybe_unused]] IOReactor* iomgr_ctx) {}
void SpdkDriveInterface::on_io_thread_stopped([[maybe_unused]] IOReactor* iomgr_ctx) {}
void SpdkDriveInterface::on_add_iodev_to_reactor(IOReactor* ctx, const io_device_ptr& iodev) {
    auto dctx = (SpdkDriveDeviceContext*)iodev->device_ctx;
    dctx->channel = spdk_bdev_get_io_channel(iodev->bdev_desc());
    if (dctx->channel == NULL) {
        folly::throwSystemError(fmt::format("Unable to get io channel for bdev={}", spdk_bdev_get_name(iodev->bdev())));
    }
}

void SpdkDriveInterface::on_remove_iodev_from_reactor(IOReactor* ctx, const io_device_ptr& iodev) {
    auto dctx = (SpdkDriveDeviceContext*)iodev->device_ctx;
    if (dctx->channel != NULL) { spdk_put_io_channel(dctx->channel); }
}

static spdk_io_channel* get_io_channel(io_device_t* iodev) {
    auto dctx = (SpdkDriveDeviceContext*)iodev->device_ctx;
    return dctx->channel;
}

static void process_completions(struct spdk_bdev_io* bdev_io, bool success, void* ctx) {
    SpdkIocb* iocb = (SpdkIocb*)ctx;
    LOGTRACEMOD(iomgr, "Received completion on bdev = {} channel = {}", (void*)iocb->iodev->bdev_desc(),
                (void*)get_io_channel(iocb->iodev));
    spdk_bdev_free_io(bdev_io);

    if (success) {
        iocb->result = 0;
        LOGTRACEMOD(iomgr, "bdev io completed successfully. io info: {}", iocb->to_string());
    } else {
        // COUNTER_INCREMENT(m_metrics, completion_errors, 1);
        iocb->result = -1;
    }

    iocb->comp_cb(*iocb->result, (uint8_t*)iocb->user_cookie);
    if (!iocb->queued) {
        // If the iocb has been queued, let the deallocation be done by the callback itself, just hand over iocb,
        // otherwise we need to deallocate here
        sisl::ObjectAllocator< SpdkIocb >::deallocate(iocb);
    }
}

static void submit_io(void* b) {
    SpdkIocb* iocb = (SpdkIocb*)b;
    int rc = 0;
    if (iocb->is_read) {
        if (iocb->user_data) {
            rc = spdk_bdev_read(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->user_data, iocb->offset,
                                iocb->size, process_completions, (void*)iocb);
        } else {
            rc = spdk_bdev_readv(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->iovs, iocb->iovcnt,
                                 iocb->offset, iocb->size, process_completions, (void*)iocb);
        }
    } else {
        if (iocb->user_data) {
            rc = spdk_bdev_write(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->user_data, iocb->offset,
                                 iocb->size, process_completions, (void*)iocb);
        } else {
            rc = spdk_bdev_writev(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->iovs, iocb->iovcnt,
                                  iocb->offset, iocb->size, process_completions, (void*)iocb);
        }
    }

    if (rc != 0) {
        if (rc == -ENOMEM) {
            LOGDEBUGMOD(iomgr, "Bdev is lacking memory to do IO right away, queueing it\n");
            iocb->copy_iovs();
            spdk_bdev_queue_io_wait(iocb->iodev->bdev(), get_io_channel(iocb->iodev), &iocb->io_wait_entry);
        }
    }
}

void SpdkDriveInterface::async_write(io_device_t* iodev, const char* data, uint32_t size, uint64_t offset,
                                     uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(iodev, false /*is_read*/, size, offset, cookie);
    iocb->user_data = (char*)data;
    iocb->io_wait_entry.cb_fn = submit_io;
    iomanager.this_reactor()->is_tight_loop_thread() ? submit_io(iocb) : do_async_in_iomgr_thread(iocb);
}

void SpdkDriveInterface::async_read(io_device_t* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                    bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(iodev, true /*is_read*/, size, offset, cookie);
    iocb->user_data = (char*)data;
    iocb->io_wait_entry.cb_fn = submit_io;
    iomanager.this_reactor()->is_tight_loop_thread() ? submit_io(iocb) : do_async_in_iomgr_thread(iocb);
}

void SpdkDriveInterface::async_writev(io_device_t* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(iodev, false /*is_read*/, size, offset, cookie);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    iocb->io_wait_entry.cb_fn = submit_io;
    iomanager.this_reactor()->is_tight_loop_thread() ? submit_io(iocb) : do_async_in_iomgr_thread(iocb);
}

void SpdkDriveInterface::async_readv(io_device_t* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(iodev, true /*is_read*/, size, offset, cookie);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    iocb->io_wait_entry.cb_fn = submit_io;
    iomanager.this_reactor()->is_tight_loop_thread() ? submit_io(iocb) : do_async_in_iomgr_thread(iocb);
}

ssize_t SpdkDriveInterface::sync_write(io_device_t* iodev, const char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.this_reactor()->is_tight_loop_thread());

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(iodev, false /*is_read*/, size, offset, nullptr);
    iocb->user_data = (char*)data;
    return do_sync_io(iocb);
}

ssize_t SpdkDriveInterface::sync_writev(io_device_t* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                        uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.this_reactor()->is_tight_loop_thread());

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(iodev, false /*is_read*/, size, offset, nullptr);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    return do_sync_io(iocb);
}

ssize_t SpdkDriveInterface::sync_read(io_device_t* iodev, char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.this_reactor()->is_tight_loop_thread());

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(iodev, true /*is_read*/, size, offset, nullptr);
    iocb->user_data = (char*)data;
    return do_sync_io(iocb);
}

ssize_t SpdkDriveInterface::sync_readv(io_device_t* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                       uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.this_reactor()->is_tight_loop_thread());

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(iodev, true /*is_read*/, size, offset, nullptr);
    iocb->iovs = (iovec*)iov;
    iocb->iovcnt = iovcnt;
    return do_sync_io(iocb);
}

ssize_t SpdkDriveInterface::do_sync_io(SpdkIocb* iocb) {
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->comp_cb = [&](int64_t res, uint8_t* cookie) {
        std::unique_lock< std::mutex > lk(m_sync_cv_mutex);
        iocb->result = res;
        m_sync_cv.notify_all();
    };

    auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
    iomanager.send_msg_to(thread_regex::any_tloop, msg);

    {
        std::unique_lock< std::mutex > lk(m_sync_cv_mutex);
        m_sync_cv.wait(lk, [&]() { return iocb->result; });
    }

    auto ret = (*iocb->result == 0) ? iocb->size : 0;
    sisl::ObjectAllocator< SpdkIocb >::deallocate(iocb);
    return ret;
}

void SpdkDriveInterface::do_async_in_iomgr_thread(SpdkIocb* iocb) {
    auto reply_thread_id = iomanager.my_io_thread_id();

    iocb->comp_cb = [this, iocb, reply_thread_id](int64_t res, uint8_t* cookie) {
        iocb->result = res;
        auto reply = iomgr_msg::create(spdk_msg_type::ASYNC_IO_DONE, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
        iomanager.send_msg(reply_thread_id, reply);
    };

    auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
    iomanager.send_msg_to(thread_regex::any_tloop, msg);
}

void SpdkDriveInterface::handle_msg(iomgr_msg* msg) {
    switch (msg->m_type) {
    case spdk_msg_type::QUEUE_IO:
        submit_io((void*)msg->data_buf().bytes);
        break;

    case spdk_msg_type::ASYNC_IO_DONE:
        auto iocb = (SpdkIocb*)msg->data_buf().bytes;
        m_comp_cb(*iocb->result, (uint8_t*)iocb->user_cookie);
        sisl::ObjectAllocator< SpdkIocb >::deallocate(iocb);
        break;
    }
}
} // namespace iomgr

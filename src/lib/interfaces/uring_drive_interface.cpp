//
// Created by Kadayam, Hari on 2021-08-16.
//
#include "uring_drive_interface.hpp"
#include "iomgr.hpp"

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif
#include <folly/Exception.h>
#include "iomgr_config.hpp"
#include <sisl/fds/obj_allocator.hpp>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

#ifdef __linux__
#include <sys/epoll.h>
#include <linux/version.h>
#endif

#include <sisl/fds/utils.hpp>
#include <sisl/logging/logging.h>

namespace iomgr {
thread_local uring_drive_channel* UringDriveInterface::t_uring_ch{nullptr};

uring_drive_channel::uring_drive_channel(UringDriveInterface* iface) {
    // TODO: For now setup as interrupt mode instead of pollmode
#if LINUX_VERSION_CODE < KERNEL_VERSION(5, 4, 0)
    RELEASE_ASSERT(0, "Not expected to run io_uring below kernel 5.4!");
#endif

    int ret = io_uring_queue_init(UringDriveInterface::per_thread_qdepth, &m_ring, 0);
    if (ret) { folly::throwSystemError(fmt::format("Unable to create uring queue created ret={}", ret)); }

    int ev_fd = eventfd(0, EFD_NONBLOCK);
    if (ev_fd == -1) { folly::throwSystemError("Unable to create eventfd to listen for uring queue events"); }

    ret = io_uring_register_eventfd(&m_ring, ev_fd);
    if (ret == -1) { folly::throwSystemError("Unable to register event fd to uring queue"); }

    // Create io device and add it local thread
    using namespace std::placeholders;
    m_ring_ev_iodev = iomanager.generic_interface()->make_io_device(
        backing_dev_t(ev_fd), EPOLLIN, 0, nullptr, true,
        std::bind(&UringDriveInterface::on_event_notification, iface, _1, _2, _3));
}

uring_drive_channel::~uring_drive_channel() {
    io_uring_queue_exit(&m_ring);
    if (m_ring_ev_iodev != nullptr) {
        iomanager.generic_interface()->remove_io_device(m_ring_ev_iodev);
        close(m_ring_ev_iodev->fd());
    }
}

struct io_uring_sqe* uring_drive_channel::get_sqe_or_enqueue(drive_iocb* iocb) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
    if (!sqe) {
        // No available slots. Before enqueing we submit ios which were added as part of batch processing.
        submit_ios();
        m_iocb_waitq.push(iocb);
        return nullptr;
    }

    return sqe;
}

void uring_drive_channel::submit_ios() {
    if (m_prepared_ios != 0) {
        io_uring_submit(&m_ring);
        m_prepared_ios = 0;
    }
}

void uring_drive_channel::submit_if_needed(drive_iocb* iocb, struct io_uring_sqe* sqe, bool part_of_batch) {
    io_uring_sqe_set_data(sqe, (void*)iocb);
    ++m_prepared_ios;
    if (!part_of_batch) { submit_ios(); }
}

static void prep_sqe_from_iocb(drive_iocb* iocb, struct io_uring_sqe* sqe) {
    switch (iocb->op_type) {
    case DriveOpType::WRITE:
        if (iocb->has_iovs()) {
            io_uring_prep_writev(sqe, iocb->iodev->fd(), iocb->get_iovs(), iocb->iovcnt, iocb->offset);
        } else {
            io_uring_prep_write(sqe, iocb->iodev->fd(), (const void*)iocb->get_data(), iocb->size, iocb->offset);
        }
        break;

    case DriveOpType::READ:
        if (iocb->has_iovs()) {
            io_uring_prep_readv(sqe, iocb->iodev->fd(), iocb->get_iovs(), iocb->iovcnt, iocb->offset);
        } else {
            io_uring_prep_read(sqe, iocb->iodev->fd(), (void*)iocb->get_data(), iocb->size, iocb->offset);
        }
        break;

    case DriveOpType::FSYNC:
        io_uring_prep_fsync(sqe, iocb->iodev->fd(), IORING_FSYNC_DATASYNC);
        break;

    default:
        break;
    }
}

void uring_drive_channel::drain_waitq() {
    while (m_iocb_waitq.size() != 0) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
        if (sqe == nullptr) { assert(0); }

        drive_iocb* iocb = pop_waitq();
        prep_sqe_from_iocb(iocb, sqe);
        submit_if_needed(iocb, sqe, false /* batch */);
    }
}

///////////////////////////// UringDriveInterface /////////////////////////////////////////
UringDriveInterface::UringDriveInterface(const io_interface_comp_cb_t& cb) : KernelDriveInterface(cb) {}

void UringDriveInterface::init_iface_thread_ctx(const io_thread_t& thr) {
    if (t_uring_ch == nullptr) { t_uring_ch = new uring_drive_channel(this); }
}

void UringDriveInterface::clear_iface_thread_ctx(const io_thread_t& thr) {
    if (t_uring_ch != nullptr) {
        delete t_uring_ch;
        t_uring_ch = nullptr;
    }
}

io_device_ptr UringDriveInterface::open_dev(const std::string& devname, drive_type dev_type, int oflags) {
    LOGMSG_ASSERT(((dev_type == drive_type::block_nvme) || (dev_type == drive_type::block_hdd) ||
                   (dev_type == drive_type::file_on_hdd) || (dev_type == drive_type::file_on_nvme)),
                  "Unexpected dev type to open {}", dev_type);
    init_write_zero_buf(devname, dev_type);

    auto fd = open(devname.c_str(), oflags, 0640);
    if (fd == -1) {
        folly::throwSystemError(fmt::format("Unable to open the device={} dev_type={}, errno={} strerror={}", devname,
                                            dev_type, errno, strerror(errno)));
        return nullptr;
    }

    auto iodev = alloc_io_device(backing_dev_t(fd), 9 /* pri */, thread_regex::all_io);
    iodev->devname = devname;
    iodev->creator = iomanager.am_i_io_reactor() ? iomanager.iothread_self() : nullptr;
    iodev->dtype = dev_type;

    // We don't need to add the device to each thread, because each AioInterface thread context add an
    // event fd and read/write use this device fd to control with iocb.
    LOGINFOMOD(iomgr, "Device={} of type={} opened with flags={} successfully, fd={}", devname, dev_type, oflags, fd);
    return iodev;
}

void UringDriveInterface::close_dev(const io_device_ptr& iodev) {
    // TODO: This is where we would wait for any outstanding io's to complete

    IOInterface::close_dev(iodev);

    // AIO base devices are not added to any poll list, so it can be closed as is.
    close(iodev->fd());
    iodev->clear();
}

void UringDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(5, 6, 0)
    std::array< iovec, 1 > iov;
    iov[0].iov_base = (void*)data;
    iov[0].iov_len = size;

    async_writev(iodev, iov.data(), 1, size, offset, cookie, part_of_batch);
#else
    RELEASE_ASSERT(0, "async_write not expected to arrive here.");
    auto iocb = sisl::ObjectAllocator< drive_iocb >::make_object(iodev, DriveOpType::WRITE, size, offset, cookie);
    iocb->set_data((char*)data);
    auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
    if (sqe == nullptr) { return; }

    io_uring_prep_write(sqe, iodev->fd(), (const void*)iocb->get_data(), iocb->size, offset);
    t_uring_ch->submit_if_needed(iocb, sqe, part_of_batch);
#endif
}

void UringDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                       uint8_t* cookie, bool part_of_batch) {
    auto iocb = sisl::ObjectAllocator< drive_iocb >::make_object(iodev, DriveOpType::WRITE, size, offset, cookie);
    iocb->set_iovs(iov, iovcnt);

    auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
    if (sqe == nullptr) { return; }

    io_uring_prep_writev(sqe, iodev->fd(), iocb->get_iovs(), iocb->iovcnt, offset);
    t_uring_ch->submit_if_needed(iocb, sqe, part_of_batch);
}

void UringDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(5, 6, 0)
    std::array< iovec, 1 > iov;
    iov[0].iov_base = data;
    iov[0].iov_len = size;

    async_readv(iodev, iov.data(), 1, size, offset, cookie, part_of_batch);
#else
    RELEASE_ASSERT(0, "async_read not expected to arrive here.");
    auto iocb = sisl::ObjectAllocator< drive_iocb >::make_object(iodev, DriveOpType::READ, size, offset, cookie);
    iocb->set_data(data);

    auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
    if (sqe == nullptr) { return; }

    io_uring_prep_read(sqe, iodev->fd(), (void*)iocb->get_data(), iocb->size, offset);
    t_uring_ch->submit_if_needed(iocb, sqe, part_of_batch);
#endif
}

void UringDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
    auto iocb = sisl::ObjectAllocator< drive_iocb >::make_object(iodev, DriveOpType::READ, size, offset, cookie);
    iocb->set_iovs(iov, iovcnt);

    auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
    if (sqe == nullptr) { return; }

    io_uring_prep_readv(sqe, iodev->fd(), iocb->get_iovs(), iocb->iovcnt, offset);
    t_uring_ch->submit_if_needed(iocb, sqe, part_of_batch);
}

void UringDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                                      bool part_of_batch) {
    RELEASE_ASSERT(0, "async_unmap is not supported for uring yet");
}

void UringDriveInterface::fsync(IODevice* iodev, uint8_t* cookie) {
    auto iocb = sisl::ObjectAllocator< drive_iocb >::make_object(iodev, DriveOpType::FSYNC, 0, 0, cookie);
    auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
    if (sqe == nullptr) { return; }

    io_uring_prep_fsync(sqe, iodev->fd(), IORING_FSYNC_DATASYNC);
    t_uring_ch->submit_if_needed(iocb, sqe, false /* batching */);
}

void UringDriveInterface::submit_batch() { t_uring_ch->submit_ios(); }

void UringDriveInterface::on_event_notification(IODevice* iodev, [[maybe_unused]] void* cookie,
                                                [[maybe_unused]] int event) {
    uint64_t temp = 0;
    [[maybe_unused]] auto rsize = read(iodev->fd(), &temp, sizeof(uint64_t));
    LOGTRACEMOD(iomgr, "Received completion on ev_fd = {}", iodev->fd());

    do {
        struct io_uring_cqe* cqe;
        int ret = io_uring_peek_cqe(&t_uring_ch->m_ring, &cqe);
        if (ret < 0) {
            if (ret != -EAGAIN) {
                folly::throwSystemError(fmt::format("io_uring_wait_cqe throw error={}", strerror(errno)));
            } else {
                LOGTRACEMOD(iomgr, "Received EAGAIN on uring peek cqe");
            }
            break;
        }
        if (cqe == nullptr) { break; }

        auto iocb = (drive_iocb*)io_uring_cqe_get_data(cqe);
        iocb->result = cqe->res;
        io_uring_cqe_seen(&t_uring_ch->m_ring, cqe);

        // Don't access cqe beyond this point.
        if (iocb->result >= 0) {
            LOGTRACEMOD(iomgr, "Received completion event, iocb={} Result={}", (void*)iocb, iocb->result);
            complete_io(iocb);
        } else {
            LOGERRORMOD(iomgr, "Error in completion of io, iocb={}, result={}", (void*)iocb, iocb->result);
            if (iocb->resubmit_cnt++ > IM_DYNAMIC_CONFIG(max_resubmit_cnt)) {
                complete_io(iocb);
            } else {
                // Retry IO by pushing it to waitq which will get scheduled later.
                t_uring_ch->m_iocb_waitq.push(iocb);
            }
        }
        t_uring_ch->drain_waitq();
    } while (true);
}

void UringDriveInterface::complete_io(drive_iocb* iocb) {
    if (m_comp_cb) {
        auto res = (iocb->result > 0) ? 0 : iocb->result;
        m_comp_cb(res, (uint8_t*)iocb->user_cookie);
    }
    sisl::ObjectAllocator< drive_iocb >::deallocate(iocb);
}
} // namespace iomgr

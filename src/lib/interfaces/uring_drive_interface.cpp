//
// Created by Kadayam, Hari on 2021-08-16.
//
#include "uring_drive_interface.hpp"
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif
#include <folly/Exception.h>
#include "iomgr_config.hpp"
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

uring_drive_channel::uring_drive_channel() {
    // TODO: For now setup as interrupt mode instead of pollmode
    int ret = io_uring_queue_init(UringDriveInterface::max_entries, &m_ring, 0);
    if (ret) { folly::throwSystemError(fmt::format("Unable to create uring queue created ret={}", ret)); }

    int ev_fd = eventfd(0, EFD_NONBLOCK);
    if (ev_fd == -1) { folly::throwSystemError("Unable to create eventfd to listen for uring queue events"); }

    ret = io_uring_register_eventfd(&m_ring, ev_fd);
    if (ret == -1) { folly::throwSystemError("Unable to register event fd to uring queue"); }

    // Create io device and add it local thread
    m_ring_ev_iodev = iomanager.generic_interface()->make_io_device(
        backing_dev_t(ev_fd), EPOLLIN, 0, nullptr, true, bind_this(UringDriveInterface::on_event_notification, 3));
}

uring_drive_channel::~uring_drive_channel() {
    io_uring_queue_exit(&m_ring);
    if (m_ring_ev_iodev != nullptr) {
        iomanager.generic_interface()->remove_io_device(m_ring_ev_iodev);
        close(m_ring_ev_iodev->fd());
    }
}

UringDriveInterface::UringDriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb{cb} {}

void UringDriveInterface::init_iface_thread_ctx(const io_thread_t& thr) {
    if (t_uring_ch == nullptr) { t_uring_ch = new uring_drive_channel(); }
}

void UringDriveInterface::clear_iface_thread_ctx(const io_thread_t& thr) {
    if (t_uring_ch != nullptr) {
        delete t_uring_ch;
        t_uring_ch = nullptr;
    }
}

io_device_ptr UringDriveInterface::open_dev(const std::string& devname, iomgr_drive_type dev_type, int oflags) {
    if (dev_type == iomgr_drive_type::unknown) { dev_type = get_drive_type(devname); }

    LOGMSG_ASSERT(((dev_type == iomgr_drive_type::block) || (dev_type == iomgr_drive_type::file)),
                  "Unexpected dev type to open {}", dev_type);

#ifdef __linux__
    if ((dev_type == iomgr_drive_type::block) && IM_DYNAMIC_CONFIG(uring.zeros_by_ioctl)) {
        static std::once_flag flag1;
        std::call_once(flag1, [this, &devname]() { m_max_write_zeros = get_max_write_zeros(devname); });
    }
#endif

    if (m_max_write_zeros == 0) {
        static std::once_flag flag2;
        std::call_once(flag2, [this, &devname, &dev_type]() {
            m_zero_buf = sisl::AlignedAllocator::allocator().aligned_alloc(get_attributes(devname, dev_type).align_size,
                                                                           max_buf_size, sisl::buftag::common);
            std::memset(m_zero_buf, 0, max_buf_size);
        });
    }

    auto fd = open(devname.c_str(), oflags, 0640);
    if (fd == -1) {
        folly::throwSystemError(fmt::format("Unable to open the device={} dev_type={}, errno={} strerror={}", devname,
                                            dev_type, errno, strerror(errno)));
        return nullptr;
    }

    auto iodev = alloc_io_device(backing_dev_t(fd), 9 /* pri */, thread_regex::all_io);
    iodev->devname = devname;
    iodev->creator = iomanager.am_i_io_reactor() ? iomanager.iothread_self() : nullptr;
    iodev->drive_type = dev_type;

    // We don't need to add the device to each thread, because each AioInterface thread context add an
    // event fd and read/write use this device fd to control with iocb.
    LOGINFO("Device={} of type={} opened with flags={} successfully, fd={}", devname, dev_type, oflags, fd);
    return iodev;
}

void UringDriveInterface::close_dev(const io_device_ptr& iodev) {
    // AIO base devices are not added to any poll list, so it can be closed as is.
    close(iodev->fd());
    iodev->clear();
}

void UringDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&t_uring_ch->m_ring);
    if (!sqe) {
        folly::throwSystemError("Unable to get submission queue entry for uring");
        m_comp_cb(errno, cookie)
    }

    io_uring_prep_write(sqe, iodev->fd(), (const void*)data, size, offset);
    t_uring_ch->submit_ios_if_needed(sqe, part_of_batch);
}

void UringDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                       uint8_t* cookie, bool part_of_batch) {

    struct io_uring_sqe* sqe = io_uring_get_sqe(&t_uring_ch->m_ring);
    if (!sqe) {
        folly::throwSystemError("Unable to get submission queue entry for uring");
        m_comp_cb(errno, cookie)
    }

    io_uring_prep_writev(sqe, iodev->fd(), iov, iovcnt, offset);
    t_uring_ch->submit_ios_if_needed(sqe, part_of_batch);
}

void UringDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&t_uring_ch->m_ring);
    if (!sqe) {
        folly::throwSystemError("Unable to get submission queue entry for uring");
        m_comp_cb(errno, cookie)
    }

    io_uring_prep_read(sqe, iodev->fd(), (void*)data, size, offset);
    t_uring_ch->submit_ios_if_needed(sqe, part_of_batch);
}

void UringDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&t_uring_ch->m_ring);
    if (!sqe) {
        folly::throwSystemError("Unable to get submission queue entry for uring");
        m_comp_cb(errno, cookie)
    }

    io_uring_prep_readv(sqe, iodev->fd(), iov, iovcnt, offset);
    t_uring_ch->submit_ios_if_needed(sqe, part_of_batch);
}

//
// Created by Kadayam, Hari on 02/04/18.
//
#include <folly/Exception.h>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>
#include <sys/uio.h>
#include <fstream>
#include <sys/epoll.h>
#include <fmt/format.h>
#include <filesystem>
#include "iomgr_config.hpp"

#ifdef __linux__
#include <linux/fs.h>
#include <sys/ioctl.h>
#endif

#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/aio_drive_interface.hpp"

namespace iomgr {
#ifdef __APPLE__

ssize_t preadv(int fd, const iovec* iov, int iovcnt, off_t offset) {
    lseek(fd, offset, SEEK_SET);
    return ::readv(fd, iov, iovcnt);
}

ssize_t pwritev(int fd, const iovec* iov, int iovcnt, off_t offset) {
    lseek(fd, offset, SEEK_SET);
    return ::writev(fd, iov, iovcnt);
}

#endif
using namespace std;

thread_local aio_thread_context* AioDriveInterface::_aio_ctx;

sisl::aligned_unique_ptr< uint8_t > AioDriveInterface::zero_buf;

AioDriveInterface::AioDriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb){};

io_device_ptr AioDriveInterface::open_dev(const std::string& devname, iomgr_drive_type dev_type, int oflags) {
#ifndef NDEBUG
    if (dev_type == iomgr_drive_type::unknown) { dev_type = get_drive_type(devname); }
    LOGMSG_ASSERT(((dev_type == iomgr_drive_type::block) || (dev_type == iomgr_drive_type::file)),
                  "Unexpected dev type to open {}", dev_type);
#endif

    /* it doesn't need to keep track of any fds */
    auto fd = open(devname.c_str(), oflags, 0640);
    if (fd == -1) {
        folly::throwSystemError(fmt::format("Unable to open the device={} dev_type={}, errno={} strerror={}", devname,
                                            dev_type, errno, strerror(errno)));
        return nullptr;
    }

    auto iodev = std::make_shared< IODevice >();
    iodev->dev = backing_dev_t(fd);
    iodev->thread_scope = thread_regex::all_io;
    iodev->pri = 9;
    iodev->io_interface = this;
    iodev->devname = devname;
    iodev->creator = iomanager.am_i_io_reactor() ? iomanager.iothread_self() : nullptr;

    LOGINFO("Device={} of type={} opened with flags={} successfully, fd={}", devname, dev_type, oflags, fd);
    return iodev;
}

void AioDriveInterface::close_dev(const io_device_ptr& iodev) {
    // AIO base devices are not added to any poll list, so it can be closed as is.
    close(iodev->fd());
    iodev->clear();
}

void AioDriveInterface::init_iface_thread_ctx([[maybe_unused]] const io_thread_t& thr) {
    _aio_ctx = new aio_thread_context();
    _aio_ctx->ev_fd = eventfd(0, EFD_NONBLOCK);
    _aio_ctx->ev_io_dev =
        iomanager.generic_interface()->make_io_device(backing_dev_t(_aio_ctx->ev_fd), EPOLLIN, 0, nullptr, true,
                                                      bind_this(AioDriveInterface::process_completions, 3));

    int err = io_setup(MAX_OUTSTANDING_IO, &_aio_ctx->ioctx);
    if (err) {
        LOGCRITICAL("io_setup failed with ret status {} errno {}", err, errno);
        folly::throwSystemError(fmt::format("io_setup failed with ret status {} errno {}", err, errno));
    }

    _aio_ctx->iocb_info_prealloc(MAX_OUTSTANDING_IO);
}

void AioDriveInterface::clear_iface_thread_ctx([[maybe_unused]] const io_thread_t& thr) {
    iomanager.generic_interface()->remove_io_device(_aio_ctx->ev_io_dev);
    close(_aio_ctx->ev_fd);
    delete _aio_ctx;
}

void AioDriveInterface::process_completions(IODevice* iodev, void* cookie, int event) {
    assert(iodev->fd() == _aio_ctx->ev_fd);

    (void)cookie;
    (void)event;

    /* TODO need to handle the error events */
    uint64_t temp = 0;

    [[maybe_unused]] auto rsize = read(_aio_ctx->ev_fd, &temp, sizeof(uint64_t));
    int nevents = io_getevents(_aio_ctx->ioctx, 0, MAX_COMPLETIONS, _aio_ctx->events, NULL);

    LOGTRACEMOD(iomgr, "Received completion on fd = {} ev_fd = {} nevents = {}", iodev->fd(), _aio_ctx->ev_fd, nevents);
    if (nevents == 0) {
        COUNTER_INCREMENT(m_metrics, spurious_events, 1);
    } else if (nevents < 0) {
        /* TODO: Handle error by reporting failures */
        LOGERROR("process_completions nevents is less then zero {}", nevents);
        COUNTER_INCREMENT(m_metrics, completion_errors, 1);
    } else {
        COUNTER_INCREMENT(m_metrics, total_io_callbacks, 1);
    }

    for (int i = 0; i < nevents; i++) {
        auto& e = _aio_ctx->events[i];

        auto ret = static_cast< int64_t >(e.res);
        auto iocb = (struct iocb*)e.obj;
        auto info = (iocb_info_t*)iocb;

        LOGTRACEMOD(iomgr, "Event[{}]: Result {} res2={}", i, e.res, e.res2);
        if (ret < 0) {
            COUNTER_INCREMENT(m_metrics, completion_errors, 1);
            LOGDFATAL("Error in completion of aio, result: {} info: {}", ret, info->to_string());
        } else if ((e.res != info->size) || e.res2) {
            COUNTER_INCREMENT(m_metrics, completion_errors, 1);
            LOGERROR("io is not completed properly. size read/written {} info {} error {}", e.res, info->to_string(),
                     e.res2);
            if (e.res2 == 0) { e.res2 = EIO; }
        }

        auto user_cookie = (uint8_t*)iocb->data;
        _aio_ctx->dec_submitted_aio();
        _aio_ctx->free_iocb(iocb);
        retry_io();
        if (m_comp_cb) m_comp_cb(e.res2, user_cookie);
    }

    // Call any batch completion hints
    if (nevents && m_io_end_of_batch_cb) {
        LOGTRACEMOD(iomgr, "Making end of batch cb list callback with nevents={}", nevents);
        m_io_end_of_batch_cb(nevents);
    }
}

void AioDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                    bool part_of_batch) {
    if (!_aio_ctx->can_submit_aio()) {
        auto iocb = _aio_ctx->prep_iocb(false, iodev->fd(), false, data, size, offset, cookie);
        push_retry_list(iocb);
        return;
    }

    if (part_of_batch && _aio_ctx->can_be_batched(0)) {
        _aio_ctx->prep_iocb(true /* batch_io */, iodev->fd(), false /* is_read */, data, size, offset, cookie);
    } else {
        auto iocb = _aio_ctx->prep_iocb(false, iodev->fd(), false, data, size, offset, cookie);
        COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        _aio_ctx->inc_submitted_aio(ret);
        if (ret != 1) {
            handle_io_failure(iocb);
            return;
        }
    }
}

void AioDriveInterface::write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) {
    if (zero_buf == nullptr) {
        zero_buf = sisl::aligned_unique_ptr< uint8_t >::make_sized(get_attributes().align_size, max_buf_size);
        bzero(zero_buf.get(), max_buf_size);
    }

    if (size > max_zero_write_size) {
        LOGINFO("size {} exceed max write size {}", size, max_zero_write_size);
        size = max_zero_write_size; // written size is returned in completion callback
    }

    auto iocb = _aio_ctx->prep_iocb_write_zero(iodev->fd(), size, offset, cookie, zero_buf.get());
    if (!_aio_ctx->can_submit_aio()) {
        push_retry_list(iocb);
        return;
    }

    COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
    auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
    _aio_ctx->inc_submitted_aio(ret);
    if (ret != 1) {
        handle_io_failure(iocb);
        return;
    }
}

void AioDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                   bool part_of_batch) {

    if (!_aio_ctx->can_submit_aio()) {
        auto iocb = _aio_ctx->prep_iocb(false, iodev->fd(), true, data, size, offset, cookie);
        push_retry_list(iocb);
        return;
    }
    if (part_of_batch && _aio_ctx->can_be_batched(0)) {
        _aio_ctx->prep_iocb(true /* batch_io */, iodev->fd(), true /* is_read */, data, size, offset, cookie);
    } else {
        auto iocb = _aio_ctx->prep_iocb(false, iodev->fd(), true, data, size, offset, cookie);
        COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        _aio_ctx->inc_submitted_aio(ret);
        if (ret != 1) {
            handle_io_failure(iocb);
            return;
        }
    }
}

void AioDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie, bool part_of_batch) {

    if (!_aio_ctx->can_submit_aio()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_write_iocb_empty_flip")
#endif
    ) {
        auto iocb = _aio_ctx->prep_iocb_v(false, iodev->fd(), false, iov, iovcnt, size, offset, cookie);
        push_retry_list(iocb);
        return;
    }
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_write_error_flip")) {
        m_comp_cb(homestore::homestore_error::write_failed, cookie);
        return;
    }
#endif
    if (part_of_batch && _aio_ctx->can_be_batched(iovcnt)) {
        _aio_ctx->prep_iocb_v(true /* batch_io */, iodev->fd(), false /* is_read */, iov, iovcnt, size, offset, cookie);
    } else {
        auto iocb = _aio_ctx->prep_iocb_v(false, iodev->fd(), false, iov, iovcnt, size, offset, cookie);
        COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        _aio_ctx->inc_submitted_aio(ret);
        if (ret != 1) {
            handle_io_failure(iocb);
            return;
        }
    }
}

void AioDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                    uint8_t* cookie, bool part_of_batch) {

    if (!_aio_ctx->can_submit_aio()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_read_iocb_empty_flip")
#endif
    ) {
        auto iocb = _aio_ctx->prep_iocb_v(false, iodev->fd(), true, iov, iovcnt, size, offset, cookie);
        push_retry_list(iocb);
        return;
    }
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_read_error_flip", iovcnt, size)) {
        m_comp_cb(homestore::homestore_error::read_failed, cookie);
        return;
    }
#endif

    if (part_of_batch && _aio_ctx->can_be_batched(iovcnt)) {
        _aio_ctx->prep_iocb_v(true /* batch_io */, iodev->fd(), true /* is_read */, iov, iovcnt, size, offset, cookie);
    } else {
        auto iocb = _aio_ctx->prep_iocb_v(false, iodev->fd(), true, iov, iovcnt, size, offset, cookie);
        COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        _aio_ctx->inc_submitted_aio(ret);
        if (ret != 1) {
            handle_io_failure(iocb);
            return;
        }
    }
}

void AioDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                                    bool part_of_batch) {}

void AioDriveInterface::submit_batch() {
    auto ibatch = _aio_ctx->move_cur_batch();
    LOGTRACEMOD(iomgr, "submit pending batch n_iocbs={}", ibatch.n_iocbs);
    if (ibatch.n_iocbs == 0) { return; } // No batch to submit

    COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
    auto n_issued = io_submit(_aio_ctx->ioctx, ibatch.n_iocbs, ibatch.get_iocb_list());
    if (n_issued < 0) { n_issued = 0; }
    _aio_ctx->inc_submitted_aio(n_issued);

    // For those which we are not able to issue, convert that to sync io
    auto n_iocbs = ibatch.n_iocbs;
    for (auto i = n_issued; i < n_iocbs; ++i) {
        auto info = ibatch.iocb_info[i];
        auto iocb = (struct iocb*)info;
        handle_io_failure(iocb);
    }
}

void AioDriveInterface::retry_io() {
    struct iocb* iocb = nullptr;
    while ((_aio_ctx->can_submit_aio()) && (iocb = _aio_ctx->pop_retry_list()) != nullptr) {
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        _aio_ctx->inc_submitted_aio(ret);
        if (ret != 1 && handle_io_failure(iocb)) { break; }
    }
}

void AioDriveInterface::push_retry_list(struct iocb* iocb) {
    auto info = (iocb_info_t*)iocb;
    COUNTER_INCREMENT(m_metrics, retry_io_eagain_error, 1);
    LOGINFO("adding io into retry list: {}", info->to_string());
    _aio_ctx->push_retry_list(iocb);
    if (!_aio_ctx->timer_set) {
        _aio_ctx->timer_set = true;
        iomanager.schedule_thread_timer(IM_DYNAMIC_CONFIG(aio->retry_timeout), false, nullptr, [this](void* cookie) {
            _aio_ctx->timer_set = false;
            retry_io();
        });
    }
}

bool AioDriveInterface::handle_io_failure(struct iocb* iocb) {
    auto info = (iocb_info_t*)iocb;
    bool ret = true;

    if (errno == EAGAIN) {
        COUNTER_INCREMENT(m_metrics, retry_io_eagain_error, 1);
        LOGINFO("adding io into retry list: {}", info->to_string());
        push_retry_list(iocb);
    } else {
        LOGERROR("io submit fail: io info: {}, errno: {}", info->to_string(), errno);
        COUNTER_INCREMENT_IF_ELSE(m_metrics, info->is_read, read_io_submission_errors, write_io_submission_errors, 1);
        ret = false;
        _aio_ctx->free_iocb(iocb);
        if (m_comp_cb) m_comp_cb(errno, (uint8_t*)iocb->data);
    }
    return ret;
}

ssize_t AioDriveInterface::sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) {
    return _sync_write(iodev->fd(), data, size, offset);
}

ssize_t AioDriveInterface::_sync_write(int fd, const char* data, uint32_t size, uint64_t offset) {
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_write_error_flip", size)) { folly::throwSystemError("flip error"); }
#endif

    ssize_t written_size = pwrite(fd, data, (ssize_t)size, (off_t)offset);
    if (written_size != size) {
        folly::throwSystemError(fmt::format("Error during write offset={} write_size={} written_size={} errno={} fd={}",
                                            offset, size, written_size, errno, fd));
    }

    return written_size;
}

ssize_t AioDriveInterface::sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    return _sync_writev(iodev->fd(), iov, iovcnt, size, offset);
}

ssize_t AioDriveInterface::_sync_writev(int fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_write_error_flip", iovcnt, size)) { folly::throwSystemError("flip error"); }
#endif
    ssize_t written_size = pwritev(fd, iov, iovcnt, offset);
    if (written_size != size) {
        folly::throwSystemError(
            fmt::format("Error during writev offset={} write_size={} written_size={} iovcnt={} errno={} fd={}", offset,
                        size, written_size, iovcnt, errno, fd));
    }

    return written_size;
}

ssize_t AioDriveInterface::sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) {
    return _sync_read(iodev->fd(), data, size, offset);
}

ssize_t AioDriveInterface::_sync_read(int fd, char* data, uint32_t size, uint64_t offset) {
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_read_error_flip", size)) { folly::throwSystemError("flip error"); }
#endif
    ssize_t read_size = pread(fd, data, (ssize_t)size, (off_t)offset);
    if (read_size != size) {
        folly::throwSystemError(fmt::format("Error during read offset={} to_read_size={} read_size={} errno={} fd={}",
                                            offset, size, read_size, errno, fd));
    }

    return read_size;
}

ssize_t AioDriveInterface::sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    return _sync_readv(iodev->fd(), iov, iovcnt, size, offset);
}

ssize_t AioDriveInterface::_sync_readv(int fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_read_error_flip", iovcnt, size)) { folly::throwSystemError("flip error"); }
#endif
    ssize_t read_size = preadv(fd, iov, iovcnt, (off_t)offset);
    if (read_size != size) {
        folly::throwSystemError(
            fmt::format("Error during readv offset={} to_read_size={} read_size={} iovcnt={} errno={} fd={}", offset,
                        size, read_size, iovcnt, errno, fd));
    }

    return read_size;
}

size_t AioDriveInterface::get_size(IODevice* iodev) {
    if (std::filesystem::is_regular_file(std::filesystem::status(iodev->devname))) {
        struct stat buf;
        if (fstat(iodev->fd(), &buf) >= 0) { return buf.st_size; }
    } else {
        assert(std::filesystem::is_block_file(std::filesystem::status(iodev->devname)));
        size_t devsize;
        if (ioctl(iodev->fd(), BLKGETSIZE64, &devsize) >= 0) { return devsize; }
    }

    folly::throwSystemError(fmt::format("device stat failed for dev {} errno = {}", iodev->fd(), errno));
    return 0;
}

drive_attributes AioDriveInterface::get_attributes([[maybe_unused]] const io_device_ptr& dev) const {
    // TODO: Get this information from SSD using /sys commands
    drive_attributes attr;
    attr.phys_page_size = 4096;
    attr.align_size = 512;
#ifndef NDEBUG
    attr.atomic_phys_page_size = 512;
#else
    attr.atomic_phys_page_size = 4096;
#endif
    return attr;
}

drive_attributes AioDriveInterface::get_attributes(const std::string& devname, const iomgr_drive_type drive_type) {
    return get_attributes(nullptr);
}

} // namespace iomgr

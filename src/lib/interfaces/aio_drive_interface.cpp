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

AioDriveInterface::AioDriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb){};

int AioDriveInterface::open_dev(std::string devname, int oflags) {
    /* it doesn't need to keep track of any fds */
    auto fd = open(devname.c_str(), oflags);
    if (fd == -1) {
        std::cerr << "Unable to open the device " << devname << " errorno = " << errno << " error = " << strerror(errno)
                  << "\n";
    }
    LOGINFO("Device {} opened with flags={} successfully, fd={}", devname, oflags, fd);
    return fd;
}

void AioDriveInterface::on_io_thread_start(ioMgrThreadContext* iomgr_ctx) {
    (void)iomgr_ctx;
    _aio_ctx = new aio_thread_context();
    _aio_ctx->ev_fd = eventfd(0, EFD_NONBLOCK);
    _aio_ctx->ev_fd_info =
        iomanager.add_per_thread_fd(this, _aio_ctx->ev_fd,
                                    std::bind(&AioDriveInterface::process_completions, this, std::placeholders::_1,
                                              std::placeholders::_2, std::placeholders::_3),
                                    EPOLLIN, 0, NULL);
    int err = io_setup(MAX_OUTSTANDING_IO, &_aio_ctx->ioctx);
    if (err) {
        LOGCRITICAL("io_setup failed with ret status {} errno {}", err, errno);
        std::stringstream ss;
        ss << "io_setup failed with ret status " << err << " errno = " << errno;
        folly::throwSystemError(ss.str());
    }

    for (int i = 0; i < MAX_OUTSTANDING_IO; i++) {
        struct iocb_info* info = (struct iocb_info*)malloc(sizeof(struct iocb_info));
        _aio_ctx->iocb_list.push(info);
    }
}

void AioDriveInterface::add_fd(int fd, int priority) {
    iomanager.add_fd(this, fd,
                     std::bind(&AioDriveInterface::process_completions, this, std::placeholders::_1,
                               std::placeholders::_2, std::placeholders::_3),
                     EPOLLIN, priority, nullptr);
}

void AioDriveInterface::on_io_thread_stopped(ioMgrThreadContext* iomgr_ctx) {
    iomanager.remove_fd(this, _aio_ctx->ev_fd_info, iomgr_ctx);
    delete _aio_ctx;
}

void AioDriveInterface::process_completions(int fd, void* cookie, int event) {
    assert(fd == _aio_ctx->ev_fd);

    (void)cookie;
    (void)event;

    LOGTRACEMOD(iomgr, "Received completion on fd = {} ev_fd = {}", fd, _aio_ctx->ev_fd);
    /* TODO need to handle the error events */
    uint64_t temp = 0;

    [[maybe_unused]] auto rsize = read(_aio_ctx->ev_fd, &temp, sizeof(uint64_t));
    int nevents = io_getevents(_aio_ctx->ioctx, 0, MAX_COMPLETIONS, _aio_ctx->events, NULL);
    if (nevents == 0) { COUNTER_INCREMENT(m_metrics, spurious_events, 1); }
    if (nevents < 0) {
        /* TODO: Handle error by reporting failures */
        LOGERROR("process_completions nevents is less then zero {}", nevents);
        COUNTER_INCREMENT(m_metrics, completion_errors, 1);
    }

    for (int i = 0; i < nevents; i++) {
        auto& e = _aio_ctx->events[i];

        auto ret = static_cast< int64_t >(e.res);
        struct iocb_info* info = static_cast< iocb_info* >(e.obj);
        _aio_ctx->iocb_list.push(info);

        if (ret < 0) {
            COUNTER_INCREMENT(m_metrics, completion_errors, 1);
            LOGDFATAL("Error in completion of aio, result: {} info: {}", ret, info->to_string());
        } else if ((e.res != info->size) || e.res2) {
            COUNTER_INCREMENT(m_metrics, completion_errors, 1);
            LOGERROR("io is not completed properly. size read/written {} info {} error {}", e.res, info->to_string(),
                     e.res2);
            if (e.res2 == 0) { e.res2 = EIO; }
        }

        if (m_comp_cb) m_comp_cb(e.res2, (uint8_t*)e.data);
    }

    // Call any batch completions hints
    if (nevents) {
        for (auto& cb : m_io_batch_sentinel_cb_list) {
            cb(nevents);
        }
    }
}

void AioDriveInterface::async_write(int data_fd, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie) {
    if (!_aio_ctx || _aio_ctx->iocb_list.empty()) {
        COUNTER_INCREMENT(m_metrics, force_sync_io_empty_iocb, 1);
        LOGWARN("Not enough available iocbs to schedule an async write: size {}, offset {}, doing sync write instead",
                size, offset);
        sync_write(data_fd, data, size, offset);
        if (m_comp_cb) m_comp_cb(0, cookie);
        return;
    }

    struct iocb_info* info = _aio_ctx->iocb_list.top();
    struct iocb* iocb = static_cast< struct iocb* >(info);
    _aio_ctx->iocb_list.pop();

    io_prep_pwrite(iocb, data_fd, (void*)data, size, offset);
    io_set_eventfd(iocb, _aio_ctx->ev_fd);
    iocb->data = cookie;
    info->is_read = false;
    info->size = size;
    info->offset = offset;
    info->fd = data_fd;

    LOGTRACE("Writing data info: {}", info->to_string());
    auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
    if (ret != 1) {
        if (errno == EAGAIN) {
            COUNTER_INCREMENT(m_metrics, force_sync_io_eagain_error, 1);
            sync_write(data_fd, data, size, offset);
        } else {
            COUNTER_INCREMENT(m_metrics, write_io_submission_errors, 1);
            LOGERROR("io submit fail: io info: {}, errno: {}", info->to_string(), errno);
        }
        if (m_comp_cb) m_comp_cb(errno, cookie);
        return;
    }
    COUNTER_INCREMENT(m_metrics, async_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));
}

void AioDriveInterface::async_read(int data_fd, char* data, uint32_t size, uint64_t offset, uint8_t* cookie) {
    if (!_aio_ctx || _aio_ctx->iocb_list.empty()) {
        COUNTER_INCREMENT(m_metrics, force_sync_io_empty_iocb, 1);
        LOGWARN("Not enough available iocbs to schedule an async read: size {}, offset {}, doing sync read instead",
                size, offset);
        sync_read(data_fd, data, size, offset);
        if (m_comp_cb) m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = _aio_ctx->iocb_list.top();
    struct iocb* iocb = static_cast< struct iocb* >(info);
    _aio_ctx->iocb_list.pop();

    io_prep_pread(iocb, data_fd, data, size, offset);
    io_set_eventfd(iocb, _aio_ctx->ev_fd);
    iocb->data = cookie;
    info->is_read = true;
    info->size = size;
    info->offset = offset;
    info->fd = data_fd;

    LOGTRACE("Reading data info: {}", info->to_string());
    auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
    if (ret != 1) {
        if (errno == EAGAIN) {
            COUNTER_INCREMENT(m_metrics, force_sync_io_eagain_error, 1);
            sync_read(data_fd, data, size, offset);
        } else {
            COUNTER_INCREMENT(m_metrics, read_io_submission_errors, 1);
            LOGERROR("io submit fail: io info: {}, errno: {}", info->to_string(), errno);
        }
        if (m_comp_cb) m_comp_cb(errno, cookie);
        return;
    }
    COUNTER_INCREMENT(m_metrics, async_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));
}

void AioDriveInterface::async_writev(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie) {
    if (!_aio_ctx || _aio_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_write_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, force_sync_io_empty_iocb, 1);
        LOGWARN("Not enough available iocbs to schedule an async writev: size {}, offset {}, doing sync read instead",
                size, offset);
        sync_writev(data_fd, iov, iovcnt, size, offset);
        if (m_comp_cb) m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = _aio_ctx->iocb_list.top();
    struct iocb* iocb = static_cast< struct iocb* >(info);
    _aio_ctx->iocb_list.pop();
    io_prep_pwritev(iocb, data_fd, iov, iovcnt, offset);
    io_set_eventfd(iocb, _aio_ctx->ev_fd);
    iocb->data = cookie;
    info->is_read = false;
    info->size = size;
    info->offset = offset;
    info->fd = data_fd;

#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_write_error_flip")) {
        m_comp_cb(homestore::homestore_error::write_failed, cookie);
        return;
    }
#endif

    LOGTRACE("Writing data: info: {} iovcnt: {}", info->to_string(), iovcnt);
    auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
    if (ret != 1) {
        if (errno == EAGAIN) {
            COUNTER_INCREMENT(m_metrics, force_sync_io_eagain_error, 1);
            sync_writev(data_fd, iov, iovcnt, size, offset);
        } else {
            COUNTER_INCREMENT(m_metrics, write_io_submission_errors, 1);
            LOGERROR("io submit fail: io info: {}, iovcnt {} errno: {}", info->to_string(), iovcnt, errno);
        }
        if (m_comp_cb) m_comp_cb(errno, cookie);
        return;
    }
    COUNTER_INCREMENT(m_metrics, async_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));
}

void AioDriveInterface::async_readv(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                    uint8_t* cookie) {

    if (!_aio_ctx || _aio_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_read_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, force_sync_io_empty_iocb, 1);
        LOGWARN("Not enough available iocbs to schedule an async readv: size {}, offset {}, doing sync read instead",
                size, offset);
        sync_readv(data_fd, iov, iovcnt, size, offset);
        if (m_comp_cb) m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = _aio_ctx->iocb_list.top();
    struct iocb* iocb = static_cast< struct iocb* >(info);
    _aio_ctx->iocb_list.pop();

    io_prep_preadv(iocb, data_fd, iov, iovcnt, offset);
    io_set_eventfd(iocb, _aio_ctx->ev_fd);
    iocb->data = cookie;
    info->is_read = true;
    info->size = size;
    info->offset = offset;
    info->fd = data_fd;

#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_read_error_flip", iovcnt, size)) {
        m_comp_cb(homestore::homestore_error::read_failed, cookie);
        return;
    }
#endif

    LOGTRACE("Reading data: info: {} iovcnt: {}", info->to_string(), iovcnt);
    auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
    if (ret != 1) {
        if (errno == EAGAIN) {
            COUNTER_INCREMENT(m_metrics, force_sync_io_eagain_error, 1);
            sync_readv(data_fd, iov, iovcnt, size, offset);
        } else {
            COUNTER_INCREMENT(m_metrics, read_io_submission_errors, 1);
            LOGERROR("io submit fail: io info: {}, iovcnt {} errno: {}", info->to_string(), iovcnt, errno);
        }
        if (m_comp_cb) m_comp_cb(errno, cookie);
        return;
    }
    COUNTER_INCREMENT(m_metrics, async_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));
}

ssize_t AioDriveInterface::sync_write(int data_fd, const char* data, uint32_t size, uint64_t offset) {
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_write_error_flip", size)) { folly::throwSystemError("flip error"); }
#endif

    ssize_t written_size = pwrite(data_fd, data, (ssize_t)size, (off_t)offset);
    if (written_size != size) {
        std::stringstream ss;
        ss << "Error trying to write offset " << offset << " size to write = " << size
           << " size written = " << written_size << " err no" << errno << " fd =" << data_fd << "\n";
        folly::throwSystemError(ss.str());
    }
    COUNTER_INCREMENT(m_metrics, sync_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));

    return written_size;
}

ssize_t AioDriveInterface::sync_writev(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_write_error_flip", iovcnt, size)) { folly::throwSystemError("flip error"); }
#endif
    ssize_t written_size = pwritev(data_fd, iov, iovcnt, offset);
    if (written_size != size) {
        std::stringstream ss;
        ss << "Error trying to write offset " << offset << " size to write = " << size
           << " size written = " << written_size << " err no" << errno << " fd =" << data_fd << "\n";
        folly::throwSystemError(ss.str());
    }
    COUNTER_INCREMENT(m_metrics, sync_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));

    return written_size;
}

ssize_t AioDriveInterface::sync_read(int data_fd, char* data, uint32_t size, uint64_t offset) {
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_read_error_flip", size)) { folly::throwSystemError("flip error"); }
#endif
    ssize_t read_size = pread(data_fd, data, (ssize_t)size, (off_t)offset);
    if (read_size != size) {
        std::stringstream ss;
        ss << "Error trying to read offset " << offset << " size to read = " << size << " size read = " << read_size
           << " err no" << errno << " fd =" << data_fd << "\n";
        folly::throwSystemError(ss.str());
    }
    COUNTER_INCREMENT(m_metrics, sync_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));

    return read_size;
}

ssize_t AioDriveInterface::sync_readv(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_read_error_flip", iovcnt, size)) { folly::throwSystemError("flip error"); }
#endif
    ssize_t read_size = preadv(data_fd, iov, iovcnt, (off_t)offset);
    if (read_size != size) {
        std::stringstream ss;
        ss << "Error trying to read offset " << offset << " size to read = " << size << " size read = " << read_size
           << " err no" << errno << " fd =" << data_fd << "\n";
        folly::throwSystemError(ss.str());
    }
    COUNTER_INCREMENT(m_metrics, sync_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));

    return read_size;
}
} // namespace iomgr

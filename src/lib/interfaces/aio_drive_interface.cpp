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
        return fd;
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

    _aio_ctx->iocb_info_prealloc(MAX_OUTSTANDING_IO);
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

    /* TODO need to handle the error events */
    uint64_t temp = 0;

    [[maybe_unused]] auto rsize = read(_aio_ctx->ev_fd, &temp, sizeof(uint64_t));
    int nevents = io_getevents(_aio_ctx->ioctx, 0, MAX_COMPLETIONS, _aio_ctx->events, NULL);

    LOGTRACEMOD(iomgr, "Received completion on fd = {} ev_fd = {} nevents = {}", fd, _aio_ctx->ev_fd, nevents);
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
        _aio_ctx->free_iocb(iocb);
        if (m_comp_cb) m_comp_cb(e.res2, user_cookie);
    }

    // Call any batch completions hints
    if (nevents) {
        for (auto& cb : m_io_end_of_batch_cb_list) {
            LOGTRACEMOD(iomgr, "Making end of batch cb list callback with nevents={}", nevents);
            cb(nevents);
        }
    }
}

void AioDriveInterface::async_write(int data_fd, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                    bool part_of_batch) {
    if (!_aio_ctx || !_aio_ctx->can_submit_aio()) {
        COUNTER_INCREMENT(m_metrics, force_sync_io_empty_iocb, 1);
        LOGWARN("Not enough available iocbs to schedule an async write: size {}, offset {}, doing sync write instead",
                size, offset);
        sync_write(data_fd, data, size, offset);
        if (m_comp_cb) m_comp_cb(0, cookie);
        return;
    }

    if (part_of_batch && _aio_ctx->can_be_batched(0)) {
        _aio_ctx->prep_iocb(true /* batch_io */, data_fd, false /* is_read */, data, size, offset, cookie);
    } else {
        auto iocb = _aio_ctx->prep_iocb(false, data_fd, false, data, size, offset, cookie);
        COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        if (ret != 1) {
            handle_io_failure(iocb);
            _aio_ctx->free_iocb(iocb);
            if (m_comp_cb) m_comp_cb(errno, cookie);
            return;
        }
    }
    COUNTER_INCREMENT(m_metrics, async_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));
}

void AioDriveInterface::async_read(int data_fd, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                   bool part_of_batch) {
    if (!_aio_ctx || !_aio_ctx->can_submit_aio()) {
        COUNTER_INCREMENT(m_metrics, force_sync_io_empty_iocb, 1);
        LOGWARN("Not enough available iocbs to schedule an async read: size {}, offset {}, doing sync read instead",
                size, offset);
        sync_read(data_fd, data, size, offset);
        if (m_comp_cb) m_comp_cb(0, cookie);
        return;
    }

    if (part_of_batch && _aio_ctx->can_be_batched(0)) {
        _aio_ctx->prep_iocb(true /* batch_io */, data_fd, true /* is_read */, data, size, offset, cookie);
    } else {
        auto iocb = _aio_ctx->prep_iocb(false, data_fd, true, data, size, offset, cookie);
        COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        if (ret != 1) {
            handle_io_failure(iocb);
            _aio_ctx->free_iocb(iocb);
            if (m_comp_cb) m_comp_cb(errno, cookie);
            return;
        }
    }
    COUNTER_INCREMENT(m_metrics, async_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));
}

void AioDriveInterface::async_writev(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie, bool part_of_batch) {
    if (!_aio_ctx || !_aio_ctx->can_submit_aio()
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

#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_write_error_flip")) {
        m_comp_cb(homestore::homestore_error::write_failed, cookie);
        return;
    }
#endif

    if (part_of_batch && _aio_ctx->can_be_batched(iovcnt)) {
        _aio_ctx->prep_iocb_v(true /* batch_io */, data_fd, false /* is_read */, iov, iovcnt, size, offset, cookie);
    } else {
        auto iocb = _aio_ctx->prep_iocb_v(false, data_fd, false, iov, iovcnt, size, offset, cookie);
        COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        if (ret != 1) {
            handle_io_failure(iocb);
            _aio_ctx->free_iocb(iocb);
            if (m_comp_cb) m_comp_cb(errno, cookie);
            return;
        }
    }
    COUNTER_INCREMENT(m_metrics, async_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));
}

void AioDriveInterface::async_readv(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                    uint8_t* cookie, bool part_of_batch) {

    if (!_aio_ctx || !_aio_ctx->can_submit_aio()
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

#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_read_error_flip", iovcnt, size)) {
        m_comp_cb(homestore::homestore_error::read_failed, cookie);
        return;
    }
#endif

    if (part_of_batch && _aio_ctx->can_be_batched(iovcnt)) {
        _aio_ctx->prep_iocb_v(true /* batch_io */, data_fd, true /* is_read */, iov, iovcnt, size, offset, cookie);
    } else {
        auto iocb = _aio_ctx->prep_iocb_v(false, data_fd, true, iov, iovcnt, size, offset, cookie);
        COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
        auto ret = io_submit(_aio_ctx->ioctx, 1, &iocb);
        if (ret != 1) {
            handle_io_failure(iocb);
            _aio_ctx->free_iocb(iocb);
            if (m_comp_cb) m_comp_cb(errno, cookie);
            return;
        }
    }
    COUNTER_INCREMENT(m_metrics, async_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));
}

void AioDriveInterface::submit_batch() {
    auto ibatch = _aio_ctx->move_cur_batch();
    LOGTRACEMOD(iomgr, "submit pending batch n_iocbs={}", ibatch.n_iocbs);
    if (ibatch.n_iocbs == 0) { return; } // No batch to submit

    COUNTER_INCREMENT(m_metrics, total_io_submissions, 1);
    auto n_issued = io_submit(_aio_ctx->ioctx, ibatch.n_iocbs, ibatch.get_iocb_list());
    if (n_issued < 0) { n_issued = 0; }

    // For those which we are not able to issue, convert that to sync io
    auto n_iocbs = ibatch.n_iocbs;
    for (auto i = n_issued; i < n_iocbs; ++i) {
        auto info = ibatch.iocb_info[i];
        auto iocb = (struct iocb*)info;
        handle_io_failure(iocb);
        _aio_ctx->free_iocb(iocb);
        if (m_comp_cb) m_comp_cb(errno, (uint8_t*)iocb->data);
    }
}

void AioDriveInterface::handle_io_failure(struct iocb* iocb) {
    auto info = (iocb_info_t*)iocb;

    if (errno == EAGAIN) {
        COUNTER_INCREMENT(m_metrics, force_sync_io_eagain_error, 1);
        if (info->is_read) {
            info->user_data
                ? sync_read(info->fd, info->user_data, info->size, info->offset)
                : sync_readv(info->fd, (const iovec*)&info->iovs[0], info->iovcnt, info->size, info->offset);
        } else {
            info->user_data
                ? sync_write(info->fd, info->user_data, info->size, info->offset)
                : sync_writev(info->fd, (const iovec*)&info->iovs[0], info->iovcnt, info->size, info->offset);
        }
    } else {
        LOGERROR("io submit fail: io info: {}, errno: {}", info->to_string(), errno);
        COUNTER_INCREMENT_IF_ELSE(m_metrics, info->is_read, read_io_submission_errors, write_io_submission_errors, 1);
    }
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

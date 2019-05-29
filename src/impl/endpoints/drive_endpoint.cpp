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
#include "drive_endpoint.hpp"
#include <fstream>
#include <sys/epoll.h>
#include <sds_logging/logging.h>
#include "include/iomgr.hpp"

namespace iomgr {
#ifdef __APPLE__

ssize_t preadv(int fd, const struct iovec* iov, int iovcnt, off_t offset) {
    lseek(fd, offset, SEEK_SET);
    return ::readv(fd, iov, iovcnt);
}

ssize_t pwritev(int fd, const struct iovec* iov, int iovcnt, off_t offset) {
    lseek(fd, offset, SEEK_SET);
    return ::writev(fd, iov, iovcnt);
}

#endif
using namespace std;

thread_local struct io_event            DriveEndPoint::events[MAX_COMPLETIONS] = {{}};
thread_local int                        DriveEndPoint::ev_fd = 0;
thread_local io_context_t               DriveEndPoint::ioctx = 0;
thread_local stack< struct iocb_info* > DriveEndPoint::iocb_list;

DriveEndPoint::DriveEndPoint(drive_comp_callback cb) : m_comp_cb(cb) {}

int DriveEndPoint::open_dev(std::string devname, int oflags) {
    /* it doesn't need to keep track of any fds */
    return (open(devname.c_str(), oflags));
}

void DriveEndPoint::on_thread_start() {
    ev_fd = eventfd(0, EFD_NONBLOCK);
    iomgr_instance.add_per_thread_fd(this, ev_fd,
                        std::bind(&DriveEndPoint::process_completions, this, std::placeholders::_1,
                                  std::placeholders::_2, std::placeholders::_3),
                        EPOLLIN, 0, NULL);
    io_setup(MAX_OUTSTANDING_IO, &ioctx);
    for (int i = 0; i < MAX_OUTSTANDING_IO; i++) {
        struct iocb_info* info = (struct iocb_info*)malloc(sizeof(struct iocb_info));
        iocb_list.push(info);
    }
}

void DriveEndPoint::process_completions(int fd, void* cookie, int event) {
    assert(fd == ev_fd);

    (void)cookie; (void)event;

    /* TODO need to handle the error events */
    uint64_t temp = 0;

    [[maybe_unused]] auto rsize = read(ev_fd, &temp, sizeof(uint64_t));
    int                   ret = io_getevents(ioctx, 0, MAX_COMPLETIONS, events, NULL);
    if (ret == 0) {
        spurious_events++;
    }
    if (ret < 0) {
        /* TODO how to handle it */
        cmp_err++;
    }
    for (int i = 0; i < ret; i++) {
        assert(static_cast< int64_t >(events[i].res) >= 0);
        struct iocb_info* info = static_cast< iocb_info* >(events[i].obj);
        iocb_list.push(info);
        m_comp_cb(events[i].res, (uint8_t*)events[i].data);
    }
}

void DriveEndPoint::async_write(int m_sync_fd, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie) {
    if (iocb_list.empty()) {
        sync_write(m_sync_fd, data, size, offset);
        m_comp_cb(0, cookie);
        return;
    }

    struct iocb_info* info = iocb_list.top();
    struct iocb*      iocb = static_cast< struct iocb* >(info);
    iocb_list.pop();

    io_prep_pwrite(iocb, m_sync_fd, (void*)data, size, offset);
    io_set_eventfd(iocb, ev_fd);
    iocb->data = cookie;
    info->is_read = false;

    LOGTRACE("Writing: {}", size);
    if (io_submit(ioctx, 1, &iocb) != 1) {
        m_comp_cb(errno, cookie);
    }
}

void DriveEndPoint::async_read(int m_sync_fd, char* data, uint32_t size, uint64_t offset, uint8_t* cookie) {
    if (iocb_list.empty()) {
        sync_read(m_sync_fd, data, size, offset);
        m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = iocb_list.top();
    struct iocb*      iocb = static_cast< struct iocb* >(info);
    iocb_list.pop();

    io_prep_pread(iocb, m_sync_fd, data, size, offset);
    io_set_eventfd(iocb, ev_fd);
    iocb->data = cookie;
    info->is_read = true;

    LOGTRACE("Reading: {}", size);
    if (io_submit(ioctx, 1, &iocb) != 1) {
        m_comp_cb(errno, cookie);
    }
}

void DriveEndPoint::async_writev(int m_sync_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                 uint8_t* cookie) {

    if (iocb_list.empty()) {
        sync_writev(m_sync_fd, iov, iovcnt, size, offset);
        m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = iocb_list.top();
    struct iocb*      iocb = static_cast< struct iocb* >(info);
    iocb_list.pop();
    io_prep_pwritev(iocb, m_sync_fd, iov, iovcnt, offset);
    io_set_eventfd(iocb, ev_fd);
    iocb->data = cookie;
    info->is_read = false;
    if (io_submit(ioctx, 1, &iocb) != 1) {
        m_comp_cb(errno, cookie);
    }
}

void DriveEndPoint::async_readv(int m_sync_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                uint8_t* cookie) {

    if (iocb_list.empty()) {
        sync_readv(m_sync_fd, iov, iovcnt, size, offset);
        m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = iocb_list.top();
    struct iocb*      iocb = static_cast< struct iocb* >(info);
    iocb_list.pop();

    io_prep_preadv(iocb, m_sync_fd, iov, iovcnt, offset);
    io_set_eventfd(iocb, ev_fd);
    iocb->data = cookie;
    info->is_read = true;

    LOGTRACE("Reading: {} vectors", iovcnt);
    if (io_submit(ioctx, 1, &iocb) != 1) {
        m_comp_cb(errno, cookie);
    }
}

void DriveEndPoint::sync_write(int m_sync_fd, const char* data, uint32_t size, uint64_t offset) {
    ssize_t written_size = pwrite(m_sync_fd, data, (ssize_t)size, (off_t)offset);
    if (written_size != size) {
        std::stringstream ss;
        ss << "Error trying to write offset " << offset << " size to write = " << size
           << " size written = " << written_size << "\n";
        folly::throwSystemError(ss.str());
    }
}

void DriveEndPoint::sync_writev(int m_sync_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    ssize_t written_size = pwritev(m_sync_fd, iov, iovcnt, offset);
    if (written_size != size) {
        std::stringstream ss;
        ss << "Error trying to write offset " << offset << " size to write = " << size
           << " size written = " << written_size << "\n";
        folly::throwSystemError(ss.str());
    }
}

void DriveEndPoint::sync_read(int m_sync_fd, char* data, uint32_t size, uint64_t offset) {
    ssize_t read_size = pread(m_sync_fd, data, (ssize_t)size, (off_t)offset);
    if (read_size != size) {
        std::stringstream ss;
        ss << "Error trying to read offset " << offset << " size to read = " << size << " size read = " << read_size
           << "\n";
        folly::throwSystemError(ss.str());
    }
}

void DriveEndPoint::sync_readv(int m_sync_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    ssize_t read_size = preadv(m_sync_fd, iov, iovcnt, (off_t)offset);
    if (read_size != size) {
        std::stringstream ss;
        ss << "Error trying to read offset " << offset << " size to read = " << size << " size read = " << read_size
           << "\n";
        folly::throwSystemError(ss.str());
    }
}

} // namespace homeio

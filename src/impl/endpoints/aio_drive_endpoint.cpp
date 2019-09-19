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
#include "include/aio_drive_endpoint.hpp"

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

thread_local struct io_event            AioDriveEndPoint::events[MAX_COMPLETIONS] = {{}};
thread_local int                        AioDriveEndPoint::ev_fd = 0;
thread_local io_context_t               AioDriveEndPoint::ioctx = 0;
thread_local stack< struct iocb_info* > AioDriveEndPoint::iocb_list;

AioDriveEndPoint::AioDriveEndPoint(const endpoint_comp_closure& comp_closure,
                                   const thread_state_notifier& thread_notifier) :
        m_comp_cb(comp_closure),
        m_thread_notifier(thread_notifier) {}

int AioDriveEndPoint::open_dev(std::string devname, int oflags) {
    /* it doesn't need to keep track of any fds */
    auto fd = open(devname.c_str(), oflags);
    if (fd == -1) {
        std::cerr << "Unable to open the device " << devname << " errorno = " << errno << " error = " << strerror(errno)
                  << "\n";
    }
    LOGINFO("Device {} opened with flags={} successfully, fd={}", devname, oflags, fd);
    /*iomanager.add_fd(this, fd,
                     std::bind(&AioDriveEndPoint::process_completions, this, std::placeholders::_1,
                               std::placeholders::_2, std::placeholders::_3),
                     EPOLLIN, 9, nullptr); */
    return fd;
}

void AioDriveEndPoint::on_thread_start() {
    ev_fd = eventfd(0, EFD_NONBLOCK);
    iomanager.add_per_thread_fd(this, ev_fd,
                                std::bind(&AioDriveEndPoint::process_completions, this, std::placeholders::_1,
                                          std::placeholders::_2, std::placeholders::_3),
                                EPOLLIN, 0, NULL);
    io_setup(MAX_OUTSTANDING_IO, &ioctx);
    for (int i = 0; i < MAX_OUTSTANDING_IO; i++) {
        struct iocb_info* info = (struct iocb_info*)malloc(sizeof(struct iocb_info));
        iocb_list.push(info);
    }

    if (m_thread_notifier) {
        m_thread_notifier(true /* thread_started */);
    }
}

void AioDriveEndPoint::on_thread_exit() {
    // TODO: Fixme Need to handle thread exit
    if (m_thread_notifier) {
        m_thread_notifier(false /* thread_started */);
    }
}

void AioDriveEndPoint::process_completions(int fd, void* cookie, int event) {
    assert(fd == ev_fd);

    (void)cookie;
    (void)event;

    LOGTRACEMOD(iomgr, "Received completion on fd = {} ev_fd = {}", fd, ev_fd);
    /* TODO need to handle the error events */
    uint64_t temp = 0;

    [[maybe_unused]] auto rsize = read(ev_fd, &temp, sizeof(uint64_t));
    int                   nevents = io_getevents(ioctx, 0, MAX_COMPLETIONS, events, NULL);
    if (nevents == 0) {
        spurious_events++;
    }
    if (nevents < 0) {
        /* TODO how to handle it */
        cmp_err++;
    }
    for (int i = 0; i < nevents; i++) {
        assert(static_cast< int64_t >(events[i].res) >= 0);
        struct iocb_info* info = static_cast< iocb_info* >(events[i].obj);
        iocb_list.push(info);
        m_comp_cb(events[i].res, (uint8_t*)events[i].data);
    }
}

void AioDriveEndPoint::async_write(int data_fd, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie) {
    if (iocb_list.empty()) {
        sync_write(data_fd, data, size, offset);
        m_comp_cb(0, cookie);
        return;
    }

    struct iocb_info* info = iocb_list.top();
    struct iocb*      iocb = static_cast< struct iocb* >(info);
    iocb_list.pop();

    io_prep_pwrite(iocb, data_fd, (void*)data, size, offset);
    io_set_eventfd(iocb, ev_fd);
    iocb->data = cookie;
    info->is_read = false;

    LOGTRACE("Writing: {}", size);
    if (io_submit(ioctx, 1, &iocb) != 1) {
        m_comp_cb(errno, cookie);
    }
}

void AioDriveEndPoint::async_read(int data_fd, char* data, uint32_t size, uint64_t offset, uint8_t* cookie) {
    if (iocb_list.empty()) {
        sync_read(data_fd, data, size, offset);
        m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = iocb_list.top();
    struct iocb*      iocb = static_cast< struct iocb* >(info);
    iocb_list.pop();

    io_prep_pread(iocb, data_fd, data, size, offset);
    io_set_eventfd(iocb, ev_fd);
    iocb->data = cookie;
    info->is_read = true;

    LOGTRACE("Reading: {}", size);
    if (io_submit(ioctx, 1, &iocb) != 1) {
        m_comp_cb(errno, cookie);
    }
}

void AioDriveEndPoint::async_writev(int data_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                    uint8_t* cookie) {

    if (iocb_list.empty()) {
        sync_writev(data_fd, iov, iovcnt, size, offset);
        m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = iocb_list.top();
    struct iocb*      iocb = static_cast< struct iocb* >(info);
    iocb_list.pop();
    io_prep_pwritev(iocb, data_fd, iov, iovcnt, offset);
    io_set_eventfd(iocb, ev_fd);
    iocb->data = cookie;
    info->is_read = false;
    if (io_submit(ioctx, 1, &iocb) != 1) {
        m_comp_cb(errno, cookie);
    }
}

void AioDriveEndPoint::async_readv(int data_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                   uint8_t* cookie) {

    if (iocb_list.empty()) {
        sync_readv(data_fd, iov, iovcnt, size, offset);
        m_comp_cb(0, cookie);
        return;
    }
    struct iocb_info* info = iocb_list.top();
    struct iocb*      iocb = static_cast< struct iocb* >(info);
    iocb_list.pop();

    io_prep_preadv(iocb, data_fd, iov, iovcnt, offset);
    io_set_eventfd(iocb, ev_fd);
    iocb->data = cookie;
    info->is_read = true;

    LOGTRACE("Reading: {} vectors", iovcnt);
    if (io_submit(ioctx, 1, &iocb) != 1) {
        m_comp_cb(errno, cookie);
    }
}

void AioDriveEndPoint::sync_write(int data_fd, const char* data, uint32_t size, uint64_t offset) {
    ssize_t written_size = pwrite(data_fd, data, (ssize_t)size, (off_t)offset);
    if (written_size != size) {
        std::stringstream ss;
        ss << "Error trying to write offset " << offset << " size to write = " << size
           << " size written = " << written_size << "\n";
        folly::throwSystemError(ss.str());
    }
}

void AioDriveEndPoint::sync_writev(int data_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    ssize_t written_size = pwritev(data_fd, iov, iovcnt, offset);
    if (written_size != size) {
        std::stringstream ss;
        ss << "Error trying to write offset " << offset << " size to write = " << size
           << " size written = " << written_size << "\n";
        folly::throwSystemError(ss.str());
    }
}

void AioDriveEndPoint::sync_read(int data_fd, char* data, uint32_t size, uint64_t offset) {
    ssize_t read_size = pread(data_fd, data, (ssize_t)size, (off_t)offset);
    if (read_size != size) {
        std::stringstream ss;
        ss << "Error trying to read offset " << offset << " size to read = " << size << " size read = " << read_size
           << "\n";
        folly::throwSystemError(ss.str());
    }
}

void AioDriveEndPoint::sync_readv(int data_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    ssize_t read_size = preadv(data_fd, iov, iovcnt, (off_t)offset);
    if (read_size != size) {
        std::stringstream ss;
        ss << "Error trying to read offset " << offset << " size to read = " << size << " size read = " << read_size
           << "\n";
        folly::throwSystemError(ss.str());
    }
}

} // namespace iomgr

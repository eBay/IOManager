//
// Created by Rishabh Mittal 04/20/2018
//
#pragma once

#include <unistd.h>
#include <string>
#include <stack>
#include <atomic>
#include <mutex>
#include "endpoint.hpp"

#ifdef linux
#include <fcntl.h>
#include <libaio.h>
#include <sys/eventfd.h>
#include <stdio.h>
#endif

using namespace std;
using Clock = std::chrono::steady_clock;

namespace iomgr {
#define MAX_OUTSTANDING_IO 200               // if max outstanding IO is more than 200 then io_submit will fail.
#define MAX_COMPLETIONS (MAX_OUTSTANDING_IO) // how many completions to process in one shot

#ifdef linux
struct iocb_info : public iocb {
    bool              is_read;
    Clock::time_point start_time;
};

template < typename T, typename Container = std::deque< T > >
class iterable_stack : public std::stack< T, Container > {
    using std::stack< T, Container >::c;

public:
    // expose just the iterators of the underlying container
    auto begin() { return std::begin(c); }
    auto end() { return std::end(c); }

    auto begin() const { return std::begin(c); }
    auto end() const { return std::end(c); }
};

struct fd_info;
class ioMgrThreadContext;
struct aio_thread_context {
    struct io_event                 events[MAX_COMPLETIONS] = {{}};
    int                             ev_fd = 0;
    io_context_t                    ioctx = 0;
    std::stack< struct iocb_info* > iocb_list;
    fd_info*                        ev_fd_info = nullptr; // fd info after registering with IOManager

    ~aio_thread_context() {
        if (ev_fd) { close(ev_fd); }
        io_destroy(ioctx);

        while (!iocb_list.empty()) {
            auto info = iocb_list.top();
            free(info);
            iocb_list.pop();
        }
    }
};

class AioDriveEndPoint : public EndPoint {
public:
    AioDriveEndPoint(const endpoint_comp_cb_t& cb);

    static int open_dev(std::string devname, int oflags);

    void add_fd(int fd, int priority = 9);
    void sync_write(int m_sync_fd, const char* data, uint32_t size, uint64_t offset);
    void sync_writev(int m_sync_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset);
    void sync_read(int m_sync_fd, char* data, uint32_t size, uint64_t offset);
    void sync_readv(int m_sync_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset);
    void async_write(int m_sync_fd, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie);
    void async_writev(int m_sync_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                      uint8_t* cookie);
    void async_read(int m_sync_fd, char* data, uint32_t size, uint64_t offset, uint8_t* cookie);
    void async_readv(int m_sync_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                     uint8_t* cookie);
    void process_completions(int fd, void* cookie, int event);
    void on_io_thread_start(ioMgrThreadContext* iomgr_ctx) override;
    void on_io_thread_stopped(ioMgrThreadContext* iomgr_ctx) override;

private:
    static thread_local aio_thread_context* _aio_ctx;

    atomic< uint64_t > spurious_events = 0;
    atomic< uint64_t > cmp_err = 0;
    endpoint_comp_cb_t m_comp_cb;
};
#else
class AioDriveEndPoint : public EndPoint {
public:
    AioDriveEndPoint(const endpoint_comp_cb_t& cb) {}
    void on_io_thread_start(ioMgrThreadContext* iomgr_ctx) override {}
    void on_io_thread_stopped(ioMgrThreadContext* iomgr_ctx) override {}
};
#endif
} // namespace iomgr

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
#define MAX_OUTSTANDING_IO                                                                                             \
    200                                      // if max outstanding IO is more then
                                             //  200 then io_submit will fail.
#define MAX_COMPLETIONS (MAX_OUTSTANDING_IO) // how many completions to process in one shot

#ifdef linux
struct iocb_info : public iocb {
    bool              is_read;
    Clock::time_point start_time;
};

class AioDriveEndPoint : public EndPoint {
public:
    AioDriveEndPoint(const endpoint_comp_closure& comp_closure, const thread_state_notifier& thread_notifier = nullptr);

    int  open_dev(std::string devname, int oflags);
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
    void on_thread_start() override;
    void on_thread_exit() override;

private:
    static thread_local int                        ev_fd;
    static thread_local io_context_t               ioctx;
    static thread_local stack< struct iocb_info* > iocb_list;
    static thread_local struct io_event            events[MAX_COMPLETIONS];

    atomic< uint64_t >    spurious_events = 0;
    atomic< uint64_t >    cmp_err = 0;
    endpoint_comp_closure m_comp_cb;
    thread_state_notifier m_thread_notifier;
};
#else
class AioDriveEndPoint : public EndPoint {
public:
    AioDriveEndPoint(const endpoint_comp_closure& comp_closure,
                     const thread_state_notifier& thread_notifier = nullptr) {}
    void on_thread_start() override {}
    void on_thread_exit() override {}
};
#endif
} // namespace iomgr

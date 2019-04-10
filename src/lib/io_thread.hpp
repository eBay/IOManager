/**
 * Copyright eBay Corporation 2018
 */

#pragma once

#include <sds_logging/logging.h>
#include <pthread.h>
#include <iostream>

SDS_LOGGING_DECL(iomgr);

namespace iomgr {

constexpr size_t MAX_PRI = 10;
using ev_callback = std::function< void(int fd, void* cookie, uint32_t events) >;

struct ioMgrImpl;
struct fd_info;

class ioMgrThreadContext {
    friend class ioMgrImpl;

public:
    explicit ioMgrThreadContext(ioMgrImpl* iomgr);
    void run();
    void listen();
    fd_info* add_fd_to_thread(int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie);
    bool is_io_thread() const;

private:
    void context_init(bool wait_till_ready);

private:
    ioMgrImpl* m_iomgr;                 // Backpointer to iomgr
    int        m_epollfd;               // Parent epoll context for this thread
    int        m_epollfd_pri[MAX_PRI];  // Priority based child epoll context for this thread
    int        m_thread_num;            // Thread num
    uint64_t   m_count = 0;
    uint64_t   m_time_spent_ns = 0;
    int        m_ev_slot;               // Slot number within the ev fd list in each of the fd_info
    int        m_ev_fd;                 // This thread event fd to listen for events
    int        m_inited = false;
};
} // namespace iomgr

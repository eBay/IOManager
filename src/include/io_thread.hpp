/**
 * Copyright eBay Corporation 2018
 */

#pragma once

#include <sds_logging/logging.h>
#include <pthread.h>
#include <iostream>
#include <folly/MPMCQueue.h>
#include "endpoint.hpp"
#include "iomgr_msg.hpp"

SDS_LOGGING_DECL(iomgr);

namespace iomgr {

constexpr size_t MAX_PRI = 10;
using ev_callback = std::function< void(int fd, void* cookie, uint32_t events) >;

struct fd_info;

class EndPoint;
class ioMgrThreadContext {
    friend class IOManager;

public:
    ioMgrThreadContext();
    ~ioMgrThreadContext();
    void run(bool is_iomgr_thread = false);
    void listen();
    // fd_info* add_fd_to_thread(EndPoint* ep, int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie);
    int  add_fd_to_thread(fd_info* info);
    int  remove_fd_from_thread(fd_info* info);
    bool is_io_thread() const;

    /***
     * Put the message to the message q for this thread.
     * @param msg
     */
    // void put_msg(iomgr_msg&& msg);
    void put_msg(const iomgr_msg& msg);
    void put_msg(iomgr_msg_type type, fd_info* info, int event, void* buf = nullptr, uint32_t size = 0);

private:
    void iothread_init(bool wait_till_ready);
    void on_msg_fd_notification();

private:
    int                        m_epollfd = -1; // Parent epoll context for this thread
    int                        m_thread_num;   // Thread num
    std::unique_ptr< fd_info > m_msg_fd_info;  // fd_info for the message fd
    uint64_t                   m_count = 0;    // Count of operations this thread is handling.
    uint64_t                   m_time_spent_ns = 0;
    bool                       m_is_io_thread = false;
    bool                       m_is_iomgr_thread = false; // Is this thread created by iomanager itself
    bool                       m_keep_running = true;

    folly::MPMCQueue< iomgr_msg, std::atomic, true > m_msg_q; // Q of message for this thread
};
} // namespace iomgr

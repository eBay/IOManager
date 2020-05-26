/**
 * Copyright eBay Corporation 2018
 */
#pragma once
#include "io_thread.hpp"
#include "io_interface.hpp"
#include <folly/MPMCQueue.h>

namespace iomgr {
class IOReactorEPoll : public IOReactor {
    friend class IOManager;

public:
    IOReactorEPoll();
    io_thread_id_t my_io_thread_id() const override { return io_thread_id_t(m_thread_num); };

private:
    bool iocontext_init() override;
    void iocontext_exit() override;
    void listen() override;
    void on_msg_fd_notification();
    int _add_iodev_to_reactor(const io_device_ptr& iodev) override;
    int _remove_iodev_from_reactor(const io_device_ptr& iodev) override;
    bool put_msg(iomgr_msg* msg) override;

    bool is_tight_loop_thread() const override { return false; };

private:
    int m_epollfd = -1;                                        // Parent epoll context for this thread
    io_device_ptr m_msg_iodev;                                 // iodev for the messages
    folly::MPMCQueue< iomgr_msg*, std::atomic, true > m_msg_q; // Q of message for this thread
};
} // namespace iomgr

/**
 * Copyright eBay Corporation 2018
 */
#pragma once
#include "reactor.hpp"
#include <folly/MPMCQueue.h>

namespace iomgr {
class IOReactorEPoll : public IOReactor {
    friend class IOManager;

public:
    IOReactorEPoll();

private:
    const char* loop_type() const override { return "Epoll"; }
    bool reactor_specific_init_thread(const io_thread_t& thr) override;
    void reactor_specific_exit_thread(const io_thread_t& thr) override;
    void listen() override;
    void on_msg_fd_notification();
    void on_user_iodev_notification(IODevice* iodev, int event);
    int _add_iodev_to_thread(const io_device_ptr& iodev, const io_thread_t& thr) override;
    int _remove_iodev_from_thread(const io_device_ptr& iodev, const io_thread_t& thr) override;
    bool put_msg(iomgr_msg* msg) override;

    bool is_tight_loop_reactor() const override { return false; };

private:
    int m_epollfd = -1;                                        // Parent epoll context for this thread
    io_device_ptr m_msg_iodev;                                 // iodev for the messages
    folly::MPMCQueue< iomgr_msg*, std::atomic, true > m_msg_q; // Q of message for this thread
};
} // namespace iomgr

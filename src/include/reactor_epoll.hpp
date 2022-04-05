/**
 * Copyright eBay Corporation 2018
 */
#pragma once
#include "reactor.hpp"
#include <folly/concurrency/UnboundedQueue.h>

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
    void process_messages();
    void on_user_iodev_notification(IODevice* iodev, int event);
    int add_iodev_internal(const io_device_const_ptr& iodev, const io_thread_t& thr) override;
    int remove_iodev_internal(const io_device_const_ptr& iodev, const io_thread_t& thr) override;
    bool put_msg(iomgr_msg* msg) override;

    bool is_tight_loop_reactor() const override { return false; };
    bool is_iodev_addable(const io_device_const_ptr& iodev, const io_thread_t& thread) const override;

    void idle_time_wakeup_poller();

private:
    std::atomic< bool > m_msg_handler_on;           // Is Message handling ongoing now
    int m_epollfd = -1;                             // Parent epoll context for this thread
    io_device_ptr m_msg_iodev;                      // iodev for the messages
    folly::UMPSCQueue< iomgr_msg*, false > m_msg_q; // Q of message for this thread
};
} // namespace iomgr

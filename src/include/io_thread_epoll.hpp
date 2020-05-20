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
    int add_iodev_to_reactor(const io_device_ptr& iodev) override;
    int remove_iodev_from_reactor(const io_device_ptr& iodev) override;
    bool send_msg(const iomgr_msg& msg) override;

    io_thread_id_t my_io_thread_id() const override { return io_thread_id_t(m_thread_num); };

private:
    bool iocontext_init() override;
    void iocontext_exit() override;
    void listen() override;

    void put_msg(const iomgr_msg& msg);
    void on_msg_fd_notification();

private:
    int m_epollfd = -1;                                       // Parent epoll context for this thread
    io_device_ptr m_msg_iodev;                                // iodev for the messages
    folly::MPMCQueue< iomgr_msg, std::atomic, true > m_msg_q; // Q of message for this thread
};
} // namespace iomgr

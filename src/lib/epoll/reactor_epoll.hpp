/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Harihara Kadayam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **************************************************************************/
#pragma once
#include "reactor/reactor.hpp"
#include <folly/concurrency/UnboundedQueue.h>

namespace iomgr {
class IOReactorEPoll : public IOReactor {
    friend class IOManager;

public:
    IOReactorEPoll();

private:
    const char* loop_type() const override { return "Epoll"; }
    void init_impl() override;
    void stop_impl() override;
    void listen() override;
    void on_msg_fd_notification();
    void process_messages();
    void on_user_iodev_notification(IODevice* iodev, int event);
    int add_iodev_impl(const io_device_ptr& iodev) override;
    int remove_iodev_impl(const io_device_ptr& iodev) override;
    void put_msg(iomgr_msg* msg) override;

    bool is_tight_loop_reactor() const override { return false; };
    bool is_iodev_addable(const io_device_const_ptr& iodev) const override;

    void idle_time_wakeup_poller();

private:
    std::atomic< bool > m_msg_handler_on;           // Is Message handling ongoing now
    int m_epollfd = -1;                             // Parent epoll context for this thread
    io_device_ptr m_msg_iodev;                      // iodev for the messages
    folly::UMPSCQueue< iomgr_msg*, false > m_msg_q; // Q of message for this thread
};
} // namespace iomgr

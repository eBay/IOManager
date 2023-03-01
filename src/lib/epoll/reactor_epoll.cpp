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
extern "C" {
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <time.h>
}

#include <sisl/logging/logging.h>
#include <iomgr/iomgr.hpp>
#include "epoll/reactor_epoll.hpp"
#include "iomgr_config.hpp"

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {

using Clock = std::chrono::steady_clock;

uint64_t get_elapsed_time_ns(Clock::time_point startTime) {
    std::chrono::nanoseconds ns = std::chrono::duration_cast< std::chrono::nanoseconds >(Clock::now() - startTime);
    return ns.count();
}

#define MAX_EVENTS 20

static bool compare_priority(const epoll_event& ev1, const epoll_event& ev2) {
    const IODevice* iodev1 = (const IODevice*)ev1.data.ptr;
    const IODevice* iodev2 = (const IODevice*)ev2.data.ptr;

    return (iodev1->priority() > iodev2->priority());
}

IOReactorEPoll::IOReactorEPoll() : m_msg_q() {}

bool IOReactorEPoll::reactor_specific_init_thread(const io_thread_t& thr) {
    int evfd{-1};

    // Create a epollset for one per thread
    m_epollfd = epoll_create1(0);
    if (m_epollfd < 1) {
        assert(0);
        REACTOR_LOG(ERROR, base, thr->thread_addr, "epoll_create failed: {}", strerror(errno));
        goto error;
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    thr->thread_impl = m_reactor_num;

    REACTOR_LOG(TRACE, iomgr, thr->thread_addr, "EPoll created: {}", m_epollfd);

    // Create a message fd and add it to tht epollset
    evfd = eventfd(0, EFD_NONBLOCK);
    if (evfd == -1) {
        assert(0);
        REACTOR_LOG(ERROR, base, thr->thread_addr, "Unable to open the eventfd, marking this as non-io reactor");
        goto error;
    }
    m_msg_iodev = iomanager.generic_interface()->make_io_device(backing_dev_t{evfd}, EPOLLIN, 1 /* pri */, nullptr,
                                                                true /* thread_dev */, nullptr);

    // Create a per thread timer
    m_thread_timer = std::make_unique< timer_epoll >(iothread_self());
    return true;

error:
    if (m_epollfd > 0) {
        close(m_epollfd);
        m_epollfd = -1;
    }

    if (m_msg_iodev) {
        if (m_msg_iodev->fd() > 0) { close(m_msg_iodev->fd()); }
        m_msg_iodev = nullptr;
    }
    return false;
}

void IOReactorEPoll::reactor_specific_exit_thread(const io_thread_t& thr) {
    if (m_msg_iodev && (m_msg_iodev->fd() != -1)) {
        remove_iodev(m_msg_iodev, thr);
        close(m_msg_iodev->fd());
    }

    m_thread_timer->stop();
    if (m_epollfd != -1) { close(m_epollfd); }

    // Drain the message q and drop the message.
    auto dropped = 0u;
    iomgr_msg* msg;
    while (m_msg_q.try_dequeue(msg)) {
        iomgr_msg::completed(msg);
    }
    if (dropped) { LOGINFO("Exiting the reactor with {} messages yet to handle, dropping them", dropped); }
}

void IOReactorEPoll::listen() {
    std::array< struct epoll_event, MAX_EVENTS > events;

    int num_fds{0};
    do {
        num_fds = epoll_wait(m_epollfd, &events[0], MAX_EVENTS, get_poll_interval());
    } while (num_fds < 0 && errno == EINTR);

    if (num_fds == 0) {
        idle_time_wakeup_poller();
        return;
    } else if (num_fds < 0) {
        REACTOR_LOG(ERROR, base, , "epoll wait failed: {} strerror {}", errno, strerror(errno));
        return;
    }
    m_metrics->fds_on_event_count += num_fds;

    // Next sort the events based on priority and handle them in that order
    std::sort(events.begin(), (events.begin() + num_fds), compare_priority);
    for (auto i = 0; i < num_fds; ++i) {
        auto& e = events[i];
        if (e.data.ptr == (void*)m_msg_iodev.get()) {
            REACTOR_LOG(TRACE, iomgr, , "Processing event on msg fd: {}", m_msg_iodev->fd());
            ++m_metrics->msg_event_wakeup_count;
            on_msg_fd_notification();

            // It is possible for io thread status by the msg processor. Catch at the exit and return
            if (!is_io_reactor()) {
                REACTOR_LOG(INFO, base, , "listen will exit because this is no longer an io reactor");
                return;
            }
        } else {
            IODevice* iodev = (IODevice*)e.data.ptr;
            if (iodev->tinfo) {
                ++m_metrics->timer_wakeup_count;
                timer_epoll::on_timer_fd_notification(iodev);
            } else {
                on_user_iodev_notification(iodev, e.events);
            }
        }
    }
}

int IOReactorEPoll::add_iodev_internal(const io_device_const_ptr& iodev, [[maybe_unused]] const io_thread_t& thr) {
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLEXCLUSIVE | iodev->ev;
    ev.data.ptr = (void*)iodev.get();
    if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, iodev->fd(), &ev) == -1) {
        LOGDFATAL("Adding fd {} to this thread's epoll fd {} failed, error = {}", iodev->fd(), m_epollfd,
                  strerror(errno));
        return -1;
    }
    REACTOR_LOG(DEBUG, iomgr, thr->thread_addr, "Added fd {} to this io thread's epoll fd {}, data.ptr={}", iodev->fd(),
                m_epollfd, (void*)ev.data.ptr);
    return 0;
}

int IOReactorEPoll::remove_iodev_internal(const io_device_const_ptr& iodev, [[maybe_unused]] const io_thread_t& thr) {
    if (epoll_ctl(m_epollfd, EPOLL_CTL_DEL, iodev->fd(), nullptr) == -1) {
        LOGDFATAL("Removing fd {} to this thread's epoll fd {} failed, error = {}", iodev->fd(), m_epollfd,
                  strerror(errno));
        return -1;
    }
    REACTOR_LOG(DEBUG, iomgr, thr->thread_addr, "Removed fd {} from this io thread's epoll fd {}", iodev->fd(),
                m_epollfd);
    return 0;
}

bool IOReactorEPoll::put_msg(iomgr_msg* msg) {
    if (!m_msg_iodev) return false;

    REACTOR_LOG(DEBUG, iomgr, msg->m_dest_thread, "Put msg of type {} to its msg fd = {}, ptr = {}", msg->m_type,
                m_reactor_num, msg->m_dest_thread, m_msg_iodev->fd(), (void*)m_msg_iodev.get());

    m_msg_q.enqueue(msg);

    // Raise an event only in case msg handler is not currently running
    if (!m_msg_handler_on.load(std::memory_order_acquire)) {
        const uint64_t temp{1};
        while ((write(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) < 0) && (errno == EAGAIN)) {
            ++m_metrics->msg_iodev_busy_count;
        }
    }

    return true;
}

void IOReactorEPoll::on_msg_fd_notification() {
    uint64_t temp;
    while ((read(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) < 0) && errno == EAGAIN) {
        ++m_metrics->msg_iodev_busy_count;
    }

    process_messages();
}

void IOReactorEPoll::process_messages() {
    const auto max_msg_batch_size{IM_DYNAMIC_CONFIG(max_msgs_before_yield)};
    uint32_t msg_count{0};
    bool in_retry{false};

    m_msg_handler_on.store(true, std::memory_order_release);
    while (true) {
        // Start pulling all the messages and handle them.
        while (msg_count < max_msg_batch_size) {
            iomgr_msg* msg;
            if (!m_msg_q.try_dequeue(msg)) { break; }
            handle_msg(msg);
            ++msg_count;
        }

        if ((msg_count == max_msg_batch_size) && (!m_msg_q.empty())) {
            REACTOR_LOG(DEBUG, iomgr, , "Reached max msg_count batch {}, yielding and will process again", msg_count);
            const uint64_t temp{1};
            while ((write(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) < 0) && (errno == EAGAIN)) {
                ++m_metrics->msg_iodev_busy_count;
            }
            m_msg_handler_on.store(false, std::memory_order_release);
            break;
        } else if (in_retry) { // Already retrying after msg handler on unset
            break;
        } else {
            m_msg_handler_on.store(false, std::memory_order_release);
            in_retry = true;
        }
    }
}

void IOReactorEPoll::on_user_iodev_notification(IODevice* iodev, int event) {
    ++m_metrics->outstanding_ops;
    ++m_metrics->io_event_wakeup_count;

    REACTOR_LOG(TRACE, iomgr, , "Processing event on user iodev: {}", iodev->dev_id());
    iodev->cb(iodev, iodev->cookie, event);

    --m_metrics->outstanding_ops;
}

bool IOReactorEPoll::is_iodev_addable(const io_device_const_ptr& iodev, const io_thread_t& thread) const {
    return (!iodev->is_spdk_dev() && IOReactor::is_iodev_addable(iodev, thread));
}

void IOReactorEPoll::idle_time_wakeup_poller() {
    ++m_metrics->idle_wakeup_count;

    // Idle time wakeup poller process messages and make any registered callers which look for any
    // other completions.
    process_messages();
    for (auto& cb : m_poll_interval_cbs) {
        if (cb) { cb(); }
    }
}

} // namespace iomgr

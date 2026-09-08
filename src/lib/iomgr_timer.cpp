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
#include <unordered_set>

#include <iomgr/iomgr.hpp>
#include "iomgr_timer_impl.hpp"
#include "iomgr_helper.hpp"
#include "reactor/reactor.hpp"

extern "C" {
#include <sys/timerfd.h>
#include <sys/epoll.h>
}
namespace iomgr {

// Define the static members declared in timer base class
std::mutex timer::s_pending_mutex{};
std::condition_variable timer::s_pending_cv{};
int32_t timer::s_pending_timers{0};

timer_epoll::timer_epoll(const thread_specifier& scope) : timer(scope) {
    m_common_timer_io_dev = setup_timer_fd(false, true /* wait_to_setup */);
    if (!m_common_timer_io_dev) {
        throw std::system_error(errno, std::generic_category(),
                                "Unable to create/add timer fd for non-recurring timer");
    }
    m_common_timer_io_dev->tinfo = std::make_unique< timer_info >(this);
}

timer_epoll::~timer_epoll() {
    if (!m_stopped) { stop(); }
}

void timer_epoll::stop() {
    // Remove all timers in the non-recurring timer list
    while (!m_timer_list.empty()) {
        m_timer_list.pop();
    }
    // remove_io_device with wait=true calls post_remove -> iodev->close() which closes the fd;
    // do NOT call close(iodev->fd()) afterwards or the fd is double-closed.
    if (m_common_timer_io_dev && (m_common_timer_io_dev->fd() != -1)) {
        iomanager.generic_interface()->remove_io_device(m_common_timer_io_dev, true /* wait */);
    }
    for (auto& iodev : m_recurring_timer_iodevs) {
        iomanager.generic_interface()->remove_io_device(iodev, true /* wait */);
    }
    m_stopped = true;
}

timer_handle_t timer_epoll::schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn,
                                     bool wait_to_schedule) {
    struct itimerspec tspec;
    timer_handle_t thdl;
    IODevice* raw_iodev = nullptr;

    if (recurring) {
        tspec.it_interval.tv_sec = nanos_after / 1000000000;
        tspec.it_interval.tv_nsec = nanos_after % 1000000000;

        // For a recurring timer, create a new timer fd and ask epoll to listen on them
        auto iodev = setup_timer_fd(true, wait_to_schedule);
        if (!iodev) {
            throw std::system_error(errno, std::generic_category(), "Unable to add timer fd for recurring timer");
        }
        raw_iodev = iodev.get();

        // Associate recurring timer to the fd since they have 1-1 relationship for fd
        iodev->tinfo = std::make_unique< timer_info >(nanos_after, cookie, std::move(timer_fn), this);

        PROTECTED_REGION(m_recurring_timer_iodevs.insert(iodev)); // Add to list of recurring timer fds
        thdl = make_timer_handle(this, iodev);
    } else {
        tspec.it_interval.tv_sec = 0;
        tspec.it_interval.tv_nsec = 0;

        if (m_common_timer_io_dev == nullptr) {
            LOGDFATAL("Attempt to add non-recurring timer before calling setup_common of timer");
        }
        raw_iodev = m_common_timer_io_dev.get();

        // Create a timer_info and add it to the heap.
        timer_heap_t::handle_type heap_hdl;
        LOCK_IF_GLOBAL();
        heap_hdl = m_timer_list.emplace(nanos_after, cookie, std::move(timer_fn), this);
        UNLOCK_IF_GLOBAL();
        thdl = make_timer_handle(this, heap_hdl);
    }

    tspec.it_value.tv_sec = nanos_after / 1000000000;
    tspec.it_value.tv_nsec = nanos_after % 1000000000;

    if (!raw_iodev || (timerfd_settime(raw_iodev->fd(), 0, &tspec, NULL) == -1)) {
        LOGDFATAL("Unable to set a timer using timer fd = {}, errno={}", raw_iodev->fd(), errno);
        throw std::system_error(errno, std::generic_category(), "timer fd set time failed");
    }

    return thdl;
}

void timer_epoll::cancel(timer_handle_t thandle, bool wait_to_cancel) {
    if (!thandle) return;
    std::visit(overloaded{
                   [&](cshared< IODevice >& iodev) {
                       LOGINFO("Removing recurring {} timer fd {} device ",
                               (is_thread_local() ? "per-thread" : "global"), iodev->fd());
                       if (iodev->fd() != -1) {
                           iomanager.generic_interface()->remove_io_device(iodev, wait_to_cancel);
                       }
                       PROTECTED_REGION(m_recurring_timer_iodevs.erase(iodev));
                   },
                   [&](timer_heap_t::handle_type heap_hdl) { PROTECTED_REGION(m_timer_list.erase(heap_hdl)); },
               },
               timer_state(thandle).backing);
}

void timer_epoll::on_timer_fd_notification(IODevice* iodev) {
    // Read the timer fd and see the number of completions
    uint64_t exp_count = 0;
    if ((read(iodev->fd(), &exp_count, sizeof(uint64_t)) <= 0) || (exp_count == 0)) {
        return; // Nothing is expired. TODO: Update some spurious counter
    }

    if (exp_count > 1) {
        LOGWARN("Timer fd={} expired {} times without being processed, invoking callback once", iodev->fd(), exp_count);
    }
    ((timer_epoll*)iodev->tinfo->parent_timer)->on_timer_armed(iodev, exp_count);
}

void timer_epoll::on_timer_armed(IODevice* iodev, uint64_t exp_count) {
    if (iodev == m_common_timer_io_dev.get()) {
        // This is a non-recurring timer, loop in all timers in heap and call which are expired
        LOCK_IF_GLOBAL();
        while (!m_timer_list.empty()) {
            auto time_now = std::chrono::steady_clock::now();
            auto tinfo = m_timer_list.top();
            if (tinfo.expiry_time <= time_now) {
                m_timer_list.pop();
                UNLOCK_IF_GLOBAL();
                tinfo.cb(tinfo.context, 1);
                LOCK_IF_GLOBAL();
            } else {
                break;
            }
        }
        UNLOCK_IF_GLOBAL();
    } else {
        if (!m_stop_pending) { iodev->tinfo->cb(iodev->tinfo->context, exp_count); }
    }
}

std::shared_ptr< IODevice > timer_epoll::setup_timer_fd(bool is_recurring, bool wait_to_setup) {
    // Create a timer fd
    auto fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (fd == -1) { throw ::std::system_error(errno, std::generic_category(), "timer_fd creation failed"); }

    LOGINFO("Creating {} {} timer fd {} and adding it into fd poll list",
            (is_recurring ? "recurring" : "non-recurring"), (is_thread_local() ? "per-thread" : "global"), fd);
    auto iodev = iomanager.generic_interface()->alloc_io_device(fd, EPOLLIN, 1, nullptr, m_scope, nullptr);
    iomanager.generic_interface()->add_io_device(iodev, wait_to_setup);
    if (iodev == nullptr) {
        close(fd);
        return nullptr;
    }
    return iodev;
}

} // namespace iomgr

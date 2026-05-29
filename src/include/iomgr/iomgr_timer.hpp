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
#ifndef IOMGR_IOMGR_TIMER_HPP
#define IOMGR_IOMGR_TIMER_HPP

#include <functional>
#include <chrono>
#include <set>
#include <boost/heap/binomial_heap.hpp>
#include <iomgr/iomgr_types.hpp>

namespace iomgr {
typedef std::function< void(void*) > timer_callback_t;

class timer;
struct timer_info {
    std::chrono::steady_clock::time_point expiry_time;
    timer_callback_t cb = nullptr;
    void* context = nullptr;
    timer* parent_timer = nullptr; // Parent timer this info associated to

    timer_info(timer* t) { parent_timer = t; }

    timer_info(uint64_t nanos_after, void* cookie, timer_callback_t&& timer_fn, timer* t) {
        expiry_time = std::chrono::steady_clock::now() + std::chrono::nanoseconds(nanos_after);
        cb = std::move(timer_fn);
        context = cookie;
        parent_timer = t;
    }
};

struct compare_timer {
    bool operator()(const timer_info& ti1, const timer_info& ti2) const { return ti1.expiry_time > ti2.expiry_time; }
};

class timer;

struct IODevice;

using timer_heap_t = boost::heap::binomial_heap< timer_info, boost::heap::compare< compare_timer > >;
using timer_backing_handle_t = std::variant< timer_heap_t::handle_type, shared< IODevice > >;
using timer_handle_t = std::pair< timer*, timer_backing_handle_t >;
static const timer_handle_t null_timer_handle = timer_handle_t(nullptr, shared< IODevice >(nullptr));

/**
 * @brief IOManager Timer: Class that provides timer functionality in async manner.
 *
 * IOManager Timer supports 2 classes of timers
 * a) Recurring
 * b) Non-recurring
 *
 * Each of these 2 classes supports 2 sub-classes, per thread or global. So in all there are 4 types of timers
 * possible.
 *
 * Recurring: Timer that automatically recurs and called frequent interval until cancelled. This timer is generally
 * accurate provide the entire application is not completely swamped with CPU usage. It is almost a pass-through to
 * system level timer, wherein every time a recurring timer is created an timer fd is created and added to
 * corresponding epoll set (if per thread timer, added only to that thread's epoll set, global timer gets its timer
 * fd added to all threads).
 *
 * Non-recurring: While non-recurring can technically work like recurring, where it can create timer fd everytime it
 * is created, it is expected that non-recurring will be called frequently (say for every IO to start a timer) and
 * doing this way is very expensive, since it needs to create fd add to epoll set etc (causing multiple expensive
 * system calls). Hence it is avoided by registering one common timer fd
 */
class timer {
public:
    timer(const thread_specifier& scope) { m_scope = scope; }
    virtual ~timer() = default;

    /**
     * @brief Schedule a timer to be called back. Actual working is detailed in above section
     *
     * @param nanos_after Nano seconds after which timer method needs to be called
     * @param recurring Is the timer needs to be called in recurring fashion or one time only
     * @param cookie Any cookie that needs to be passed into the timer function
     * @param timer_fn Callback to be called by the timeout routine
     * @param wait_to_schedule Wait for the schedule timer to be scheduled completely or it is done in async manner.
     *
     * @return timer_handle_t Returns a handle which it needs to use to cancel the timer. In case of recurring
     * timer, the caller needs to call cancel, failing which causes a memory leak.
     */
    virtual timer_handle_t schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn,
                                    bool wait_to_schedule = false) = 0;
    virtual void cancel(timer_handle_t thandle, bool wait_to_cancel = false) = 0;

    /* all Timers are stopped on this thread. It is called when a thread is not part of iomgr */
    virtual void stop() = 0;

    static void cancel_pending() { s_pending_scheduled_canceled.fetch_add(1, std::memory_order_relaxed); }
    static void cancel_done() { s_pending_scheduled_canceled.fetch_sub(1, std::memory_order_relaxed); }
    static void wait_for_pending() {
        // This is a low tech way of waiting for pending, but we don't want to take mtx and cv route because
        // it will be used only during iomanager stop and cancel timer is always in critical path. So we will use
        // conventional sleep
        using namespace std::chrono_literals;
        while (s_pending_scheduled_canceled.load(std::memory_order_relaxed) > 0) {
            std::this_thread::sleep_for(10ms);
        }
    }

protected:
    bool is_thread_local() const { return std::holds_alternative< IOReactor* >(m_scope); }

protected:
    std::mutex m_list_mutex;   // Mutex that protects list and set
    timer_heap_t m_timer_list; // Timer info of non-recurring timers
    thread_specifier m_scope;
    bool m_stop_pending{false};
    bool m_stopped{false};

    static std::atomic< int64_t > s_pending_scheduled_canceled;
};

class timer_epoll : public timer {
public:
    timer_epoll(const thread_specifier& scope);
    ~timer_epoll() override;

    timer_handle_t schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn,
                            bool wait_to_schedule = false) override;
    void cancel(timer_handle_t thandle, bool wait_to_cancel = false) override;

    /* all Timers are stopped on this thread. It is called when a thread is not part of iomgr */
    void stop() override;

    static void on_timer_fd_notification(IODevice* iodev);

private:
    std::shared_ptr< IODevice > setup_timer_fd(bool is_recurring, bool wait_to_setup = false);
    void on_timer_armed(IODevice* iodev);

private:
    std::shared_ptr< IODevice > m_common_timer_io_dev;                // fd_info for the common timer fd
    std::set< std::shared_ptr< IODevice > > m_recurring_timer_iodevs; // fd infos of recurring timers
};

} // namespace iomgr

#endif

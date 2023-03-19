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

struct spdk_poller;
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

struct spdk_timer_info;
typedef std::shared_ptr< spdk_timer_info > spdk_timer_ptr;

struct spdk_thread_timer_info {
    spdk_thread_timer_info(const spdk_timer_ptr& sti);
    bool call_timer_cb_once();

    uint64_t term_num = 0;
    spdk_poller* poller = nullptr;
    std::shared_ptr< spdk_timer_info > tinfo;
    io_fiber_t owner_fiber;
};

typedef std::shared_ptr< spdk_thread_timer_info > spdk_thread_timer_ptr;
struct spdk_timer_info : public timer_info {
public:
    spdk_timer_info(uint64_t nanos_after, void* cookie, timer_callback_t&& timer_fn, timer* t, bool multi_threads) :
            timer_info(nanos_after, cookie, std::move(timer_fn), t) {
        timeout_nanos = nanos_after;
        is_multi_threaded = multi_threads;
    }

    void add_thread_timer_info(const spdk_thread_timer_ptr& stt_info);
    void delete_thread_timer_info();
    spdk_thread_timer_ptr get_thread_timer_info();

    // Following fields are applicable only for SPDK Timer
    uint64_t timeout_nanos = 0;
    std::atomic< uint64_t > cur_term_num = 0; // Term # for timer (where single timer cb to be called among all threads)
    std::map< uint32_t, spdk_thread_timer_ptr > thread_timer_list;
    spdk_thread_timer_ptr single_thread_timer; // In case single thread timer
    bool is_multi_threaded = true;
    std::mutex timer_list_mtx;
};

struct compare_timer {
    bool operator()(const timer_info& ti1, const timer_info& ti2) const { return ti1.expiry_time > ti2.expiry_time; }
};

class timer;

struct IODevice;

using timer_heap_t = boost::heap::binomial_heap< timer_info, boost::heap::compare< compare_timer > >;
using timer_backing_handle_t = std::variant< timer_heap_t::handle_type, std::shared_ptr< IODevice >, spdk_timer_ptr >;
using timer_handle_t = std::pair< timer*, timer_backing_handle_t >;
static const timer_handle_t null_timer_handle = timer_handle_t(nullptr, std::shared_ptr< IODevice >(nullptr));

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

protected:
    bool is_thread_local() const { return std::holds_alternative< io_fiber_t >(m_scope); }

protected:
    std::mutex m_list_mutex;   // Mutex that protects list and set
    timer_heap_t m_timer_list; // Timer info of non-recurring timers
    thread_specifier m_scope;
    bool m_stopped = false;
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

class timer_spdk : public timer {
public:
    timer_spdk(const thread_specifier& scope);
    ~timer_spdk() override;

    timer_handle_t schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn,
                            bool wait_to_schedule = false) override;
    void cancel(timer_handle_t thandle, bool wait_to_cancel = false) override;

    /* all Timers are stopped on this thread. It is called when a thread is not part of iomgr */
    void stop() override;

private:
    static spdk_thread_timer_ptr create_register_spdk_thread_timer(const spdk_timer_ptr& stinfo);
    static void unregister_spdk_thread_timer(const spdk_thread_timer_ptr& stinfo);
    void cancel_thread_timer(const spdk_timer_ptr& st_info, bool wait_to_cancel = false) const;
    void cancel_global_timer(const spdk_timer_ptr& st_info) const;

private:
    std::unordered_set< spdk_timer_ptr > m_active_global_timer_infos;
    std::unordered_set< spdk_timer_ptr > m_active_thread_timer_infos;
};

} // namespace iomgr

#endif

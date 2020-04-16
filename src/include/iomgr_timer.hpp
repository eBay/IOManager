//
// Created by Kadayam, Hari on 2/10/20.
//

#ifndef IOMGR_IOMGR_TIMER_HPP
#define IOMGR_IOMGR_TIMER_HPP

#include <functional>
#include <chrono>
#include <set>
#include <boost/heap/binomial_heap.hpp>

namespace iomgr {
typedef std::function< void(void*) > timer_callback_t;

class timer;
struct timer_info {
    std::chrono::steady_clock::time_point expiry_time;
    timer_callback_t                      cb = nullptr;
    void*                                 context = nullptr;
    timer*                                parent_timer = nullptr; // Parent timer this info associated to

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

struct fd_info;
using timer_heap_t = boost::heap::binomial_heap< timer_info, boost::heap::compare< compare_timer > >;
using timer_handle_t = std::variant< timer_heap_t::handle_type, std::shared_ptr< fd_info > >;
static const timer_handle_t null_timer_handle = timer_handle_t(std::shared_ptr< fd_info >(nullptr));

/**
 * @brief IOManager Timer: Class that provides timer functionality in async manner.
 *
 * IOManager Timer supports 2 classes of timers
 * a) Recurring
 * b) Non-recurring
 *
 * Each of these 2 classes supports 2 sub-classes, per thread or global. So in all there are 4 types of timers possible.
 *
 * Recurring: Timer that automatically recurrs and called frequent interval until cancelled. This timer is generally
 * accurate provide the entire application is not completely swamped with CPU usage. It is almost a pass-through to
 * system level timer, wherein every time a recurring timer is created an timer fd is created and added to corresponding
 * epoll set (if per thread timer, added only to that thread's epoll set, global timer gets its timer fd added to all
 * threads).
 *
 * Non-recurring: While non-recurring can technically work like recurring, where it can create timer fd everytime it is
 * created, it is expected that non-recurring will be called frequently (say for every IO to start a timer) and doing
 * this way is very expensive, since it needs to create fd add to epoll set etc (causing multiple expensive system
 * calls). Hence it is avoided by registering one common timer fd
 */
class timer {
public:
    timer(bool is_per_thread);
    ~timer();

    /**
     * @brief Schedule a timer to be called back. Actual working is detailed in above section
     *
     * @param nanos_after Nano seconds after which timer method needs to be called
     * @param recurring Is the timer needs to be called in recurring fashion or one time only
     * @param cookie Any cookie that needs to be passed into the timer function
     * @param timer_fn Callback to be called by the timeout routine
     *
     * @return timer_handle_t Returns a handle which it needs to use to cancel the timer. In case of recurring timer,
     * the caller needs to call cancel, failing which causes a memory leak.
     */
    timer_handle_t schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn);
    void           cancel(timer_handle_t thandle);
    /* all Timers are stopped on this thread. It is called when a thread is not part of iomgr */
    void io_thread_stopped();

    static void on_timer_fd_notification(fd_info* finfo);

private:
    std::shared_ptr< fd_info > setup_timer_fd();
    void                       on_timer_armed(fd_info* finfo);

private:
    std::mutex                             m_list_mutex;           // Mutex that protects list and set
    timer_heap_t                           m_timer_list;           // Timer info of non-recurring timers
    std::set< std::shared_ptr< fd_info > > m_recurring_timer_fds;  // fd infos of recurring timers
    std::shared_ptr< fd_info >             m_common_timer_fd_info; // fd_info for the common timer fd
    bool                                   m_is_thread_local;
};

} // namespace iomgr

#endif

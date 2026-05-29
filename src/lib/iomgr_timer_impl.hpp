#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_set>

#include <boost/heap/binomial_heap.hpp>

#include <iomgr/iomgr.hpp> // timer_callback_t, timer_handle_t, timer_info fwd decl

namespace iomgr {

class timer;
struct IODevice;

struct timer_info {
    std::chrono::steady_clock::time_point expiry_time;
    timer_callback_t cb = nullptr;
    void* context = nullptr;
    timer* parent_timer = nullptr;

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

using timer_heap_t = boost::heap::binomial_heap< timer_info, boost::heap::compare< compare_timer > >;
using timer_backing_handle_t = std::variant< timer_heap_t::handle_type, shared< IODevice > >;

// Internal timer state stored behind the opaque timer_handle_t (shared_ptr<void>) pointer.
struct _timer_state {
    timer* tmr{nullptr};
    timer_backing_handle_t backing;
};

inline timer_handle_t make_timer_handle(timer* t, timer_backing_handle_t b) {
    return std::make_shared< _timer_state >(_timer_state{t, std::move(b)});
}

inline _timer_state& timer_state(const timer_handle_t& h) { return *static_cast< _timer_state* >(h.get()); }

#define LOCK_IF_GLOBAL()                                                                                               \
    if (!is_thread_local()) m_list_mutex.lock();
#define UNLOCK_IF_GLOBAL()                                                                                             \
    if (!is_thread_local()) m_list_mutex.unlock();
#define PROTECTED_REGION(op)                                                                                           \
    {                                                                                                                  \
        LOCK_IF_GLOBAL();                                                                                              \
        op;                                                                                                            \
        UNLOCK_IF_GLOBAL();                                                                                            \
    }

class timer {
public:
    timer(const thread_specifier& scope) { m_scope = scope; }
    virtual ~timer() = default;

    virtual timer_handle_t schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn,
                                    bool wait_to_schedule = false) = 0;
    virtual void cancel(timer_handle_t thandle, bool wait_to_cancel = false) = 0;
    virtual void stop() = 0;

    bool is_thread_local() const { return std::holds_alternative< IOReactor* >(m_scope); }

    static void wait_for_pending() {
        std::unique_lock< std::mutex > lg(s_pending_mutex);
        s_pending_cv.wait(lg, [] { return (s_pending_timers == 0); });
    }

protected:
    static std::mutex s_pending_mutex;
    static std::condition_variable s_pending_cv;
    static int32_t s_pending_timers;

protected:
    thread_specifier m_scope;
    std::mutex m_list_mutex;
};

class timer_epoll : public timer {
public:
    timer_epoll(const thread_specifier& scope);
    ~timer_epoll() override;

    timer_handle_t schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn,
                            bool wait_to_schedule = false) override;
    void cancel(timer_handle_t thandle, bool wait_to_cancel = false) override;
    void stop() override;

    static void on_timer_fd_notification(IODevice* iodev);
    void on_timer_armed(IODevice* iodev);

private:
    shared< IODevice > setup_timer_fd(bool recurring, bool wait_to_add);

private:
    std::shared_ptr< IODevice > m_common_timer_io_dev;
    std::set< std::shared_ptr< IODevice > > m_recurring_timer_iodevs;
    timer_heap_t m_timer_list;
    bool m_stopped{false};
    bool m_stop_pending{false};
};

} // namespace iomgr

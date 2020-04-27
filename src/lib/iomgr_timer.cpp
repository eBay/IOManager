#include "iomgr.hpp"
#include "iomgr_timer.hpp"
#include "io_thread.hpp"
extern "C" {
#include <sys/timerfd.h>
#include <sys/epoll.h>
}
namespace iomgr {

#define PROTECTED_REGION(instructions)                                                                                 \
    if (!m_is_thread_local) m_list_mutex.lock();                                                                       \
    instructions;                                                                                                      \
    if (!m_is_thread_local) m_list_mutex.unlock();

#define LOCK_IF_GLOBAL()                                                                                               \
    if (!m_is_thread_local) m_list_mutex.lock();

#define UNLOCK_IF_GLOBAL()                                                                                             \
    if (!m_is_thread_local) m_list_mutex.unlock();

template < class... Ts >
struct overloaded : Ts... {
    using Ts::operator()...;
};
template < class... Ts >
overloaded(Ts...)->overloaded< Ts... >;

timer::timer(bool is_thread_local) {
    m_is_thread_local = is_thread_local;
    m_common_timer_fd_info = setup_timer_fd();
    if (!m_common_timer_fd_info) {
        throw std::system_error(errno, std::generic_category(),
                                "Unable to create/add timer fd for non-recurring timer");
    }
    m_common_timer_fd_info->tinfo = std::make_unique< timer_info >(this);
}

timer::~timer() {
    if (!m_stopped) { stop(); }
}

void timer::stop() {
    // Remove all timers in the non-recurring timer list
    while (!m_timer_list.empty()) {
        // auto& tinfo = m_timer_list.top(); // TODO: Check if we need to make upcall that timer is cancelled
        m_timer_list.pop();
    }
    // Now close the common timer
    if (m_common_timer_fd_info && (m_common_timer_fd_info->fd != -1)) {
        iomanager.remove_fd(m_common_timer_fd_info);
        close(m_common_timer_fd_info->fd);
    }
    // Now iterate over recurring timer list and remove them
    for (auto& finfo : m_recurring_timer_fds) {
        iomanager.remove_fd(finfo);
        close(finfo->fd);
    }
    m_stopped = true;
}

timer_handle_t timer::schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn) {
    struct timespec now;
    struct itimerspec tspec;
    timer_handle_t thdl;
    fd_info* raw_finfo = nullptr;

    if (recurring) {
        tspec.it_interval.tv_sec = nanos_after / 1000000000;
        tspec.it_interval.tv_nsec = nanos_after % 1000000000;

        // For a recurring timer, create a new timer fd and ask epoll to listen on them
        auto finfo = setup_timer_fd();
        if (!finfo) {
            throw std::system_error(errno, std::generic_category(), "Unable to add timer fd for recurring timer");
        }
        raw_finfo = finfo.get();

        // Associate recurring timer to the fd since they have 1-1 relotionship for fd
        finfo->tinfo = std::make_unique< timer_info >(nanos_after, cookie, std::move(timer_fn), this);

        PROTECTED_REGION(m_recurring_timer_fds.insert(finfo)); // Add to list of recurring timer fd infos
        thdl = timer_handle_t(finfo);
    } else {
        tspec.it_interval.tv_sec = 0;
        tspec.it_interval.tv_nsec = 0;

        if (m_common_timer_fd_info == nullptr) {
            LOGDFATAL("Attempt to add non-recurring timer before calling setup_common of timer");
        }
        raw_finfo = m_common_timer_fd_info.get();

        // Create a timer_info and add it to the heap.
        PROTECTED_REGION(auto heap_hdl = m_timer_list.emplace(nanos_after, cookie, std::move(timer_fn), this));
        thdl = timer_handle_t(heap_hdl);
    }

    if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
        LOGDFATAL("Unable to get the current time, errno={}", errno);
        throw std::system_error(errno, std::generic_category(), "Unable to get cur time");
    }
    tspec.it_value.tv_sec = now.tv_sec + nanos_after / 1000000000;
    tspec.it_value.tv_nsec = now.tv_nsec + nanos_after % 1000000000;

    //    LOGTRACEMOD(iomgr, "Setting per thread timer with timeout: [sec={} nsec={}] cur_time: [sec={}, nsec={}]",
    //        tspec.it_value.tv_sec, tpsec.)

    if (!raw_finfo || (timerfd_settime(raw_finfo->fd, TFD_TIMER_ABSTIME, &tspec, NULL) == -1)) {
        LOGDFATAL("Unable to set a timer using timer fd = {}, errno={}", m_common_timer_fd_info->fd, errno);
        throw std::system_error(errno, std::generic_category(), "timer fd set time failed");
    }

    return thdl;
}

void timer::cancel(timer_handle_t thandle) {
    if (thandle == null_timer_handle) return;
    std::visit(overloaded{[&](std::shared_ptr< fd_info > info) {
                              if (info->fd != -1) {
                                  iomanager.remove_fd(info);
                                  close(info->fd);
                              }
                              PROTECTED_REGION(m_recurring_timer_fds.erase(info));
                          },
                          [&](timer_heap_t::handle_type heap_hdl) { PROTECTED_REGION(m_timer_list.erase(heap_hdl)); }},
               thandle);
}

void timer::on_timer_fd_notification(fd_info* finfo) {
    // Read the timer fd and see the number of completions
    uint64_t exp_count = 0;
    if ((read(finfo->fd, &exp_count, sizeof(uint64_t)) <= 0) || (exp_count == 0)) {
        return; // Nothing is expired. TODO: Update some spurious counter
    }

    // Call the corresponding timer that timer is armed
    finfo->tinfo->parent_timer->on_timer_armed(finfo);
}

void timer::on_timer_armed(fd_info* finfo) {
    if (finfo == m_common_timer_fd_info.get()) {
        // This is a non-recurring timer, loop in all timers in heap and call which are expired
        LOCK_IF_GLOBAL();
        while (!m_timer_list.empty()) {
            auto time_now = std::chrono::steady_clock::now();
            auto tinfo = m_timer_list.top();
            if (tinfo.expiry_time <= time_now) {
                m_timer_list.pop();
                UNLOCK_IF_GLOBAL();
                tinfo.cb(tinfo.context);
                LOCK_IF_GLOBAL();
            } else {
                break;
            }
        }
        UNLOCK_IF_GLOBAL();
    } else {
        finfo->tinfo->cb(finfo->tinfo->context);
    }
}

std::shared_ptr< fd_info > timer::setup_timer_fd() {
    // Create a timer fd
    auto fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (fd == -1) { throw ::std::system_error(errno, std::generic_category(), "timer_fd creation failed"); }

    LOGINFO("Creating timer fd {} and adding it into fd poll list", fd);
    auto finfo = std::shared_ptr< fd_info >(
        iomanager._add_fd(iomanager.default_generic_interface(), fd, nullptr, EPOLLIN, 1, nullptr, m_is_thread_local));
    if (finfo == nullptr) {
        close(fd);
        return nullptr;
    }
    return finfo;
}
} // namespace iomgr

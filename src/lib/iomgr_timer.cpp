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

timer_epoll::timer_epoll(bool is_thread_local) : timer(is_thread_local) {
    m_common_timer_io_dev = setup_timer_fd();
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
        // auto& tinfo = m_timer_list.top(); // TODO: Check if we need to make upcall that timer is cancelled
        m_timer_list.pop();
    }
    // Now close the common timer
    if (m_common_timer_io_dev && (m_common_timer_io_dev->fd() != -1)) {
        iomanager.remove_io_device(m_common_timer_io_dev);
        close(m_common_timer_io_dev->fd());
    }
    // Now iterate over recurring timer list and remove them
    for (auto& iodev : m_recurring_timer_iodevs) {
        iomanager.remove_io_device(iodev);
        close(iodev->fd());
    }
    m_stopped = true;
}

timer_handle_t timer_epoll::schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn) {
    struct timespec now;
    struct itimerspec tspec;
    timer_handle_t thdl;
    io_device_t* raw_iodev = nullptr;

    if (recurring) {
        tspec.it_interval.tv_sec = nanos_after / 1000000000;
        tspec.it_interval.tv_nsec = nanos_after % 1000000000;

        // For a recurring timer, create a new timer fd and ask epoll to listen on them
        auto iodev = setup_timer_fd();
        if (!iodev) {
            throw std::system_error(errno, std::generic_category(), "Unable to add timer fd for recurring timer");
        }
        raw_iodev = iodev.get();

        // Associate recurring timer to the fd since they have 1-1 relotionship for fd
        iodev->tinfo = std::make_unique< timer_info >(nanos_after, cookie, std::move(timer_fn), this);

        PROTECTED_REGION(m_recurring_timer_iodevs.insert(iodev)); // Add to list of recurring timer fds
        thdl = timer_handle_t(iodev);
    } else {
        tspec.it_interval.tv_sec = 0;
        tspec.it_interval.tv_nsec = 0;

        if (m_common_timer_io_dev == nullptr) {
            LOGDFATAL("Attempt to add non-recurring timer before calling setup_common of timer");
        }
        raw_iodev = m_common_timer_io_dev.get();

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

    if (!raw_iodev || (timerfd_settime(raw_iodev->fd(), TFD_TIMER_ABSTIME, &tspec, NULL) == -1)) {
        LOGDFATAL("Unable to set a timer using timer fd = {}, errno={}", raw_iodev->fd(), errno);
        throw std::system_error(errno, std::generic_category(), "timer fd set time failed");
    }

    return thdl;
}

void timer_epoll::cancel(timer_handle_t thandle) {
    if (thandle == null_timer_handle) return;
    std::visit(overloaded{[&](std::shared_ptr< io_device_t > iodev) {
                              if (iodev->fd() != -1) {
                                  iomanager.remove_io_device(iodev);
                                  close(iodev->fd());
                              }
                              PROTECTED_REGION(m_recurring_timer_iodevs.erase(iodev));
                          },
                          [&](timer_heap_t::handle_type heap_hdl) { PROTECTED_REGION(m_timer_list.erase(heap_hdl)); }},
               thandle);
}

void timer_epoll::on_timer_fd_notification(io_device_t* iodev) {
    // Read the timer fd and see the number of completions
    uint64_t exp_count = 0;
    if ((read(iodev->fd(), &exp_count, sizeof(uint64_t)) <= 0) || (exp_count == 0)) {
        return; // Nothing is expired. TODO: Update some spurious counter
    }

    // Call the corresponding timer that timer is armed
    ((timer_epoll*)iodev->tinfo->parent_timer)->on_timer_armed(iodev);
}

void timer_epoll::on_timer_armed(io_device_t* iodev) {
    if (iodev == m_common_timer_io_dev.get()) {
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
        iodev->tinfo->cb(iodev->tinfo->context);
    }
}

std::shared_ptr< io_device_t > timer_epoll::setup_timer_fd() {
    // Create a timer fd
    auto fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (fd == -1) { throw ::std::system_error(errno, std::generic_category(), "timer_fd creation failed"); }

    LOGINFO("Creating timer fd {} and adding it into fd poll list", fd);
    auto iodev = iomanager.generic_interface()->make_io_device(backing_dev_t(fd), EPOLLIN, 1, nullptr,
                                                               m_is_thread_local, nullptr);
    if (iodev == nullptr) {
        close(fd);
        return nullptr;
    }
    return iodev;
}
} // namespace iomgr

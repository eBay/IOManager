#include "iomgr.hpp"
#include "iomgr_timer.hpp"
#include "reactor.hpp"
#include <unordered_set>

extern "C" {
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <spdk/thread.h>
}
namespace iomgr {

#define PROTECTED_REGION(instructions)                                                                                 \
    if (!is_thread_local()) m_list_mutex.lock();                                                                       \
    instructions;                                                                                                      \
    if (!is_thread_local()) m_list_mutex.unlock();

#define LOCK_IF_GLOBAL()                                                                                               \
    if (!is_thread_local()) m_list_mutex.lock();

#define UNLOCK_IF_GLOBAL()                                                                                             \
    if (!is_thread_local()) m_list_mutex.unlock();

timer_epoll::timer_epoll(const thread_specifier& scope) : timer(scope) {
    m_common_timer_io_dev = setup_timer_fd(false);
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
        iomanager.generic_interface()->remove_io_device(m_common_timer_io_dev, false,
                                                        [](io_device_ptr iodev) { close(iodev->fd()); });
    }
    // Now iterate over recurring timer list and remove them
    for (auto& iodev : m_recurring_timer_iodevs) {
        iomanager.generic_interface()->remove_io_device(iodev, false, [](io_device_ptr iodev) { close(iodev->fd()); });
    }
    m_stopped = true;
}

timer_handle_t timer_epoll::schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn) {
    struct timespec now;
    struct itimerspec tspec;
    timer_handle_t thdl;
    IODevice* raw_iodev = nullptr;

    if (recurring) {
        tspec.it_interval.tv_sec = nanos_after / 1000000000;
        tspec.it_interval.tv_nsec = nanos_after % 1000000000;

        // For a recurring timer, create a new timer fd and ask epoll to listen on them
        auto iodev = setup_timer_fd(true);
        if (!iodev) {
            throw std::system_error(errno, std::generic_category(), "Unable to add timer fd for recurring timer");
        }
        raw_iodev = iodev.get();

        // Associate recurring timer to the fd since they have 1-1 relotionship for fd
        iodev->tinfo = std::make_unique< timer_info >(nanos_after, cookie, std::move(timer_fn), this);

        PROTECTED_REGION(m_recurring_timer_iodevs.insert(iodev)); // Add to list of recurring timer fds
        thdl = timer_handle_t(this, iodev);
    } else {
        tspec.it_interval.tv_sec = 0;
        tspec.it_interval.tv_nsec = 0;

        if (m_common_timer_io_dev == nullptr) {
            LOGDFATAL("Attempt to add non-recurring timer before calling setup_common of timer");
        }
        raw_iodev = m_common_timer_io_dev.get();

        // Create a timer_info and add it to the heap.
        PROTECTED_REGION(auto heap_hdl = m_timer_list.emplace(nanos_after, cookie, std::move(timer_fn), this));
        thdl = timer_handle_t(this, heap_hdl);
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
    std::visit(overloaded{
                   [&](std::shared_ptr< IODevice > iodev) {
                       LOGINFO("Removing recurring {} timer fd {} device ",
                               (is_thread_local() ? "per-thread" : "global"), iodev->fd());
                       if (iodev->fd() != -1) {
                           iomanager.generic_interface()->remove_io_device(
                               iodev, false, [](io_device_ptr iodev) { close(iodev->fd()); });
                       }
                       PROTECTED_REGION(m_recurring_timer_iodevs.erase(iodev));
                   },
                   [&](timer_heap_t::handle_type heap_hdl) { PROTECTED_REGION(m_timer_list.erase(heap_hdl)); },
                   [&](spdk_timer_info* stinfo) { assert(0); },
                   [&](spdk_thread_timer_info* stt_info) { assert(0); },
               },
               thandle.second);
}

void timer_epoll::on_timer_fd_notification(IODevice* iodev) {
    // Read the timer fd and see the number of completions
    uint64_t exp_count = 0;
    if ((read(iodev->fd(), &exp_count, sizeof(uint64_t)) <= 0) || (exp_count == 0)) {
        return; // Nothing is expired. TODO: Update some spurious counter
    }

    // Call the corresponding timer that timer is armed
    ((timer_epoll*)iodev->tinfo->parent_timer)->on_timer_armed(iodev);
}

void timer_epoll::on_timer_armed(IODevice* iodev) {
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

std::shared_ptr< IODevice > timer_epoll::setup_timer_fd(bool is_recurring) {
    // Create a timer fd
    auto fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (fd == -1) { throw ::std::system_error(errno, std::generic_category(), "timer_fd creation failed"); }

    LOGINFO("Creating {} {} timer fd {} and adding it into fd poll list",
            (is_recurring ? "recurring" : "non-recurring"), (is_thread_local() ? "per-thread" : "global"), fd);
    auto iodev =
        iomanager.generic_interface()->make_io_device(backing_dev_t(fd), EPOLLIN, 1, nullptr, m_scope, nullptr);
    if (iodev == nullptr) {
        close(fd);
        return nullptr;
    }
    return iodev;
}

/************************ Timer Spdk *****************************/
timer_spdk::timer_spdk(const thread_specifier& scope) : timer(scope) {}
timer_spdk::~timer_spdk() = default;

timer_handle_t timer_spdk::schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn) {
    timer_handle_t thdl;

    // Multi-thread timer only for global recurring timers, rest are single threaded timers
    auto stinfo =
        new spdk_timer_info(nanos_after, cookie, std::move(timer_fn), this, (recurring && !is_thread_local()));

    if (recurring && !is_thread_local()) {
        // In case of global timer, create multi-threaded version for recurring and let the timer callback choose to
        // run only one. For non-recurring, pick a random io thread and from that point onwards its single threaded
        iomanager.run_on(
            thread_regex::all_worker,
            [&stinfo, this](io_thread_addr_t taddr) {
                stinfo->add_thread_timer_info(create_register_spdk_thread_timer(stinfo));
            },
            true);
        thdl = timer_handle_t(this, stinfo);
        PROTECTED_REGION(m_active_global_timer_infos.insert(stinfo));
    } else {
        spdk_thread_timer_info* stt_info = nullptr;
        if (is_thread_local()) {
            stt_info = create_register_spdk_thread_timer(stinfo);
        } else {
            iomanager.run_on(
                thread_regex::random_worker,
                [&stt_info, &stinfo, this](io_thread_addr_t taddr) {
                    stt_info = create_register_spdk_thread_timer(stinfo);
                },
                true);
        }
        thdl = timer_handle_t(this, stt_info);
        PROTECTED_REGION(m_active_thread_timer_infos.insert(stt_info));
    }

    return thdl;
}

void timer_spdk::cancel(timer_handle_t thdl) {
    std::visit(overloaded{[&](spdk_timer_info* stinfo) {
                              cancel_global_timer(stinfo);
                              PROTECTED_REGION(m_active_global_timer_infos.erase(stinfo));
                          },
                          [&](spdk_thread_timer_info* stt_info) {
                              cancel_thread_timer(stt_info);
                              PROTECTED_REGION(m_active_thread_timer_infos.erase(stt_info));
                          },
                          [&](timer_heap_t::handle_type heap_hdl) { assert(0); },
                          [&](std::shared_ptr< IODevice > iodev) { assert(0); }},
               thdl.second);
}

void timer_spdk::stop() {
    for (auto it = m_active_global_timer_infos.begin(); it != m_active_global_timer_infos.end();) {
        cancel_global_timer(*it);
        it = m_active_global_timer_infos.erase(it);
    }

    for (auto it = m_active_thread_timer_infos.begin(); it != m_active_thread_timer_infos.end();) {
        cancel_thread_timer(*it);
        it = m_active_thread_timer_infos.erase(it);
    }
}

void timer_spdk::cancel_thread_timer(spdk_thread_timer_info* const stt_info) const {
    if (is_thread_local()) {
        unregister_spdk_thread_timer(stt_info);
    } else {
        iomanager.run_on(
            stt_info->owner_thread,
            [this, &stt_info](io_thread_addr_t taddr) { unregister_spdk_thread_timer(stt_info); }, true);
    }
    delete stt_info;
}

void timer_spdk::cancel_global_timer(spdk_timer_info* const stinfo) const {
    iomanager.run_on(
        thread_regex::all_worker,
        [&stinfo, this](io_thread_addr_t taddr) { unregister_spdk_thread_timer(stinfo->get_thread_timer_info()); },
        true);
    delete stinfo;
}

spdk_thread_timer_info* timer_spdk::create_register_spdk_thread_timer(spdk_timer_info* const stinfo) const {
    auto stt_info = new spdk_thread_timer_info(stinfo);
    stt_info->poller = spdk_poller_register(
        [](void* context) -> int {
            auto stt_info = (spdk_thread_timer_info*)context;
            stt_info->call_timer_cb_once();
            return 0;
        },
        (void*)stt_info, stinfo->timeout_nanos / 1000);

    return stt_info;
}

void timer_spdk::unregister_spdk_thread_timer(spdk_thread_timer_info* const stinfo) const {
    spdk_poller_unregister(&stinfo->poller);
}

void spdk_timer_info::add_thread_timer_info(spdk_thread_timer_info* stt_info) {
    std::unique_lock l(timer_list_mtx);
    thread_timer_list[iomanager.this_reactor()->reactor_idx()] = stt_info;
}

spdk_thread_timer_info* spdk_timer_info::get_thread_timer_info() {
    std::unique_lock l(timer_list_mtx);
    return thread_timer_list[iomanager.this_reactor()->reactor_idx()];
}

spdk_thread_timer_info::spdk_thread_timer_info(spdk_timer_info* sti) {
    tinfo = sti;
    owner_thread = iomanager.iothread_self();
}

bool spdk_thread_timer_info::call_timer_cb_once() {
    bool ret = false;
    if (!tinfo->is_multi_threaded ||
        tinfo->cur_term_num.compare_exchange_strong(term_num, term_num + 1, std::memory_order_acq_rel)) {
        tinfo->cb(tinfo->context);
        ret = true;
    }
    ++term_num;
    return ret;
}

#if 0
void timer_spdk::check_and_call_expired_timers() {
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
}
#endif

} // namespace iomgr

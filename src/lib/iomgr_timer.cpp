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
#include <iomgr/iomgr_timer.hpp>
#include "reactor.hpp"

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
        // auto& tinfo = m_timer_list.top(); // TODO: Check if we need to make upcall that timer is cancelled
        m_timer_list.pop();
    }
    // Now close the common timer
    if (m_common_timer_io_dev && (m_common_timer_io_dev->fd() != -1)) {
        iomanager.generic_interface()->remove_io_device(m_common_timer_io_dev, true /* wait */);
        close(m_common_timer_io_dev->fd());
    }
    // Now iterate over recurring timer list and remove them
    for (auto& iodev : m_recurring_timer_iodevs) {
        iomanager.generic_interface()->remove_io_device(iodev, true /* wait */);
        close(iodev->fd());
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

    tspec.it_value.tv_sec = nanos_after / 1000000000;
    tspec.it_value.tv_nsec = nanos_after % 1000000000;

    if (!raw_iodev || (timerfd_settime(raw_iodev->fd(), 0, &tspec, NULL) == -1)) {
        LOGDFATAL("Unable to set a timer using timer fd = {}, errno={}", raw_iodev->fd(), errno);
        throw std::system_error(errno, std::generic_category(), "timer fd set time failed");
    }

    return thdl;
}

void timer_epoll::cancel(timer_handle_t thandle, bool wait_to_cancel) {
    if (thandle == null_timer_handle) return;
    std::visit(overloaded{
                   [&](cshared< IODevice >& iodev) {
                       LOGINFO("Removing recurring {} timer fd {} device ",
                               (is_thread_local() ? "per-thread" : "global"), iodev->fd());
                       if (iodev->fd() != -1) {
                           auto remove_lambda = [iodev]() {
                               // We run sync version of remove_io_device, since we need to close fd only after all
                               // reactors removed the fd.
                               iomanager.generic_interface()->remove_io_device(iodev, true);
                               close(iodev->fd());
                           };

                           if (is_thread_local() || wait_to_cancel) {
                               remove_lambda();
                           } else {
                               iomanager.run_on_forget(reactor_regex::random_worker, fiber_regex::syncio_only,
                                                       remove_lambda);
                           }
                       }
                       PROTECTED_REGION(m_recurring_timer_iodevs.erase(iodev));
                   },
                   [&](timer_heap_t::handle_type heap_hdl) { PROTECTED_REGION(m_timer_list.erase(heap_hdl)); },
                   [&](spdk_timer_ptr stinfo) { assert(0); },
               },
               thandle.second);
}

void timer_epoll::on_timer_fd_notification(IODevice* iodev) {
    // Read the timer fd and see the number of completions
    uint64_t exp_count = 0;
    if ((read(iodev->fd(), &exp_count, sizeof(uint64_t)) <= 0) || (exp_count == 0)) {
        return; // Nothing is expired. TODO: Update some spurious counter
    }

    // Call the corresponding timer that timer is armed for number of times it has expired
    for (uint64_t i{0}; i < exp_count; ++i) {
        ((timer_epoll*)iodev->tinfo->parent_timer)->on_timer_armed(iodev);
    }
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

std::shared_ptr< IODevice > timer_epoll::setup_timer_fd(bool is_recurring, bool wait_to_setup) {
    // Create a timer fd
    auto fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (fd == -1) { throw ::std::system_error(errno, std::generic_category(), "timer_fd creation failed"); }

    LOGINFO("Creating {} {} timer fd {} and adding it into fd poll list",
            (is_recurring ? "recurring" : "non-recurring"), (is_thread_local() ? "per-thread" : "global"), fd);
    auto iodev =
        iomanager.generic_interface()->alloc_io_device(backing_dev_t(fd), EPOLLIN, 1, nullptr, m_scope, nullptr);
    iomanager.generic_interface()->add_io_device(iodev, wait_to_setup);
    if (iodev == nullptr) {
        close(fd);
        return nullptr;
    }
    return iodev;
}

/************************ Timer Spdk *****************************/
timer_spdk::timer_spdk(const thread_specifier& scope) : timer(scope) {}
timer_spdk::~timer_spdk() = default;

timer_handle_t timer_spdk::schedule(uint64_t nanos_after, bool recurring, void* cookie, timer_callback_t&& timer_fn,
                                    bool wait_to_schedule) {
    timer_handle_t thdl;

    // Multi-thread timer only for global recurring timers, rest are single threaded timers
    auto stinfo = std::make_shared< spdk_timer_info >(nanos_after, cookie, std::move(timer_fn), this,
                                                      (recurring && !is_thread_local()));

    if (recurring && !is_thread_local()) {
        // In case of global timer, create multi-threaded version for recurring and let the timer callback choose to
        // run only one. For non-recurring, pick a random io thread and from that point onwards its single threaded
        iomanager.run_on(wait_to_schedule, reactor_regex::all_worker,
                         [stinfo]() { stinfo->add_thread_timer_info(create_register_spdk_thread_timer(stinfo)); });

        thdl = timer_handle_t(this, stinfo);
        PROTECTED_REGION(m_active_global_timer_infos.insert(stinfo));
    } else {
        auto sched_in_thread = [stinfo]() { stinfo->single_thread_timer = create_register_spdk_thread_timer(stinfo); };

        if (is_thread_local()) {
            sched_in_thread();
        } else {
            iomanager.run_on(wait_to_schedule, reactor_regex::random_worker, sched_in_thread);
        }
        thdl = timer_handle_t(this, stinfo);
        PROTECTED_REGION(m_active_thread_timer_infos.insert(stinfo));
    }

    return thdl;
}

void timer_spdk::cancel(timer_handle_t thdl, bool wait_to_cancel) {
    std::visit(overloaded{[&](spdk_timer_ptr stinfo) {
                              if (stinfo->single_thread_timer) {
                                  cancel_thread_timer(stinfo, wait_to_cancel);
                                  PROTECTED_REGION(m_active_thread_timer_infos.erase(stinfo));
                              } else {
                                  cancel_global_timer(stinfo);
                                  PROTECTED_REGION(m_active_global_timer_infos.erase(stinfo));
                              }
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
        cancel_thread_timer(*it, true);
        it = m_active_thread_timer_infos.erase(it);
    }
}

void timer_spdk::cancel_thread_timer(const spdk_timer_ptr& st_info, bool wait_to_cancel) const {
    iomanager.run_on(wait_to_cancel, st_info->single_thread_timer->owner_fiber, [st_info]() {
        unregister_spdk_thread_timer(st_info->single_thread_timer);
        st_info->single_thread_timer = nullptr;
    });
}

void timer_spdk::cancel_global_timer(const spdk_timer_ptr& stinfo) const {
    // Reset to max to prevent a callback while cancel is triggerred. In essence, this ensures that callback is never
    // called upon timer is cancelled, ths there is no reason to wait_to_cancel.
    stinfo->cur_term_num = std::numeric_limits< uint64_t >::max();

    // Do a non-wait version of broadcast unconditionally, so that we can avoid poller deregister and listening on them
    // issue on spdk thread
    iomanager.run_on_forget(reactor_regex::all_worker, [stinfo]() {
        unregister_spdk_thread_timer(stinfo->get_thread_timer_info());
        stinfo->delete_thread_timer_info();
    });
}

spdk_thread_timer_ptr timer_spdk::create_register_spdk_thread_timer(const spdk_timer_ptr& stinfo) {
    auto stt_info = std::make_shared< spdk_thread_timer_info >(stinfo);
    stt_info->poller = spdk_poller_register(
        [](void* context) -> int {
            auto stt_info = (spdk_thread_timer_info*)context;
            stt_info->call_timer_cb_once();
            return 0;
        },
        (void*)stt_info.get(), stinfo->timeout_nanos / 1000);

    return stt_info;
}

void timer_spdk::unregister_spdk_thread_timer(const spdk_thread_timer_ptr& sttinfo) {
    LOGDEBUGMOD(iomgr, "Unregistering per thread timer={} thread_timer={} poller={}", (void*)sttinfo->tinfo.get(),
                (void*)sttinfo.get(), (void*)sttinfo->poller);
    spdk_poller_unregister(&sttinfo->poller);
}

void spdk_timer_info::add_thread_timer_info(const spdk_thread_timer_ptr& stt_info) {
    std::unique_lock l(timer_list_mtx);
    thread_timer_list[iomanager.this_reactor()->reactor_idx()] = stt_info;
}

void spdk_timer_info::delete_thread_timer_info() {
    std::unique_lock l(timer_list_mtx);
    thread_timer_list[iomanager.this_reactor()->reactor_idx()] = nullptr;
}

spdk_thread_timer_ptr spdk_timer_info::get_thread_timer_info() {
    std::unique_lock l(timer_list_mtx);
    return thread_timer_list[iomanager.this_reactor()->reactor_idx()];
}

spdk_thread_timer_info::spdk_thread_timer_info(const spdk_timer_ptr& sti) {
    tinfo = sti;
    owner_fiber = iomanager.iofiber_self();
}

bool spdk_thread_timer_info::call_timer_cb_once() {
    bool ret = false;
    auto cur_term_num = term_num++;
    if (!tinfo->is_multi_threaded ||
        tinfo->cur_term_num.compare_exchange_strong(cur_term_num, term_num, std::memory_order_acq_rel)) {
        ++(iomanager.this_thread_metrics().timer_wakeup_count);
        tinfo->cb(tinfo->context);
        ret = true;
    }
    // NOTE: Do not access this pointer or tinfo from this point, as once callback is called, it could
    // cancel the timer and thus delete this pointer.
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

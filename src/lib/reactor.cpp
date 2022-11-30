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
#include <sisl/fds/obj_allocator.hpp>
#include "include/iomgr.hpp"
#include "include/reactor_epoll.hpp"
#include "include/iomgr_config.hpp"

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {
thread_local IOReactor* IOReactor::this_reactor{nullptr};

io_thread::io_thread(IOReactor* reactor) : thread_addr(reactor->reactor_idx()), reactor(reactor) {}

IOReactor::~IOReactor() {
    if (is_io_reactor()) { stop(); }
}

void IOReactor::run(int worker_slot_num, loop_type_t ltype, const std::string& name,
                    const iodev_selector_t& iodev_selector, thread_state_notifier_t&& thread_state_notifier) {
    auto state = iomanager.get_state();
    if ((state == iomgr_state::stopping) || (state == iomgr_state::stopped)) {
        LOGINFO("Starting a new IOReactor while iomanager is stopping or stopped, not starting io loop");
        return;
    }

    this_reactor = this;
    m_poll_interval = IM_DYNAMIC_CONFIG(poll.force_wakeup_by_time_ms);
    m_user_controlled_loop = ((ltype & USER_CONTROLLED_LOOP) != 0);

    m_is_adaptive_loop = ((ltype & ADAPTIVE_LOOP) != 0);
    m_backoff_delay_min_us = IM_DYNAMIC_CONFIG(poll.backoff_delay_min_us);
    m_cur_backoff_delay_us = m_is_adaptive_loop ? m_backoff_delay_min_us : 0;

    if (!is_io_reactor()) {
        m_worker_slot_num = worker_slot_num;
        m_iodev_selector = iodev_selector;
        m_this_thread_notifier = std::move(thread_state_notifier);

        m_reactor_num = sisl::ThreadLocalContext::my_thread_num();
        m_reactor_name = name.empty() ? fmt::format("{}-{}", m_reactor_num, loop_type()) : name;
        REACTOR_LOG(INFO, base, , "IOReactor {} started of loop type={} and assigned reactor id {}", m_reactor_name,
                    loop_type(), m_reactor_num);

        init();
        if (m_keep_running) { REACTOR_LOG(INFO, base, , "IOReactor is ready to go to listen loop"); }
    }

    if (!m_user_controlled_loop && m_keep_running) {
        while (listen_once())
            ;
    }
}

void IOReactor::init() {
#if 0
    if (!iomanager.is_interface_registered()) {
        if (!wait_for_iface_register) {
            REACTOR_LOG(INFO, base, , "iomgr interfaces are not registered yet and not requested to wait, ",
                        "it will not be an IO Reactor");
            return;
        }
        REACTOR_LOG(INFO, base, ,
                    "IOManager interfaces are not registered yet, waiting for interfaces to get registered");
        iomanager.wait_for_interface_registration();
        REACTOR_LOG(TRACE, iomgr, ,
                    "All interfaces are registered to IOManager, can proceed with this thread initialization");
    }
#endif

    m_metrics = std::make_unique< IOThreadMetrics >(m_reactor_name);

    // Create a new IO lightweight thread (if need be) and add it to its list, notify everyone about the new thread
    start_io_thread(iomanager.make_io_thread(this));

    // Notify the caller registered to iomanager for it.
    iomanager.reactor_started(shared_from_this());
}

bool IOReactor::listen_once() {
    listen();
    if (m_keep_running) {
        auto& sentinel_cb = iomanager.generic_interface()->get_listen_sentinel_cb();
        if (sentinel_cb) { sentinel_cb(); }
        if (m_iomgr_sentinel_cb) { m_iomgr_sentinel_cb(); }

        bool need_backoff{false};
        for (const auto& backoff_cb : m_can_backoff_cbs) {
            if (backoff_cb == nullptr) { continue; }
            for (auto thr : m_io_threads) {
                if (!backoff_cb(thr)) {
                    need_backoff = false;
                    break;
                } else {
                    need_backoff = true;
                }
            }
        }

        if (need_backoff) {
            m_cur_backoff_delay_us = m_cur_backoff_delay_us * IM_DYNAMIC_CONFIG(poll.backoff_delay_increase_factor);
            auto max_us = IM_DYNAMIC_CONFIG(poll.backoff_delay_max_us);
            if (m_cur_backoff_delay_us > max_us) { m_cur_backoff_delay_us = max_us; }
            std::this_thread::sleep_for(std::chrono::microseconds(m_cur_backoff_delay_us));
        } else {
            m_cur_backoff_delay_us = m_backoff_delay_min_us;
        }
    }
    return m_keep_running;
}

void IOReactor::stop() {
    m_keep_running = false;

    for (auto thr : m_io_threads) {
        stop_io_thread(thr);
    }
    m_metrics.reset();

    iomanager.reactor_stopped();
}

bool IOReactor::can_add_iface(const std::shared_ptr< IOInterface >& iface) const {
    if (iface->scope() == thread_regex::all_io) { return true; }
    return is_worker() ? (iface->scope() == thread_regex::all_worker) : (iface->scope() == thread_regex::all_user);
}

void IOReactor::start_io_thread(const io_thread_t& thr) {
    m_io_threads.emplace_back(thr);
    thr->thread_addr = m_io_threads.size() - 1;

    // Initialize any thing specific to specialized reactors
    if (!m_user_controlled_loop && !reactor_specific_init_thread(thr)) {
        REACTOR_LOG(INFO, iomgr, thr->thread_addr,
                    "IOThreadContext is not started for this io thread, user_controlled_loop={}",
                    m_user_controlled_loop);
        return;
    }

    // Notify all the interfaces about new thread, which in turn will add all relevant devices to current reactor.
    uint32_t added_iface{0};
    iomanager.foreach_interface([this, thr, &added_iface](std::shared_ptr< IOInterface > iface) {
        if (can_add_iface(iface)) {
            iface->on_io_thread_start(thr);
            ++added_iface;
        } else {
            REACTOR_LOG(INFO, iomgr, thr->thread_addr, "{} with scope={} ignored to add", iface->name(),
                        iface->scope());
        }
    });
    m_io_thread_count.increment();
    REACTOR_LOG(INFO, iomgr, thr->thread_addr, "IOThreadContext started in this reactor, added {} interfaces",
                added_iface);
}

void IOReactor::stop_io_thread(const io_thread_t& thr) {
    // LOGMSG_ASSERT_EQ(m_io_threads[thr->thread_addr].get(), thr.get(), "Expected io thread {} to present in the
    // list",
    //                 *(thr.get()));
    uint32_t removed_iface{0};
    iomanager.foreach_interface([this, thr, &removed_iface](std::shared_ptr< IOInterface > iface) {
        if (can_add_iface(iface)) {
            iface->on_io_thread_stopped(thr);
            ++removed_iface;
        } else {
            REACTOR_LOG(INFO, iomgr, thr->thread_addr, "{} with scope={} ignored to remove", iface->name(),
                        iface->scope());
        }
    });
    m_io_thread_count.decrement();
    REACTOR_LOG(INFO, iomgr, thr->thread_addr, "IOThreadContext stopped in this reactor, removed {} interfaces",
                removed_iface);

    // Clear all the IO carrier specific context (epoll or spdk etc..)
    if (!m_user_controlled_loop) { reactor_specific_exit_thread(thr); }
    m_io_threads[thr->thread_addr] = nullptr;
}

int IOReactor::add_iodev(const io_device_const_ptr& iodev, const io_thread_t& thr) {
    auto ret = add_iodev_internal(iodev, thr);
    if (ret == 0) { ++m_n_iodevices; }
    return ret;
}

int IOReactor::remove_iodev(const io_device_const_ptr& iodev, const io_thread_t& thr) {
    auto ret = remove_iodev_internal(iodev, thr);
    if (ret == 0) { --m_n_iodevices; }
    return ret;
}

const io_thread_t& IOReactor::iothread_self() const { return m_io_threads[0]; };

bool IOReactor::deliver_msg(io_thread_addr_t taddr, iomgr_msg* msg, IOReactor* sender_reactor) {
    msg->m_dest_thread = taddr;
    msg->set_pending();

    // If the sender and receiver are same thread, take a shortcut to directly handle the message. Of course, this
    // will cause out-of-order delivery of messages. However, there is no good way to prevent deadlock
    if (sender_reactor == this) {
        handle_msg(msg);
        return true;
    } else {
        return put_msg(msg);
    }
}

const io_thread_t& IOReactor::msg_thread(iomgr_msg* msg) { return addr_to_thread(msg->m_dest_thread); }

void IOReactor::handle_msg(iomgr_msg* msg) {
    ++m_metrics->msg_recvd_count;

    if (msg->m_is_reply) {
        iomgr_msg::completed(msg);
        return;
    }

    // If the message is for a different module, pass it on to their handler
    if (msg->m_dest_module != iomanager.m_internal_msg_module_id) {
        auto& handler = iomanager.get_msg_module(msg->m_dest_module);
        if (!handler) {
            REACTOR_LOG(INFO, base, msg->m_dest_module,
                        "Received a msg of type={}, but no handler registered. Ignoring this msg", msg->m_type);
        } else {
            handler(msg);
        }
    } else {
        switch (msg->m_type) {
        case iomgr_msg_type::RESCHEDULE: {
            auto iodev = msg->iodevice_data();
            ++m_metrics->rescheduled_in;
            if (msg->event() & EPOLLIN) { iodev->cb(iodev.get(), iodev->cookie, EPOLLIN); }
            if (msg->event() & EPOLLOUT) { iodev->cb(iodev.get(), iodev->cookie, EPOLLOUT); }
            break;
        }

        case iomgr_msg_type::RELINQUISH_IO_THREAD:
            REACTOR_LOG(INFO, base, ,
                        "This reactor is asked to be reliquished its status as io reactor. Will exit io loop");
            stop();
            break;

        case iomgr_msg_type::DESIGNATE_IO_THREAD:
            REACTOR_LOG(INFO, base, ,
                        "This reactor is asked to be designated its status as io reactor. Will start running io loop");
            m_keep_running = true;
            break;

        case iomgr_msg_type::RUN_METHOD: {
            msg->method_data()(msg->m_dest_thread);
            break;
        }

#if 0
        case iomgr_msg_type::ADD_DEVICE: {
            add_iodev(msg->iodevice_data());
            break;
        }

        case iomgr_msg_type::REMOVE_DEVICE: {
            remove_iodev(msg->iodevice_data());
            break;
        }
#endif

        case iomgr_msg_type::UNKNOWN:
        default:
            LOGMSG_ASSERT(0, "Received a unknown msg type={}, to internal message handler. Ignoring this message",
                          msg->m_type);
            break;
        }
    }

    iomgr_msg::completed(msg);
}

bool IOReactor::is_iodev_addable(const io_device_const_ptr& iodev, const io_thread_t& thread) const {
    return (!m_iodev_selector || m_iodev_selector(iodev));
}

void IOReactor::notify_thread_state(bool is_started) {
    if (m_this_thread_notifier) { m_this_thread_notifier(is_started); }
    if (iomanager.thread_state_notifier()) { iomanager.thread_state_notifier()(is_started); }
}

const io_thread_t& IOReactor::select_thread() { return m_io_threads[m_total_op++ % m_io_threads.size()]; }

io_thread_idx_t IOReactor::default_thread_idx() const { return m_io_threads[0]->thread_idx; }

const io_thread_t& IOReactor::addr_to_thread(io_thread_addr_t addr) {
    if (addr >= m_io_threads.size()) {
        LOGMSG_ASSERT(0, "Accessing invalid thread on reactor={} thread_addr={} num_of_threads_in_reactor={}",
                      m_reactor_num, addr, m_io_threads.size());
        return m_io_threads[0];
    }
    return m_io_threads[addr];
}

poll_cb_idx_t IOReactor::register_poll_interval_cb(std::function< void(void) >&& cb) {
    m_poll_interval_cbs.emplace_back(std::move(cb));
    return static_cast< poll_cb_idx_t >(m_poll_interval_cbs.size() - 1);
}

void IOReactor::unregister_poll_interval_cb(const poll_cb_idx_t idx) {
    DEBUG_ASSERT(idx < m_poll_interval_cbs.size(), "Invalid poll interval cb idx {} to unregister", idx);
    DEBUG_ASSERT(m_poll_interval_cbs[idx] != nullptr,
                 "Poll interval cb idx {} already unregistered or never registered", idx);
    m_poll_interval_cbs[idx] = nullptr;
}

void IOReactor::add_backoff_cb(can_backoff_cb_t&& cb) { m_can_backoff_cbs.push_back(std::move(cb)); }

void IOReactor::attach_iomgr_sentinel_cb(const listen_sentinel_cb_t& cb) { m_iomgr_sentinel_cb = cb; }
void IOReactor::detach_iomgr_sentinel_cb() { m_iomgr_sentinel_cb = nullptr; }
} // namespace iomgr

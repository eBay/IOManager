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
#include <iomgr/iomgr.hpp>
#include "iomgr_msg.hpp"
#include "reactor/reactor.hpp"
#include "iomgr_config.hpp"

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {
thread_local IOReactor* IOReactor::this_reactor{nullptr};

IOReactor::IOReactor() = default;

IOReactor::~IOReactor() {
    if (is_io_reactor()) { stop(); }
}

void IOReactor::run(int worker_slot_num, loop_type_t ltype, uint32_t /*num_fibers*/, const std::string& name,
                    const iodev_selector_t& iodev_selector, thread_state_notifier_t&& thread_state_notifier) {
    auto state = iomanager.get_state();
    if ((state == iomgr_state::stopping) || (state == iomgr_state::stopped)) {
        LOGINFO("Starting a new IOReactor while iomanager is stopping or stopped, not starting io loop");
        return;
    }

    m_poll_interval = IM_DYNAMIC_CONFIG(poll.force_wakeup_by_time_ms);
    m_user_controlled_loop = ((ltype & USER_CONTROLLED_LOOP) != 0);

    m_is_adaptive_loop = ((ltype & ADAPTIVE_LOOP) != 0);
    m_backoff_delay_min_us = IM_DYNAMIC_CONFIG(poll.backoff_delay_min_us);
    m_cur_backoff_delay_us = m_is_adaptive_loop ? m_backoff_delay_min_us : 0;

    if (!is_io_reactor()) {
        this_reactor = this;
        m_worker_slot_num = worker_slot_num;
        m_iodev_selector = iodev_selector;
        m_this_thread_notifier = std::move(thread_state_notifier);

        m_reactor_num = sisl::ThreadLocalContext::my_thread_num();
        m_reactor_name = name.empty() ? fmt::format("{}-{}", m_reactor_num, loop_type()) : name;
        REACTOR_LOG(INFO, "IOReactor {} started of loop type={} and assigned reactor id {}", m_reactor_name,
                    loop_type(), m_reactor_num);

        init();
        if (m_keep_running) { REACTOR_LOG(INFO, "IOReactor is ready to go to listen loop"); }
    }

    if (!m_user_controlled_loop && m_keep_running) {
        while (listen_once())
            ;
    }
}

void IOReactor::init() {
    m_metrics = std::make_unique< IOThreadMetrics >(m_reactor_name);
    m_io_fiber_count.increment(1);

    // Do reactor specific initializations
    init_impl();

    // Notify all the interfaces about new reactor, which in turn will add all relevant devices to current reactor.
    uint32_t added_iface{0};
    iomanager.foreach_interface([this, &added_iface](cshared< IOInterface >& iface) {
        if (can_add_iface(iface)) {
            iface->on_reactor_start(this);
            ++added_iface;
        } else {
            REACTOR_LOG(INFO, "{} with scope={} ignored to add", iface->name(), iface->scope());
        }
    });
    REACTOR_LOG(INFO, "Reactor added {} interfaces", added_iface);

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
            if (backoff_cb && backoff_cb(this)) {
                need_backoff = true;
                break;
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

    uint32_t removed_iface{0};
    iomanager.foreach_interface([this, &removed_iface](cshared< IOInterface >& iface) {
        if (can_add_iface(iface)) {
            iface->on_reactor_stop(this);
            ++removed_iface;
        } else {
            REACTOR_LOG(INFO, "{} with scope={} ignored to remove", iface->name(), iface->scope());
        }
    });
    REACTOR_LOG(INFO, "Reactor stop removed {} interfaces", removed_iface);

    m_io_fiber_count.decrement(1);

    if (!m_user_controlled_loop) { stop_impl(); }

    m_metrics.reset();
    iomanager.reactor_stopped();
}

int IOReactor::add_iodev(const io_device_ptr& iodev) {
    int ret{0};
    if (is_iodev_addable(iodev)) {
        ret = add_iodev_impl(iodev);
        if (ret == 0) { ++m_n_iodevices; }
    }
    return ret;
}

int IOReactor::remove_iodev(const io_device_ptr& iodev) {
    int ret{0};
    if (is_iodev_addable(iodev)) {
        ret = remove_iodev_impl(iodev);
        if (ret == 0) { --m_n_iodevices; }
    }
    return ret;
}

////////////////////// Message Section ////////////////////////////////////////
void IOReactor::deliver_msg(iomgr_msg* msg) {
    if (iomanager.this_reactor() == this) {
        handle_msg(msg);
    } else {
        put_msg(msg);
    }
}

void IOReactor::handle_msg(iomgr_msg* msg) {
    ++m_metrics->msg_recvd_count;
    (msg->m_method)();
    if (msg->need_reply()) { msg->completed(); }
    iomgr_msg::free(msg);
}

//////////////////////////////// Device/Interface Section /////////////////////////////
bool IOReactor::can_add_iface(cshared< IOInterface >& iface) const {
    if (iface->scope() == reactor_regex::all_io) { return true; }
    return is_worker() ? (iface->scope() == reactor_regex::all_worker) : (iface->scope() == reactor_regex::all_user);
}

bool IOReactor::is_iodev_addable(const io_device_const_ptr& iodev) const {
    return (!m_iodev_selector || m_iodev_selector(iodev));
}

void IOReactor::notify_thread_state(bool is_started) {
    if (m_this_thread_notifier) { m_this_thread_notifier(is_started); }
    if (iomanager.thread_state_notifier()) { iomanager.thread_state_notifier()(is_started); }
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

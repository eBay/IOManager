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
#include <iomgr/iomgr.hpp>
#include "epoll/reactor_epoll.hpp"
#include "iomgr_config.hpp"
#include "fiber_picker.hpp"

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {
thread_local IOReactor* IOReactor::this_reactor{nullptr};

IOReactor::IOReactor() : m_this_fiber{[](IOFiber* f) {}} {}
IOReactor::~IOReactor() {
    if (is_io_reactor()) { stop(); }
}

void IOReactor::run(int worker_slot_num, loop_type_t ltype, uint32_t num_fibers, const std::string& name,
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

        init(num_fibers);
        if (m_keep_running) { REACTOR_LOG(INFO, "IOReactor is ready to go to listen loop"); }
    }

    if (!m_user_controlled_loop && m_keep_running) {
        while (listen_once())
            ;
    }
}

void IOReactor::init(uint32_t num_fibers) {
    m_metrics = std::make_unique< IOThreadMetrics >(m_reactor_name);

    // boost::fibers::use_scheduling_algorithm< iomgr::io_fiber_picker >();

    // Create all fibers
    for (uint32_t i{0}; i < num_fibers; ++i) {
        m_io_fibers.emplace_back(std::make_unique< IOFiber >(this, iomanager.m_fiber_ordinal_reserver.reserve()));
        // First fiber is always main fiber loop and we don't want to start fiber_loop there
        if (i == 0) {
            m_this_fiber.reset(m_io_fibers[0].get());
        } else {
            m_io_fibers.back()->start(bind_this(IOReactor::fiber_loop, 1));
        }
    }
    m_io_fiber_count.increment(num_fibers);

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

    m_rand_fiber_dist = std::uniform_int_distribution< size_t >(0, m_io_fibers.size() - 1);
    m_rand_sync_fiber_dist = std::uniform_int_distribution< size_t >(1, m_io_fibers.size() - 1);

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

        boost::this_fiber::yield(); // Yield to make sure other fibers gets to handle messages/completions

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
    boost::this_fiber::yield(); // Yield to make sure other fibers stop their loop

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

    for (size_t i{1}; i < m_io_fibers.size(); ++i) {
        auto msg = iomgr_msg::create([]() {}); // Send empty message for loop to come out and yield
        m_io_fibers[i]->channel.push(msg);
    }

    // Wait for all fiber loops to exit
    m_io_fiber_count.decrement(1);      // Decrement main fiber
    while (!m_io_fiber_count.testz()) { // Wait for all fiber loop to exit
        boost::this_fiber::yield();
    }

    // Clear all the IO carrier specific context (epoll or spdk etc..)
    if (!m_user_controlled_loop) { stop_impl(); }

    m_io_fibers.clear();
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

void IOReactor::fiber_loop(io_fiber_t fiber) {
    iomgr_msg* msg;
    while (fiber->channel.pop(msg) == boost::fibers::channel_op_status::success) {
        REACTOR_LOG(DEBUG, "Fiber {} picked the msg and handling it", fiber->ordinal);
        handle_msg(msg);

        if (!m_keep_running) { break; }

        // We handled a msg, so overflow msgs, it can be pushed at the tail of the fiber channel queue
        if (!fiber->m_overflow_msgs.empty()) {
            auto status = fiber->channel.try_push(fiber->m_overflow_msgs.front());
            if (status != boost::fibers::channel_op_status::success) {
                LOGMSG_ASSERT_EQ((int)status, (int)boost::fibers::channel_op_status::success,
                                 "Moving msg from overflow to fiber loop channel has failed, unexpected");
            } else {
                fiber->m_overflow_msgs.pop();
            }
        }
    }
    fiber->channel.close();
    m_io_fiber_count.decrement(1);
    boost::this_fiber::yield();
}

io_fiber_t IOReactor::iofiber_self() const { return &(*m_this_fiber); };

io_fiber_t IOReactor::pick_fiber(fiber_regex r) {
    static thread_local std::random_device s_rd{};
    static thread_local std::default_random_engine s_re{s_rd()};

    if (r == fiber_regex::main_only) {
        return m_io_fibers[0].get();
    } else if (r == fiber_regex::syncio_only) {
        return m_io_fibers[m_rand_sync_fiber_dist(s_re)].get();
    } else if (r == fiber_regex::random) {
        return m_io_fibers[m_rand_fiber_dist(s_re)].get();
    } else {
        static thread_local size_t t_next_slot{0};
        return m_io_fibers[t_next_slot++ % m_io_fibers.size()].get();
    }
}

io_fiber_t IOReactor::main_fiber() const { return m_io_fibers[0].get(); }

std::vector< io_fiber_t > IOReactor::sync_io_capable_fibers() const {
    std::vector< io_fiber_t > v;
    std::transform(m_io_fibers.begin() + 1, m_io_fibers.end(), std::back_inserter(v),
                   [](const auto& f) { return f.get(); });
    return v;
}

////////////////////// Message Section ////////////////////////////////////////
void IOReactor::deliver_msg(io_fiber_t fiber, iomgr_msg* msg) {
    msg->m_dest_fiber = fiber;

    // If the sender and receiver are same thread, take a shortcut to directly handle the message. Of course, this
    // will cause out-of-order delivery of messages. However, there is no good way to prevent deadlock
    if (iomanager.this_reactor() == this) {
        handle_msg(msg);
    } else {
        put_msg(msg);
    }
}

void IOReactor::handle_msg(iomgr_msg* msg) {
    ++m_metrics->msg_recvd_count;
    if ((msg->m_dest_fiber == nullptr) || (msg->m_dest_fiber == iofiber_self())) {
        (msg->m_method)();
        if (msg->need_reply()) { msg->completed(); }
        iomgr_msg::free(msg);
    } else {
        auto status = msg->m_dest_fiber->channel.try_push(msg);
        if (status == boost::fibers::channel_op_status::full) { msg->m_dest_fiber->m_overflow_msgs.push(msg); }
    }
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

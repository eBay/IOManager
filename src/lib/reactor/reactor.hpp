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
#pragma once

#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>
#include <sisl/fds/sparse_vector.hpp>
#include <sisl/utility/atomic_counter.hpp>
#include <sisl/utility/enum.hpp>
#include <chrono>
#include <iomgr/iomgr_types.hpp>
#include <iomgr/iomgr_timer.hpp>
#include <iomgr/fiber_lib.hpp>

struct spdk_thread;
struct spdk_bdev_desc;
struct spdk_nvmf_qpair;
struct spdk_bdev;

namespace iomgr {
#define REACTOR_LOG(level, __l, ...)                                                                                   \
    {                                                                                                                  \
        LOG##level##MOD_FMT(                                                                                           \
            iomgr, ([&](fmt::memory_buffer& buf, const char* __m, auto&&... args) -> bool {                            \
                fmt::vformat_to(fmt::appender(buf), fmt::string_view{"[{}:{}] "},                                      \
                                fmt::make_format_args(file_name(__FILE__), __LINE__));                                 \
                fmt::vformat_to(fmt::appender(buf), fmt::string_view{"[IOThread {}.{}] "},                             \
                                fmt::make_format_args(m_reactor_num, m_fiber_mgr_lib->iofiber_self_ordinal()));        \
                fmt::vformat_to(fmt::appender(buf), fmt::string_view{__m}, fmt::make_format_args(args...));            \
                return true;                                                                                           \
            }),                                                                                                        \
            __l, ##__VA_ARGS__);                                                                                       \
    }

class IOThreadMetrics : public sisl::MetricsGroup {
public:
    explicit IOThreadMetrics(const std::string& thread_name) : sisl::MetricsGroup("IOThreadMetrics", thread_name) {
        LOGINFO("Registring metrics group name = IOThreadMetrics, thread_name = {}", thread_name);

        REGISTER_GAUGE(iomgr_thread_msg_wakeup_count, "Times thread woken up on msg event");
        REGISTER_GAUGE(iomgr_thread_timer_wakeup_count, "Times thread woken up on timer event");
        REGISTER_GAUGE(iomgr_thread_io_event_wakeup_count, "Times thread woken up on io event");
        REGISTER_GAUGE(iomgr_thread_idle_wakeup_count, "Times thread woken up on idle timer");
        REGISTER_GAUGE(iomgr_thread_iodevs_on_event_count, "Count of number iodevs armed in this thread");

        REGISTER_GAUGE(iomgr_thread_total_msg_recvd, "Total message received for this thread");
        REGISTER_GAUGE(iomgr_thread_msg_iodev_busy, "Times event read/write EAGAIN for this thread");
        REGISTER_GAUGE(iomgr_thread_rescheduled_in, "Times IOs rescheduled into this thread");
        REGISTER_GAUGE(iomgr_thread_outstanding_ops, "IO ops outstanding in this thread");

        REGISTER_GAUGE(iomgr_thread_iface_io_batch_count, "Number of io batches submitted to this thread");
        REGISTER_GAUGE(iomgr_thread_iface_io_actual_count, "Number of actual ios to this thread including batch");
        REGISTER_GAUGE(iomgr_thread_drive_io_count, "Total IOs issued to driver below");
        REGISTER_GAUGE(iomgr_thread_drive_latency_avg, "Average latency of drive ios in this thread");
        REGISTER_GAUGE(iomgr_thread_io_callbacks, "Times IO callback from driver to this thread");
        REGISTER_GAUGE(iomgr_thread_aio_event_in_callbacks, "Total aio events received to this thread");

        register_me_to_farm();
        attach_gather_cb(std::bind(&IOThreadMetrics::on_gather, this));
    }

    ~IOThreadMetrics() {
        detach_gather_cb();
        deregister_me_from_farm();
    }

    void on_gather() {
        GAUGE_UPDATE(*this, iomgr_thread_msg_wakeup_count, msg_event_wakeup_count);
        GAUGE_UPDATE(*this, iomgr_thread_timer_wakeup_count, timer_wakeup_count);
        GAUGE_UPDATE(*this, iomgr_thread_io_event_wakeup_count, io_event_wakeup_count);
        GAUGE_UPDATE(*this, iomgr_thread_idle_wakeup_count, idle_wakeup_count);
        GAUGE_UPDATE(*this, iomgr_thread_iodevs_on_event_count, fds_on_event_count);

        GAUGE_UPDATE(*this, iomgr_thread_total_msg_recvd, msg_recvd_count);
        GAUGE_UPDATE(*this, iomgr_thread_msg_iodev_busy, msg_iodev_busy_count);
        GAUGE_UPDATE(*this, iomgr_thread_rescheduled_in, rescheduled_in);
        GAUGE_UPDATE(*this, iomgr_thread_outstanding_ops, outstanding_ops);

        GAUGE_UPDATE(*this, iomgr_thread_iface_io_batch_count, iface_io_batch_count);
        GAUGE_UPDATE(*this, iomgr_thread_iface_io_actual_count, iface_io_actual_count);
        GAUGE_UPDATE(*this, iomgr_thread_drive_io_count, drive_io_count);
        if (drive_io_count != 0) {
            GAUGE_UPDATE(*this, iomgr_thread_drive_latency_avg, drive_latency_sum_us / drive_io_count);
        }
        GAUGE_UPDATE(*this, iomgr_thread_io_callbacks, io_callbacks);
        GAUGE_UPDATE(*this, iomgr_thread_aio_event_in_callbacks, aio_events_in_callback);
    }

    uint64_t msg_event_wakeup_count{0};
    uint64_t timer_wakeup_count{0};
    uint64_t io_event_wakeup_count{0};
    uint64_t idle_wakeup_count{0};
    uint64_t fds_on_event_count{0};

    uint64_t msg_recvd_count{0};
    uint64_t msg_iodev_busy_count{0};
    uint64_t rescheduled_in{0};
    int64_t outstanding_ops{0};

    uint64_t iface_io_batch_count{0};
    uint64_t iface_io_actual_count{0};
    uint64_t drive_io_count{0};
    uint64_t drive_latency_sum_us{0};
    uint64_t io_callbacks{0};
    uint64_t aio_events_in_callback{0};
};

/******************* Thread Related ************************/
class IOReactor;
class IOInterface;
class DriveInterface;

/****************** Reactor related ************************/
struct iomgr_msg;
struct timer;
struct IOFiber;
class FiberManagerLib;

class IOReactor : public std::enable_shared_from_this< IOReactor > {
    friend class IOManager;
    friend class IOFiber;
    friend class SpdkDriveInterface;

public:
    static thread_local IOReactor* this_reactor;

public:
    IOReactor();
    virtual ~IOReactor();
    void run(int worker_num, loop_type_t loop_type, uint32_t num_fibers, const std::string& name = nullptr,
             const iodev_selector_t& iodev_selector = nullptr,
             thread_state_notifier_t&& thread_state_notifier = nullptr);
    void stop();

    int add_iodev(const io_device_ptr& iodev);
    int remove_iodev(const io_device_ptr& iodev);

    void deliver_msg(io_fiber_t fiber, iomgr_msg* msg);

    io_fiber_t iofiber_self() const;
    reactor_idx_t reactor_idx() const { return m_reactor_num; }
    io_fiber_t pick_fiber(fiber_regex r);
    io_fiber_t main_fiber() const;
    std::vector< io_fiber_t > sync_io_capable_fibers() const;

    // TODO: Can we find more effective way to find out if reactor is started without using atomics
    bool is_io_reactor() const { return !(m_io_fiber_count.testz()); };
    virtual bool is_tight_loop_reactor() const = 0;
    virtual bool is_worker() const { return (m_worker_slot_num != -1); }
    virtual bool is_adaptive_loop() const { return m_is_adaptive_loop; }
    virtual void set_adaptive_loop(bool is_adaptive) { m_is_adaptive_loop = is_adaptive; }
    virtual int iomgr_slot_num() const {
        assert(is_worker());
        return m_worker_slot_num;
    }

    virtual void listen() = 0;

    virtual bool is_iodev_addable(const io_device_const_ptr& iodev) const;
    virtual uint32_t get_num_iodevs() const { return m_n_iodevices; }
    virtual const char* loop_type() const = 0;

    void set_poll_interval(const int interval) { m_poll_interval = interval; }
    int get_poll_interval() const { return m_poll_interval; }
    poll_cb_idx_t register_poll_interval_cb(std::function< void(void) >&& cb);
    void unregister_poll_interval_cb(const poll_cb_idx_t idx);
    IOThreadMetrics& thread_metrics() { return *(m_metrics.get()); }
    void add_backoff_cb(can_backoff_cb_t&& cb);
    void attach_iomgr_sentinel_cb(const listen_sentinel_cb_t& cb);
    void detach_iomgr_sentinel_cb();
    virtual void handle_msg(iomgr_msg* msg);

protected:
    virtual void init_impl() = 0;
    virtual void stop_impl() = 0;
    virtual void put_msg(iomgr_msg* msg) = 0;

    virtual int add_iodev_impl(const io_device_ptr& iodev) = 0;
    virtual int remove_iodev_impl(const io_device_ptr& iodev) = 0;

    void notify_thread_state(bool is_started);

private:
    void init(uint32_t num_fibers);
    bool listen_once();
    void fiber_loop(IOFiber* fiber);
    bool can_add_iface(const std::shared_ptr< IOInterface >& iface) const;

protected:
    reactor_idx_t m_reactor_num; // Index into global system wide thread list

protected:
    std::unique_ptr< IOThreadMetrics > m_metrics;
    sisl::atomic_counter< int32_t > m_io_fiber_count{0};

    int m_worker_slot_num = -1; // Is this thread created by iomanager itself
    bool m_keep_running = true;
    bool m_user_controlled_loop = false;
    bool m_is_adaptive_loop{false};

    std::unique_ptr< timer > m_thread_timer;
    thread_state_notifier_t m_this_thread_notifier;

    std::string m_reactor_name;
    iodev_selector_t m_iodev_selector = nullptr;
    uint32_t m_n_iodevices = 0;

    int m_poll_interval{-1};
    uint64_t m_total_op = 0;

    std::vector< std::unique_ptr< IOFiber > > m_io_fibers; // List of io threads within the reactor
    std::vector< std::function< void(void) > > m_poll_interval_cbs;
    std::vector< can_backoff_cb_t > m_can_backoff_cbs;
    uint64_t m_cur_backoff_delay_us{0};
    uint64_t m_backoff_delay_min_us{0};
    listen_sentinel_cb_t m_iomgr_sentinel_cb;
    std::uniform_int_distribution< size_t > m_rand_fiber_dist;
    std::uniform_int_distribution< size_t > m_rand_sync_fiber_dist;
    std::unique_ptr< FiberManagerLib > m_fiber_mgr_lib;
};
} // namespace iomgr

namespace fmt {
template <>
struct formatter< iomgr::IOFiber > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template < typename FormatContext >
    auto format(const iomgr::IOFiber& f, FormatContext& ctx) {
        return format_to(fmt::appender(ctx.out()), "[reactor={}]", f.reactor->reactor_idx());
    }
};

} // namespace fmt

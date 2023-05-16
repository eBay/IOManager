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
#include "iomgr_types.hpp"
#include "iomgr_timer.hpp"

//#include "drive_type.hpp"

#define IOMGR_LOG_MODS iomgr, spdk, io_wd
SISL_LOGGING_DECL(IOMGR_LOG_MODS);

struct spdk_thread;
struct spdk_bdev_desc;
struct spdk_nvmf_qpair;
struct spdk_bdev;

namespace iomgr {
#define REACTOR_LOG(level, mod, thr_addr, __l, ...)                                                                    \
    {                                                                                                                  \
        LOG##level##MOD_FMT(                                                                                           \
            BOOST_PP_IF(BOOST_PP_IS_EMPTY(mod), base, mod),                                                            \
            ([&](fmt::memory_buffer& buf, const char* __m, auto&&... args) -> bool {                                   \
                fmt::vformat_to(fmt::appender(buf), fmt::string_view{"[{}:{}] "},                                      \
                                fmt::make_format_args(file_name(__FILE__), __LINE__));                                 \
                fmt::vformat_to(                                                                                       \
                    fmt::appender(buf), fmt::string_view{"[IOThread {}.{}] "},                                         \
                    fmt::make_format_args(m_reactor_num, (BOOST_PP_IF(BOOST_PP_IS_EMPTY(thr_addr), "*", thr_addr))));  \
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

struct io_thread {
    backing_thread_t thread_impl; // What type of thread it is backed by
    io_thread_idx_t thread_idx;   // Index into the io thread list. This is internal and don't decipher this
    io_thread_addr_t thread_addr; // Index within the reactor list
    IOReactor* reactor;           // Reactor this thread is currently attached to

    friend class IOManager;

    bool is_spdk_thread_impl() const { return std::holds_alternative< spdk_thread* >(thread_impl); }
    spdk_thread* spdk_thread_impl() const { return std::get< spdk_thread* >(thread_impl); }
    io_thread(IOReactor* reactor);
    io_thread() = default;
};

typedef uint64_t loop_type_t;
static constexpr loop_type_t TIGHT_LOOP = 1 << 0;     // Completely tight loop consuming 100% cpu
static constexpr loop_type_t INTERRUPT_LOOP = 1 << 1; // Interrupt drive loop using epoll or similar mechanism
static constexpr loop_type_t ADAPTIVE_LOOP = 1 << 2;  // Adaptive approach by backing off before polling upon no-load
static constexpr loop_type_t USER_CONTROLLED_LOOP = 1 << 3; // User controlled loop where iomgr will poll on-need basis

/****************** Device related *************************/
inline backing_dev_t null_backing_dev() { return backing_dev_t{std::in_place_type< spdk_bdev_desc* >, nullptr}; }

struct IODeviceThreadContext {
    virtual ~IODeviceThreadContext() = default;
};

class IODevice {
public:
    IODevice(const int pri, const thread_specifier scope);
    virtual ~IODevice() = default;

public:
    ev_callback cb{nullptr};
    std::string devname;
    std::string alias_name;
    backing_dev_t dev;
    int ev{0};
    io_thread_t creator;
    void* cookie{nullptr};
    std::unique_ptr< timer_info > tinfo;
    IOInterface* io_interface{nullptr};
    std::mutex m_ctx_init_mtx; // Mutex to protect iodev thread ctx
    sisl::sparse_vector< std::unique_ptr< IODeviceThreadContext > > m_iodev_thread_ctx;
    bool ready{false};
    std::atomic< int32_t > thread_op_pending_count{0}; // Number of add/remove of iodev to thread pending
    drive_type dtype{drive_type::unknown};

#ifdef REFCOUNTED_OPEN_DEV
    sisl::atomic_counter< int > opened_count{0};
#endif

private:
    thread_specifier thread_scope{thread_regex::all_io};
    int pri{1};

public:
    int fd() const { return std::get< int >(dev); }
    spdk_bdev_desc* bdev_desc() const;
    spdk_bdev* bdev() const;
    bool is_spdk_dev() const {
        return (std::holds_alternative< spdk_bdev_desc* >(dev) || std::holds_alternative< spdk_nvmf_qpair* >(dev));
    }
    spdk_nvmf_qpair* nvmf_qp() const;

    bool is_global() const;
    bool is_my_thread_scope() const;
    const io_thread_t& per_thread_scope() const { return std::get< io_thread_t >(thread_scope); }
    thread_regex global_scope() const { return std::get< thread_regex >(thread_scope); }

    inline int priority() const { return pri; }
    std::string dev_id() const;
    void clear();
    DriveInterface* drive_interface();
};

/****************** Reactor related ************************/
struct iomgr_msg;
struct timer;
class IOReactor : public std::enable_shared_from_this< IOReactor > {
    friend class IOManager;
    friend class SpdkDriveInterface;

public:
    static thread_local IOReactor* this_reactor;

public:
    virtual ~IOReactor();
    virtual void run(int worker_num, loop_type_t loop_type, const std::string& name = nullptr,
                     const iodev_selector_t& iodev_selector = nullptr,
                     thread_state_notifier_t&& thread_state_notifier = nullptr);
    bool is_io_reactor() const { return !(m_io_thread_count.testz()); };
    bool deliver_msg(io_thread_addr_t taddr, iomgr_msg* msg, IOReactor* sender_reactor);

    virtual bool is_tight_loop_reactor() const = 0;
    virtual bool is_worker() const { return (m_worker_slot_num != -1); }
    virtual bool is_adaptive_loop() const { return m_is_adaptive_loop; }
    virtual void set_adaptive_loop(bool is_adaptive) { m_is_adaptive_loop = is_adaptive; }
    virtual int iomgr_slot_num() const {
        assert(is_worker());
        return m_worker_slot_num;
    }
    virtual const io_thread_t& iothread_self() const;
    virtual reactor_idx_t reactor_idx() const { return m_reactor_num; }
    virtual bool listen_once();
    virtual void listen() = 0;

    void start_io_thread(const io_thread_t& thr);
    void stop_io_thread(const io_thread_t& thr);

    const io_thread_t& addr_to_thread(io_thread_addr_t addr);
    int add_iodev(const io_device_const_ptr& iodev, const io_thread_t& thr);
    int remove_iodev(const io_device_const_ptr& iodev, const io_thread_t& thr);

    const std::vector< io_thread_t >& io_threads() const { return m_io_threads; }

    virtual bool put_msg(iomgr_msg* msg) = 0;
    virtual void init();
    virtual void stop();
    virtual bool is_iodev_addable(const io_device_const_ptr& iodev, const io_thread_t& thread) const;
    virtual uint32_t get_num_iodevs() const { return m_n_iodevices; }
    virtual void handle_msg(iomgr_msg* msg);
    virtual const char* loop_type() const = 0;
    const io_thread_t& select_thread();
    io_thread_idx_t default_thread_idx() const;
    void set_poll_interval(const int interval) { m_poll_interval = interval; }
    int get_poll_interval() const { return m_poll_interval; }
    poll_cb_idx_t register_poll_interval_cb(std::function< void(void) >&& cb);
    void unregister_poll_interval_cb(const poll_cb_idx_t idx);
    IOThreadMetrics& thread_metrics() { return *(m_metrics.get()); }
    void add_backoff_cb(can_backoff_cb_t&& cb);
    void attach_iomgr_sentinel_cb(const listen_sentinel_cb_t& cb);
    void detach_iomgr_sentinel_cb();

protected:
    virtual bool reactor_specific_init_thread(const io_thread_t& thr) = 0;
    virtual void reactor_specific_exit_thread(const io_thread_t& thr) = 0;
    virtual int add_iodev_internal(const io_device_const_ptr& iodev, const io_thread_t& thr) = 0;
    virtual int remove_iodev_internal(const io_device_const_ptr& iodev, const io_thread_t& thr) = 0;

    void notify_thread_state(bool is_started);
    // const io_thread_t& sthread_from_addr(io_thread_addr_t addr);

private:
    const io_thread_t& msg_thread(iomgr_msg* msg);
    bool can_add_iface(const std::shared_ptr< IOInterface >& iface) const;

protected:
    reactor_idx_t m_reactor_num; // Index into global system wide thread list

protected:
    std::unique_ptr< IOThreadMetrics > m_metrics;
    sisl::atomic_counter< int32_t > m_io_thread_count = 0;
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

    std::vector< io_thread_t > m_io_threads; // List of io threads within the reactor
    std::vector< std::function< void(void) > > m_poll_interval_cbs;
    std::vector< can_backoff_cb_t > m_can_backoff_cbs;
    uint64_t m_cur_backoff_delay_us{0};
    uint64_t m_backoff_delay_min_us{0};
    listen_sentinel_cb_t m_iomgr_sentinel_cb;
};
} // namespace iomgr

namespace fmt {
template <>
struct formatter< iomgr::io_thread > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template < typename FormatContext >
    auto format(const iomgr::io_thread& t, FormatContext& ctx) {
        if (std::holds_alternative< spdk_thread* >(t.thread_impl)) {
            return format_to(fmt::appender(ctx.out()), "[addr={} idx={} reactor={}]",
                             (void*)std::get< spdk_thread* >(t.thread_impl), t.thread_idx, t.reactor->reactor_idx());
        } else {
            return format_to(fmt::appender(ctx.out()), "[addr={} idx={} reactor={}]",
                             std::get< iomgr::reactor_idx_t >(t.thread_impl), t.thread_idx, t.reactor->reactor_idx());
        }
    }
};
} // namespace fmt

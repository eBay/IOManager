/**
 * Copyright eBay Corporation 2018
 */

#pragma once

#include <sds_logging/logging.h>
#include <metrics/metrics.hpp>
#include <fds/sparse_vector.hpp>
#include <utility/atomic_counter.hpp>
#include <utility/enum.hpp>
#include <chrono>

SDS_LOGGING_DECL(iomgr);

struct spdk_thread;
struct spdk_bdev_desc;
struct spdk_nvmf_qpair;
struct spdk_bdev;

namespace iomgr {
#define REACTOR_LOG(level, mod, thr_addr, __l, ...)                                                                    \
    {                                                                                                                  \
        LOG##level##MOD_FMT(BOOST_PP_IF(BOOST_PP_IS_EMPTY(mod), base, mod),                                            \
                            ([&](fmt::memory_buffer& buf, const char* __m, auto&&... args) {                           \
                                fmt::format_to(buf, "[{}:{}] ", file_name(__FILE__), __LINE__);                        \
                                fmt::format_to(buf, "[IOThread {}.{}] ", m_reactor_num,                                \
                                               (BOOST_PP_IF(BOOST_PP_IS_EMPTY(thr_addr), "*", thr_addr)));             \
                                fmt::format_to(buf, __m, args...);                                                     \
                            }),                                                                                        \
                            __l, ##__VA_ARGS__);                                                                       \
    }

using reactor_idx_t = uint32_t;
typedef std::function< void(bool) > thread_state_notifier_t;

class IOThreadMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit IOThreadMetrics(const std::string& thread_name) :
            sisl::MetricsGroupWrapper("IOThreadMetrics", thread_name) {
        LOGINFO("Registring metrics group name = IOThreadMetrics, thread_name = {}", thread_name);

        REGISTER_GAUGE(iomgr_thread_io_count, "IO Manager per thread IO count");
        REGISTER_GAUGE(iomgr_thread_total_msg_recvd, "Total message received for this thread");
        REGISTER_GAUGE(iomgr_thread_rescheduled_in, "Count of times IOs rescheduled into this thread");
        REGISTER_GAUGE(iomgr_thread_outstanding_ops, "Count of IO ops outstanding in this thread");
        REGISTER_GAUGE(iomgr_thread_outstanding_msgs, "Count of msgs outstanding in this thread");

        register_me_to_farm();

        attach_gather_cb(std::bind(&IOThreadMetrics::on_gather, this));
    }

    ~IOThreadMetrics() {
        detach_gather_cb();
        deregister_me_from_farm();
    }

    void on_gather() {
        GAUGE_UPDATE(*this, iomgr_thread_io_count, io_count);
        GAUGE_UPDATE(*this, iomgr_thread_total_msg_recvd, msg_recvd_count);
        GAUGE_UPDATE(*this, iomgr_thread_rescheduled_in, rescheduled_in);
        GAUGE_UPDATE(*this, iomgr_thread_outstanding_ops, outstanding_ops);
        GAUGE_UPDATE(*this, iomgr_thread_outstanding_msgs, outstanding_msgs);
    }

    uint64_t io_count = 0;
    uint64_t msg_recvd_count = 0;
    uint64_t rescheduled_in = 0;
    int64_t outstanding_ops = 0;
    int64_t outstanding_msgs = 0;
};

// typedef std::function< bool(std::shared_ptr< fd_info >) > fd_selector_t;

/******************* Thread Related ************************/
using backing_thread_t = std::variant< reactor_idx_t, spdk_thread* >;
using io_thread_idx_t = uint32_t;
using io_thread_addr_t = uint32_t;
class IOReactor;

struct io_thread {
    backing_thread_t thread_impl; // What type of thread it is backed by
    io_thread_idx_t thread_idx;   // Index into the io thread list. This is internal and don't decipher this
    io_thread_addr_t thread_addr; // Index within the reactor list
    IOReactor* reactor;           // Reactor this thread is currently attached to

    friend class IOManager;
    std::unique_ptr< IOThreadMetrics > m_metrics;

    spdk_thread* spdk_thread_impl() const { return std::get< spdk_thread* >(thread_impl); }
    io_thread(IOReactor* reactor);
    io_thread() = default;
};
using io_thread_t = std::shared_ptr< io_thread >;

#if 0
enum class thread_regex {
    least_busy_io,         // Represents least busy io (could be iloop or tloop thread)
    all_io,                // Represents all io threads
    least_busy_tloop,      // Represents one tight loop thread which least busy at present
    all_tloop,             // Represents all tight loop threads
    least_busy_iloop,      // Represents one interrupt/epoll loop thread which least busy at present
    all_iloop,             // Represents all interrupt loop threads
    all_iomgr_created_io,  // Represents all IO threads created by iomgr (could be iloop or tloop)
    all_user_created_io,   // Represents all user created threads (could be iloop or tloop)
    rand_iomgr_created_io, // Represents any one random IO thread created by iomgr
};
#endif

ENUM(thread_regex, uint8_t,
     all_io,            // Represents all io threads
     least_busy_io,     // Represents least busy io thread (including worker + user)
     all_worker,        // Represents all worker io threads (either tloop or iloop)
     least_busy_worker, // Represents least busy worker io thread
     random_worker,     // Represents a random worker io thread
     all_user,          // Represents all user created io threads
     least_busy_user    // Represents least busy user io thread
);
using thread_specifier = std::variant< thread_regex, io_thread_t >;

/****************** Device related *************************/
class IOInterface;
struct IODevice;
struct timer_info;
using ev_callback = std::function< void(IODevice* iodev, void* cookie, int events) >;
using backing_dev_t = std::variant< int, spdk_bdev_desc*, spdk_nvmf_qpair* >;

struct IODevice {
    ev_callback cb = nullptr;
    std::string devname;
    backing_dev_t dev;
    int ev = 0;
    thread_specifier thread_scope = thread_regex::all_io;
    int pri = 1;
    void* cookie = nullptr;
    std::unique_ptr< timer_info > tinfo;
    IOInterface* io_interface = nullptr;
    sisl::sparse_vector< void* > m_thread_local_ctx;
    bool ready = false;
    std::atomic< int32_t > thread_op_pending_count = 0; // Number of add/remove of iodev to thread pending

    IODevice();
    ~IODevice() = default;

    int fd() { return std::get< int >(dev); }
    spdk_bdev_desc* bdev_desc();
    spdk_bdev* bdev();
    bool is_spdk_dev() const {
        return (std::holds_alternative< spdk_bdev_desc* >(dev) || std::holds_alternative< spdk_nvmf_qpair* >(dev));
    }
    spdk_nvmf_qpair* nvmf_qp() const;

    bool is_global() const;
    bool is_my_thread_scope() const;
    const io_thread_t& per_thread_scope() const { return std::get< io_thread_t >(thread_scope); }
    thread_regex global_scope() { return std::get< thread_regex >(thread_scope); }

    std::string dev_id();
    void clear();
    /*bool is_initialized() const {
        return (valid_for_nthreads.load(std::memory_order_acquire) == added_to_threads.load(std::memory_order_acquire));
    }*/
};
using io_device_ptr = std::shared_ptr< IODevice >;
using iodev_selector_t = std::function< bool(const io_device_ptr&) >;

/****************** Reactor related ************************/
struct iomgr_msg;
struct timer;
class IOReactor : public std::enable_shared_from_this< IOReactor > {
    friend class IOManager;

public:
    virtual ~IOReactor();
    virtual void run(int iomgr_slot_num, const iodev_selector_t& iodev_selector = nullptr,
                     const thread_state_notifier_t& thread_state_notifier = nullptr);
    bool is_io_reactor() const { return !(m_io_thread_count.testz()); };
    bool deliver_msg(io_thread_addr_t taddr, iomgr_msg* msg, IOReactor* sender_reactor);

    virtual bool is_tight_loop_reactor() const = 0;
    virtual bool is_worker() const { return (m_worker_slot_num != -1); }
    virtual int iomgr_slot_num() const {
        assert(is_worker());
        return m_worker_slot_num;
    }
    virtual const io_thread_t& iothread_self() const;
    virtual reactor_idx_t reactor_idx() const { return m_reactor_num; }
    virtual void listen() = 0;

    void start_io_thread(const io_thread_t& thr);
    void stop_io_thread(const io_thread_t& thr);

    const io_thread_t& addr_to_thread(io_thread_addr_t addr);
    int add_iodev_to_thread(const io_device_ptr& iodev, const io_thread_t& thr);
    int remove_iodev_from_thread(const io_device_ptr& iodev, const io_thread_t& thr);

    const std::vector< io_thread_t >& io_threads() const { return m_io_threads; }

    virtual bool put_msg(iomgr_msg* msg) = 0;
    virtual void init();
    virtual void stop();
    virtual bool is_iodev_addable(const io_device_ptr& iodev, const io_thread_t& thread) const;
    virtual uint32_t get_num_iodevs() const { return m_n_iodevices; }
    virtual void handle_msg(iomgr_msg* msg);
    virtual const char* loop_type() const = 0;
    const io_thread_t& select_thread();
    void start_interface(IOInterface* iface);

protected:
    virtual bool reactor_specific_init_thread(const io_thread_t& thr) = 0;
    virtual void reactor_specific_exit_thread(const io_thread_t& thr) = 0;
    virtual int _add_iodev_to_thread(const io_device_ptr& iodev, const io_thread_t& thr) = 0;
    virtual int _remove_iodev_from_thread(const io_device_ptr& iodev, const io_thread_t& thr) = 0;

    void notify_thread_state(bool is_started);
    // const io_thread_t& sthread_from_addr(io_thread_addr_t addr);

private:
    const io_thread_t& msg_thread(iomgr_msg* msg);
    bool can_add_iface(const std::shared_ptr< IOInterface >& iface, const io_thread_t& thr);

protected:
    reactor_idx_t m_reactor_num; // Index into global system wide thread list

protected:
    sisl::atomic_counter< int32_t > m_io_thread_count = 0;
    int m_worker_slot_num = -1; // Is this thread created by iomanager itself
    bool m_keep_running = true;

    std::unique_ptr< timer > m_thread_timer;
    thread_state_notifier_t m_this_thread_notifier;

    iodev_selector_t m_iodev_selector = nullptr;
    uint32_t m_n_iodevices = 0;

    std::vector< io_thread_t > m_io_threads; // List of io threads within the reactor
    uint64_t m_total_op = 0;
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
            return format_to(ctx.out(), "[addr={} idx={} reactor={}]", (void*)std::get< spdk_thread* >(t.thread_impl),
                             t.thread_idx, t.reactor->reactor_idx());
        } else {
            return format_to(ctx.out(), "[addr={} idx={} reactor={}]", std::get< iomgr::reactor_idx_t >(t.thread_impl),
                             t.thread_idx, t.reactor->reactor_idx());
        }
    }
};
} // namespace fmt
/**
 * Copyright eBay Corporation 2018
 */

#pragma once

extern "C" {
#include <event.h>
#include <sys/time.h>
}
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <map>
#include <memory>
#include <vector>
#include <utility/thread_buffer.hpp>
#include <utility/atomic_counter.hpp>
#include <fds/sparse_vector.hpp>
#include <fds/malloc_helper.hpp>

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif
#include <folly/Synchronized.h>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

#include "iomgr_msg.hpp"
#include "reactor.hpp"
#include "iomgr_timer.hpp"
#include "io_interface.hpp"
#include "drive_interface.hpp"
#include <functional>
#include <fds/utils.hpp>
#include <fds/id_reserver.hpp>
#include <utility/enum.hpp>
#include <sds_logging/logging.h>
#include <semver/semver200.h>

struct spdk_bdev_desc;
struct spdk_bdev;
struct spdk_nvmf_qpair;
struct spdk_mempool;
struct rte_mempool;

namespace iomgr {

struct timer_info;

// TODO: Make this part of an enum, to force add count upon adding new inbuilt io interface.
static constexpr int inbuilt_interface_count = 1;

class DriveInterface;

ENUM(iomgr_state, uint16_t,
     stopped,        // Stopped - this is the initial state
     interface_init, // Interface initialization is ongoing.
     reactor_init,   // All worker reactors are being initialized
     sys_init,       // Any system wide init, say timer, spdk bdev initialization etc..
     running,        // Active, ready to take traffic
     stopping);

template < class... Ts >
struct overloaded : Ts... {
    using Ts::operator()...;
};
template < class... Ts >
overloaded(Ts...) -> overloaded< Ts... >;

using msg_handler_t = std::function< void(iomgr_msg*) >;
using interface_adder_t = std::function< void(void) >;
using reactor_info_t = std::pair< std::thread, std::shared_ptr< IOReactor > >;
typedef void (*spdk_msg_signature_t)(void*);

class IOManagerMetrics : public sisl::MetricsGroup {
public:
    IOManagerMetrics() : sisl::MetricsGroup{"IOManager", "Singelton"} {
#ifdef _PRERELEASE
        REGISTER_COUNTER(iomem_retained, "IO Memory maintained", sisl::_publish_as::publish_as_gauge);
#endif
        register_me_to_farm();
    }
    IOManagerMetrics(const IOManagerMetrics&) = delete;
    IOManagerMetrics(IOManagerMetrics&&) noexcept = delete;
    IOManagerMetrics& operator=(const IOManagerMetrics&) = delete;
    IOManagerMetrics& operator=(IOManagerMetrics&&) noexcept = delete;
};

class IOMempoolMetrics : public sisl::MetricsGroup {
public:
    IOMempoolMetrics(const std::string& pool_name, const struct spdk_mempool* mp);
    ~IOMempoolMetrics() {
        detach_gather_cb();
        deregister_me_from_farm();
    }

    void on_gather();

private:
    const struct spdk_mempool* m_mp;
};

struct synchronized_async_method_ctx {
public:
    std::mutex m;
    std::condition_variable cv;
    int outstanding_count{0};
    void* custom_ctx{nullptr};

    ~synchronized_async_method_ctx() {
        DEBUG_ASSERT_EQ(outstanding_count, 0, "Expecting no outstanding ref of method");
    }

private:
    static void done(void* arg, [[maybe_unused]] int rc) {
        synchronized_async_method_ctx* pmctx = static_cast< synchronized_async_method_ctx* >(arg);
        {
            std::unique_lock< std::mutex > lk{pmctx->m};
            --pmctx->outstanding_count;
        }
        pmctx->cv.notify_one();
    }

public:
    auto get_done_cb() {
        std::unique_lock< std::mutex > lk{m};
        ++outstanding_count;
        return (synchronized_async_method_ctx::done);
    }
};

class IOManager {
public:
    friend class IOReactor;
    friend class IOReactorEPoll;
    friend class IOInterface;

    static IOManager& instance() {
        static IOManager inst;
        return inst;
    }

    // TODO: Make this a dynamic config (albeit non-hotswap)
    static constexpr uint32_t max_msg_modules = 64;
    static constexpr uint32_t max_io_threads = 1024; // Keep in mind increasing this cause increased mem footprint

    /********* Start/Stop Control Related Operations ********/

    /**
     * @brief Start the IOManager. This is expected to be among the first call while application is started to enable
     * for it to do IO. Without this start, any other iomanager call would fail.
     *
     * @param num_threads Total number of worker reactors to start with. Expected to be > 0
     * @param is_spdk Is the IOManager to be started in spdk mode or not. If set to true, all worker reactors are
     * automatically started as spdk worker reactors.
     * @param notifier [OPTONAL] A callback every time a new reactor is started or stopped. This will be called from the
     * reactor thread which is starting or stopping.
     * @param iface_adder [OPTIONAL] Callback to add interface by the caller during iomanager start. If null, then
     * iomanager will add all the default interfaces essential to do the IO.
     */
    void start(size_t num_threads, bool is_spdk = false, const thread_state_notifier_t& notifier = nullptr,
               const interface_adder_t& iface_adder = nullptr);

    /**
     * @brief Stop the IOManager. It is expected to the last call after all IOs are completed and before application
     * shutdown. If an application does a start, but goes down without stop call, then resource leaks and race
     * condition problems could happen.
     */
    void stop();

    /**
     * @brief Get the IOManager version
     */
    version::semver200_version get_version() const;

    /**
     * @brief A way to start the User Reactor and run an IO Loop. This method makes the current thread run a loop
     * and thus will return only after the loop is exited
     *
     * @param is_tloop_reactor Is the loop it needs to run a tight loop or interrupt based loop (spdk vs epoll)
     * @param iodev_selector [OPTIONAL] A selector callback which will be called when an iodevice is added to the
     * reactor. Consumer of this callback can return true to allow the device to be added or false if this reactor
     * needs to ignore this device.
     * @param addln_notifier Callback which notifies after reactor is ready or shutting down (with true or false)
     * parameter. This is per reactor override of the same callback as iomanager start.
     */
    void run_io_loop(bool is_tloop_reactor, const iodev_selector_t& iodev_selector = nullptr,
                     const thread_state_notifier_t& addln_notifier = nullptr) {
        _run_io_loop(-1, is_tloop_reactor, iodev_selector, addln_notifier);
    }

    /**
     * @brief Convert the current thread to new user reactor
     *
     * @param is_tloop_reactor Is the loop it needs to run a tight loop or interrupt based loop (spdk vs epoll)
     * @param start_loop Should the API create a IO loop or is user themselves running an io loop.
     * @param iodev_selector [OPTIONAL] A selector callback which will be called when an iodevice is added to the
     * reactor. Consumer of this callback can return true to allow the device to be added or false if this reactor
     * needs to ignore this device.
     * @param addln_notifier  Callback which notifies after reactor is ready or shutting down (with true or false)
     * parameter. This is per reactor override of the same callback as iomanager start.
     */
    void become_user_reactor(bool is_tloop_reactor = true, bool user_controlled_loop = true,
                             const iodev_selector_t& iodev_selector = nullptr,
                             const thread_state_notifier_t& addln_notifier = nullptr);

    /**
     * @brief Stop the IO Loop and cease to being a reactor for the current thread. The current thread can choose
     * to exit much later, but once called, it will stop being a reactor and iomanager will stop tracking this thread.
     */
    void stop_io_loop();

    /********* Interface/Device Related Operations ********/

    /**
     * @brief Add a new IOInterface to the iomanager. All iodevice added to IOManager have to be part of some interface.
     * By default iomanager automatically creates genericinterface and driveinterfaces. Any additional interface can
     * be added through this API.
     *
     * @param iface Shared pointer to the IOInterface to be added
     * @param iface_scope [OPTIONAL] Scope of which reactors these interface is to be added. By default it will be
     * added to all IO reactors. While it can accept any thread_regex, really it is not practical to use
     * thread_regex::random_user or thread_regex::any_worker.
     */
    void add_interface(std::shared_ptr< IOInterface > iface, thread_regex iface_scope = thread_regex::all_io);
    void add_drive_interface(std::shared_ptr< DriveInterface > iface, bool is_default,
                             thread_regex iface_scope = thread_regex::all_io);

    /***
     * @brief Remove the IOInterface from the iomanager. Once removed, it will remove all the devices added to that
     * interface and cleanup their resources.
     *
     * @param iface: Shared pointer to the IOInterface to be removed.
     */
    void remove_interface(const std::shared_ptr< IOInterface >& iface);

    /**
     * @brief Reschedule the IO to a different device. This is used for SCST interfaces and given that it could be
     * depreacted, this API is not used actively anymore. One can do this with using run_on() APIs
     *
     * @param iodev
     * @param event
     */
    void device_reschedule(const io_device_ptr& iodev, int event);

    /**
     * @brief Run a method on one or more reactors and optionally wait for that method to complete.
     *
     * @param r One of the thread_regex indicating which all reactors this method has to run.
     * @param fn
     * @param wait_for_completion
     * @return int
     */
    int run_on(thread_regex r, const auto& fn, bool wait_for_completion = false) {
        int sent_to = 0;
        if (wait_for_completion) {
            sync_iomgr_msg smsg(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn);
            sent_to = multicast_msg_and_wait(r, smsg);
            LOGDEBUGMOD(iomgr, "Run method sync msg completion done"); // TODO: Remove this line
        } else {
            sent_to = multicast_msg(r, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn));
        }
        return sent_to;
    }

    // Direct run_on method for spdk without any msg creation
    int run_on(const io_thread_t& thread, spdk_msg_signature_t fn, void* context);

    int run_on(const io_thread_t& thread, const auto& fn, bool wait_for_completion = false) {
        bool sent = false;
        if (wait_for_completion) {
            sync_iomgr_msg smsg(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn);
            sent = send_msg_and_wait(thread, smsg);
        } else {
            sent = send_msg(thread, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn));
        }
        return ((int)sent);
    }

    void run_async_method_synchronized(thread_regex r, const auto& fn) {
        static synchronized_async_method_ctx mctx;
        int executed_on = run_on(
            r,
            [&fn]([[maybe_unused]] auto taddr) {
                fn(mctx);
                {
                    std::unique_lock< std::mutex > lk{mctx.m};
                    --mctx.outstanding_count;
                }
                mctx.cv.notify_one();
            },
            false);

        {
            std::unique_lock< std::mutex > lk{mctx.m};
            mctx.outstanding_count += executed_on;
            mctx.cv.wait(lk, [] { return (mctx.outstanding_count == 0); });
        }
    }

    /********* Access related methods ***********/
    const io_thread_t& iothread_self() const;
    IOReactor* this_reactor() const;

    DriveInterface* default_drive_interface() { return m_default_drive_iface.get(); }
    GenericIOInterface* generic_interface() { return m_default_general_iface.get(); }
    bool am_i_io_reactor() const {
        auto r = this_reactor();
        return r && r->is_io_reactor();
    }

    bool am_i_tight_loop_reactor() const {
        auto r = this_reactor();
        return r && r->is_tight_loop_reactor();
    }

    bool am_i_worker_reactor() const {
        auto r = this_reactor();
        return r && r->is_worker();
    }
    IOManagerMetrics& metrics() { return m_iomgr_metrics; }

    /********* State Machine Related Operations ********/
    bool is_ready() const { return (get_state() == iomgr_state::running); }
    // bool is_interface_registered() const { return ((uint16_t)get_state() > (uint16_t)iomgr_state::interface_init); }
    void wait_to_be_ready() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_cv.wait(lck, [this] { return (get_state() == iomgr_state::running); });
    }

    void wait_to_be_stopped() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        if (get_state() != iomgr_state::stopped) {
            m_cv.wait(lck, [this] { return (get_state() == iomgr_state::stopped); });
        }
    }

    /*void wait_for_interface_registration() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_cv.wait(lck, [this] { return is_interface_registered(); });
    }*/

    void wait_for_state(iomgr_state expected_state) {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        if (get_state() != expected_state) {
            m_cv.wait(lck, [&] { return (get_state() == expected_state); });
        }
    }

    void ensure_running() {
        if (get_state() != iomgr_state::running) {
            LOGINFO("IOManager is not running, will wait for it to be ready");
            wait_to_be_ready();
            LOGINFO("IOManager is ready now");
        }
    }
    thread_state_notifier_t& thread_state_notifier() { return m_common_thread_state_notifier; }

    /******** IO Thread related infra ********/
    io_thread_t make_io_thread(IOReactor* reactor);

    /******** Message related infra ********/
    bool send_msg(const io_thread_t& thread, iomgr_msg* msg);
    bool send_msg_and_wait(const io_thread_t& thread, sync_iomgr_msg& smsg);
    int multicast_msg(thread_regex r, iomgr_msg* msg);
    int multicast_msg_and_wait(thread_regex r, sync_iomgr_msg& smsg);

    msg_module_id_t register_msg_module(const msg_handler_t& handler);
    msg_handler_t& get_msg_module(msg_module_id_t id);

    /******** IO Buffer related ********/
    uint8_t* iobuf_alloc(size_t align, size_t size);
    void iobuf_free(uint8_t* buf);
    uint8_t* iobuf_realloc(uint8_t* buf, size_t align, size_t new_size);
    size_t iobuf_size(uint8_t* buf) const;
    void set_io_memory_limit(size_t limit);
    [[nodiscard]] size_t soft_mem_threshold() const { return m_mem_soft_threshold_size; }
    [[nodiscard]] size_t aggressive_mem_threshold() const { return m_mem_aggressive_threshold_size; }

    /******** Timer related Operations ********/
    int64_t idle_timeout_interval_usec() const { return -1; };
    void idle_timeout_expired() {
        if (m_idle_timeout_expired_cb) { m_idle_timeout_expired_cb(); }
    }

    timer_handle_t schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                         timer_callback_t&& timer_fn);
    timer_handle_t schedule_global_timer(uint64_t nanos_after, bool recurring, void* cookie, thread_regex r,
                                         timer_callback_t&& timer_fn);

    void cancel_timer(timer_handle_t thdl) { return thdl.first->cancel(thdl); }
    [[nodiscard]] uint32_t num_workers() const { return m_num_workers; }

private:
    IOManager();
    ~IOManager();

    void foreach_interface(const auto& iface_cb);

    void _run_io_loop(int iomgr_slot_num, bool is_tloop_reactor, const iodev_selector_t& iodev_selector,
                      const thread_state_notifier_t& addln_notifier);

    void reactor_started(std::shared_ptr< IOReactor > reactor); // Notification that iomanager thread is ready to serve
    void reactor_stopped();                                     // Notification that IO thread is reliquished

    void start_spdk();
    void stop_spdk();

    void hugetlbfs_umount();

    void mempool_metrics_populate();
    void register_mempool_metrics(struct rte_mempool* mp);

    void set_state(iomgr_state state) { m_state.store(state, std::memory_order_release); }
    iomgr_state get_state() const { return m_state.load(std::memory_order_acquire); }
    void set_state_and_notify(iomgr_state state) {
        set_state(state);
        m_cv.notify_all();
    }

    void _pick_reactors(thread_regex r, const auto& cb);
    void all_reactors(const auto& cb);
    void specific_reactor(int thread_num, const auto& cb);

    [[nodiscard]] auto iface_wlock() { return m_iface_list.wlock(); }
    [[nodiscard]] auto iface_rlock() { return m_iface_list.rlock(); }
    [[nodiscard]] bool is_spdk_inited() const;

private:
    // size_t m_expected_ifaces = inbuilt_interface_count;        // Total number of interfaces expected
    std::atomic< iomgr_state > m_state = iomgr_state::stopped;    // Current state of IOManager
    sisl::atomic_counter< int16_t > m_yet_to_start_nreactors = 0; // Total number of iomanager threads yet to start
    sisl::atomic_counter< int16_t > m_yet_to_stop_nreactors = 0;
    uint32_t m_num_workers = 0;

    folly::Synchronized< std::vector< std::shared_ptr< IOInterface > > > m_iface_list;
    folly::Synchronized< std::unordered_map< backing_dev_t, io_device_ptr > > m_iodev_map;
    folly::Synchronized< std::vector< std::shared_ptr< DriveInterface > > > m_drive_ifaces;

    std::shared_ptr< DriveInterface > m_default_drive_iface;
    std::shared_ptr< GenericIOInterface > m_default_general_iface;
    folly::Synchronized< std::vector< uint64_t > > m_global_thread_contexts;

    sisl::ActiveOnlyThreadBuffer< std::shared_ptr< IOReactor > > m_reactors;

    std::mutex m_cv_mtx;
    std::condition_variable m_cv;
    std::function< void() > m_idle_timeout_expired_cb = nullptr;

    sisl::sparse_vector< reactor_info_t > m_worker_reactors;

    std::unique_ptr< timer_epoll > m_global_user_timer;
    std::unique_ptr< timer > m_global_worker_timer;

    std::mutex m_msg_hdlrs_mtx;
    std::array< msg_handler_t, max_msg_modules > m_msg_handlers;
    uint32_t m_msg_handlers_count = 0;
    msg_module_id_t m_internal_msg_module_id;
    thread_state_notifier_t m_common_thread_state_notifier = nullptr;
    sisl::IDReserver m_thread_idx_reserver;

    // SPDK Specific parameters. TODO: We could move this to a separate instance if needbe
    bool m_is_spdk{false};
    bool m_spdk_reinit_needed{false};

    IOManagerMetrics m_iomgr_metrics;
    folly::Synchronized< std::unordered_map< std::string, IOMempoolMetrics > > m_mempool_metrics_set;
    size_t m_mem_size_limit{std::numeric_limits< size_t >::max()};
    size_t m_mem_soft_threshold_size{m_mem_size_limit};
    size_t m_mem_aggressive_threshold_size{m_mem_size_limit};
};

struct SpdkAlignedAllocImpl : public sisl::AlignedAllocatorImpl {
    uint8_t* aligned_alloc(size_t align, size_t sz) override;
    void aligned_free(uint8_t* b) override;
    uint8_t* aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz = 0) override;
    size_t buf_size(uint8_t* buf) const override;
};

struct IOMgrAlignedAllocImpl : public sisl::AlignedAllocatorImpl {
    uint8_t* aligned_alloc(size_t align, size_t sz) override;
    void aligned_free(uint8_t* b) override;
    uint8_t* aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz = 0) override;
};
#define iomanager iomgr::IOManager::instance()
} // namespace iomgr

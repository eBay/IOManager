/**
 * Copyright eBay Corporation 2018
 */

#pragma once

extern "C" {
#include <event.h>
#include "spdk/util.h"
#include <sys/time.h>
}
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <map>
#include <memory>
#include <random>
#include <vector>
#include <sisl/utility/thread_buffer.hpp>
#include <sisl/utility/atomic_counter.hpp>
#include <sisl/fds/sparse_vector.hpp>
#include <sisl/fds/malloc_helper.hpp>
#include <sisl/fds/buffer.hpp>

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
#include "iomgr_types.hpp"
#include "drive_interface.hpp"
#include <functional>
#include <sisl/fds/id_reserver.hpp>
#include <sisl/utility/enum.hpp>
#include <sisl/logging/logging.h>
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
class GrpcInterface;

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

/**
 * @brief Get the IOManager version
 */
extern const version::Semver200_version get_version();

class IOWatchDog;
class IOManager {
public:
    friend class IOReactor;
    friend class IOReactorEPoll;
    friend class IOReactorSPDK;
    friend class IOInterface;
    friend class DriveInterface;
    friend class GenericIOInterface;

    static IOManager& instance() {
        static IOManager inst;
        return inst;
    }

    // TODO: Make this a dynamic config (albeit non-hotswap)
    static constexpr uint32_t max_msg_modules = 64;
    static constexpr uint32_t max_io_threads = 1024; // Keep in mind increasing this cause increased mem footprint
    static constexpr uint64_t max_mempool_buf_size = 256 * 1024;
    static constexpr uint64_t min_mempool_buf_size = 512;
    static constexpr uint64_t max_mempool_count = std::log2(max_mempool_buf_size - min_mempool_buf_size);
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
     * @brief A way to start the User Reactor and run an IO Loop. This method makes the current thread run a loop
     * and thus will return only after the loop is exited
     *
     * @param loop_type_t Is the loop it needs to run a tight loop or interrupt based loop (spdk vs epoll) etc..
     * @param iodev_selector [OPTIONAL] A selector callback which will be called when an iodevice is added to the
     * reactor. Consumer of this callback can return true to allow the device to be added or false if this reactor
     * needs to ignore this device.
     * @param addln_notifier Callback which notifies after reactor is ready or shutting down (with true or false)
     * parameter. This is per reactor override of the same callback as iomanager start.
     */
    void run_io_loop(loop_type_t loop_type, const iodev_selector_t& iodev_selector = nullptr,
                     const thread_state_notifier_t& addln_notifier = nullptr) {
        _run_io_loop(-1, loop_type, iodev_selector, addln_notifier);
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
    void become_user_reactor(loop_type_t loop_type, const iodev_selector_t& iodev_selector = nullptr,
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
    void add_drive_interface(std::shared_ptr< DriveInterface > iface, thread_regex iface_scope = thread_regex::all_io);
    std::shared_ptr< DriveInterface > get_drive_interface(const drive_interface_type type);

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

    // Direct run_on method for spdk without any msg creation
    int run_on(const io_thread_t& thread, spdk_msg_signature_t fn, void* context);

    /**
     * @brief Run the lambda/function passed on specific thread and optionally wait for its completion. If the
     * caller is same as destination thread, it will run the method right away.
     *
     * @param thread The IO Thread returned from the destination iothread_self() to which the method is to run
     * @param fn  Method to run
     * @param wait_type wait_type A variant supporting 4 types,
     *         1. nowait - Fire and forget, no need of waiting for the function to be completed.
     *         2. sleep -  Wait by sleeping till the thread that executing completed running the method
     *                     NOTE: This needs to be used carefully because if the caller sends another signal inside
     *                     the running method, then program will be deadlocked.
     *         3. spin  -  Wait by spinning till the thread that executing completed running the method. While
     *                     waiting, it can process other messages. So potentially stack could grow. If the calling
     *                     thread is not an IO thread, it sleeps (as if wait_type = sleep) instead of spinning
     *         4. Closure - Closure to be called after method is run in the caller thread.
     *
     * @return 1 for able to schedule the method to run, 0 otherwise
     */
    int run_on(const io_thread_t& thread, const auto& fn, const wait_type_t wtype = wait_type_t::no_wait,
               const run_on_closure_t& cb_wait_closure = nullptr) {
        bool sent{false};
        if (wtype == wait_type_t::no_wait) {
            sent = send_msg(thread, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn));
        } else if (wtype == wait_type_t::callback) {
            DEBUG_ASSERT(0, "run_on direct thread with async closure is not supported yet");
        } else if ((wtype == wait_type_t::spin) && IOManager::instance().am_i_io_reactor()) {
            auto smsg = spin_iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn);
            sent = send_msg_and_wait(thread, std::dynamic_pointer_cast< sync_msg_base >(smsg));
        } else {
            auto smsg = sync_iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn);
            sent = send_msg_and_wait(thread, std::dynamic_pointer_cast< sync_msg_base >(smsg));
        }

        return (sent ? 1 : 0);
    }

    /**
     * @brief Run the lambda/function passed on multipled threads and optionally wait for theirs completion. If the
     * caller is one among the destination thread, it will run the method right away in that thread.
     *
     * @param thread_regex Thread regex representing all the threads the method needs to run
     * @param fn  Method to run (can be a lambda or std::function)
     * @param wait_type wait_type A variant supporting 4 types,
     *         1. nowait - Fire and forget, no need of waiting for the function to be completed.
     *         2. sleep -  Wait by sleeping till all threads that executing completed running the method
     *                     NOTE: This needs to be used carefully because if the caller sends another signal inside
     *                     the running method, then program will be deadlocked.
     *         3. spin  -  Wait by spinning till all threads that executing completed running the method. While
     *                     waiting, it can process other messages. So potentially stack could grow. If the calling
     *                     thread is not an IO thread, it sleeps (as if wait_type = sleep) instead of spinning
     *         4. Closure - Closure to be called after method is run on all the threads. NOTE that this closure can be
     *                      called on any thread, not neccessarily issuing thread only.
     *
     * @return number of threads this method was run.
     */
    int run_on(const thread_regex r, const auto& fn, const wait_type_t wtype = wait_type_t::no_wait,
               const run_on_closure_t& cb_wait_closure = nullptr) {
        int sent_count{0};

        if ((wtype == wait_type_t::callback) && cb_wait_closure) {
            auto pending_count = new std::atomic< int >(0);
            auto temp_cb = [fn, cb_wait_closure, pending_count](io_thread_addr_t addr) {
                fn(addr);
                if (pending_count->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    cb_wait_closure();
                    delete pending_count;
                }
            };

            sent_count =
                multicast_msg(r, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, temp_cb));
            if ((pending_count->fetch_add(sent_count, std::memory_order_acq_rel) == -sent_count)) {
                cb_wait_closure();
                delete pending_count;
            }
            return sent_count;
        }

        // If the closure is not provided, its same as no_wait, so switch to no_wait
        if ((wtype == wait_type_t::no_wait) || (wtype == wait_type_t::callback)) {
            sent_count = multicast_msg(r, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn));
        } else if ((wtype == wait_type_t::spin) && IOManager::instance().am_i_io_reactor()) {
            auto smsg = spin_iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn);
            sent_count = multicast_msg_and_wait(r, std::dynamic_pointer_cast< sync_msg_base >(smsg));
        } else {
            auto smsg = sync_iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn);
            sent_count = multicast_msg_and_wait(r, std::dynamic_pointer_cast< sync_msg_base >(smsg));
        }

        if (cb_wait_closure) { cb_wait_closure(); }
        return sent_count;
    }

    void run_async_method_synchronized(thread_regex r, const auto& fn) {
        static synchronized_async_method_ctx mctx;
        int executed_on = run_on(r, [&fn]([[maybe_unused]] auto taddr) {
            fn(mctx);
            {
                std::unique_lock< std::mutex > lk{mctx.m};
                --mctx.outstanding_count;
            }
            mctx.cv.notify_one();
        });

        {
            std::unique_lock< std::mutex > lk{mctx.m};
            mctx.outstanding_count += executed_on;
            mctx.cv.wait(lk, [] { return (mctx.outstanding_count == 0); });
        }
    }

    /********* Access related methods ***********/
    const io_thread_t& iothread_self() const;
    IOReactor* this_reactor() const;

    GenericIOInterface* generic_interface() { return m_default_general_iface.get(); }
    GrpcInterface* grpc_interface() { return m_default_grpc_iface.get(); }

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

    bool am_i_adaptive_reactor() const {
        auto r = this_reactor();
        return r && r->is_adaptive_loop();
    }

    void set_my_reactor_adaptive(bool adaptive) {
        auto r = this_reactor();
        if (r) { r->set_adaptive_loop(adaptive); }
    }

    [[nodiscard]] uint32_t num_workers() const { return m_num_workers; }
    [[nodiscard]] bool is_spdk_mode() const { return m_is_spdk; }
    [[nodiscard]] bool is_uring_capable() const { return m_is_uring_capable; }

    /********* State Machine Related Operations ********/
    bool is_ready() const { return (get_state() == iomgr_state::running); }

    void set_state(iomgr_state state) {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_state = state;
    }

    iomgr_state get_state() const {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        return m_state;
    }

    void set_state_and_notify(iomgr_state state) {
        set_state(state);
        m_cv.notify_all();
    }

    void wait_to_be_ready() { wait_for_state(iomgr_state::running); }

    void wait_to_be_stopped() { wait_for_state(iomgr_state::stopped); }

    void wait_for_state(iomgr_state expected_state) {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        if (m_state != expected_state) {
            m_cv.wait(lck, [&] { return (m_state == expected_state); });
        }
    }

    void ensure_running() {
        if (get_state() != iomgr_state::running) {
            LOGINFO("IOManager is not running, will wait for it to be ready");
            wait_to_be_ready();
            LOGINFO("IOManager is ready now");
        }
    }

    uint64_t get_mempool_idx(size_t size);
    spdk_mempool* get_mempool(size_t size);
    void* create_mempool(size_t element_size, size_t element_count);

    /******** IO Thread related infra ********/
    io_thread_t make_io_thread(IOReactor* reactor);
    thread_state_notifier_t& thread_state_notifier() { return m_common_thread_state_notifier; }

    /******** Message related infra ********/
    bool send_msg(const io_thread_t& thread, iomgr_msg* msg);
    bool send_msg_and_wait(const io_thread_t& thread, const std::shared_ptr< sync_msg_base >& smsg);
    int multicast_msg(thread_regex r, iomgr_msg* msg);
    int multicast_msg_and_wait(thread_regex r, const std::shared_ptr< sync_msg_base >& smsg);

    msg_module_id_t register_msg_module(const msg_handler_t& handler);
    msg_handler_t& get_msg_module(msg_module_id_t id);

    /******** IO Buffer related ********/
    uint8_t* iobuf_alloc(size_t align, size_t size, const sisl::buftag tag = sisl::buftag::common);
    void iobuf_free(uint8_t* buf, const sisl::buftag tag = sisl::buftag::common);
    uint8_t* iobuf_pool_alloc(size_t align, size_t size, const sisl::buftag tag = sisl::buftag::common);
    void iobuf_pool_free(uint8_t* buf, size_t size, const sisl::buftag tag = sisl::buftag::common);
    uint8_t* iobuf_realloc(uint8_t* buf, size_t align, size_t new_size);
    size_t iobuf_size(uint8_t* buf) const;
    void set_io_memory_limit(size_t limit);
    [[nodiscard]] size_t soft_mem_threshold() const { return m_mem_soft_threshold_size; }
    [[nodiscard]] size_t aggressive_mem_threshold() const { return m_mem_aggressive_threshold_size; }

    /******** Timer related Operations ********/
    timer_handle_t schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                         timer_callback_t&& timer_fn);
    timer_handle_t schedule_global_timer(uint64_t nanos_after, bool recurring, void* cookie, thread_regex r,
                                         timer_callback_t&& timer_fn, bool wait_to_schedule = false);
    void cancel_timer(timer_handle_t thdl, bool wait_to_cancel = false) {
        return thdl.first->cancel(thdl, wait_to_cancel);
    }
    void set_poll_interval(const int interval);
    int get_poll_interval() const;

    IOWatchDog* get_io_wd() const { return m_io_wd.get(); };

private:
    IOManager();
    ~IOManager();

    void foreach_interface(const interface_cb_t& iface_cb);
    void start_reactors();
    void _run_io_loop(int iomgr_slot_num, loop_type_t loop_type, const iodev_selector_t& iodev_selector,
                      const thread_state_notifier_t& addln_notifier);

    void reactor_started(std::shared_ptr< IOReactor > reactor); // Notification that iomanager thread is ready to serve
    void reactor_stopped();                                     // Notification that IO thread is reliquished

    void start_spdk();
    void stop_spdk();

    void hugetlbfs_umount();

    void mempool_metrics_populate();
    void register_mempool_metrics(struct rte_mempool* mp);

    void _pick_reactors(thread_regex r, const auto& cb);
    void all_reactors(const auto& cb);
    void specific_reactor(int thread_num, const auto& cb);
    IOReactor* round_robin_reactor() const;

    [[nodiscard]] bool is_spdk_inited() const;

private:
    // size_t m_expected_ifaces = inbuilt_interface_count;        // Total number of interfaces expected
    iomgr_state m_state = iomgr_state::stopped;                   // Current state of IOManager
    sisl::atomic_counter< int16_t > m_yet_to_start_nreactors = 0; // Total number of iomanager threads yet to start
    sisl::atomic_counter< int16_t > m_yet_to_stop_nreactors = 0;
    uint32_t m_num_workers = 0;
    std::array< spdk_mempool*, max_mempool_count > m_iomgr_internal_pools;

    std::shared_mutex m_iface_list_mtx;
    std::vector< std::shared_ptr< IOInterface > > m_iface_list;
    std::vector< std::shared_ptr< DriveInterface > > m_drive_ifaces;

    std::shared_ptr< GenericIOInterface > m_default_general_iface;
    std::shared_ptr< GrpcInterface > m_default_grpc_iface;
    folly::Synchronized< std::vector< uint64_t > > m_global_thread_contexts;

    sisl::ActiveOnlyThreadBuffer< std::shared_ptr< IOReactor > > m_reactors;

    mutable std::mutex m_cv_mtx;
    std::condition_variable m_cv;

    std::vector< std::shared_ptr< IOReactor > > m_worker_reactors;
    std::vector< sys_thread_id_t > m_worker_threads;
    std::uniform_int_distribution< size_t > m_rand_worker_distribution;

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

    bool m_is_uring_capable{false};
    bool m_is_cpu_pinning_enabled{false};

    folly::Synchronized< std::unordered_map< std::string, IOMempoolMetrics > > m_mempool_metrics_set;
    size_t m_mem_size_limit{std::numeric_limits< size_t >::max()};
    size_t m_mem_soft_threshold_size{m_mem_size_limit};
    size_t m_mem_aggressive_threshold_size{m_mem_size_limit};

    std::unique_ptr< IOWatchDog > m_io_wd{nullptr};
};

struct SpdkAlignedAllocImpl : public sisl::AlignedAllocatorImpl {
    uint8_t* aligned_alloc(size_t align, size_t sz, const sisl::buftag tag) override;
    void aligned_free(uint8_t* b, const sisl::buftag tag) override;
    uint8_t* aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz = 0) override;
    uint8_t* aligned_pool_alloc(const size_t align, const size_t sz, const sisl::buftag tag) override;
    void aligned_pool_free(uint8_t* const b, const size_t sz, const sisl::buftag tag) override;
    size_t buf_size(uint8_t* buf) const override;
};

struct IOMgrAlignedAllocImpl : public sisl::AlignedAllocatorImpl {
    uint8_t* aligned_alloc(size_t align, size_t sz, const sisl::buftag tag) override;
    void aligned_free(uint8_t* b, const sisl::buftag tag) override;
    uint8_t* aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz = 0) override;
};
#define iomanager iomgr::IOManager::instance()
} // namespace iomgr

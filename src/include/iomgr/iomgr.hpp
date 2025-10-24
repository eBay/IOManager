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

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <semver200.h>
#include <boost/fiber/all.hpp>
#include <sisl/fds/bitword.hpp>
#include <sisl/fds/buffer.hpp>
#include <sisl/fds/id_reserver.hpp>
#include <sisl/fds/malloc_helper.hpp>
#include <sisl/fds/sparse_vector.hpp>
#include <sisl/logging/logging.h>
#include <sisl/utility/atomic_counter.hpp>
#include <sisl/utility/enum.hpp>
#include <sisl/utility/thread_buffer.hpp>

#include <iomgr/iomgr_msg.hpp>
#include <iomgr/iomgr_timer.hpp>
#include <iomgr/iomgr_types.hpp>
#include <iomgr/drive_interface.hpp>
#include <iomgr/io_device.hpp>
#include <iomgr/fiber_lib.hpp>

namespace iomgr {

struct timer_info;

// TODO: Make this part of an enum, to force add count upon adding new inbuilt io interface.
static constexpr int inbuilt_interface_count = 1;

class DriveInterface;
class IOManagerImpl;
class IOThreadMetrics;
class GenericIOInterface;

ENUM(iomgr_state, uint16_t,
     stopped,        // Stopped - this is the initial state
     interface_init, // Interface initialization is ongoing.
     reactor_init,   // All worker reactors are being initialized
     sys_init,       // Any system wide init, say timer, spdk bdev initialization etc..
     running,        // Active, ready to take traffic
     stopping);

struct iomgr_params {
    size_t num_threads{0};
    bool is_spdk{false};
    uint32_t num_fibers{0};
    uint32_t app_mem_size_mb{0};
    uint32_t hugepage_size_mb{0};
};

template < class... Ts >
struct overloaded : Ts... {
    using Ts::operator()...;
};
template < class... Ts >
overloaded(Ts...) -> overloaded< Ts... >;

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
    friend class AioDriveInterface;
    friend class SpdkDriveInterface;
    friend class IOManagerSpdkImpl;
    friend class IOManagerEpollImpl;

    static IOManager& instance() {
        static IOManager inst;
        return inst;
    }

    // TODO: Make this a dynamic config (albeit non-hotswap)
    static constexpr uint32_t max_io_fibers{1024}; // Keep in mind increasing this cause increased mem footprint

    /////////////////////////////////// Start/Stop Control Related Operations //////////////////////////////
    /**
     * @brief Start the IOManager. This is expected to be among the first call while application is started to enable
     * for it to do IO. Without this start, any other iomanager call would fail.
     *
     * @param Parameters containing
     *  num_threads: Total number of worker reactors to start with. Expected to be > 0
     *  is_spdk: Is the IOManager to be started in spdk mode or not. If set to true, all worker reactors are
     *  automatically started as spdk worker reactors.
     *  app_mem_size_mb: If the application using IOManager to be limited to specific size. If set to `0` takes
     *  system memory into account.
     *  hugepage_size_mb: Huge page size to be allocated. If set to `0`, will use system huge page size restriction
     * @param notifier [OPTONAL] A callback every time a new reactor is started or stopped. This will be called from the
     * reactor thread which is starting or stopping.
     * @param iface_adder [OPTIONAL] Callback to add interface by the caller during iomanager start. If null, then
     * iomanager will add all the default interfaces essential to do the IO.
     */
    void start(const iomgr_params& params, const thread_state_notifier_t& notifier = nullptr,
               interface_adder_t&& iface_adder = nullptr);

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
     * @param num_fibers [OPTIONAL] Total number of fibers this reactor to start. More fibers means more concurrent
     * sync io, but it comes at the cost of more stack memory. Defaults to 4
     * @param iodev_selector [OPTIONAL] A selector callback which will be called when an iodevice is added to the
     * reactor. Consumer of this callback can return true to allow the device to be added or false if this reactor
     * needs to ignore this device.
     * @param addln_notifier Callback which notifies after reactor is ready or shutting down (with true or false)
     * parameter. This is per reactor override of the same callback as iomanager start.
     */
    void run_io_loop(loop_type_t loop_type, uint32_t num_fibers = 4, const iodev_selector_t& iodev_selector = nullptr,
                     thread_state_notifier_t&& addln_notifier = nullptr) {
        _run_io_loop(-1, loop_type, num_fibers, "", iodev_selector, std::move(addln_notifier));
    }

    /// @brief Create a new thread and start reactor loop of given type in that thread. This created reactor will be
    /// added to list of user reactors.
    /// @param name : Name of the reactor (used for logging)
    /// @param loop_type : Type of loop (tight loop), can be a combination either, TIGHT_LOOP | ADAPTIVE_LOOP or
    /// INTERRUPT_LOOP | ADAPTIVE_LOOP or
    /// @param num_fibers [OPTIONAL] Total number of fibers this reactor to start. More fibers means more concurrent
    /// sync io, but it comes at the cost of more stack memory. Defaults to 4
    /// @param notifier : [OPTIONAL] Callback called from the new reactor thread with bool (start/stop)
    void create_reactor(const std::string& name, loop_type_t loop_type, uint32_t num_fibers = 4u,
                        thread_state_notifier_t&& notifier = nullptr);

    /**
     * @brief Convert the current thread to new user reactor
     *
     * @param is_tloop_reactor Is the loop it needs to run a tight loop or interrupt based loop (spdk vs epoll)
     * @param num_fibers [OPTIONAL] Total number of fibers this reactor to start. More fibers means more concurrent sync
     * io, but it comes at the cost of more stack memory. Defaults to 4
     * @param iodev_selector [OPTIONAL] A selector callback which will be called when an iodevice is added to the
     * reactor. Consumer of this callback can return true to allow the device to be added or false if this reactor
     * needs to ignore this device.
     * @param addln_notifier  Callback which notifies after reactor is ready or shutting down (with true or false)
     * parameter. This is per reactor override of the same callback as iomanager start.
     */
    void become_user_reactor(loop_type_t loop_type, uint32_t num_fibers = 4u,
                             const iodev_selector_t& iodev_selector = nullptr,
                             thread_state_notifier_t&& addln_notifier = nullptr);

    /**
     * @brief Stop the IO Loop and cease to being a reactor for the current thread. The current thread can choose
     * to exit much later, but once called, it will stop being a reactor and iomanager will stop tracking this thread.
     */
    void stop_io_loop();

    ////////////////////////////////// Interface/Device Related Operations ////////////////////////////////
    /**
     * @brief Add a new IOInterface to the iomanager. All iodevice added to IOManager have to be part of some interface.
     * By default iomanager automatically creates genericinterface and driveinterfaces. Any additional interface can
     * be added through this API.
     *
     * @param iface Shared pointer to the IOInterface to be added
     * @param iface_scope [OPTIONAL] Scope of which reactors these interface is to be added. By default it will be
     * added to all IO reactors. While it can accept any reactor_regex, really it is not practical to use
     * reactor_regex::random_user or reactor_regex::any_worker.
     */
    void add_interface(cshared< IOInterface >& iface, reactor_regex iface_scope = reactor_regex::all_io);

    /***
     * @brief Remove the IOInterface from the iomanager. Once removed, it will remove all the devices added to that
     * interface and cleanup their resources.
     *
     * @param iface: Shared pointer to the IOInterface to be removed.
     */
    void remove_interface(cshared< IOInterface >& iface);

    ////////////////////////////////// Message Passing Section ////////////////////////////////
    /// @brief Direct method to execute the spdk method into a fiber (running on remote reactor). This method doesn't
    /// wait for any success or its return, it will simply fire it and return.
    /// @param fiber: Fiber to run this method on. It could be current fiber in case it executes the method right away
    /// @param fn: Function to execute
    /// @param context: Any void context
    int run_on_forget(io_fiber_t fiber, spdk_msg_signature_t fn, void* context);

    int run_on_forget(io_fiber_t fiber, const auto& fn) {
        return send_msg(fiber, iomgr_msg::create(std::remove_reference_t< std::remove_cv_t< decltype(fn) > >{fn}));
    }

    int run_on_forget(reactor_regex rr, fiber_regex fr, const auto& fn) {
        static thread_local std::vector< FiberManagerLib::Future< bool > > s_future_list;
        return multicast_msg(rr, fr, iomgr_msg::create(std::remove_reference_t< std::remove_cv_t< decltype(fn) > >{fn}),
                             s_future_list);
    }

    int run_on_forget(reactor_regex rr, const auto& fn) { return run_on_forget(rr, fiber_regex::main_only, fn); }

    int run_on_wait(io_fiber_t fiber, const auto& fn) {
        DEBUG_ASSERT_EQ(am_i_sync_io_capable(), true,
                        "It is prohibited to be waiting from a main fiber of io reactor as it can cause deadlock. If "
                        "wait is needed, message can be executed on sync_io fibers");
        return send_msg_and_wait(
            fiber, iomgr_waitable_msg::create(std::remove_reference_t< std::remove_cv_t< decltype(fn) > >{fn}));
    }

    int run_on_wait(reactor_regex rr, fiber_regex fr, const auto& fn) {
        DEBUG_ASSERT_EQ(am_i_sync_io_capable(), true,
                        "It is prohibited to be waiting from a main fiber of io reactor as it can cause deadlock. If "
                        "wait is needed, message can be executed on sync_io fibers");
        return multicast_msg_and_wait(
            rr, fr, iomgr_waitable_msg::create(std::remove_reference_t< std::remove_cv_t< decltype(fn) > >{fn}));
    }

    int run_on_wait(reactor_regex rr, const auto& fn) { return run_on_wait(rr, fiber_regex::main_only, fn); }

    template < typename... Args >
    int run_on(bool wait, Args&&... args) {
        if (wait) {
            return run_on_wait(args...);
        } else {
            return run_on_forget(args...);
        }
    }

    ///////////////////////////// Access related methods /////////////////////////////
    GenericIOInterface* generic_interface() { return m_default_general_iface.get(); }
    uint32_t num_workers() const { return m_num_workers; }
    bool is_spdk_mode() const { return m_is_spdk; }
    bool is_uring_capable() const { return m_is_uring_capable; }

    //////////////////////////// Reactor/Fiber related methods ///////////////////////
    bool am_i_io_reactor() const;
    bool am_i_tight_loop_reactor() const;
    bool am_i_worker_reactor() const;
    bool am_i_adaptive_reactor() const;
    bool am_i_sync_io_capable() const;
    void set_my_reactor_adaptive(bool adaptive);
    io_fiber_t iofiber_self() const;
    IOReactor* this_reactor() const;
    std::vector< io_fiber_t > sync_io_capable_fibers() const;

    /******** IO Buffer related ********/
    uint8_t* iobuf_alloc(size_t align, size_t size, const sisl::buftag tag = sisl::buftag::common);
    void iobuf_free(uint8_t* buf, const sisl::buftag tag = sisl::buftag::common);
    uint8_t* iobuf_pool_alloc(size_t align, size_t size, const sisl::buftag tag = sisl::buftag::common);
    void iobuf_pool_free(uint8_t* buf, size_t size, const sisl::buftag tag = sisl::buftag::common);
    uint8_t* iobuf_realloc(uint8_t* buf, size_t align, size_t new_size);
    size_t iobuf_size(uint8_t* buf) const;
    size_t soft_mem_threshold() const { return m_mem_soft_threshold_size; }
    size_t aggressive_mem_threshold() const { return m_mem_aggressive_threshold_size; }

    /******** Timer related Operations ********/
    timer_handle_t schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                         timer_callback_t&& timer_fn);
    timer_handle_t schedule_global_timer(uint64_t nanos_after, bool recurring, void* cookie, reactor_regex r,
                                         timer_callback_t&& timer_fn, bool wait_to_schedule = false);
    void cancel_timer(timer_handle_t thdl, bool wait_to_cancel = false);
    void set_poll_interval(const int interval);
    int get_poll_interval() const;

    IOWatchDog* get_io_wd() const { return m_io_wd.get(); };
    void drive_interface_submit_batch();

    IOThreadMetrics& this_thread_metrics();

private:
    IOManager();
    ~IOManager();

    void foreach_interface(const interface_cb_t& iface_cb);
    void create_worker_reactors(uint32_t num_fibers);
    void _run_io_loop(int iomgr_slot_num, loop_type_t loop_type, uint32_t num_fibers, const std::string& name,
                      const iodev_selector_t& iodev_selector, thread_state_notifier_t&& addln_notifier);

    void reactor_started(std::shared_ptr< IOReactor > reactor); // Notification that iomanager thread is ready to serve
    void reactor_stopped();                                     // Notification that IO thread is reliquished

    void _pick_reactors(reactor_regex r, const auto& cb);
    void all_reactors(const auto& cb);
    void specific_reactor(uint32_t reactor_id, const auto& cb);
    IOReactor* round_robin_reactor() const;

    std::shared_ptr< DriveInterface > get_drive_interface(drive_interface_type type);
    void add_drive_interface(cshared< DriveInterface >& iface, reactor_regex iface_scope = reactor_regex::all_io);

    /******** IO Thread related infra ********/
    thread_state_notifier_t& thread_state_notifier() { return m_common_thread_state_notifier; }

    int send_msg(io_fiber_t fiber, iomgr_msg* msg);
    int send_msg_and_wait(io_fiber_t fiber, iomgr_waitable_msg* msg);

    int multicast_msg(reactor_regex rr, fiber_regex fr, iomgr_msg* msg,
                      std::vector< FiberManagerLib::Future< bool > >& out_msgs_list);
    int multicast_msg_and_wait(reactor_regex rr, fiber_regex fr, iomgr_msg* msg);

    /********* State Machine Related Operations ********/
    bool is_ready() const { return (get_state() == iomgr_state::running); }

    void set_state(const iomgr_state state) {
        std::unique_lock< std::mutex > lck{m_cv_mtx};
        m_state = state;
    }

    iomgr_state get_state() const {
        std::unique_lock< std::mutex > lck{m_cv_mtx};
        return m_state;
    }

    void set_state_and_notify(const iomgr_state state) {
        set_state(state);
        m_cv.notify_all();
    }

    void wait_to_be_ready() { wait_for_state(iomgr_state::running); }

    void wait_to_be_stopped() { wait_for_state(iomgr_state::stopped); }

    void wait_for_state(const iomgr_state expected_state) {
        std::unique_lock< std::mutex > lck{m_cv_mtx};
        m_cv.wait(lck, [&] { return (m_state == expected_state); });
    }

    void ensure_running() {
        if (get_state() != iomgr_state::running) {
            LOGINFO("IOManager is not running, will wait for it to be ready");
            wait_to_be_ready();
            LOGINFO("IOManager is ready now");
        }
    }

private:
    // size_t m_expected_ifaces = inbuilt_interface_count;        // Total number of interfaces expected
    iomgr_state m_state{iomgr_state::stopped};                   // Current state of IOManager
    sisl::atomic_counter< int16_t > m_yet_to_start_nreactors{0}; // Total number of iomanager threads yet to start
    sisl::atomic_counter< int16_t > m_yet_to_stop_nreactors{0};
    uint32_t m_num_workers{0};

    std::unique_ptr< IOManagerImpl > m_impl;

    std::shared_mutex m_iface_list_mtx;
    std::vector< std::shared_ptr< IOInterface > > m_iface_list;
    std::vector< std::shared_ptr< DriveInterface > > m_drive_ifaces;

    std::shared_ptr< GenericIOInterface > m_default_general_iface;

    sisl::ActiveOnlyThreadBuffer< std::shared_ptr< IOReactor > > m_reactors;

    mutable std::mutex m_cv_mtx;
    std::condition_variable m_cv;

    std::vector< std::shared_ptr< IOReactor > > m_worker_reactors;
    std::vector< sys_thread_id_t > m_worker_threads;
    std::uniform_int_distribution< size_t > m_rand_worker_distribution;

    std::unique_ptr< timer_epoll > m_global_user_timer;
    std::unique_ptr< timer > m_global_worker_timer;

    thread_state_notifier_t m_common_thread_state_notifier{nullptr};
    sisl::IDReserver m_fiber_ordinal_reserver;

    // SPDK Specific parameters. TODO: We could move this to a separate instance if needbe
    bool m_is_spdk{false};
    bool m_is_uring_capable{false};

    size_t m_mem_size_limit{0};
    size_t m_hugepage_limit{0};
    size_t m_mem_soft_threshold_size{0};
    size_t m_mem_aggressive_threshold_size{0};

    std::unique_ptr< IOWatchDog > m_io_wd{nullptr};
};

#define iomanager iomgr::IOManager::instance()
} // namespace iomgr

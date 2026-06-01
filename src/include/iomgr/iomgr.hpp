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

#include <future>
#include <thread>
#include <semver200.h>
#include <sisl/fds/bitword.hpp>
#include <sisl/fds/buffer.hpp>
#include <sisl/fds/id_reserver.hpp>
#include <sisl/fds/malloc_helper.hpp>
#include <sisl/logging/logging.h>
#include <sisl/utility/atomic_counter.hpp>
#include <sisl/utility/enum.hpp>
#include <sisl/utility/thread_buffer.hpp>

#include <iomgr/iomgr_types.hpp>

namespace iomgr {
using timer_callback_t = std::function< void(void*) >;
using timer_handle_t = std::shared_ptr< void >; // opaque; null == no active timer

struct iomgr_msg;
struct iomgr_waitable_msg;

// TODO: Make this part of an enum, to force add count upon adding new inbuilt io interface.
static constexpr int inbuilt_interface_count = 1;

class DriveInterface;
class IOManagerImpl;
class IOThreadMetrics;
class GenericIOInterface;
class timer;       // defined in iomgr_timer_impl.hpp (internal)
class timer_epoll; // defined in iomgr_timer_impl.hpp (internal)

// Internal lifecycle state — not part of the public API contract.
ENUM(iomgr_state, uint16_t, stopped, interface_init, reactor_init, sys_init, running, stopping);

struct iomgr_params {
    size_t num_threads{0};
    uint32_t app_mem_size_mb{0};
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
    friend class IOInterface;
    friend class DriveInterface;
    friend class GenericIOInterface;
    friend class IOManagerEpollImpl;
    // Reach the internal generic_interface() through the iomanager singleton (friendship isn't inherited,
    // so DriveInterface's friendship doesn't cover these):
    friend class uring_drive_channel; // per-reactor uring queue: registers its eventfd iodevice
    friend class timer_epoll;         // arms/cancels its timer fds

    static IOManager& instance() {
        static IOManager inst;
        return inst;
    }

    // TODO: Make this a dynamic config (albeit non-hotswap)

    /////////////////////////////////// Start/Stop Control Related Operations //////////////////////////////
    /**
     * @brief Start the IOManager. This is expected to be among the first call while application is started to enable
     * for it to do IO. Without this start, any other iomanager call would fail.
     *
     * @param params  num_threads: worker reactor count (0 = use dynamic config default).
     *               app_mem_size_mb: application memory limit in MB (0 = detect from system).
     * @param notifier [OPTIONAL] Called from each reactor thread on start (true) or stop (false).
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
     * @param loop_type_t Type of loop (TIGHT_LOOP, INTERRUPT_LOOP, ADAPTIVE_LOOP, or combination)
     * @param iodev_selector [OPTIONAL] Callback to filter which iodevices are added to this reactor
     * @param addln_notifier [OPTIONAL] Callback on reactor start/stop (true/false)
     */
    void run_io_loop(loop_type_t loop_type, const iodev_selector_t& iodev_selector = nullptr,
                     thread_state_notifier_t&& addln_notifier = nullptr) {
        _run_io_loop(-1, loop_type, "", iodev_selector, std::move(addln_notifier));
    }

    /// @brief Create a new thread and start reactor loop of given type in that thread. This created reactor will be
    /// added to list of user reactors.
    /// @param name : Name of the reactor (used for logging)
    /// @param loop_type : Type of loop (tight loop), can be a combination either, TIGHT_LOOP | ADAPTIVE_LOOP or
    /// INTERRUPT_LOOP | ADAPTIVE_LOOP or
    /// @param notifier : [OPTIONAL] Callback called from the new reactor thread with bool (start/stop)
    void create_reactor(const std::string& name, loop_type_t loop_type, thread_state_notifier_t&& notifier = nullptr);

    /**
     * @brief Convert the current thread to a user reactor and run the IO loop. Returns only after the loop exits.
     *
     * @param loop_type  Type of loop (TIGHT_LOOP, INTERRUPT_LOOP, ADAPTIVE_LOOP, or combination)
     * @param iodev_selector [OPTIONAL] Callback to filter which iodevices are added to this reactor
     * @param addln_notifier  [OPTIONAL] Callback on reactor start/stop (true/false)
     */
    void become_user_reactor(loop_type_t loop_type, const iodev_selector_t& iodev_selector = nullptr,
                             thread_state_notifier_t&& addln_notifier = nullptr);

    /**
     * @brief Stop the IO Loop and cease to being a reactor for the current thread. The current thread can choose
     * to exit much later, but once called, it will stop being a reactor and iomanager will stop tracking this thread.
     */
    void stop_io_loop();

    ////////////////////////////////// Message Passing Section ////////////////////////////////
    // Templates keep the exact callable type visible to the compiler; dispatch goes through
    // erased void*/function-pointer bridge so iomgr_msg stays out of the public header.
    int run_on_forget(IOReactor* reactor, const auto& fn) {
        using F = std::decay_t< decltype(fn) >;
        return _run_forget(
            reactor, new F(fn), +[](void* p) { (*static_cast< F* >(p))(); },
            +[](void* p) noexcept { delete static_cast< F* >(p); });
    }
    int run_on_forget(reactor_regex rr, const auto& fn) {
        using F = std::decay_t< decltype(fn) >;
        return _run_forget(
            rr, new F(fn), +[](void* p) { (*static_cast< F* >(p))(); },
            +[](void* p) noexcept { delete static_cast< F* >(p); });
    }

    int run_on_wait(IOReactor* reactor, const auto& fn) {
        using F = std::decay_t< decltype(fn) >;
        return _run_wait(
            reactor, new F(fn), +[](void* p) { (*static_cast< F* >(p))(); },
            +[](void* p) noexcept { delete static_cast< F* >(p); });
    }
    int run_on_wait(reactor_regex rr, const auto& fn) {
        using F = std::decay_t< decltype(fn) >;
        return _run_wait(
            rr, new F(fn), +[](void* p) { (*static_cast< F* >(p))(); },
            +[](void* p) noexcept { delete static_cast< F* >(p); });
    }

    template < typename... Args >
    int run_on(bool wait, Args&&... args) {
        if (wait) {
            return run_on_wait(std::forward< Args >(args)...);
        } else {
            return run_on_forget(std::forward< Args >(args)...);
        }
    }

    ///////////////////////////// Access related methods /////////////////////////////
    uint32_t num_workers() const { return m_num_workers; }
    bool is_uring_capable() const { return m_is_uring_capable; }

    //////////////////////////// Reactor related methods ///////////////////////
    bool am_i_io_reactor() const;
    bool am_i_tight_loop_reactor() const;
    bool am_i_worker_reactor() const;
    bool am_i_adaptive_reactor() const;
    void set_my_reactor_adaptive(bool adaptive);
    IOReactor* this_reactor() const;

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

    // Returns an iomgr-internal metrics type; used only by the internal drive path (not a consumer API),
    // but left public because its callers are free functions rather than friendable members.
    IOThreadMetrics& this_thread_metrics();

private:
    IOManager();
    ~IOManager();

    // Internal accessors: these take/return iomgr-internal types (IOInterface / GenericIOInterface /
    // IOWatchDog), so they are not part of the consumer-facing API. Friends above (interfaces,
    // reactors, uring_drive_channel, timer_epoll) reach them through the iomanager singleton.
    void add_interface(cshared< IOInterface >& iface, reactor_regex iface_scope = reactor_regex::all_io);
    void remove_interface(cshared< IOInterface >& iface);
    GenericIOInterface* generic_interface() { return m_default_general_iface.get(); }
    IOWatchDog* get_io_wd() const { return m_io_wd.get(); };

    void foreach_interface(const std::function< void(const cshared< IOInterface >&) >& iface_cb);
    void create_worker_reactors();
    void _run_io_loop(int iomgr_slot_num, loop_type_t loop_type, const std::string& name,
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

    // Erased-dispatch bridge: fn_copy is heap-owned; call invokes it; del frees it.
    // Defined in iomgr.cpp which includes the internal iomgr_msg header.
    using _fn_call_t = void (*)(void*);
    using _fn_del_t = void (*)(void*) noexcept;
    int _run_forget(IOReactor* reactor, void* fn_copy, _fn_call_t call, _fn_del_t del);
    int _run_forget(reactor_regex rr, void* fn_copy, _fn_call_t call, _fn_del_t del);
    int _run_wait(IOReactor* reactor, void* fn_copy, _fn_call_t call, _fn_del_t del);
    int _run_wait(reactor_regex rr, void* fn_copy, _fn_call_t call, _fn_del_t del);

    int send_msg(IOReactor* reactor, iomgr_msg* msg);
    int send_msg_and_wait(IOReactor* reactor, iomgr_waitable_msg* msg);

    int multicast_msg(reactor_regex rr, iomgr_msg* msg, std::vector< std::future< bool > >& out_msgs_list);
    int multicast_msg_and_wait(reactor_regex rr, iomgr_msg* msg);

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
    std::vector< std::thread > m_worker_threads;
    std::uniform_int_distribution< size_t > m_rand_worker_distribution;

    std::unique_ptr< timer_epoll > m_global_user_timer;
    std::unique_ptr< timer > m_global_worker_timer;

    thread_state_notifier_t m_common_thread_state_notifier{nullptr};

    bool m_is_uring_capable{false};

    size_t m_mem_size_limit{0};
    size_t m_mem_soft_threshold_size{0};
    size_t m_mem_aggressive_threshold_size{0};

    std::unique_ptr< IOWatchDog > m_io_wd{nullptr};
};

#define iomanager iomgr::IOManager::instance()
} // namespace iomgr

SISL_LOGGING_DECL(IOMGR_LOG_MODS)

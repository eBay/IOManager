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
#include <map>
#include <memory>
#include <vector>
#include <utility/thread_buffer.hpp>
#include <utility/atomic_counter.hpp>
#include <fds/sparse_vector.hpp>
#include <folly/Synchronized.h>
#include "iomgr_msg.hpp"
#include "io_thread.hpp"
#include "iomgr_timer.hpp"
#include "io_interface.hpp"
#include "drive_interface.hpp"
#include <functional>
#include <fds/utils.hpp>

struct spdk_bdev_desc;
struct spdk_bdev;

namespace iomgr {

struct timer_info;
struct io_device_t {
    ev_callback cb = nullptr;
    backing_dev_t dev;
    int ev = 0;
    bool is_global = false;
    int pri = 1;
    void* cookie = nullptr;
    std::unique_ptr< timer_info > tinfo;
    IOInterface* io_interface = nullptr;
    void* device_ctx = nullptr; // Any context information for this device

    ~io_device_t() {
        if (device_ctx) free(device_ctx);
    }

    int fd() { return std::get< int >(dev); }
    spdk_bdev_desc* bdev_desc();
    spdk_bdev* bdev();
    bool is_spdk_dev() const { return std::holds_alternative< spdk_bdev_desc* >(dev); }

    std::string dev_id();
};

// TODO: Make this part of an enum, to force add count upon adding new inbuilt io interface.
static constexpr int inbuilt_interface_count = 1;

class DriveInterface;

enum class iomgr_state : uint16_t {
    stopped = 0,
    waiting_for_interfaces = 1,
    waiting_for_threads = 2,
    running = 3,
    stopping = 4,
};

using msg_handler_t = std::function< void(const iomgr_msg&) >;

class IOManager {
public:
    friend class IOReactor;
    friend class IOReactorEPoll;

    static IOManager& instance() {
        static IOManager inst;
        return inst;
    }

    static constexpr uint32_t max_msg_modules = 64;

    IOManager();
    ~IOManager();

    /********* Start/Stop Control Related Operations ********/
    void start(size_t num_iface, size_t num_threads = 0, bool is_spdk = false,
               const io_thread_msg_handler& notifier = nullptr);
    void stop();
    void run_io_loop(bool is_iomgr_thread = false, const iodev_selector_t& iodev_selector = nullptr,
                     const io_thread_msg_handler& override_msg_handler = nullptr);
    void stop_io_loop();

    /********* Interface/Device Related Operations ********/
    void add_interface(std::shared_ptr< IOInterface > iface);
    void add_drive_interface(std::shared_ptr< DriveInterface > iface, bool is_default);
    void add_io_device(const io_device_ptr& iodev);
    void remove_io_device(const io_device_ptr& iodev);
    void device_reschedule(const io_device_ptr& iodev, int event);
    void run_in_io_thread(const run_method_t& fn);

    /********* Access related methods ***********/
    io_thread_id_t my_io_thread_id() const;
    IOReactor* this_reactor() const;
    DriveInterface* default_drive_interface() { return m_default_drive_iface.get(); }
    GenericIOInterface* generic_interface() { return m_default_general_iface.get(); }

    /********* State Machine Related Operations ********/
    bool is_io_thread() { return this_reactor()->is_io_thread(); }
    bool is_ready() const { return (get_state() == iomgr_state::running); }
    bool is_interface_registered() const {
        return ((uint16_t)get_state() > (uint16_t)iomgr_state::waiting_for_interfaces);
    }
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

    void wait_for_interface_registration() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_cv.wait(lck, [this] { return is_interface_registered(); });
    }

    /******** Message related infra ********/
    bool send_msg(io_thread_id_t to_thread, const iomgr_msg& msg);
    uint32_t broadcast_msg(const iomgr_msg& msg);
    void send_to_least_busy_iomgr_thread(const iomgr_msg& msg);
    msg_module_id_t register_msg_module(const msg_handler_t& handler);
    msg_handler_t& get_msg_module(msg_module_id_t id);
    io_thread_msg_handler& msg_handler() { return m_common_thread_msg_handler; }

    /******** IO Buffer related ********/
    uint8_t* iobuf_alloc(size_t align, size_t size);
    sisl::aligned_unique_ptr< uint8_t > iobuf_alloc_unique(size_t align, size_t size);
    std::shared_ptr< uint8_t > iobuf_alloc_shared(size_t align, size_t size);
    void iobuf_free(uint8_t* buf);

    /******** Timer related Operations ********/
    int64_t idle_timeout_interval_usec() const { return -1; };
    void idle_timeout_expired() {
        if (m_idle_timeout_expired_cb) { m_idle_timeout_expired_cb(); }
    }

    timer_handle_t schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                         timer_callback_t&& timer_fn) {
        return schedule_timer(nanos_after, recurring, cookie, true, std::move(timer_fn));
    }

    timer_handle_t schedule_global_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                         timer_callback_t&& timer_fn) {
        return schedule_timer(nanos_after, recurring, cookie, false, std::move(timer_fn));
    }

    void cancel_thread_timer(timer_handle_t thdl) { cancel_timer(thdl, true); }
    void cancel_global_timer(timer_handle_t thdl) { cancel_timer(thdl, false); }

    timer_handle_t schedule_timer(uint64_t nanos_after, bool recurring, void* cookie, bool is_per_thread,
                                  timer_callback_t&& timer_fn) {
        return (is_per_thread
                    ? this_reactor()->m_thread_timer->schedule(nanos_after, recurring, cookie, std::move(timer_fn))
                    : m_global_timer->schedule(nanos_after, recurring, cookie, std::move(timer_fn)));
    }

    void cancel_timer(timer_handle_t thdl, bool is_per_thread) {
        return (is_per_thread ? this_reactor()->m_thread_timer->cancel(thdl) : m_global_timer->cancel(thdl));
    }

private:
    void foreach_iodevice(std::function< void(const io_device_ptr&) > iodev_cb);
    void foreach_interface(std::function< void(IOInterface*) > iface_cb);

    void io_thread_started(bool is_iomgr_thread); // Notification that iomanager thread is ready to serve
    void io_thread_stopped();                     // Notification that IO thread is reliquished

    void start_spdk();
    void set_state(iomgr_state state) { m_state.store(state, std::memory_order_release); }
    iomgr_state get_state() const { return m_state.load(std::memory_order_acquire); }
    void set_state_and_notify(iomgr_state state) {
        set_state(state);
        // Setup the global timer if we are moving to running state
        if (state == iomgr_state::running) {
            m_global_timer = std::make_unique< timer_epoll >(false /* is_per_thread */);
        }
        m_cv.notify_all();
    }

    io_thread_id_t find_least_busy_iomgr_thread_id();
    void all_reactors(const std::function< void(IOReactor* ctx) >& cb);
    void specific_reactor(int thread_num, const std::function< void(IOReactor* ctx) >& cb);

private:
    size_t m_expected_ifaces = inbuilt_interface_count;          // Total number of interfaces expected
    std::atomic< iomgr_state > m_state = iomgr_state::stopped;   // Current state of IOManager
    sisl::atomic_counter< int16_t > m_yet_to_start_nthreads = 0; // Total number of iomanager threads yet to start
    sisl::atomic_counter< int16_t > m_yet_to_stop_nthreads = 0;

    folly::Synchronized< std::vector< std::shared_ptr< IOInterface > > > m_iface_list;
    folly::Synchronized< std::unordered_map< backing_dev_t, io_device_ptr > > m_iodev_map;
    folly::Synchronized< std::vector< std::shared_ptr< DriveInterface > > > m_drive_ifaces;

    std::shared_ptr< DriveInterface > m_default_drive_iface;
    std::shared_ptr< GenericIOInterface > m_default_general_iface;
    folly::Synchronized< std::vector< uint64_t > > m_global_thread_contexts;

    sisl::ActiveOnlyThreadBuffer< std::unique_ptr< IOReactor > > m_reactors;

    std::mutex m_cv_mtx;
    std::condition_variable m_cv;
    std::function< void() > m_idle_timeout_expired_cb = nullptr;

    std::vector< std::thread > m_iomgr_threads;
    io_thread_msg_handler m_common_thread_msg_handler = nullptr;
    bool m_is_spdk = false;
    std::unique_ptr< timer > m_global_timer;

    std::mutex m_msg_hdlrs_mtx;
    std::array< msg_handler_t, max_msg_modules > m_msg_handlers;
    uint32_t m_msg_handlers_count = 0;
    msg_module_id_t m_internal_msg_module_id;
};

#define iomanager iomgr::IOManager::instance()
} // namespace iomgr

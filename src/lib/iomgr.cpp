//
// Created by Rishabh Mittal on 04/20/2018
//

#include "iomgr.hpp"

extern "C" {
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/types.h>

#include <spdk/log.h>
#include <spdk/env.h>
#include <spdk/thread.h>
#include "spdk/bdev.h"
}

#include <cerrno>
#include <chrono>
#include <ctime>
#include <functional>
#include <vector>
#include <thread>

#include <sds_logging/logging.h>

#include "include/drive_interface.hpp"
#include "include/iomgr.hpp"
#include "include/io_thread_epoll.hpp"
#include "include/io_thread_spdk.hpp"
#include <utility/thread_factory.hpp>
#include <fds/obj_allocator.hpp>

namespace iomgr {

IOManager::IOManager() { m_iface_list.wlock()->reserve(inbuilt_interface_count + 5); }

IOManager::~IOManager() = default;

void IOManager::start(size_t const expected_custom_ifaces, size_t const num_threads, bool is_tloop,
                      const thread_state_notifier_t& notifier) {
    LOGINFO("Starting IOManager");
    m_is_tloop = is_tloop;
    m_expected_ifaces += expected_custom_ifaces;
    m_yet_to_start_nthreads.set(num_threads);

    // One common module and other internal handler
    m_common_thread_state_notifier = notifier;
    m_internal_msg_module_id = register_msg_module([this](iomgr_msg* msg) { this_reactor()->handle_msg(msg); });

    set_state(iomgr_state::waiting_for_interfaces);

    // Start the SPDK
    if (is_tloop) { start_spdk(); }

    /* Create all in-built interfaces here.
     * TODO: Can we create aio_drive_end_point by default itself
     * */
    m_default_general_iface = std::make_shared< GenericIOInterface >();
    add_interface(m_default_general_iface);
}

void IOManager::start_spdk() {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "IOManager";
    opts.shm_id = -1;

    int rc = spdk_env_init(&opts);
    if (rc != 0) { throw std::runtime_error("SPDK Iniitalization failed"); }

    spdk_unaffinitize_thread();

    // TODO: Do spdk_thread_lib_init_ext to handle spdk thread switching etc..
    rc = spdk_thread_lib_init(NULL, 0);
    if (rc != 0) { throw std::runtime_error("SPDK Thread Lib Init failed"); }
}

void IOManager::stop() {
    LOGINFO("Stopping IOManager");
    set_state(iomgr_state::stopping);

    // Increment stopping threads by 1 and then decrement after sending message to prevent case where there are no
    // IO threads, which hangs the iomanager stop
    m_yet_to_stop_nthreads.increment();

    // Free up and unregister fds for global timer
    m_global_timer.reset(nullptr);

    // Send all the threads to reliquish its io thread status
    send_msg_to(thread_regex::all_io,
                iomgr_msg::create(iomgr_msg_type::RELINQUISH_IO_THREAD, m_internal_msg_module_id));

    // Now decrement and check if all io threads have already reliquished the io thread status.
    if (m_yet_to_stop_nthreads.decrement_testz()) {
        set_state(iomgr_state::stopped);
    } else {
        // Few threads are still in process of coming out io loop, wait for them.
        wait_to_be_stopped();
    }

    LOGINFO("All IO threads have stopped and hence IOManager is moved to stopped state, joining any iomanager threads");
    // Join all the iomanager threads
    for (auto& t : m_iomgr_threads) {
        t.join();
    }

    m_iomgr_threads.clear();
    m_yet_to_start_nthreads.set(0);
    m_expected_ifaces = inbuilt_interface_count;
    m_drive_ifaces.wlock()->clear();
    m_iface_list.wlock()->clear();
    assert(get_state() == iomgr_state::stopped);

    LOGINFO("IOManager Stopped and all IO threads are relinquished");
}

void IOManager::add_drive_interface(std::shared_ptr< DriveInterface > iface, bool default_iface) {
    add_interface(std::dynamic_pointer_cast< IOInterface >(iface));
    m_drive_ifaces.wlock()->push_back(iface);
    if (default_iface) m_default_drive_iface = iface;
}

void IOManager::add_interface(std::shared_ptr< IOInterface > iface) {
    m_iface_list.wlock()->push_back(iface);

    auto iface_count = m_iface_list.rlock()->size();
    if (iface_count == m_expected_ifaces) {
        LOGINFO("Registered expected {} interfaces, marking iomanager waiting for threads", iface_count);

        auto nthreads = m_yet_to_start_nthreads.get();
        if (nthreads) {
            set_state_and_notify(iomgr_state::waiting_for_threads);
            LOGINFO("IOManager is asked to start {} number of threads, starting them", nthreads);
            for (auto i = 0; i < nthreads; i++) {
                m_iomgr_threads.push_back(std::move(sisl::thread_factory("iomgr_thread", &IOManager::_run_io_loop, this,
                                                                         true, m_is_tloop, nullptr, nullptr)));
                LOGTRACEMOD(iomgr, "Created iomanager reactor thread...", i);
                // t.detach();
            }
        } else {
            set_state_and_notify(iomgr_state::running);
        }
    } else if (iface_count < m_expected_ifaces) {
        LOGINFO("Only added {} interfaces, need to wait till we get {} interfaces registered", iface_count,
                m_expected_ifaces);
    }
}

void IOManager::_run_io_loop(bool is_iomgr_created, bool is_tloop_thread, const iodev_selector_t& iodev_selector,
                             const thread_state_notifier_t& addln_notifier) {
    if (is_tloop_thread) {
        *(m_reactors.get()) = std::make_unique< IOReactorSPDK >();
    } else {
        *(m_reactors.get()) = std::make_unique< IOReactorEPoll >();
    }
    this_reactor()->run(is_iomgr_created, iodev_selector, addln_notifier);
}

void IOManager::stop_io_loop() { this_reactor()->stop(); }

void IOManager::io_thread_started(bool is_iomgr_created) {
    m_yet_to_stop_nthreads.increment();
    if (is_iomgr_created && m_yet_to_start_nthreads.decrement_testz()) { set_state_and_notify(iomgr_state::running); }
}

void IOManager::io_thread_stopped() {
    if (m_yet_to_stop_nthreads.decrement_testz()) { set_state_and_notify(iomgr_state::stopped); }
}

void IOManager::add_io_device(const io_device_ptr& iodev) {
    // We can add per thread device even when iomanager is not ready. However, global devices need IOManager
    // to be initialized, since it has to maintain global map
    if (iodev->is_global && (get_state() != iomgr_state::running)) {
        LOGINFO("IOManager is not ready to add iodevice, will wait for it to be ready");
        wait_to_be_ready();
        LOGINFO("IOManager is ready now, proceed to add devices to the list");
    }

    if (iodev->is_global) {
        // Send a sync message to add device to all io threads
        auto smsg = sync_iomgr_msg::create(iomgr_msg_type::ADD_DEVICE, m_internal_msg_module_id, iodev, 0);
        all_reactors([&](IOReactor* reactor) {
            if (reactor && reactor->is_io_thread() && reactor->is_iodev_addable(iodev)) { reactor->deliver_msg(smsg); }
        });
        smsg->wait();
        smsg->free_yourself();
        m_iodev_map.wlock()->insert(std::pair< backing_dev_t, io_device_ptr >(iodev->dev, iodev));
    } else {
        auto r = this_reactor();
        if (r) {
            if (r->is_iodev_addable(iodev)) { r->add_iodev_to_reactor(iodev); }
        } else {
            LOGDFATAL("IOManager does not support adding local iodevices through non-io threads yet. Send a message to "
                      "an io thread");
        }
    }
}

void IOManager::remove_io_device(const io_device_ptr& iodev) {
    auto state = get_state();
    if ((state != iomgr_state::running) && (state != iomgr_state::stopping)) {
        LOGDFATAL("Expected IOManager to be running or stopping state before we receive remove io device");
        return;
    }

    if (iodev->is_global) {
        auto smsg = sync_iomgr_msg::create(iomgr_msg_type::REMOVE_DEVICE, m_internal_msg_module_id, iodev, 0);
        all_reactors([&](IOReactor* reactor) {
            if (reactor && reactor->is_io_thread()) { reactor->deliver_msg(smsg); }
        });
        smsg->wait();
        smsg->free_yourself();
        m_iodev_map.wlock()->erase(iodev->dev);
    } else {
        auto r = this_reactor();
        if (r) {
            if (r->is_iodev_addable(iodev)) { r->remove_iodev_from_reactor(iodev); }
        } else {
            LOGDFATAL("IOManager does not support removing local iodevices through non-io threads yet. Send a "
                      "message to an io thread");
        }
    }
}

void IOManager::device_reschedule(const io_device_ptr& iodev, int event) {
    send_msg_to(m_is_tloop ? thread_regex::any_tloop : thread_regex::any_io,
                iomgr_msg::create(iomgr_msg_type::RESCHEDULE, m_internal_msg_module_id, iodev, event));
}

static bool match_regex(thread_regex t, IOReactor* reactor) {
    if (!reactor || !reactor->is_io_thread()) { return false; }
    if ((t == thread_regex::all_io) || (t == thread_regex::any_io)) { return true; }
    if (reactor->is_tight_loop_thread()) {
        return (t == thread_regex::all_tloop) || (t == thread_regex::any_tloop);
    } else {
        return (t == thread_regex::all_iloop) || (t == thread_regex::any_iloop);
    }
}

void IOManager::run_on(const thread_specifier& specifier, const run_method_t& fn) {
    send_msg_to(specifier, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn));
}

bool IOManager::send_msg_to(const thread_specifier& specifier, iomgr_msg* msg) {
    bool sent = true;
    int64_t min_cnt = INTMAX_MAX;
    io_thread_id_t min_id = io_thread_id_t(0);

    iomgr_msg* unsent_msg = nullptr;
    iomgr_msg* cur_msg = const_cast< iomgr_msg* >(msg);
    std::visit(overloaded{[&](thread_regex t) {
                              do {
                                  all_reactors([&](IOReactor* reactor) {
                                      if (match_regex(t, reactor)) {
                                          if ((t == thread_regex::any_tloop) || (t == thread_regex::any_iloop)) {
                                              min_id = reactor->my_io_thread_id();
                                              min_cnt = reactor->m_count;
                                          } else {
                                              auto new_msg = cur_msg->clone();
                                              reactor->deliver_msg(cur_msg);
                                              unsent_msg = cur_msg = new_msg;
                                          }
                                      }
                                  });
                                  if (min_cnt != INTMAX_MAX) { sent = send_msg(min_id, cur_msg); }
                              } while (!sent);
                          },
                          [&](io_thread_id_t thread) { sent = send_msg(thread, cur_msg); }},
               specifier);

    if (unsent_msg) { unsent_msg->free_yourself(); }
    return sent;
}

#if 0
void IOManager::run_in_any_io_thread(const run_method_t& fn) {
    auto run_method = sisl::ObjectAllocator< run_method_t >::make_object();
    *run_method = fn;

    iomgr_msg msg(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, nullptr, -1, (void*)run_method,
                  sizeof(run_method_t));
    send_to_least_busy_iomgr_thread(msg);
}
#endif

bool IOManager::run_in_specific_thread(io_thread_id_t thread, const run_method_t& fn, bool wait_for_completion) {
    bool ret;
    if (wait_for_completion) {
        auto smsg = sync_iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, msg_data_t(fn));
        ret = send_msg(thread, smsg);
        smsg->wait();
        smsg->free_yourself();
    } else {
        ret = send_msg(thread, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, msg_data_t(fn)));
    }
    return ret;
}

#if 0
void IOManager::send_to_least_busy_iomgr_thread(const iomgr_msg& msg) {
    bool sent = false;
    do {
        auto min_id = find_least_busy_iomgr_thread_id();

        // Try to send msg to the thread. send_msg could fail if thread is not alive (i,e between
        // access_all_threads) and next method, thread exits.
        sent = send_msg(min_id, msg);
    } while (!sent);
}

io_thread_id_t IOManager::find_least_busy_iomgr_thread_id() {
    int64_t min_cnt = INTMAX_MAX;
    io_thread_id_t min_id = io_thread_id_t(0);
    all_reactors([&min_id, &min_cnt](IOReactor* reactor) {
        if (!reactor || !reactor->is_iomgr_thread()) { return; }
        if (reactor->m_count < min_cnt) {
            min_id = reactor->my_io_thread_id();
            min_cnt = reactor->m_count;
        }
    });
    return min_id;
}
#endif

bool IOManager::send_msg(io_thread_id_t to_thread, iomgr_msg* msg) {
    bool ret = false;

    if (std::holds_alternative< spdk_thread* >(to_thread)) {
        // Shortcut to deliver the message without taking reactor list lock.
        IOReactorSPDK::deliver_to_thread(std::get< spdk_thread* >(to_thread), msg);
        ret = true;
    } else {
        specific_reactor(std::get< int >(to_thread), [&msg, &ret](IOReactor* reactor) {
            if (reactor && reactor->is_io_thread() && reactor->deliver_msg(msg)) { ret = true; }
        });
    }
    return ret;
}

uint8_t* IOManager::iobuf_alloc(size_t align, size_t size) {
    size = sisl::round_up(size, align);
    return m_is_tloop ? (uint8_t*)spdk_malloc(size, align, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA)
                      : (uint8_t*)std::aligned_alloc(align, size);
}

sisl::aligned_unique_ptr< uint8_t > IOManager::iobuf_alloc_unique(size_t align, size_t size) {
    return m_is_tloop ? nullptr : sisl::make_aligned_unique< uint8_t >(align, size);
}

std::shared_ptr< uint8_t > IOManager::iobuf_alloc_shared(size_t align, size_t size) {
    return m_is_tloop ? nullptr : sisl::make_aligned_shared< uint8_t >(align, size);
}

void IOManager::iobuf_free(uint8_t* buf) { m_is_tloop ? spdk_free((void*)buf) : free(buf); }

void IOManager::foreach_iodevice(std::function< void(const io_device_ptr&) > iodev_cb) {
    m_iodev_map.withRLock([&](auto& iodevs) {
        for (auto& iodev : iodevs) {
            iodev_cb(iodev.second);
        }
    });
}

void IOManager::foreach_interface(std::function< void(IOInterface*) > iface_cb) {
    m_iface_list.withRLock([&](auto& iface_list) {
        for (auto iface : iface_list) {
            iface_cb(iface.get());
        }
    });
}

IOReactor* IOManager::this_reactor() const { return m_reactors.get()->get(); }
void IOManager::all_reactors(const std::function< void(IOReactor* reactor) >& cb) {
    m_reactors.access_all_threads([&cb](std::unique_ptr< IOReactor >* preactor) { cb(preactor->get()); });
}

void IOManager::specific_reactor(int thread_num, const std::function< void(IOReactor* reactor) >& cb) {
    m_reactors.access_specific_thread(thread_num,
                                      [&cb](std::unique_ptr< IOReactor >* preactor) { cb(preactor->get()); });
}

msg_module_id_t IOManager::register_msg_module(const msg_handler_t& handler) {
    std::unique_lock lk(m_msg_hdlrs_mtx);
    DEBUG_ASSERT_LT(m_msg_handlers_count, m_msg_handlers.size(), "More than expected msg modules registered");
    m_msg_handlers[m_msg_handlers_count++] = handler;
    return m_msg_handlers_count - 1;
}

// It is ok not to take a lock to get msg modules, since we don't support unregister a module. Taking a lock
// here defeats the purpose of per thread messages here.
msg_handler_t& IOManager::get_msg_module(msg_module_id_t id) { return m_msg_handlers[id]; }

io_thread_id_t IOManager::my_io_thread_id() const { return this_reactor()->my_io_thread_id(); };

std::string io_device_t::dev_id() {
    if (std::holds_alternative< int >(dev)) {
        return std::to_string(fd());
    } else if (std::holds_alternative< spdk_bdev_desc* >(dev)) {
        return spdk_bdev_get_name(bdev());
    } else {
        return "";
    }
}

spdk_bdev_desc* io_device_t::bdev_desc() { return std::get< spdk_bdev_desc* >(dev); }
spdk_bdev* io_device_t::bdev() { return spdk_bdev_desc_get_bdev(bdev_desc()); }

} // namespace iomgr

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
#include <cerrno>
#include <fstream>
#include <functional>
#include <random>
#include <thread>
#include <vector>

#ifdef __FreeBSD__
#include <pthread_np.h>
#endif

#include <sisl/fds/obj_allocator.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/utility/thread_factory.hpp>
#include <sisl/version.hpp>

#include <iomgr/iomgr.hpp>
#include "iomgr_impl.hpp"
#ifdef WITH_SPDK
#include "spdk/iomgr_impl_spdk.hpp"
#endif
#include "epoll/iomgr_impl_epoll.hpp"
#include "interfaces/aio_drive_interface.hpp"
#ifdef WITH_SPDK
#include "interfaces/spdk_drive_interface.hpp"
#endif
#include "interfaces/uring_drive_interface.hpp"

#include "iomgr_helper.hpp"
#include "iomgr_config.hpp"
#include "epoll/reactor_epoll.hpp"
#ifdef WITH_SPDK
#include "spdk/reactor_spdk.hpp"

// Must be included after sisl headers to avoid macro definition clash
extern "C" {
#include <spdk/log.h>
#include <spdk/env.h>
#include <spdk/thread.h>
#include <spdk/bdev.h>
#include <spdk/env_dpdk.h>
#include <spdk/init.h>
#include <spdk/rpc.h>
#include <rte_errno.h>
#include <rte_mempool.h>
#include <rte_malloc.h>
}
#endif

SISL_OPTION_GROUP(iomgr,
                  (iova_mode, "", "iova-mode", "IO Virtual Address mode ['pa'|'va']",
                   ::cxxopts::value< std::string >()->default_value("pa"), "mode"),
                  (hdd_streams, "", "hdd_streams", "Number of streams for hdd - overridden value",
                   ::cxxopts::value< uint32_t >(), "count"),
                  (encryption, "", "encryption", "Turn on encryption", cxxopts::value< bool >(), "true or false"),
                  (authorization, "", "authorization", "Turn on authorization", cxxopts::value< bool >(),
                   "true or false"))

namespace iomgr {

IOManager::IOManager() : m_thread_idx_reserver(max_io_threads) { m_iface_list.reserve(inbuilt_interface_count + 5); }

IOManager::~IOManager() = default;

void IOManager::start(const iomgr_params& params, const thread_state_notifier_t& notifier,
                      interface_adder_t&& iface_adder) {
    if (get_state() == iomgr_state::running) {
        LOGWARN("WARNING: IOManager is asked to start, but it is already in running state. Ignoring the start request");
        return;
    }

    // Prepare all the parameters (overridden, from config or default)
    sisl::VersionMgr::addVersion(PACKAGE_NAME, version::Semver200_version(PACKAGE_VERSION));
    m_is_spdk = params.is_spdk;
    if (params.num_threads == 0) {
        m_num_workers = IM_DYNAMIC_CONFIG(thread.num_workers);
        if (auto quota = get_cpu_quota(); quota > 0) { m_num_workers = std::min(m_num_workers, quota); }
    } else {
        // Caller has overridden the thread count
        m_num_workers = params.num_threads;
    }
    m_mem_size_limit = ((params.app_mem_size_mb == 0) ? get_app_mem_limit() : params.app_mem_size_mb) * Mi;
    if (m_is_spdk) {
        m_hugepage_limit = ((params.hugepage_size_mb == 0) ? get_hugepage_limit() : params.hugepage_size_mb) * Mi;
    }

    // Setup the app memory throttling
    m_mem_soft_threshold_size = IM_DYNAMIC_CONFIG(iomem.soft_mem_release_threshold) * m_mem_size_limit / 100;
    m_mem_aggressive_threshold_size =
        IM_DYNAMIC_CONFIG(iomem.aggressive_mem_release_threshold) * m_mem_size_limit / 100;
    sisl::set_memory_release_rate(IM_DYNAMIC_CONFIG(iomem.mem_release_rate));

    LOGINFO("Starting IOManager version {} with {} threads [is_spdk={}] [mem_limit={}, hugepage={}]", PACKAGE_VERSION,
            m_num_workers, m_is_spdk, in_bytes(m_mem_size_limit), in_bytes(m_hugepage_limit));

    // m_expected_ifaces += expected_custom_ifaces;
    m_yet_to_start_nreactors.set(m_num_workers);
    m_worker_reactors.reserve(m_num_workers * 2); // Have preallocate for iomgr slots
    m_worker_threads.reserve(m_num_workers * 2);

    // One common module and other internal handler
    m_common_thread_state_notifier = notifier;
    m_internal_msg_module_id = register_msg_module([this](iomgr_msg* msg) { this_reactor()->handle_msg(msg); });

    // Initialize the impl module
    if (m_is_spdk) {
        m_impl = std::make_unique< IOManagerSpdkImpl >(m_hugepage_limit);
    } else {
        m_impl = std::make_unique< IOManagerEpollImpl >();
    }

    // Do Poller specific pre interface initialization
    m_impl->pre_interface_init();
    m_is_uring_capable = check_uring_capability();
    LOGINFOMOD(iomgr, "System has uring_capability={}", m_is_uring_capable);

    // Create all in-built interfaces here
    set_state(iomgr_state::interface_init);
    m_default_general_iface = std::make_shared< GenericIOInterface >();
    add_interface(m_default_general_iface);

    // If caller wants to add the interface by themselves, allow to do so, else add drive interface by ourselves
    if (iface_adder) {
        iface_adder();
    } else {
        if (m_is_uring_capable && !m_is_spdk) {
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(std::make_shared< UringDriveInterface >()));
        } else {
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(std::make_shared< AioDriveInterface >()));
        }

        if (m_is_spdk) {
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(std::make_shared< SpdkDriveInterface >()));
        }
    }

    // Start all reactor threads
    set_state(iomgr_state::reactor_init);
    create_reactors();
    wait_for_state(iomgr_state::sys_init);

    // Start the global timer
    m_global_user_timer = std::make_unique< timer_epoll >(thread_regex::all_user);
    m_global_worker_timer = m_is_spdk ? std::unique_ptr< timer >(new timer_spdk(thread_regex::all_worker))
                                      : std::unique_ptr< timer >(new timer_epoll(thread_regex::all_worker));
    m_rand_worker_distribution = std::uniform_int_distribution< size_t >(0, m_worker_reactors.size() - 1);

    m_impl->post_interface_init();
    set_state(iomgr_state::running);
    LOGINFO("IOManager is ready and move to running state");

    // Notify all the reactors that they are ready to make callback about thread started
    iomanager.run_on(thread_regex::all_io,
                     [this](io_thread_addr_t taddr) { iomanager.this_reactor()->notify_thread_state(true); });

    m_io_wd = std::make_unique< IOWatchDog >();
}

void IOManager::stop() {
    LOGINFO("Stopping IOManager");

    m_impl->pre_interface_stop();
    set_state(iomgr_state::stopping);

    // Increment stopping threads by 1 and then decrement after sending message to prevent case where there are no
    // IO threads, which hangs the iomanager stop
    m_yet_to_stop_nreactors.increment();

    // Free up and unregister fds for global timer
    m_global_user_timer.reset(nullptr);
    m_global_worker_timer.reset(nullptr);

    // Send all the threads to reliquish its io thread status
    auto smsg = sync_iomgr_msg::create(iomgr_msg_type::RELINQUISH_IO_THREAD, m_internal_msg_module_id);
    multicast_msg_and_wait(thread_regex::all_io, std::dynamic_pointer_cast< sync_msg_base >(smsg));

    // Now decrement and check if all io threads have already reliquished the io thread status.
    if (m_yet_to_stop_nreactors.decrement_testz()) {
        set_state(iomgr_state::stopped);
    } else {
        // Few threads are still in process of coming out io loop, wait for them.
        wait_for_state(iomgr_state::stopped);
    }

    LOGINFO("All IO threads have stopped and hence IOManager is moved to stopped state, joining any iomanager threads");

    try {
        // Join all the iomanager threads
        for (auto& thr : m_worker_threads) {
            if (std::holds_alternative< std::thread >(thr)) {
                auto& t = std::get< std::thread >(thr);
                if (t.joinable()) { t.join(); }
            }
        }
    } catch (const std::exception& e) { LOGCRITICAL_AND_FLUSH("Caught exception {} during thread join", e.what()); }

    try {
        m_worker_reactors.clear();
        m_worker_threads.clear();
        m_yet_to_start_nreactors.set(0);
        // m_expected_ifaces = inbuilt_interface_count;
        m_default_general_iface.reset();
        // m_default_grpc_iface.reset();
        m_drive_ifaces.clear();
        m_iface_list.clear();
    } catch (const std::exception& e) { LOGCRITICAL_AND_FLUSH("Caught exception {} during clear lists", e.what()); }
    assert(get_state() == iomgr_state::stopped);

    m_impl->post_interface_stop();
    m_impl.reset();
    LOGINFO("IOManager Stopped and all IO threads are relinquished");
}

void IOManager::create_reactors() {
    // First populate the full sparse vector of m_worker_reactors before starting workers.
    for (uint32_t i{0}; i < m_num_workers; ++i) {
        m_worker_reactors.push_back(nullptr);
    }

    for (uint32_t i{0}; i < m_num_workers; ++i) {
        m_worker_threads.emplace_back(m_impl->create_reactor(fmt::format("iomgr_thread_{}", i),
                                                             m_is_spdk ? TIGHT_LOOP : INTERRUPT_LOOP, (int)i, nullptr));
        LOGDEBUGMOD(iomgr, "Created iomanager worker reactor thread {}...", i);
    }
}

void IOManager::create_reactor(const std::string& name, loop_type_t loop_type, thread_state_notifier_t&& notifier) {
    m_impl->create_reactor(name, loop_type, -1, std::move(notifier));
}

void IOManager::become_user_reactor(loop_type_t loop_type, const iodev_selector_t& iodev_selector,
                                    thread_state_notifier_t&& addln_notifier) {
    _run_io_loop(-1, loop_type, "", std::move(iodev_selector), std::move(addln_notifier));
}

extern const version::Semver200_version get_version() { return version::Semver200_version(PACKAGE_VERSION); }

void IOManager::add_drive_interface(shared< DriveInterface > iface, thread_regex iface_scope) {
    add_interface(std::dynamic_pointer_cast< IOInterface >(iface), iface_scope);
    m_drive_ifaces.push_back(iface);
}

void IOManager::drive_interface_submit_batch() {
    for (auto& iface : m_drive_ifaces) {
        iface->submit_batch();
    }
}

shared< DriveInterface > IOManager::get_drive_interface(const drive_interface_type type) {
    if ((type == drive_interface_type::spdk) && !m_is_spdk) {
        LOGERRORMOD(iomgr, "Attempting to access spdk's drive interface on non-spdk mode");
        return nullptr;
    }
    for (auto& iface : m_drive_ifaces) {
        if (iface->interface_type() == type) { return iface; }
    }
    LOGERRORMOD(iomgr, "Unable to find drive interfaces of type {}", type);
    return nullptr;
}

void IOManager::add_interface(shared< IOInterface > iface, thread_regex iface_scope) {
    LOGINFOMOD(iomgr, "Adding new interface={} to thread_scope={}", (void*)iface.get(), enum_name(iface_scope));

    // Setup the reactor io threads to do any registration for interface specific registration
    {
        std::unique_lock lg(m_iface_list_mtx);
        m_iface_list.push_back(iface);
    }
    iface->set_scope(iface_scope);

    const auto sent_count = iomanager.run_on(
        iface_scope,
        [this, iface](io_thread_addr_t taddr) {
            iface->on_io_thread_start(iomanager.this_reactor()->addr_to_thread(taddr));
        },
        wait_type_t::sleep);

    // TODO: Removed the code to Metrics Mempool populate from here. NEED TO VALIDATE IF WE NEED THIS IN CASE
    // spdk is already inited
    LOGINFOMOD(iomgr, "Interface={} added to {} threads, total_interfaces={}", (void*)iface.get(), sent_count,
               m_iface_list.size());
}

void IOManager::remove_interface(cshared< IOInterface >& iface) {
    LOGINFOMOD(iomgr, "Removing interface={} from thread_scope={}", (void*)iface.get(), enum_name(iface->scope()));
    {
        std::unique_lock lg(m_iface_list_mtx);
        m_iface_list.erase(std::remove(m_iface_list.begin(), m_iface_list.end(), iface), m_iface_list.end());
    }

    const auto sent_count = iomanager.run_on(
        iface->scope(),
        [this, iface](io_thread_addr_t taddr) {
            iface->on_io_thread_stopped(iomanager.this_reactor()->addr_to_thread(taddr));
        },
        wait_type_t::sleep);

    LOGINFOMOD(iomgr, "Interface={} removed from {} threads, total_interfaces={}", (void*)iface.get(), sent_count,
               m_iface_list.size());
}

void IOManager::_run_io_loop(int iomgr_slot_num, loop_type_t loop_type, const std::string& name,
                             const iodev_selector_t& iodev_selector, thread_state_notifier_t&& addln_notifier) {
    loop_type_t ltype = loop_type;

    shared< IOReactor > reactor;
    if (m_is_spdk && (loop_type & TIGHT_LOOP)) {
        ltype = (loop_type & ~INTERRUPT_LOOP);
        reactor = std::make_shared< IOReactorSPDK >();
    } else {
        ltype = (loop_type & ~TIGHT_LOOP) | INTERRUPT_LOOP;
        reactor = std::make_shared< IOReactorEPoll >();
    }
    *(m_reactors.get()) = reactor;
    reactor->run(iomgr_slot_num, ltype, name, iodev_selector, std::move(addln_notifier));
}

void IOManager::stop_io_loop() { this_reactor()->stop(); }

void IOManager::reactor_started(shared< IOReactor > reactor) {
    m_yet_to_stop_nreactors.increment();
    if (reactor->is_worker()) {
        m_worker_reactors[reactor->m_worker_slot_num] = reactor;
        reactor->notify_thread_state(true);

        // All iomgr created reactors are initialized, move iomgr to sys init (next phase of start)
        if (m_yet_to_start_nreactors.decrement_testz()) {
            LOGINFO("All Worker reactors started, moving iomanager to sys_init state");
            set_state_and_notify(iomgr_state::sys_init);
        }
    } else {
        reactor->notify_thread_state(true);
    }
}

void IOManager::reactor_stopped() {
    // Notify the caller registered to iomanager for it
    this_reactor()->notify_thread_state(false /* started */);

    // stopped state is set last
    if (m_yet_to_stop_nreactors.decrement_testz()) { set_state_and_notify(iomgr_state::stopped); }
}

bool IOManager::am_i_io_reactor() const {
    auto* r{this_reactor()};
    return r && r->is_io_reactor();
}

bool IOManager::am_i_tight_loop_reactor() const {
    auto* r{this_reactor()};
    return r && r->is_tight_loop_reactor();
}

bool IOManager::am_i_worker_reactor() const {
    auto* r{this_reactor()};
    return r && r->is_worker();
}

bool IOManager::am_i_adaptive_reactor() const {
    auto* r{this_reactor()};
    return r && r->is_adaptive_loop();
}

void IOManager::set_my_reactor_adaptive(bool adaptive) {
    auto* r{this_reactor()};
    if (r) { r->set_adaptive_loop(adaptive); }
}

void IOManager::device_reschedule(const io_device_ptr& iodev, int event) {
    multicast_msg(thread_regex::least_busy_worker,
                  iomgr_msg::create(iomgr_msg_type::RESCHEDULE, m_internal_msg_module_id, iodev, event));
}

static bool match_regex(thread_regex r, const io_thread_t& thread) {
    if ((r == thread_regex::all_io) || (r == thread_regex::least_busy_io)) { return true; }
    if (r == thread_regex::all_tloop) { return thread->reactor->is_tight_loop_reactor(); }
    if (thread->reactor->is_worker()) {
        return ((r == thread_regex::all_worker) || (r == thread_regex::least_busy_worker) ||
                (r == thread_regex::random_worker));
    } else {
        return ((r == thread_regex::all_user) || (r == thread_regex::least_busy_user));
    }
}

int IOManager::run_on(const io_thread_t& thread, spdk_msg_signature_t fn, void* context) {
    assert(thread->reactor->is_tight_loop_reactor());
    spdk_thread_send_msg(thread->spdk_thread_impl(), fn, context);
    return 1;
}

int IOManager::multicast_msg(thread_regex r, iomgr_msg* msg) {
    int sent_to = 0;
    bool cloned = false;
    int64_t min_cnt = std::numeric_limits< int64_t >::max();
    io_thread_addr_t min_thread = -1U;
    IOReactor* min_reactor = nullptr;
    IOReactor* sender_reactor = iomanager.this_reactor();

    static thread_local std::random_device s_rd{};
    static thread_local std::default_random_engine s_re{s_rd()};

    if (r == thread_regex::random_worker) {
        // Send to any random iomgr created io thread
        auto& reactor = m_worker_reactors[m_rand_worker_distribution(s_re)];
        sent_to = reactor->deliver_msg(reactor->select_thread()->thread_addr, msg, sender_reactor) ? 1 : 0;
    } else {
        _pick_reactors(r, [&](IOReactor* reactor, bool is_last_thread) {
            if (reactor && reactor->is_io_reactor()) {
                for (auto& thr : reactor->io_threads()) {
                    if (match_regex(r, thr)) {
                        if ((r == thread_regex::least_busy_worker) || (r == thread_regex::least_busy_user)) {
                            if (reactor->m_metrics->outstanding_ops < min_cnt) {
                                min_thread = thr->thread_addr;
                                min_cnt = reactor->m_metrics->outstanding_ops;
                                min_reactor = reactor;
                            }
                        } else {
                            auto* new_msg = msg->clone();
                            if (reactor->deliver_msg(thr->thread_addr, new_msg, sender_reactor)) {
                                cloned = true;
                                ++sent_to;
                            } else {
                                // failed to deliver cleanup resources
                                iomgr_msg::free(new_msg);
                            }
                        }
                    }
                }
            }

            if (is_last_thread && min_reactor) {
                if (min_reactor->deliver_msg(min_thread, msg, sender_reactor)) ++sent_to;
            }
        });
    }

    if ((cloned || (sent_to == 0)) && !msg->is_sync_msg()) { iomgr_msg::free(msg); }
    return sent_to;
}

void IOManager::_pick_reactors(thread_regex r, const auto& cb) {
    if ((r == thread_regex::all_worker) || (r == thread_regex::least_busy_worker)) {
        for (size_t i{0}; i < m_worker_reactors.size(); ++i) {
            cb(m_worker_reactors[i].get(), (i == (m_worker_reactors.size() - 1)));
        }
    } else {
        all_reactors(cb);
    }
}

int IOManager::multicast_msg_and_wait(thread_regex r, cshared< sync_msg_base >& smsg) {
    const auto sent_to{multicast_msg(r, smsg->base_msg())};
    if (sent_to != 0) { smsg->wait(); }
    smsg->free_base_msg();
    return sent_to;
}

bool IOManager::send_msg(const io_thread_t& to_thread, iomgr_msg* msg) {
    bool ret = false;
    msg->m_dest_thread = to_thread->thread_addr;

    if (std::holds_alternative< spdk_thread* >(to_thread->thread_impl)) {
        // Shortcut to deliver the message without taking reactor list lock.
        IOReactorSPDK::deliver_msg_direct(std::get< spdk_thread* >(to_thread->thread_impl), msg);
        ret = true;
    } else {
        IOReactor* sender_reactor = iomanager.this_reactor();
        specific_reactor(std::get< reactor_idx_t >(to_thread->thread_impl),
                         [&ret, &to_thread, msg, sender_reactor](IOReactor* reactor) {
                             if (reactor && reactor->is_io_reactor() &&
                                 reactor->deliver_msg(to_thread->thread_addr, msg, sender_reactor)) {
                                 ret = true;
                             }
                         });
    }

    if (!ret && !msg->is_sync_msg()) { iomgr_msg::free(msg); }
    return ret;
}

bool IOManager::send_msg_and_wait(const io_thread_t& to_thread, cshared< sync_msg_base >& smsg) {
    const auto sent{send_msg(to_thread, smsg->base_msg())};
    if (sent) { smsg->wait(); }
    smsg->free_base_msg();
    return sent;
}

void sync_msg_base::free_base_msg() { iomgr_msg::free(m_base_msg); }

void spin_iomgr_msg::set_sender_thread() {
    if (!iomanager.am_i_io_reactor()) {
        LOGDFATAL("Spin messages can only be issued from io thread");
        return;
    }
    m_sender_thread = iomanager.iothread_self();
}

void spin_iomgr_msg::one_completion() {
    if (m_pending.decrement_testz()) {
        // Send reply msg here
        m_base_msg->m_is_reply = true;
        iomanager.send_msg(m_sender_thread, m_base_msg);
    }
}

void spin_iomgr_msg::wait() {
    // Check if messages delivered to all threads before wait called, then no need to spin
    if (!m_pending.decrement_testz()) {
        // Spin until we receive some reply message
        while (!m_reply_rcvd) {
            iomanager.this_reactor()->listen();
        }
    }
}

timer_handle_t IOManager::schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                                timer_callback_t&& timer_fn) {
    return this_reactor()->m_thread_timer->schedule(nanos_after, recurring, cookie, std::move(timer_fn));
}

timer_handle_t IOManager::schedule_global_timer(uint64_t nanos_after, bool recurring, void* cookie, thread_regex r,
                                                timer_callback_t&& timer_fn, bool wait_to_schedule) {
    timer* t = nullptr;
    if (r == thread_regex::all_worker) {
        t = m_global_worker_timer.get();
    } else if (r == thread_regex::all_user) {
        t = m_global_user_timer.get();
    } else {
        LOGMSG_ASSERT(0, "Setting timer with invalid regex {}", enum_name(r));
        return null_timer_handle;
    }

    return t->schedule(nanos_after, recurring, cookie, std::move(timer_fn), wait_to_schedule);
}

void IOManager::cancel_timer(timer_handle_t thdl, bool wait_to_cancel) {
    return thdl.first->cancel(thdl, wait_to_cancel);
}

void IOManager::set_poll_interval(const int interval) { this_reactor()->set_poll_interval(interval); }
int IOManager::get_poll_interval() const { return this_reactor()->get_poll_interval(); }

void IOManager::foreach_interface(const interface_cb_t& iface_cb) {
    std::shared_lock lg(m_iface_list_mtx);
    for (auto& iface : m_iface_list) {
        iface_cb(iface);
    }
}

IOReactor* IOManager::this_reactor() const { return IOReactor::this_reactor; }

IOThreadMetrics& IOManager::this_thread_metrics() {
    if (this_reactor() != nullptr) {
        return this_reactor()->thread_metrics();
    } else {
        static std::atomic< uint32_t > s_count{0};
        static thread_local std::unique_ptr< IOThreadMetrics > s_m =
            std::make_unique< IOThreadMetrics >(fmt::format("NonIOThread-{}", ++s_count));
        return *s_m;
    }
}

void IOManager::all_reactors(const auto& cb) {
    m_reactors.access_all_threads(
        [&cb](shared< IOReactor >* preactor, bool is_last_thread) { cb(preactor->get(), is_last_thread); });
}

void IOManager::specific_reactor(int thread_num, const auto& cb) {
    if ((thread_num == (int)sisl::ThreadLocalContext::my_thread_num()) && (this_reactor() != nullptr)) {
        cb(iomanager.this_reactor());
    } else {
        m_reactors.access_specific_thread(thread_num, [&cb](shared< IOReactor >* preactor) { cb(preactor->get()); });
    }
}

IOReactor* IOManager::round_robin_reactor() const {
    static std::atomic< size_t > s_idx{0};
    do {
        const size_t idx{s_idx.fetch_add(1, std::memory_order_relaxed) % m_worker_reactors.size()};
        if (m_worker_reactors[idx] != nullptr) { return m_worker_reactors[idx].get(); }
    } while (true);
}

msg_module_id_t IOManager::register_msg_module(const msg_handler_t& handler) {
    std::unique_lock lk(m_msg_hdlrs_mtx);
    DEBUG_ASSERT_LT(m_msg_handlers_count, m_msg_handlers.size(), "More than expected msg modules registered");
    m_msg_handlers[m_msg_handlers_count++] = handler;
    return m_msg_handlers_count - 1;
}

io_thread_t IOManager::make_io_thread(IOReactor* reactor) {
    io_thread_t t = std::make_shared< io_thread >(reactor);
    t->thread_idx = m_thread_idx_reserver.reserve();
    if (t->thread_idx >= max_io_threads) {
        throw std::system_error(errno, std::generic_category(), "Running IO Threads exceeds limit");
    }
    return t;
}

// It is ok not to take a lock to get msg modules, since we don't support unregister a module. Taking a lock
// here defeats the purpose of per thread messages here.
msg_handler_t& IOManager::get_msg_module(msg_module_id_t id) { return m_msg_handlers[id]; }

const io_thread_t& IOManager::iothread_self() const { return this_reactor()->iothread_self(); };

/////////////////// IOManager Memory Management APIs ///////////////////////////////////
uint8_t* IOManager::iobuf_alloc(size_t align, size_t size, const sisl::buftag tag) {
    return sisl::AlignedAllocator::allocator().aligned_alloc(align, size, tag);
}

void IOManager::iobuf_free(uint8_t* buf, const sisl::buftag tag) {
    sisl::AlignedAllocator::allocator().aligned_free(buf, tag);
}

uint8_t* IOManager::iobuf_pool_alloc(size_t align, size_t size, const sisl::buftag tag) {
    return sisl::AlignedAllocator::allocator().aligned_pool_alloc(align, size, tag);
}

void IOManager::iobuf_pool_free(uint8_t* buf, size_t size, const sisl::buftag tag) {
    sisl::AlignedAllocator::allocator().aligned_pool_free(buf, size, tag);
}

size_t IOManager::iobuf_size(uint8_t* buf) const { return sisl::AlignedAllocator::allocator().buf_size(buf); }

/////////////////// IODevice class implementation ///////////////////////////////////
IODevice::IODevice(const int p, const thread_specifier scope) : thread_scope{scope}, pri{p} {
    m_iodev_thread_ctx.reserve(IOManager::max_io_threads);
    creator = iomanager.am_i_io_reactor() ? iomanager.iothread_self() : nullptr;
}

std::string IODevice::dev_id() const {
    if (std::holds_alternative< int >(dev)) {
        return std::to_string(fd());
    } else if (std::holds_alternative< spdk_bdev_desc* >(dev)) {
        return spdk_bdev_get_name(bdev());
    } else {
        return "";
    }
}

spdk_bdev_desc* IODevice::bdev_desc() const { return std::get< spdk_bdev_desc* >(dev); }
spdk_bdev* IODevice::bdev() const { return spdk_bdev_desc_get_bdev(bdev_desc()); }
spdk_nvmf_qpair* IODevice::nvmf_qp() const { return std::get< spdk_nvmf_qpair* >(dev); }

bool IODevice::is_global() const { return (!std::holds_alternative< io_thread_t >(thread_scope)); }
bool IODevice::is_my_thread_scope() const {
    return (!is_global() && (per_thread_scope() == iomanager.iothread_self()));
}

void IODevice::clear() {
    dev = -1;
    tinfo = nullptr;
    cookie = nullptr;
    m_iodev_thread_ctx.clear();
}

DriveInterface* IODevice::drive_interface() { return static_cast< DriveInterface* >(io_interface); }
} // namespace iomgr

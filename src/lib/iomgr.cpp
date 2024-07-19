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
#include "watchdog.hpp"
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
                   ::cxxopts::value< uint32_t >()->default_value("64"), "count"))

namespace iomgr {

IOManager::IOManager() : m_fiber_ordinal_reserver(IOManager::max_io_fibers) {
    m_iface_list.reserve(inbuilt_interface_count + 5);
}

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

    // Initialize the impl module
    if (m_is_spdk) {
        m_impl = std::make_unique< IOManagerSpdkImpl >(m_hugepage_limit);
    } else {
        m_impl = std::make_unique< IOManagerEpollImpl >();
    }

    // Do Poller specific pre interface initialization
    m_impl->pre_interface_init();
    bool new_interface_supported = false;
    m_is_uring_capable = check_uring_capability(new_interface_supported);
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
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(
                std::make_shared< UringDriveInterface >(new_interface_supported)));
        } else {
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(std::make_shared< AioDriveInterface >()));
        }

        if (m_is_spdk) {
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(std::make_shared< SpdkDriveInterface >()));
        }
    }

    // Start all reactor threads
    set_state(iomgr_state::reactor_init);

    // Caller can override the number of fibers per thread; o.w., it is taken from dynamic config
    create_worker_reactors((0 < params.num_fibers) ? params.num_fibers : IM_DYNAMIC_CONFIG(thread.num_fibers));
    wait_for_state(iomgr_state::sys_init);

    // Start the global timer
    m_global_user_timer = std::make_unique< timer_epoll >(reactor_regex::all_user);
    m_global_worker_timer = m_is_spdk ? std::unique_ptr< timer >(new timer_spdk(reactor_regex::all_worker))
                                      : std::unique_ptr< timer >(new timer_epoll(reactor_regex::all_worker));
    m_rand_worker_distribution = std::uniform_int_distribution< size_t >(0, m_worker_reactors.size() - 1);

    m_impl->post_interface_init();
    set_state(iomgr_state::running);
    LOGINFO("IOManager is ready and move to running state");

    // Notify all the reactors that they are ready to make callback about thread started
    iomanager.run_on_forget(reactor_regex::all_io, [this]() { iomanager.this_reactor()->notify_thread_state(true); });

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

    // Wait for all pending timers cancellation to finish
    timer::wait_for_pending();

    // Send all the threads to reliquish its io thread status
    run_on_wait(reactor_regex::all_io, [this]() { this_reactor()->stop(); });

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

void IOManager::create_worker_reactors(uint32_t num_fibers) {
    // First populate the full sparse vector of m_worker_reactors before starting workers.
    for (uint32_t i{0}; i < m_num_workers; ++i) {
        m_worker_reactors.push_back(nullptr);
    }

    for (uint32_t i{0}; i < m_num_workers; ++i) {
        m_worker_threads.emplace_back(m_impl->create_reactor_impl(
            fmt::format("iomgr_thread_{}", i), m_is_spdk ? TIGHT_LOOP : INTERRUPT_LOOP, num_fibers, (int)i, nullptr));
        LOGDEBUGMOD(iomgr, "Created iomanager worker reactor thread {}...", i);
    }
}

void IOManager::create_reactor(const std::string& name, loop_type_t loop_type, uint32_t num_fibers,
                               thread_state_notifier_t&& notifier) {
    m_impl->create_reactor_impl(name, loop_type, num_fibers, -1, std::move(notifier));
}

void IOManager::become_user_reactor(loop_type_t loop_type, uint32_t num_fibers, const iodev_selector_t& iodev_selector,
                                    thread_state_notifier_t&& addln_notifier) {
    _run_io_loop(-1, loop_type, num_fibers, "", std::move(iodev_selector), std::move(addln_notifier));
}

extern const version::Semver200_version get_version() { return version::Semver200_version(PACKAGE_VERSION); }

void IOManager::drive_interface_submit_batch() {
    for (auto& iface : m_drive_ifaces) {
        iface->submit_batch();
    }
}

void IOManager::add_interface(cshared< IOInterface >& iface, reactor_regex iface_scope) {
    LOGINFOMOD(iomgr, "Adding new interface={} to thread_scope={}", (void*)iface.get(), enum_name(iface_scope));

    // Setup the reactor io threads to do any registration for interface specific registration
    {
        std::unique_lock lg(m_iface_list_mtx);
        m_iface_list.push_back(iface);
    }
    iface->set_scope(iface_scope);

    iomanager.run_on_wait(iface_scope, [iface]() { iface->on_reactor_start(iomanager.this_reactor()); });

    // TODO: Removed the code to Metrics Mempool populate from here. NEED TO VALIDATE IF WE NEED THIS IN CASE
    // spdk is already inited
    LOGINFOMOD(iomgr, "Interface={} added, total_interfaces={}", (void*)iface.get(), m_iface_list.size());
}

shared< DriveInterface > IOManager::get_drive_interface(drive_interface_type type) {
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

void IOManager::remove_interface(cshared< IOInterface >& iface) {
    LOGINFOMOD(iomgr, "Removing interface={} from thread_scope={}", (void*)iface.get(), enum_name(iface->scope()));
    {
        std::unique_lock lg(m_iface_list_mtx);
        m_iface_list.erase(std::remove(m_iface_list.begin(), m_iface_list.end(), iface), m_iface_list.end());
    }

    iomanager.run_on_wait(iface->scope(), [iface]() { iface->on_reactor_stop(iomanager.this_reactor()); });

    LOGINFOMOD(iomgr, "Interface={} removed, total_interfaces={}", (void*)iface.get(), m_iface_list.size());
}

void IOManager::add_drive_interface(cshared< DriveInterface >& iface, reactor_regex iface_scope) {
    add_interface(std::dynamic_pointer_cast< IOInterface >(iface), iface_scope);
    m_drive_ifaces.push_back(iface);
}

void IOManager::foreach_interface(const interface_cb_t& iface_cb) {
    std::shared_lock lg(m_iface_list_mtx);
    for (auto& iface : m_iface_list) {
        iface_cb(iface);
    }
}

void IOManager::_run_io_loop(int iomgr_slot_num, loop_type_t loop_type, uint32_t num_fibers, const std::string& name,
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
    reactor->run(iomgr_slot_num, ltype, num_fibers, name, iodev_selector, std::move(addln_notifier));
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

////////////////////////////////// Message related section ////////////////////////////////
static bool match_regex(reactor_regex r, const IOReactor* reactor) {
    if ((r == reactor_regex::all_io) || (r == reactor_regex::least_busy_io)) { return true; }
    if (r == reactor_regex::all_tloop) { return reactor->is_tight_loop_reactor(); }
    if (reactor->is_worker()) {
        return ((r == reactor_regex::all_worker) || (r == reactor_regex::least_busy_worker) ||
                (r == reactor_regex::random_worker));
    } else {
        return ((r == reactor_regex::all_user) || (r == reactor_regex::least_busy_user));
    }
}

int IOManager::run_on_forget(io_fiber_t fiber, spdk_msg_signature_t fn, void* context) {
    assert(fiber->reactor->is_tight_loop_reactor());
    spdk_thread_send_msg(fiber->spdk_thr, fn, context);
    return 1;
}

int IOManager::send_msg(io_fiber_t fiber, iomgr_msg* msg) {
    int ret{0};
    if (fiber->spdk_thr) {
        fiber->reactor->deliver_msg(fiber, msg);
        ret = 1;
    } else {
        specific_reactor(fiber->reactor->reactor_idx(), [msg, &ret, &fiber](IOReactor* reactor) {
            if (reactor && reactor->is_io_reactor()) {
                reactor->deliver_msg(fiber, msg);
                ret = 1;
            }
        });
    }
    return ret;
}

int IOManager::send_msg_and_wait(io_fiber_t fiber, iomgr_waitable_msg* msg) {
    int ret{0};
    auto f = msg->m_promise.getFuture();
    if (send_msg(fiber, msg)) {
        f.get();
        ret = 1;
    }
    return ret;
}

static void append_future_if_needed(iomgr_msg* msg, std::vector< FiberManagerLib::Future< bool > >& out_future_list) {
    if (msg->need_reply()) { out_future_list.push_back((r_cast< iomgr_waitable_msg* >(msg))->m_promise.getFuture()); }
}

int IOManager::multicast_msg(reactor_regex rr, fiber_regex fr, iomgr_msg* msg,
                             std::vector< FiberManagerLib::Future< bool > >& out_future_list) {
    int sent_to = 0;
    out_future_list.clear();

    if (rr == reactor_regex::random_worker) {
        static thread_local std::random_device s_rd{};
        static thread_local std::default_random_engine s_re{s_rd()};

        // Send to any random iomgr created io fiber
        auto& reactor = m_worker_reactors[m_rand_worker_distribution(s_re)];
        append_future_if_needed(msg, out_future_list);
        reactor->deliver_msg(reactor->pick_fiber(fr), msg);
        ++sent_to;
    } else {
        struct param_ctx {
            reactor_regex rr;
            fiber_regex fr;
            iomgr_msg* msg;
            iomgr_msg* cloned_msg{nullptr};
            std::vector< FiberManagerLib::Future< bool > >& future_list;
            IOReactor* min_reactor = nullptr;
            int64_t min_cnt{std::numeric_limits< int64_t >::max()};

            param_ctx(reactor_regex r, fiber_regex f, iomgr_msg* m,
                      std::vector< FiberManagerLib::Future< bool > >& fl) :
                    rr{r}, fr{f}, msg{m}, future_list{fl} {}
        };

        param_ctx ctx{rr, fr, msg, out_future_list};
        _pick_reactors(rr, [&ctx, &sent_to](IOReactor* reactor, bool is_last_thread) {
            if (reactor && reactor->is_io_reactor()) {
                if (match_regex(ctx.rr, reactor)) {
                    if ((ctx.rr == reactor_regex::least_busy_worker) || (ctx.rr == reactor_regex::least_busy_user)) {
                        if (reactor->m_metrics->outstanding_ops < ctx.min_cnt) {
                            ctx.min_cnt = reactor->m_metrics->outstanding_ops;
                            ctx.min_reactor = reactor;
                        }
                    } else {
                        ctx.cloned_msg = ctx.msg->clone();
                        append_future_if_needed(ctx.msg, ctx.future_list);
                        reactor->deliver_msg(reactor->pick_fiber(ctx.fr), ctx.msg);
                        ctx.msg = ctx.cloned_msg;
                        ++sent_to;
                    }
                }
            }

            if (is_last_thread && ctx.min_reactor) {
                append_future_if_needed(ctx.msg, ctx.future_list);
                ctx.min_reactor->deliver_msg(ctx.min_reactor->pick_fiber(ctx.fr), ctx.msg);
                ++sent_to;
            }
        });

        if (ctx.cloned_msg != nullptr) {
            // In case we multicasted, we will always have the last message excess, free it
            iomgr_msg::free(ctx.cloned_msg);
        }

        if (sent_to == 0) { iomgr_msg::free(msg); }
    }
    return sent_to;
}

int IOManager::multicast_msg_and_wait(reactor_regex r, fiber_regex fr, iomgr_msg* in_msg) {
    std::vector< FiberManagerLib::Future< bool > > s_future_list;
    auto const count = multicast_msg(r, fr, in_msg, s_future_list);
    if (count) {
        for (auto& f : s_future_list) {
            f.get();
        }
    }
    return count;
}

void IOManager::_pick_reactors(reactor_regex r, const auto& cb) {
    if ((r == reactor_regex::all_worker) || (r == reactor_regex::least_busy_worker)) {
        for (size_t i{0}; i < m_worker_reactors.size(); ++i) {
            cb(m_worker_reactors[i].get(), (i == (m_worker_reactors.size() - 1)));
        }
    } else {
        all_reactors(cb);
    }
}

void IOManager::all_reactors(const auto& cb) {
    m_reactors.access_all_threads(
        [&cb](shared< IOReactor >* preactor, bool is_last_thread) { cb(preactor->get(), is_last_thread); });
}

void IOManager::specific_reactor(uint32_t thread_num, const auto& cb) {
    if ((thread_num == sisl::ThreadLocalContext::my_thread_num()) && (this_reactor() != nullptr)) {
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

////////////////////////////////// Timer code section ////////////////////////////////
timer_handle_t IOManager::schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                                timer_callback_t&& timer_fn) {
    return this_reactor()->m_thread_timer->schedule(nanos_after, recurring, cookie, std::move(timer_fn));
}

timer_handle_t IOManager::schedule_global_timer(uint64_t nanos_after, bool recurring, void* cookie, reactor_regex r,
                                                timer_callback_t&& timer_fn, bool wait_to_schedule) {
    timer* t = nullptr;
    if (r == reactor_regex::all_worker) {
        t = m_global_worker_timer.get();
    } else if (r == reactor_regex::all_user) {
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

//////////////////////////// Reactor/Fiber related methods ///////////////////////
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

io_fiber_t IOManager::iofiber_self() const { return this_reactor()->iofiber_self(); };

bool IOManager::am_i_io_reactor() const {
    auto* r = this_reactor();
    return r && r->is_io_reactor();
}

bool IOManager::am_i_tight_loop_reactor() const {
    auto* r = this_reactor();
    return r && r->is_tight_loop_reactor();
}

bool IOManager::am_i_worker_reactor() const {
    auto* r = this_reactor();
    return r && r->is_worker();
}

bool IOManager::am_i_adaptive_reactor() const {
    auto* r = this_reactor();
    return r && r->is_adaptive_loop();
}

bool IOManager::am_i_sync_io_capable() const {
    auto* r = this_reactor();
    return ((r == nullptr) || (r->iofiber_self() != r->main_fiber()) || !r->is_io_reactor());
}

void IOManager::set_my_reactor_adaptive(bool adaptive) {
    auto* r = this_reactor();
    if (r) { r->set_adaptive_loop(adaptive); }
}

std::vector< io_fiber_t > IOManager::sync_io_capable_fibers() const {
    auto* r = this_reactor();
    return r ? r->sync_io_capable_fibers() : std::vector< io_fiber_t >{};
}

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
IODevice::IODevice(int p, thread_specifier scope) : thread_scope{scope}, pri{p} {
    m_iodev_fiber_ctx.reserve(IOManager::max_io_fibers);
    creator = iomanager.am_i_io_reactor() ? iomanager.iofiber_self() : nullptr;
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

bool IODevice::is_global() const { return (!std::holds_alternative< io_fiber_t >(thread_scope)); }
bool IODevice::is_my_thread_scope() const { return (!is_global() && (reactor_scope() == iomanager.this_reactor())); }

io_fiber_t IODevice::fiber_scope() const { return std::get< io_fiber_t >(thread_scope); }
reactor_regex IODevice::global_scope() const { return std::get< reactor_regex >(thread_scope); }
IOReactor* IODevice::reactor_scope() const { return fiber_scope()->reactor; }

void IODevice::clear() {
    dev = -1;
    tinfo = nullptr;
    cookie = nullptr;
    m_iodev_fiber_ctx.clear();
}

DriveInterface* IODevice::drive_interface() { return static_cast< DriveInterface* >(io_interface); }

} // namespace iomgr

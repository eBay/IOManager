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
#include <spdk/bdev.h>
#include <spdk/env_dpdk.h>
#include <spdk/init.h>
#include <rte_errno.h>
#include <rte_mempool.h>
#include <rte_malloc.h>
}

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <cerrno>
#include <chrono>
#include <ctime>
#include <functional>
#include <thread>
#include <vector>
#include <filesystem>
#include <sys/mount.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <random>

#include <liburing.h>
#include <liburing/io_uring.h>

#include <sisl/utility/thread_factory.hpp>
#include <sisl/fds/obj_allocator.hpp>
#include <sisl/logging/logging.h>
#include <sisl/version.hpp>

#ifdef __linux__
#include <sys/prctl.h>
#include <sys/eventfd.h>
#endif

#ifdef __FreeBSD__
#include <pthread_np.h>
#endif

#ifdef __linux__
#include <sys/prctl.h>
#include <sys/eventfd.h>
#endif

#ifdef __FreeBSD__
#include <pthread_np.h>
#endif

#ifdef __linux__
#include <sys/prctl.h>
#include <sys/eventfd.h>
#endif

#ifdef __FreeBSD__
#include <pthread_np.h>
#endif

#include "include/aio_drive_interface.hpp"
#include "include/spdk_drive_interface.hpp"
#include "include/uring_drive_interface.hpp"
#include "include/iomgr.hpp"
#include "include/reactor_epoll.hpp"
#include "include/reactor_spdk.hpp"
#include "include/iomgr_config.hpp"

SISL_OPTION_GROUP(iomgr,
                  (iova_mode, "", "iova-mode", "IO Virtual Address mode ['pa'|'va']",
                   ::cxxopts::value< std::string >()->default_value("pa"), "mode"),
                  (hdd_streams, "", "hdd_streams", "Number of streams for hdd - overridden value",
                   ::cxxopts::value< uint32_t >(), "count"))

namespace iomgr {

static void set_thread_name(const char* thread_name) {
#if defined(__linux__)
    prctl(PR_SET_NAME, thread_name, 0, 0, 0);
#elif defined(__FreeBSD__)
    pthread_set_name_np(pthread_self(), thread_name);
#else
    pthread_setname_np(pthread_self(), thread_name);
#endif
}

static bool is_cpu_pinning_enabled() {
    if (auto quota_file = std::ifstream("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"); quota_file.is_open()) {
        double quota, period, shares;
        quota_file >> quota;
        if (auto period_file = std::ifstream("/sys/fs/cgroup/cpu/cpu.cfs_period_us"); period_file.is_open()) {
            period_file >> period;
            if (auto shares_file = std::ifstream("/sys/fs/cgroup/cpu/cpu.shares"); shares_file.is_open()) {
                shares_file >> shares;
                LOGINFOMOD(iomgr, "cfs_quota={} cfs_period={} shares={}", quota, period, shares);
                if ((quota / period) == (shares / 1024)) {
                    LOGINFOMOD(iomgr, "CPU Pinning is enabled in this host");
                    return true;
                }
            }
        }
    }
    LOGCRITICAL("WARNING: CPU Pinning is NOT enabled in this host, running multiple spdk threads could cause deadlock");
    return false;
}

IOManager::IOManager() : m_thread_idx_reserver(max_io_threads) { m_iface_list.reserve(inbuilt_interface_count + 5); }

IOManager::~IOManager() = default;

static bool check_uring_capability() {
    std::vector< int > ops = {IORING_OP_NOP,   IORING_OP_READV, IORING_OP_WRITEV,
                              IORING_OP_FSYNC, IORING_OP_READ,  IORING_OP_WRITE};

    bool supported{true};
    struct io_uring_probe* probe = io_uring_get_probe();
    if (probe == nullptr) { return false; }

    for (auto& op : ops) {
        if (!io_uring_opcode_supported(probe, op)) {
            supported = false;
            break;
        }
    }
    free(probe);
    return supported;
}

void IOManager::start(size_t const num_threads, bool is_spdk, const thread_state_notifier_t& notifier,
                      const interface_adder_t& iface_adder) {

    if (get_state() == iomgr_state::running) {
        LOGWARN("WARNING: IOManager is asked to start, but it is already in running state. Ignoring the start request");
        return;
    }

    IOMgrDynamicConfig::init_settings_default();
    sisl::VersionMgr::addVersion(PACKAGE_NAME, version::Semver200_version(PACKAGE_VERSION));
    LOGINFO("Starting IOManager version {} with {} threads [is_spdk={}]", PACKAGE_VERSION, num_threads, is_spdk);
    m_is_spdk = is_spdk;
    m_num_workers = num_threads;

    // m_expected_ifaces += expected_custom_ifaces;
    m_yet_to_start_nreactors.set(num_threads);
    m_worker_reactors.reserve(num_threads * 2); // Have preallocate for iomgr slots
    m_worker_threads.reserve(num_threads * 2);

    // One common module and other internal handler
    m_common_thread_state_notifier = notifier;
    m_internal_msg_module_id = register_msg_module([this](iomgr_msg* msg) { this_reactor()->handle_msg(msg); });

    // Start the SPDK
    bool init_bdev{false};
    if (is_spdk) {
        init_bdev = !is_spdk_inited();
        m_is_cpu_pinning_enabled = is_cpu_pinning_enabled();
        start_spdk();
    } else {
        sisl::AlignedAllocator::instance().set_allocator(std::move(new IOMgrAlignedAllocImpl()));
    }

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
        if (m_is_uring_capable) {
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(std::make_shared< UringDriveInterface >()));
        } else {
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(std::make_shared< AioDriveInterface >()));
        }

        if (is_spdk) {
            add_drive_interface(std::dynamic_pointer_cast< DriveInterface >(std::make_shared< SpdkDriveInterface >()));
        }
    }

    // Start all reactor threads
    set_state(iomgr_state::reactor_init);
    start_reactors();
    wait_for_state(iomgr_state::sys_init);

    // Start the global timer
    m_global_user_timer = std::make_unique< timer_epoll >(thread_regex::all_user);
    m_global_worker_timer = is_spdk ? std::unique_ptr< timer >(new timer_spdk(thread_regex::all_worker))
                                    : std::unique_ptr< timer >(new timer_epoll(thread_regex::all_worker));
    m_rand_worker_distribution = std::uniform_int_distribution< size_t >(0, m_worker_reactors.size() - 1);

    if (is_spdk && init_bdev) {
        LOGINFO("Initializing all spdk subsystems");
        iomanager.run_on(thread_regex::least_busy_worker, [this](io_thread_addr_t taddr) {
            spdk_subsystem_init(
                [](int rc, void* cb_arg) {
                    IOManager* pthis = (IOManager*)cb_arg;
                    pthis->mempool_metrics_populate();
                    pthis->set_state_and_notify(iomgr_state::running);
                },
                (void*)this);
        });
        wait_for_state(iomgr_state::running);
        m_spdk_reinit_needed = false;
    } else {
        set_state(iomgr_state::running);
    }
    LOGINFO("IOManager is ready and move to running state");

    // Notify all the reactors that they are ready to make callback about thread started
    iomanager.run_on(thread_regex::all_io,
                     [this](io_thread_addr_t taddr) { iomanager.this_reactor()->notify_thread_state(true); });

    m_io_wd = std::make_unique< IOWatchDog >();

} // namespace iomgr

static enum spdk_log_level to_spdk_log_level(spdlog::level::level_enum lvl) {
    switch (lvl) {
    case spdlog::level::level_enum::off:
        return SPDK_LOG_DISABLED;
    case spdlog::level::level_enum::critical:
    case spdlog::level::level_enum::err:
        return SPDK_LOG_ERROR;
    case spdlog::level::level_enum::warn:
        return SPDK_LOG_WARN;
    case spdlog::level::level_enum::info:
        return SPDK_LOG_NOTICE;
    case spdlog::level::level_enum::debug:
        return SPDK_LOG_INFO;
    case spdlog::level::level_enum::trace:
        return SPDK_LOG_DEBUG;
    default:
        return SPDK_LOG_NOTICE;
    }
}

constexpr std::string_view hugetlbfs_path = "/mnt/huge";
void IOManager::start_spdk() {
    /* Check if /mnt/huge already exists. Create otherwise */
    if (!std::filesystem::exists(std::string(hugetlbfs_path))) {
        std::error_code ec;
        if (!std::filesystem::create_directory(std::string(hugetlbfs_path), ec)) {
            if (ec.value()) {
                LOGERROR("Failed to create hugetlbfs. Error = {}", ec.message());
                throw std::runtime_error("Failed to create /mnt/huge");
            }
            LOGINFO("{} already exists.", std::string(hugetlbfs_path));
        } else {
            /* mount -t hugetlbfs nodev /mnt/huge */
            if (mount("nodev", std::string(hugetlbfs_path).data(), "hugetlbfs", 0, "")) {
                LOGERROR("Failed to mount hugetlbfs. Error = {}", errno);
                throw std::runtime_error("Hugetlbfs mount failed");
            }
            LOGINFO("Mounted hugepages on {}", std::string(hugetlbfs_path));
        }
    } else { /* Remove old/garbage hugepages from /mnt/huge */
        std::uintmax_t n = 0;
        for (const auto& entry : std::filesystem::directory_iterator(std::string(hugetlbfs_path))) {
            n += std::filesystem::remove_all(entry.path());
        }
        LOGINFO("Deleted {} old hugepages from {}", n, std::string(hugetlbfs_path));
    }

    // Set the spdk log level based on module spdk
    spdk_log_set_flag("all");
    // spdk_log_set_level(to_spdk_log_level(sisl::logging::GetModuleLogLevel("spdk")));
    spdk_log_set_print_level(to_spdk_log_level(sisl::logging::GetModuleLogLevel("spdk")));

    // Initialize if spdk has still not been initialized
    if (!is_spdk_inited()) {
        struct spdk_env_opts opts;
        struct spdk_env_opts* p_opts{nullptr};
        std::string corelist;
        std::string va_mode;
        if (!m_spdk_reinit_needed) {
            spdk_env_opts_init(&opts);
            opts.name = "hs_code";
            opts.shm_id = -1;

            // Set VA mode if given
            va_mode = std::string("pa");
            try {
                va_mode = SISL_OPTIONS["iova-mode"].as< std::string >();
                LOGDEBUG("Using IOVA = {} mode", va_mode);
            } catch (std::exception& e) { LOGDEBUG("Using default IOVA = {} mode", va_mode); }
            opts.iova_mode = va_mode.c_str();
            //    opts.mem_size = 512;

            // Set CPU mask (if CPU pinning is active)
            std::string cpuset_path = IM_DYNAMIC_CONFIG(cpuset_path);
            if (m_is_cpu_pinning_enabled && std::filesystem::exists(cpuset_path)) {
                LOGDEBUG("Read cpuset from {}", cpuset_path);
                std::ifstream ifs(cpuset_path);
                corelist.assign((std::istreambuf_iterator< char >(ifs)), (std::istreambuf_iterator< char >()));
                corelist.erase(std::remove(corelist.begin(), corelist.end(), '\n'), corelist.end());
                corelist = "[" + corelist + "]";
                LOGINFO("CPU mask {} will be fed to DPDK EAL", corelist);
                opts.core_mask = corelist.c_str();
            } else {
                LOGINFO("DPDK will not set CPU mask since CPU pinning is not enabled");
            }
            p_opts = &opts;
        }

        int rc = spdk_env_init(p_opts);
        if (rc != 0) { throw std::runtime_error("SPDK Iniitalization failed"); }

        spdk_unaffinitize_thread();

        rc = spdk_thread_lib_init_ext(IOReactorSPDK::event_about_spdk_thread,
                                      IOReactorSPDK::reactor_thread_op_supported, 0);
        if (rc != 0) {
            LOGERROR("Thread lib init returned rte_errno = {} {}", rte_errno, rte_strerror(rte_errno));
            throw std::runtime_error("SPDK Thread Lib Init failed");
        }
    }

    // Set the sisl::allocator with spdk allocator, so that all sisl libraries start to use spdk for aligned
    // allocations
    sisl::AlignedAllocator::instance().set_allocator(std::move(new SpdkAlignedAllocImpl()));
}

void IOManager::hugetlbfs_umount() {
    if (umount2(std::string(hugetlbfs_path).data(), MNT_FORCE)) {
        LOGERROR("Failed to unmount hugetlbfs. Error = {}", errno);
        throw std::runtime_error("Hugetlbfs umount failed");
    }
}

void IOManager::stop() {
    LOGINFO("Stopping IOManager");

    if (m_is_spdk) {
        iomanager.run_on(thread_regex::least_busy_worker, [this](io_thread_addr_t taddr) {
            spdk_subsystem_fini(
                [](void* cb_arg) {
                    IOManager* pthis = (IOManager*)cb_arg;
                    pthis->set_state_and_notify(iomgr_state::stopping);
                },
                (void*)this);
        });
        wait_for_state(iomgr_state::stopping);
    } else {
        set_state(iomgr_state::stopping);
    }

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

    LOGINFO("IOManager Stopped and all IO threads are relinquished");

    if (m_is_spdk) { stop_spdk(); }
}

void IOManager::stop_spdk() {
    spdk_thread_lib_fini();
    spdk_env_fini();
    m_spdk_reinit_needed = true;
    for(spdk_mempool* mempool: m_iomgr_internal_pools) {
        if (mempool != nullptr) {
            spdk_mempool_free(mempool);
        }
    }
}

void IOManager::start_reactors() {
    for (uint32_t i{0}; i < m_num_workers; ++i) {
        m_worker_reactors.push_back(nullptr);
    }

    if (m_is_spdk && m_is_cpu_pinning_enabled) {
        const auto current_core = spdk_env_get_current_core();
        auto lcore = spdk_env_get_first_core();
        uint32_t worker{0};

        while (worker < m_num_workers) {
            RELEASE_ASSERT_NE(lcore, std::numeric_limits< uint32_t >::max(),
                              "System has less cores than iomgr threads={}", m_num_workers);
            // Skip starting the thread loop on current core
            if (lcore != current_core) {
                int* p = new int{(int)worker};
                const auto rc = spdk_env_thread_launch_pinned(
                    lcore,
                    [](void* arg) -> int {
                        IOManager& p_this = IOManager::instance();
                        const int* p = (const int*)arg;
                        const int reactor_num = *p;
                        delete p;
                        set_thread_name("iomgr_thread");
                        p_this._run_io_loop(reactor_num, p_this.m_is_spdk ? TIGHT_LOOP : INTERRUPT_LOOP, nullptr,
                                            nullptr);
                        return 0;
                    },
                    (void*)p);
                RELEASE_ASSERT_GE(rc, 0, "Unable to start reactor thread on core {}", lcore);
                m_worker_threads.emplace_back(sys_thread_id_t{lcore});
                LOGTRACEMOD(iomgr, "Created iomanager worker reactor thread {}...", worker);
                ++worker;
            }
            lcore = spdk_env_get_next_core(lcore);
        }
    } else {
        for (auto i = 0u; i < m_num_workers; ++i) {
            m_worker_threads.emplace_back(
                sys_thread_id_t(sisl::thread_factory("iomgr_thread", &IOManager::_run_io_loop, this, (int)i,
                                                     m_is_spdk ? TIGHT_LOOP : INTERRUPT_LOOP, nullptr, nullptr)));
            LOGTRACEMOD(iomgr, "Created iomanager worker reactor thread {}...", i);
        }
    }
}

extern const version::Semver200_version get_version() { return version::Semver200_version(PACKAGE_VERSION); }

void IOManager::add_drive_interface(std::shared_ptr< DriveInterface > iface, thread_regex iface_scope) {
    add_interface(std::dynamic_pointer_cast< IOInterface >(iface), iface_scope);
    {
        std::unique_lock lg(m_iface_list_mtx);
        m_drive_ifaces.push_back(iface);
    }
}

std::shared_ptr< DriveInterface > IOManager::get_drive_interface(const drive_interface_type type) {
    if ((type == drive_interface_type::spdk) && !m_is_spdk) {
        LOGERRORMOD(iomgr, "Attempting to access spdk's drive interface on non-spdk mode");
        return nullptr;
    }
    {
        std::unique_lock lg(m_iface_list_mtx);
        for (auto& iface : m_drive_ifaces) {
            if (iface->interface_type() == type) { return iface; }
        }
    }
    LOGERRORMOD(iomgr, "Unable to find drive interfaces of type {}", type);
    return nullptr;
}

void IOManager::add_interface(std::shared_ptr< IOInterface > iface, thread_regex iface_scope) {
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

    if (iface->is_spdk_interface()) {
        static std::once_flag flag1;
        std::call_once(flag1, [this] { mempool_metrics_populate(); });
    }
    LOGINFOMOD(iomgr, "Interface={} added to {} threads, total_interfaces={}", (void*)iface.get(), sent_count,
               m_iface_list.size());
}

void IOManager::remove_interface(const std::shared_ptr< IOInterface >& iface) {
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

void IOManager::become_user_reactor(loop_type_t loop_type, const iodev_selector_t& iodev_selector,
                                    const thread_state_notifier_t& addln_notifier) {
    _run_io_loop(-1, loop_type, iodev_selector, addln_notifier);
}

void IOManager::_run_io_loop(int iomgr_slot_num, loop_type_t loop_type, const iodev_selector_t& iodev_selector,
                             const thread_state_notifier_t& addln_notifier) {
    loop_type_t ltype = loop_type;

    std::shared_ptr< IOReactor > reactor;
    if (m_is_spdk && (loop_type & TIGHT_LOOP)) {
        ltype = (loop_type & ~INTERRUPT_LOOP);
        reactor = std::make_shared< IOReactorSPDK >();
    } else {
        ltype = (loop_type & ~TIGHT_LOOP) | INTERRUPT_LOOP;
        reactor = std::make_shared< IOReactorEPoll >();
    }
    *(m_reactors.get()) = reactor;
    reactor->run(iomgr_slot_num, ltype, iodev_selector, addln_notifier);
}

void IOManager::stop_io_loop() { this_reactor()->stop(); }

void IOManager::reactor_started(std::shared_ptr< IOReactor > reactor) {
    m_yet_to_stop_nreactors.increment();
    if (reactor->is_worker()) {
        m_worker_reactors[reactor->m_worker_slot_num] = reactor;

        // All iomgr created reactors are initialized, move iomgr to sys init (next phase of start)
        if (m_yet_to_start_nreactors.decrement_testz()) {
            LOGINFO("All Worker reactors started, moving iomanager to sys_init state");
            set_state_and_notify(iomgr_state::sys_init);
        }
    } else {
        // For IOMgr created reactors, the notification be called after all reactors are started and system init.
        reactor->notify_thread_state(true);
    }
}

void IOManager::reactor_stopped() {
    if (m_yet_to_stop_nreactors.decrement_testz()) { set_state_and_notify(iomgr_state::stopped); }

    // Notify the caller registered to iomanager for it
    this_reactor()->notify_thread_state(false /* started */);
}

void IOManager::device_reschedule(const io_device_ptr& iodev, int event) {
    multicast_msg(thread_regex::least_busy_worker,
                  iomgr_msg::create(iomgr_msg_type::RESCHEDULE, m_internal_msg_module_id, iodev, event));
}

static bool match_regex(thread_regex r, const io_thread_t& thr) {
    if ((r == thread_regex::all_io) || (r == thread_regex::least_busy_io)) { return true; }
    if (r == thread_regex::all_tloop) { return thr->reactor->is_tight_loop_reactor(); }
    if (thr->reactor->is_worker()) {
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
    int64_t min_cnt = INTMAX_MAX;
    io_thread_addr_t min_thread = -1U;
    IOReactor* min_reactor = nullptr;
    IOReactor* sender_reactor = iomanager.this_reactor();

    static thread_local std::random_device s_rd{};
    static thread_local std::default_random_engine s_re{s_rd()};

    if (r == thread_regex::random_worker) {
        // Send to any random iomgr created io thread
        static thread_local std::random_device s_rd{};
        static thread_local std::default_random_engine s_re{s_rd()};

        auto& reactor = m_worker_reactors[m_rand_worker_distribution(s_re)];
        sent_to = reactor->deliver_msg(reactor->select_thread()->thread_addr, msg, sender_reactor);
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
                            auto new_msg = msg->clone();
                            reactor->deliver_msg(thr->thread_addr, new_msg, sender_reactor);
                            cloned = true;
                            ++sent_to;
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

uint64_t IOManager::get_mempool_idx(size_t size) {
    DEBUG_ASSERT_EQ(size % min_mempool_buf_size, 0, "Mempool size is less than minimum mempool buf size");
    return spdk_u64log2(size / min_mempool_buf_size);
}

spdk_mempool* IOManager::get_mempool(size_t size) {
    uint64_t idx = get_mempool_idx(size);
    return m_iomgr_internal_pools[idx];
}

void* IOManager::create_mempool(size_t element_size, size_t element_count) {
    if (m_is_spdk) {
        uint64_t idx = get_mempool_idx(element_size);
        spdk_mempool* mempool = m_iomgr_internal_pools[idx];
        if (mempool != nullptr) {
            if (spdk_mempool_count(mempool) == element_count) {
                return mempool;
            } else {
                spdk_mempool_free(mempool);
            }
        }
        LOGINFO("Creating new mempool of size {}", element_size);
        mempool = spdk_mempool_create("iomgr_mempool", element_count, element_size, 0, SPDK_ENV_SOCKET_ID_ANY);
        RELEASE_ASSERT(mempool != nullptr, "Failed to create new mempool of size {}", element_size);
        m_iomgr_internal_pools[idx] = mempool;
        return mempool;
    } else {
        return nullptr;
    }
}

void IOManager::_pick_reactors(thread_regex r, const auto& cb) {
    if ((r == thread_regex::all_worker) || (r == thread_regex::least_busy_worker)) {
        for (auto i = 0u; i < m_worker_reactors.size(); ++i) {
            cb(m_worker_reactors[i].get(), (i == (m_worker_reactors.size() - 1)));
        }
    } else {
        all_reactors(cb);
    }
}

int IOManager::multicast_msg_and_wait(thread_regex r, const std::shared_ptr< sync_msg_base >& smsg) {
    auto sent_to = multicast_msg(r, smsg->base_msg());
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

bool IOManager::send_msg_and_wait(const io_thread_t& to_thread, const std::shared_ptr< sync_msg_base >& smsg) {
    auto sent = send_msg(to_thread, smsg->base_msg());
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

void IOManager::set_poll_interval(const int interval) { this_reactor()->set_poll_interval(interval); }
int IOManager::get_poll_interval() const { return this_reactor()->get_poll_interval(); }

void IOManager::foreach_interface(const interface_cb_t& iface_cb) {
    std::shared_lock lg(m_iface_list_mtx);
    for (auto& iface : m_iface_list) {
        iface_cb(iface);
    }
}

IOReactor* IOManager::this_reactor() const { return IOReactor::this_reactor; }

void IOManager::all_reactors(const auto& cb) {
    m_reactors.access_all_threads(
        [&cb](std::shared_ptr< IOReactor >* preactor, bool is_last_thread) { cb(preactor->get(), is_last_thread); });
}

void IOManager::specific_reactor(int thread_num, const auto& cb) {
    if ((thread_num == (int)sisl::ThreadLocalContext::my_thread_num()) && (this_reactor() != nullptr)) {
        cb(iomanager.this_reactor());
    } else {
        m_reactors.access_specific_thread(thread_num,
                                          [&cb](std::shared_ptr< IOReactor >* preactor) { cb(preactor->get()); });
    }
}

IOReactor* IOManager::round_robin_reactor() const {
    static uint64_t s_idx{0};
    do {
        const auto idx = s_idx++ % m_worker_reactors.size();
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

bool IOManager::is_spdk_inited() const {
    return (m_is_spdk && !m_spdk_reinit_needed && !spdk_env_dpdk_external_init());
}

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

void IOManager::set_io_memory_limit(const size_t limit) {
    m_mem_size_limit = limit;
    m_mem_soft_threshold_size = IM_DYNAMIC_CONFIG(iomem.soft_mem_release_threshold) * limit / 100;
    m_mem_aggressive_threshold_size = IM_DYNAMIC_CONFIG(iomem.aggressive_mem_release_threshold) * limit / 100;

    sisl::set_memory_release_rate(IM_DYNAMIC_CONFIG(iomem.mem_release_rate));
}

/************* Spdk Memory Allocator section ************************/
uint8_t* SpdkAlignedAllocImpl::aligned_alloc(size_t align, size_t size, [[maybe_unused]] const sisl::buftag tag) {
    auto buf = (uint8_t*)spdk_malloc(size, align, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
#ifdef _PRERELEASE
    sisl::AlignedAllocator::metrics().increment(tag, buf_size(buf));
#endif
    return buf;
}

void SpdkAlignedAllocImpl::aligned_free(uint8_t* b, [[maybe_unused]] const sisl::buftag tag) {
#ifdef _PRERELEASE
    sisl::AlignedAllocator::metrics().decrement(tag, buf_size(b));
#endif
    spdk_free((void*)b);
}

uint8_t* SpdkAlignedAllocImpl::aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz) {
#ifdef _PRERELEASE
    sisl::AlignedAllocator::metrics().increment(sisl::buftag::common, new_sz - old_sz);
#endif
    return (uint8_t*)spdk_realloc((void*)old_buf, new_sz, align);
}

uint8_t* SpdkAlignedAllocImpl::aligned_pool_alloc(const size_t align, const size_t sz, const sisl::buftag tag) {
    return (uint8_t *)spdk_mempool_get(iomanager.get_mempool(sz));
}

void SpdkAlignedAllocImpl::aligned_pool_free(uint8_t* const b, const size_t sz, const sisl::buftag tag) {
    spdk_mempool_put(iomanager.get_mempool(sz), b);
}

size_t SpdkAlignedAllocImpl::buf_size(uint8_t* buf) const {
    size_t sz;
    [[maybe_unused]] auto ret = rte_malloc_validate(buf, &sz);
    assert(ret != -1);
    return sz;
}

/************* Conventional Memory Allocator section ************************/
uint8_t* IOMgrAlignedAllocImpl::aligned_alloc(size_t align, size_t size, const sisl::buftag tag) {
    return sisl::AlignedAllocatorImpl::aligned_alloc(align, size, tag);
}

void IOMgrAlignedAllocImpl::aligned_free(uint8_t* b, const sisl::buftag tag) {
    sisl::AlignedAllocatorImpl::aligned_free(b, tag);

    static std::atomic< uint64_t > num_frees{0};
    if (((num_frees.fetch_add(1, std::memory_order_relaxed) + 1) % IM_DYNAMIC_CONFIG(iomem.limit_check_freq)) == 0) {
        sisl::release_mem_if_needed(iomanager.soft_mem_threshold(), iomanager.aggressive_mem_threshold());
    }
}

uint8_t* IOMgrAlignedAllocImpl::aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz) {
    return sisl::AlignedAllocatorImpl::aligned_realloc(old_buf, align, new_sz, old_sz);
}

/************* Mempool Metrics section ************************/
void IOManager::register_mempool_metrics(struct rte_mempool* mp) {
    std::string name = mp->name;
    m_mempool_metrics_set.withWLock([&](auto& m) { m.try_emplace(name, name, (const struct spdk_mempool*)mp); });
}

void IOManager::mempool_metrics_populate() {
    rte_mempool_walk(
        [](struct rte_mempool* mp, void* arg) {
            IOManager* iomgr = (IOManager*)arg;
            iomgr->register_mempool_metrics(mp);
        },
        (void*)this);
}

IOMempoolMetrics::IOMempoolMetrics(const std::string& pool_name, const struct spdk_mempool* mp) :
        sisl::MetricsGroup("IOMemoryPool", pool_name), m_mp{mp} {
    REGISTER_GAUGE(iomempool_obj_size, "Size of the entry for this mempool");
    REGISTER_GAUGE(iomempool_free_count, "Total count of objects which are free in this pool");
    REGISTER_GAUGE(iomempool_alloced_count, "Total count of objects which are alloced in this pool");
    REGISTER_GAUGE(iomempool_cache_size, "Total number of entries cached per lcore in this pool");

    register_me_to_farm();
    attach_gather_cb(std::bind(&IOMempoolMetrics::on_gather, this));

    // This is not going to change once created, so set it up avoiding atomic operations during gather
    GAUGE_UPDATE(*this, iomempool_obj_size, ((const struct rte_mempool*)m_mp)->elt_size);
    GAUGE_UPDATE(*this, iomempool_cache_size, ((const struct rte_mempool*)m_mp)->cache_size);
}

void IOMempoolMetrics::on_gather() {
    GAUGE_UPDATE(*this, iomempool_free_count, spdk_mempool_count(m_mp));
    GAUGE_UPDATE(*this, iomempool_alloced_count, rte_mempool_in_use_count((const struct rte_mempool*)m_mp));
}

} // namespace iomgr

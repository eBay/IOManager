#include <filesystem>
#include <sisl/utility/thread_factory.hpp>

#include <iomgr/iomgr.hpp>
#include "iomgr_impl_spdk.hpp"
#include "reactor_spdk.hpp"
#include "mempool_spdk.hpp"
#include "iomgr_config.hpp"

#ifdef __linux__
#include <sys/prctl.h>
#endif

extern "C" {
#include <sys/mount.h>
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

namespace iomgr {
bool IOManagerSpdkImpl::g_spdk_env_prepared{false};

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

static void set_thread_name(const char* thread_name) {
#if defined(__linux__)
    prctl(PR_SET_NAME, thread_name, 0, 0, 0);
#elif defined(__FreeBSD__)
    pthread_set_name_np(pthread_self(), thread_name);
#else
    pthread_setname_np(pthread_self(), thread_name);
#endif
}

static std::string hugetlbfs_path{"/mnt/huge"};

#if 0
static void hugetlbfs_umount() {
    if (umount2(hugetlbfs_path.data(), MNT_FORCE)) {
        LOGERROR("Failed to unmount hugetlbfs. Error = {}", errno);
        throw std::runtime_error("Hugetlbfs umount failed");
    }
}
#endif

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

static std::mutex s_core_assign_mutex;
static std::unordered_set< uint32_t > s_core_assignment;
static bool assign_core_if_available(uint32_t lcore) {
    std::unique_lock lg(s_core_assign_mutex);
    const auto [it, inserted] = s_core_assignment.insert(lcore);
    return inserted;
}

IOManagerSpdkImpl::IOManagerSpdkImpl(uint64_t total_hugepage_size) :
        m_mempool{std::make_shared< SpdkMemPool >()}, m_total_hugepage_size{total_hugepage_size} {}

void IOManagerSpdkImpl::pre_interface_init() {
    // m_do_init_bdev = !is_spdk_inited();
    m_is_cpu_pinning_enabled = is_cpu_pinning_enabled();
    start_spdk();

    // Create memory pool
    for (const auto& pool_entry : IM_DYNAMIC_CONFIG(iomem->pool_sizes)) {
        uint64_t count = (m_total_hugepage_size / pool_entry->size) * (pool_entry->percent) / 100;
        if (count && pool_entry->size) {
            LOGINFO("Creating hugepage mempool entry of size={} count={}", pool_entry->size, count)
            m_mempool->create(pool_entry->size, count);
        }
    }

    // Set the sisl::allocator with spdk allocator, so that all sisl libs start to use spdk for aligned allocations
    sisl::AlignedAllocator::instance().set_allocator(std::move(new SpdkAlignedAllocImpl(m_mempool)));
}

void IOManagerSpdkImpl::post_interface_init() {
    if (m_do_init_bdev) {
        LOGINFO("Initializing all spdk subsystems");
        iomanager.run_on_forget(reactor_regex::least_busy_worker, [this]() {
            spdk_subsystem_init(
                [](int rc, void* cb_arg) {
                    // Initialize rpc system
                    int ret = spdk_rpc_initialize(SPDK_DEFAULT_RPC_ADDR);
                    if (ret) {
                        LOGERROR("Initialize rpc on address={} has failed with ret={}", SPDK_DEFAULT_RPC_ADDR, ret);
#ifndef NDEBUG
                        // Exceptions only on debug build, because rpc is not essential component at this time.
                        throw std::runtime_error("SPDK RPC Initialize failed");
#endif
                    }
                    spdk_rpc_set_state(SPDK_RPC_RUNTIME);

                    auto pthis = (IOManagerSpdkImpl*)cb_arg;
                    pthis->m_mempool->metrics_populate();
                    iomanager.set_state_and_notify(iomgr_state::running);
                },
                (void*)this);
        });
        iomanager.wait_for_state(iomgr_state::running);
        // m_spdk_prepared = true;
    }
}

bool IOManagerSpdkImpl::is_spdk_inited() const { return (m_spdk_prepared && !spdk_env_dpdk_external_init()); }

void IOManagerSpdkImpl::start_spdk() {
    /* Check if /mnt/huge already exists. Create otherwise */
    if (!std::filesystem::exists(hugetlbfs_path)) {
        std::error_code ec;
        if (!std::filesystem::create_directory(hugetlbfs_path, ec)) {
            if (ec.value()) {
                LOGERROR("Failed to create hugetlbfs. Error = {}", ec.message());
                throw std::runtime_error("Failed to create /mnt/huge");
            }
            LOGINFO("{} already exists.", hugetlbfs_path);
        } else {
            /* mount -t hugetlbfs nodev /mnt/huge */
            if (mount("nodev", hugetlbfs_path.data(), "hugetlbfs", 0, "")) {
                LOGERROR("Failed to mount hugetlbfs. Error = {}", errno);
                throw std::runtime_error("Hugetlbfs mount failed");
            }
            LOGINFO("Mounted hugepages on {}", hugetlbfs_path);
        }
    } else { /* Remove old/garbage hugepages from /mnt/huge */
        std::uintmax_t n = 0;
        for (const auto& entry : std::filesystem::directory_iterator(hugetlbfs_path)) {
            n += std::filesystem::remove_all(entry.path());
        }
        LOGINFO("Deleted {} old hugepages from {}", n, hugetlbfs_path);
    }

    // Set the spdk log level based on module spdk
    spdk_log_set_flag("all");
    // spdk_log_set_level(to_spdk_log_level(sisl::logging::GetModuleLogLevel("spdk")));
    spdk_log_set_print_level(to_spdk_log_level(sisl::logging::GetModuleLogLevel("spdk")));

    // Initialize if spdk has still not been initialized
    // if (!is_spdk_inited()) {

    int rc;
    if (!g_spdk_env_prepared) {
        struct spdk_env_opts opts;
        std::string corelist;
        std::string va_mode;
        spdk_env_opts_init(&opts);
        opts.name = "hs_code";
        opts.shm_id = -1;

        // Set VA mode if given
        va_mode = SISL_OPTIONS["iova-mode"].as< std::string >();
        LOGDEBUG("Using IOVA = {} mode", va_mode);
        opts.iova_mode = va_mode.c_str();
        opts.hugedir = hugetlbfs_path.c_str();
        //    opts.mem_size = 512;

        // Set CPU mask (if CPU pinning is active)
        std::string cpuset_path = IM_DYNAMIC_CONFIG(io_env.cpuset_path);
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
        rc = spdk_env_init(&opts);
    } else {
        rc = spdk_env_init(nullptr);
    }
    g_spdk_env_prepared = true;

    if (rc != 0) { throw std::runtime_error("SPDK Iniitalization failed"); }

    spdk_unaffinitize_thread();

    // Lock the first core for non-reactor threads.
    const auto lcore = spdk_env_get_first_core();
    RELEASE_ASSERT(lcore != UINT32_MAX, "SPDK unable to get the first core, possibly no cpu available");
    assign_core_if_available(lcore);

    rc =
        spdk_thread_lib_init_ext(IOReactorSPDK::event_about_spdk_thread, IOReactorSPDK::reactor_thread_op_supported, 0);
    if (rc != 0) {
        LOGERROR("Thread lib init returned rte_errno = {} {}", rte_errno, rte_strerror(rte_errno));
        throw std::runtime_error("SPDK Thread Lib Init failed");
    }
    //}
}

sys_thread_id_t IOManagerSpdkImpl::create_reactor_impl(const std::string& name, loop_type_t loop_type,
                                                       uint32_t num_fibers, int slot_num,
                                                       thread_state_notifier_t&& notifier) {
    if (m_is_cpu_pinning_enabled && (loop_type & TIGHT_LOOP)) {
        struct param_holder {
            std::string name;
            thread_state_notifier_t notifier;
            int slot_num;
            loop_type_t loop_type;
            uint32_t num_fibers;
        };

        // Skip starting the thread loop on current core
        auto lcore = spdk_env_get_first_core();
        const auto current_core = spdk_env_get_current_core();
        while ((lcore != UINT32_MAX) && ((lcore == current_core) || !assign_core_if_available(lcore))) {
            lcore = spdk_env_get_next_core(lcore);
        }
        RELEASE_ASSERT_NE(lcore, current_core, "No more cores to schedule this reactor {}", name);
        RELEASE_ASSERT_NE(lcore, UINT32_MAX, "No more cores to schedule this reactor {}", name);

        param_holder* h = new param_holder();
        h->name = name;
        h->notifier = std::move(notifier);
        h->slot_num = slot_num;
        h->loop_type = loop_type;
        h->num_fibers = num_fibers;

        const auto rc = spdk_env_thread_launch_pinned(
            lcore,
            [](void* arg) -> int {
                param_holder* h = (param_holder*)arg;
                set_thread_name(h->name.c_str());
                iomanager._run_io_loop(h->slot_num, h->loop_type, h->num_fibers, h->name, nullptr,
                                       std::move(h->notifier));
                delete h;
                return 0;
            },
            (void*)h);

        RELEASE_ASSERT_GE(rc, 0, "Unable to start reactor thread on core {}", lcore);
        LOGTRACEMOD(iomgr, "Created tight loop user worker reactor thread pinned to core {}", lcore);
        return sys_thread_id_t{lcore};
    } else {
        auto sthread =
            sisl::named_thread(name, [slot_num, loop_type, name, num_fibers, n = std::move(notifier)]() mutable {
                iomanager._run_io_loop(slot_num, loop_type, num_fibers, name, nullptr, std::move(n));
            });
        sthread.detach();
        return sys_thread_id_t{std::move(sthread)};
    }
}

void IOManagerSpdkImpl::pre_interface_stop() {
    iomanager.run_on_forget(reactor_regex::least_busy_worker, []() {
        spdk_rpc_finish();
        spdk_subsystem_fini([](void* cb_arg) { iomanager.set_state_and_notify(iomgr_state::stopping); }, nullptr);
    });
    iomanager.wait_for_state(iomgr_state::stopping);
}

void IOManagerSpdkImpl::post_interface_stop() {
    s_core_assignment.clear();
    stop_spdk();
}

void IOManagerSpdkImpl::stop_spdk() {
    spdk_thread_lib_fini();
    // spdk_env_fini();
    //  m_spdk_prepared = false;
    m_mempool->reset();
}

} // namespace iomgr
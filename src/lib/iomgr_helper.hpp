#pragma once

#include <cstdlib>
#include <boost/algorithm/string/predicate.hpp>

#ifdef __linux__
#include <linux/version.h>
#include <liburing.h>
#include <liburing/io_uring.h>
#endif

#include "iomgr_config.hpp"

namespace iomgr {
static constexpr uint64_t Ki{1024};
static constexpr uint64_t Mi{Ki * Ki};
static constexpr uint64_t Gi{Ki * Mi};
static constexpr uint64_t Ti{Ki * Gi};
static const std::string hugepage_env{"HUGEPAGE"};

static bool check_uring_capability(bool& new_interface_supported) {
#ifdef __linux__
    bool uring_supported{true};
    new_interface_supported = true;
    if (syscall(__NR_io_uring_register, 0, IORING_UNREGISTER_BUFFERS, NULL, 0) && errno == ENOSYS) {
        // No io_uring
        new_interface_supported = false;
        uring_supported = false;
    } else {
        // io_uring
        uring_supported = true;
    }

    if (uring_supported) {
        // do futher check if new interfaces are supported (starting available with kernel 5.6);
        std::vector< int > ops = {IORING_OP_NOP,   IORING_OP_READV, IORING_OP_WRITEV,
                                  IORING_OP_FSYNC, IORING_OP_READ,  IORING_OP_WRITE};

        struct io_uring_probe* probe = io_uring_get_probe();
        if (probe == nullptr) {
            new_interface_supported = false;
            goto exit;
        }

        for (auto& op : ops) {
            if (!io_uring_opcode_supported(probe, op)) {
                new_interface_supported = false;
                break;
            }
        }

        io_uring_free_probe(probe);
    }

exit:
    return uring_supported;

#else
    return false;
#endif
}

static uint32_t get_cpu_quota() {
#ifdef __linux
    int32_t period = 0l;
    int32_t quota = 0l;

    if (auto quota_file = std::ifstream("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us"); quota_file.is_open()) {
        quota_file >> quota;
        if (auto period_file = std::ifstream("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us"); period_file.is_open()) {
            period_file >> period;
        }
    }
    if (0 < period && period < quota) { return quota / period; }
#endif
    return 0u; // nothing found
}

[[maybe_unused]] static uint32_t get_committable_mem_mb() {
#ifdef __linux
    std::string token;
    std::ifstream file("/proc/meminfo");
    while (file >> token) {
        if (token == "CommitLimit:") {
            uint64_t mem;
            if (file >> mem) {
                return (uint32_t)(mem / Ki);
            } else {
                break;
            }
        }
        // ignore rest of the line
        file.ignore(std::numeric_limits< std::streamsize >::max(), '\n');
    }
#endif
    return 0u; // nothing found
}

static uint32_t get_cgroup_mem_limit() {
#ifdef __linux
    int64_t quota = 0l;
    if (auto quota_file = std::ifstream("/sys/fs/cgroup/memory/memory.limit_in_bytes"); quota_file.is_open()) {
        quota_file >> quota;
    }
    if ((int64_t)Mi < quota) { return quota / Mi; }
#endif
    return 0ul; // nothing found
}

static uint64_t get_hugepage_size() {
    uint64_t hugepage_size{0};
#ifdef __linux
    if (auto hugepage_val = std::getenv(hugepage_env.c_str()); (hugepage_val != nullptr)) {
        std::istringstream iss{hugepage_val};
        std::string multiplier;
        iss >> hugepage_size;
        iss >> multiplier;

        if (boost::iequals(multiplier, "Ti")) {
            hugepage_size *= Ti;
        } else if (boost::iequals(multiplier, "Gi")) {
            hugepage_size *= Gi;
        } else if (boost::iequals(multiplier, "Mi")) {
            hugepage_size *= Mi;
        } else if (boost::iequals(multiplier, "Ki")) {
            hugepage_size *= Ki;
        }
    } else {
        std::string token;
        std::ifstream file("/proc/meminfo");
        uint64_t pages_total{0};
        uint64_t page_size{0};

        while (file >> token) {
            if (token == "HugePages_Free:") {
                file >> pages_total;
            } else if (token == "Hugepagesize:") {
                file >> page_size;
            }
            // ignore rest of the line
            file.ignore(std::numeric_limits< std::streamsize >::max(), '\n');
        }
        hugepage_size = page_size * Ki * pages_total;
    }
#endif
    return hugepage_size;
}

static uint32_t get_app_mem_limit() {
#ifndef NDEBUG
    uint32_t system_ram_mb = 512u;
#else
    uint32_t system_ram_mb = UINT_MAX;
    if (auto const mem_size = IM_DYNAMIC_CONFIG(iomem.app_mem_size_mb); mem_size != 0) { system_ram_mb = mem_size; }
    if (auto commit_size = get_committable_mem_mb(); commit_size > 0ul) {
        system_ram_mb = std::min((commit_size >> 1), system_ram_mb);
    }
#endif
    // CGroup limit is closer to our real limit
    if (auto quota = get_cgroup_mem_limit(); quota > 0) { system_ram_mb = std::min(system_ram_mb, quota); }

    return system_ram_mb;
}

static uint32_t get_hugepage_limit() {
    const auto hugepage_mb = IM_DYNAMIC_CONFIG(iomem.hugepage_size_mb);
    return (hugepage_mb == 0) ? (get_hugepage_size() / Mi) : hugepage_mb;
}

static std::string _format_decimals(double val, const char* suffix) {
    return (val != (uint64_t)val) ? fmt::format("{:.2f}{}", val, suffix) : fmt::format("{}{}", val, suffix);
}

static std::string in_bytes(uint64_t sz) {
    static constexpr std::array< std::pair< uint64_t, const char* >, 5 > arr{
        std::make_pair(1, ""), std::make_pair(1024, "kb"), std::make_pair(1048576, "mb"),
        std::make_pair(1073741824, "gb"), std::make_pair(1099511627776, "tb")};

    const double size = (double)sz;
    for (size_t i{1}; i < arr.size(); ++i) {
        if ((size / arr[i].first) < 1) { return _format_decimals(size / arr[i - 1].first, arr[i - 1].second); }
    }
    return _format_decimals(size / arr.back().first, arr.back().second);
}
} // namespace iomgr

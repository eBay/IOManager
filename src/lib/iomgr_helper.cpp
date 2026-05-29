#include <array>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#ifdef __linux__
#include <linux/version.h>
#include <liburing.h>
#include <liburing/io_uring.h>
#endif

#include <fmt/format.h>

#include "iomgr_helper.hpp"

namespace iomgr {

bool check_uring_capability(bool& new_interface_supported) {
#ifdef __linux__
    new_interface_supported = true;
    if (syscall(__NR_io_uring_register, 0, IORING_UNREGISTER_BUFFERS, NULL, 0) && errno == ENOSYS) {
        new_interface_supported = false;
        return false;
    }
    std::vector< int > ops = {IORING_OP_NOP,   IORING_OP_READV, IORING_OP_WRITEV,
                              IORING_OP_FSYNC, IORING_OP_READ,  IORING_OP_WRITE};
    struct io_uring_probe* probe = io_uring_get_probe();
    if (probe == nullptr) {
        new_interface_supported = false;
        return true;
    }
    for (auto& op : ops) {
        if (!io_uring_opcode_supported(probe, op)) {
            new_interface_supported = false;
            break;
        }
    }
    io_uring_free_probe(probe);
    return true;
#else
    new_interface_supported = false;
    return false;
#endif
}

uint32_t get_cpu_quota() {
#ifdef __linux__
    int32_t period = 0;
    int32_t quota = 0;
    if (auto f = std::ifstream("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us"); f.is_open()) {
        f >> quota;
        if (auto pf = std::ifstream("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us"); pf.is_open()) { pf >> period; }
    }
    if (0 < period && period < quota) { return static_cast< uint32_t >(quota / period); }
#endif
    return 0u;
}

[[maybe_unused]] static uint32_t get_committable_mem_mb() {
#ifdef __linux__
    std::string token;
    std::ifstream file("/proc/meminfo");
    while (file >> token) {
        if (token == "CommitLimit:") {
            uint64_t mem;
            if (file >> mem) { return static_cast< uint32_t >(mem / Ki); }
            break;
        }
        file.ignore(std::numeric_limits< std::streamsize >::max(), '\n');
    }
#endif
    return 0u;
}

static uint32_t get_cgroup_mem_limit() {
#ifdef __linux__
    int64_t quota = 0;
    if (auto f = std::ifstream("/sys/fs/cgroup/memory/memory.limit_in_bytes"); f.is_open()) { f >> quota; }
    if (static_cast< int64_t >(Mi) < quota) { return static_cast< uint32_t >(quota / Mi); }
#endif
    return 0u;
}

uint32_t get_app_mem_limit() {
#ifndef NDEBUG
    uint32_t limit_mb = 512u;
#else
    uint32_t limit_mb = UINT_MAX;
    if (auto ms = IM_DYNAMIC_CONFIG(iomem.app_mem_size_mb); ms != 0) { limit_mb = ms; }
    if (auto cs = get_committable_mem_mb(); cs > 0u) { limit_mb = std::min(cs >> 1, limit_mb); }
#endif
    if (auto cg = get_cgroup_mem_limit(); cg > 0u) { limit_mb = std::min(limit_mb, cg); }
    return limit_mb;
}

static std::string format_decimals(double val, const char* suffix) {
    return (val != static_cast< uint64_t >(val)) ? fmt::format("{:.2f}{}", val, suffix)
                                                 : fmt::format("{}{}", static_cast< uint64_t >(val), suffix);
}

std::string in_bytes(uint64_t sz) {
    static constexpr std::array< std::pair< uint64_t, const char* >, 5 > arr{
        std::make_pair(1ULL, ""), std::make_pair(1024ULL, "kb"), std::make_pair(1048576ULL, "mb"),
        std::make_pair(1073741824ULL, "gb"), std::make_pair(1099511627776ULL, "tb")};
    const double size = static_cast< double >(sz);
    for (size_t i = 1; i < arr.size(); ++i) {
        if ((size / arr[i].first) < 1) { return format_decimals(size / arr[i - 1].first, arr[i - 1].second); }
    }
    return format_decimals(size / arr.back().first, arr.back().second);
}

} // namespace iomgr

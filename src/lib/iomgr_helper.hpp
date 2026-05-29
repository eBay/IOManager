#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <variant>

#include "iomgr_config.hpp"

namespace iomgr {

// Internal cast helper
template < typename T, typename U >
inline T r_cast(U v) {
    return reinterpret_cast< T >(v);
}

// Internal type aliases
using interface_cb_t = std::function< void(const std::shared_ptr< class IOInterface >&) >;

template < class... Ts >
struct overloaded : Ts... {
    using Ts::operator()...;
};
template < class... Ts >
overloaded(Ts...) -> overloaded< Ts... >;

static constexpr uint64_t Ki{1024};
static constexpr uint64_t Mi{Ki * Ki};
static constexpr uint64_t Gi{Ki * Mi};
static constexpr uint64_t Ti{Ki * Gi};

// Declarations — defined in iomgr_helper.cpp
bool check_uring_capability(bool& new_interface_supported);
uint32_t get_cpu_quota();
uint32_t get_app_mem_limit();
std::string in_bytes(uint64_t sz);

} // namespace iomgr

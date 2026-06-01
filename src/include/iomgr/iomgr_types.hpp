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
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <variant>

#include <sisl/utility/enum.hpp>
#include <sisl/fds/buffer.hpp>

namespace iomgr {
////// Forward declarations
class IOReactor;
struct IODevice;

template < typename T >
using shared = std::shared_ptr< T >;

template < typename T >
using cshared = const std::shared_ptr< T >;

template < typename T >
using unique = std::unique_ptr< T >;

using Clock = std::chrono::steady_clock;

/////////////////// Types for all IODevice ////////////////////////
using io_device_ptr = shared< IODevice >;
using io_device_const_ptr = shared< const IODevice >;
using iodev_selector_t = std::function< bool(const io_device_const_ptr&) >;
using ev_callback = std::function< void(IODevice* iodev, void* cookie, int events) >;

/////////////////// Types for all IOReactors ////////////////////////
using reactor_idx_t = uint32_t;
using loop_type_t = uint64_t;

static constexpr loop_type_t TIGHT_LOOP = 1 << 0;     // Completely tight loop consuming 100% cpu
static constexpr loop_type_t INTERRUPT_LOOP = 1 << 1; // Interrupt drive loop using epoll or similar mechanism
static constexpr loop_type_t ADAPTIVE_LOOP = 1 << 2;  // Adaptive approach by backing off before polling upon no-load
static constexpr loop_type_t USER_CONTROLLED_LOOP = 1 << 3; // User controlled loop where iomgr will poll on-need basis

using thread_state_notifier_t = std::function< void(bool) >;

ENUM(reactor_regex, uint8_t,
     all_io,            // Represents all io reactors
     least_busy_io,     // Represents least busy io reactors (including worker + user)
     all_worker,        // Represents all worker io reactors (either tloop or iloop)
     least_busy_worker, // Represents least busy worker io reactor
     random_worker,     // Represents a random worker io reactor
     all_user,          // Represents all user created io reactors
     least_busy_user,   // Represents least busy user io reactor
     all_tloop          // Represents all tight loop reactors (could be either worker or user)
);

// thread_specifier: reactor_regex for "all matching reactors"; IOReactor* for a specific reactor.
using thread_specifier = std::variant< reactor_regex, IOReactor* >;

/////////////////// Types for all IOInterfaces ////////////////////////
class IOInterface;
using listen_sentinel_cb_t = std::function< void(void) >;
using interface_adder_t = std::function< void(void) >;

ENUM(drive_type, uint8_t,
     file_on_nvme, // Works on top of file system which is hosted in NVMe
     file_on_hdd,  // Works on top of file system which is hosted in HDD
     block_nvme,   // Kernel NVMe block device
     block_hdd,    // Kernel HDD block device
     memory,       // Non-persistent memory
     unknown       // Try to deduce it while loading
)

// The backend that services a drive. Only io_uring remains; kept as an enum so the (internal) drive
// registry can stay generic. Declared here (not in the now-internal drive_interface.hpp) because the
// public IOManager::get_drive_interface() signature names it.
ENUM(drive_interface_type, uint8_t, uring)

} // namespace iomgr

#define IOMGR_LOG_MODS iomgr, io_wd

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
#include <thread>
#include <variant>
#include <functional>
#include <boost/heap/binomial_heap.hpp>

#include <sisl/utility/enum.hpp>
#include <sisl/fds/buffer.hpp>

struct spdk_thread;
struct spdk_bdev_desc;
struct spdk_nvmf_qpair;

namespace iomgr {
////// Forward declarations
class IOReactor;
struct IODevice;
struct iomgr_msg;
struct IOFiber;

template < typename T >
using shared = std::shared_ptr< T >;

template < typename T >
using cshared = const std::shared_ptr< T >;

template < typename T >
using unique = std::unique_ptr< T >;

/////////////////// Types for all IODevice ////////////////////////
using io_device_ptr = shared< IODevice >;
using io_device_const_ptr = shared< const IODevice >;
using iodev_selector_t = std::function< bool(const io_device_const_ptr&) >;
using ev_callback = std::function< void(IODevice* iodev, void* cookie, int events) >;

/////////////////// Types for all IOReactors ////////////////////////
using reactor_idx_t = uint32_t;
using io_fiber_t = IOFiber*;
using loop_type_t = uint64_t;

static constexpr loop_type_t TIGHT_LOOP = 1 << 0;     // Completely tight loop consuming 100% cpu
static constexpr loop_type_t INTERRUPT_LOOP = 1 << 1; // Interrupt drive loop using epoll or similar mechanism
static constexpr loop_type_t ADAPTIVE_LOOP = 1 << 2;  // Adaptive approach by backing off before polling upon no-load
static constexpr loop_type_t USER_CONTROLLED_LOOP = 1 << 3; // User controlled loop where iomgr will poll on-need basis

typedef std::function< void(bool) > thread_state_notifier_t;
typedef std::variant< reactor_idx_t, spdk_thread* > backing_thread_t;
typedef void (*spdk_msg_signature_t)(void*);

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

ENUM(fiber_regex, uint8_t,
     main_only,   // Run only on main fiber of the reactor
     syncio_only, // Run only on the syncio capable fibers
     random,      // Run on any fiber
     round_robin  // Run in round robin manner
);

using eal_core_id_t = uint32_t;
using thread_specifier = std::variant< reactor_regex, io_fiber_t >;
using sys_thread_id_t = std::variant< std::thread, eal_core_id_t >;

using backing_dev_t = std::variant< int, spdk_bdev_desc*, spdk_nvmf_qpair* >;
using poll_cb_idx_t = uint32_t;
using can_backoff_cb_t = std::function< bool(IOReactor*) >;

/////////////////// Types for all IOInterfaces ////////////////////////
class IOInterface;
using io_interface_comp_cb_t = std::function< void(int64_t res) >;
using listen_sentinel_cb_t = std::function< void(void) >;
using interface_adder_t = std::function< void(void) >;
using interface_cb_t = std::function< void(const std::shared_ptr< IOInterface >&) >;
using io_interface_id_t = uint32_t;

ENUM(drive_type, uint8_t,
     file_on_nvme, // Works on top of file system which is hosted in NVMe
     file_on_hdd,  // Works on top of file system which is hosted in HDD
     block_nvme,   // Kernel NVMe block device
     block_hdd,    // Kernel HDD block device
     raw_nvme,     // Raw Nvme device (which can be opened only thru spdk)
     memory,       // Non-persistent memory
     spdk_bdev,    // A SDPK version of bdev
     unknown       // Try to deduce it while loading
)

#define IOMGR_LOG_MODS iomgr, spdk, io_wd
SISL_LOGGING_DECL(IOMGR_LOG_MODS);
} // namespace iomgr

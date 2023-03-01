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
struct io_thread;
struct IODevice;
struct iomgr_msg;

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
using io_thread_idx_t = uint32_t;
using io_thread_addr_t = uint32_t;
using loop_type_t = uint64_t;

static constexpr loop_type_t TIGHT_LOOP = 1 << 0;     // Completely tight loop consuming 100% cpu
static constexpr loop_type_t INTERRUPT_LOOP = 1 << 1; // Interrupt drive loop using epoll or similar mechanism
static constexpr loop_type_t ADAPTIVE_LOOP = 1 << 2;  // Adaptive approach by backing off before polling upon no-load
static constexpr loop_type_t USER_CONTROLLED_LOOP = 1 << 3; // User controlled loop where iomgr will poll on-need basis

typedef std::function< void(bool) > thread_state_notifier_t;
typedef std::variant< reactor_idx_t, spdk_thread* > backing_thread_t;
typedef std::shared_ptr< io_thread > io_thread_t;

ENUM(thread_regex, uint8_t,
     all_io,            // Represents all io threads
     least_busy_io,     // Represents least busy io thread (including worker + user)
     all_worker,        // Represents all worker io threads (either tloop or iloop)
     least_busy_worker, // Represents least busy worker io thread
     random_worker,     // Represents a random worker io thread
     all_user,          // Represents all user created io threads
     least_busy_user,   // Represents least busy user io thread
     all_tloop          // Represents all tight loop threads (could be either worker or user)
);
using eal_core_id_t = uint32_t;
using thread_specifier = std::variant< thread_regex, io_thread_t >;
using sys_thread_id_t = std::variant< std::thread, eal_core_id_t >;

using backing_dev_t = std::variant< int, spdk_bdev_desc*, spdk_nvmf_qpair* >;
using poll_cb_idx_t = uint32_t;
using can_backoff_cb_t = std::function< bool(const io_thread_t&) >;

/////////////////// Types for all Msghandlers /////////////////////
using msg_handler_t = std::function< void(iomgr_msg*) >;
typedef void (*spdk_msg_signature_t)(void*);
using run_on_closure_t = std::function< void(void) >;
using run_method_t = std::function< void(io_thread_addr_t) >;
using msg_module_id_t = uint32_t;

struct reschedule_data_t {
    io_device_ptr iodev;
    int event;
};
using msg_data_t = std::variant< sisl::blob, reschedule_data_t, run_method_t >;

ENUM(wait_type_t, uint8_t, no_wait, sleep, spin, callback);

/////////////////// Types for all IOInterfaces ////////////////////////
class IOInterface;
using io_interface_comp_cb_t = std::function< void(int64_t res, uint8_t* cookie) >;
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

#define IOMGR_LOG_MODS iomgr, spdk, io_wd, httpserver_lmod
SISL_LOGGING_DECL(IOMGR_LOG_MODS);
} // namespace iomgr

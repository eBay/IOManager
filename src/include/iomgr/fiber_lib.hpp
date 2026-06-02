/************************************************************************
 * Copyright 2026 eBay Inc.
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

// v12 FIBER COMPATIBILITY SHIM (opt-in via -DIOMGR_V12_FIBER_COMPAT).
//
// Fibers were removed in iomgr v13 (a reactor is now a single stackless coroutine context). The only
// thing a still-migrating consumer (homestore) needs from the old fiber library are the FiberManagerLib
// mutex aliases used as synchronization primitives (e.g. the btree lock); those map to real std mutexes,
// so they remain fully functional. Everything else fiber-related is a placeholder -- see
// iomgr/iomgr_types.hpp (io_fiber_t) and the IOManager fiber-compat members in iomgr/iomgr.hpp.
//
// Delete this header once the consumer is fully on coroutines.

#include <mutex>
#include <shared_mutex>

#include <iomgr/iomgr_types.hpp> // io_fiber_t (under the same macro)

#ifdef IOMGR_V12_FIBER_COMPAT
namespace iomgr {

class FiberManagerLib {
public:
    using mutex = std::mutex;               // was a fiber-aware mutex; a real mutex is correct in v13
    using shared_mutex = std::shared_mutex; // was a fiber-aware shared mutex
};

} // namespace iomgr
#else
#error "<iomgr/fiber_lib.hpp> is a v12 compatibility shim; build with -DIOMGR_V12_FIBER_COMPAT to use it"
#endif

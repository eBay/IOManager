/************************************************************************
 * Copyright 2017-2019 eBay Inc.
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

#include <coroutine>
#include <cstddef>
#include <expected>
#include <functional>
#include <memory>
#include <system_error>

namespace iomgr {

// The one error type for the public surface: bytes transferred on success, a std::error_condition on
// failure. (Matches the project-wide io_result convention; reserve exceptions for precondition bugs.)
using io_result = std::expected< std::size_t, std::error_condition >;

// Opaque awaitable for a single asynchronous drive operation. `co_await` it to get the io_result:
//
//     iomgr::io_result r = co_await iomgr::async_read(drv, buf, size, offset);
//     if (!r) { /* r.error() */ }
//
// The completion runs on the reactor thread that reaped the I/O; the coroutine resumes there, so keep
// post-await work short (or re-dispatch it). The underlying coroutine/io_uring-scheduler machinery is
// fully hidden behind a PIMPL -- this header pulls in nothing beyond <coroutine>/<system_error>, so
// consumers do not need (or see) stdexec.
class io_op {
public:
    struct impl; // defined in the drive implementation TU

    explicit io_op(std::unique_ptr< impl > p) noexcept;
    io_op(io_op&&) noexcept;
    io_op& operator=(io_op&&) noexcept;
    io_op(const io_op&) = delete;
    io_op& operator=(const io_op&) = delete;
    ~io_op();

    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> h) noexcept;
    io_result await_resume() const noexcept;

private:
    friend io_result sync_wait(io_op op);
    std::unique_ptr< impl > _impl;
};

// Fire-and-forget launcher for callers that are not themselves coroutines (e.g. a folly::Future bridge):
// launches `op` and invokes `on_done(result)` when it completes. `on_done` runs on the reactor thread
// that reaped the completion -- keep it short, or re-dispatch. Equivalent to co_await-ing `op`.
void detach(io_op op, std::function< void(io_result) > on_done);

// Blocking variant for cold-path synchronous drive I/O (superblock / metadata reads & writes): runs `op` on
// iomgr's dedicated sync reactor and blocks the calling thread until it completes, returning the io_result.
// Safe to call from ANY thread, INCLUDING another reactor -- the op is issued and reaped on the sync reactor,
// never on the (possibly blocked) caller's reactor, so the caller blocking cannot stall the reactor that must
// drive the completion. This is the v13 stackless-coroutine replacement for v12's fiber-based
// DriveInterface::sync_write/sync_read (a fiber could yield the reactor; a stackless reactor context cannot).
io_result sync_wait(io_op op);

} // namespace iomgr

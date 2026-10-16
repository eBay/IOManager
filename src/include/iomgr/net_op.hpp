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
#include <cstdint>
#include <functional>
#include <memory>
#include <span>

#include <iomgr/io_op.hpp> // io_result
#include <iomgr/iomgr_types.hpp> // thread_specifier

namespace iomgr {

class net_op;

// Owns one fd's reactor registration (one make_io_device call, for the fd's whole life) and its
// parked-waiter state -- long-lived, constructed once per socket, never per operation. Holds at most one
// parked recv waiter at a time (see the caller contract below and iomgr_net_op_design.md point 1); callers
// must serialize their own recv loop.
//
// Every net_op issued against a handle must run on the single reactor thread the handle was constructed
// on, for the handle's entire lifetime -- never from any other thread. Construction and destruction are
// the one exception: they may be called from any thread (the constructor/destructor hop onto the target
// reactor themselves if not already there), since a handle's teardown may legitimately be triggered from
// outside its own reactor (e.g. a forced connection close), unlike the per-operation calls.
class socket_handle {
public:
    struct impl;

    // Registers fd on the given reactor. Safe to call from any thread.
    socket_handle(int fd, thread_specifier const& reactor);
    // Fails any parked waiter with connection_aborted, then closes fd. Safe to call from any thread.
    ~socket_handle();
    socket_handle(socket_handle&&) noexcept;
    socket_handle& operator=(socket_handle&&) noexcept;
    socket_handle(socket_handle const&) = delete;
    socket_handle& operator=(socket_handle const&) = delete;

    int fd() const noexcept;

private:
    friend net_op async_recv(socket_handle& sock, std::span< uint8_t > buf);
    std::unique_ptr< impl > _impl;
};

// Opaque awaitable for a single asynchronous socket recv. co_await it to get the io_result, exactly like
// iomgr::io_op:
//
//     iomgr::io_result r = co_await iomgr::async_recv(sock, buf);
//
// `await_ready()` always attempts the non-blocking recv syscall itself before ever touching the handle's
// parked-waiter state; only EAGAIN/EWOULDBLOCK suspends. A recv that completes with zero bytes is the
// peer's orderly shutdown, surfaced as an ordinary io_result{0} -- never as an error, and never confused
// with "not ready yet" (which never reaches await_resume() as a value at all). Partial transfer is the
// caller's job: a net_op can complete having moved fewer bytes than asked for, same as a raw non-blocking
// socket always can.
//
// Must be driven (await_ready/await_suspend/await_resume) entirely on the socket_handle's own owning
// reactor thread -- see the caller contract on socket_handle.
class net_op {
public:
    struct impl;

    explicit net_op(std::unique_ptr< impl > p) noexcept;
    net_op(net_op&&) noexcept;
    net_op& operator=(net_op&&) noexcept;
    net_op(net_op const&) = delete;
    net_op& operator=(net_op const&) = delete;
    ~net_op();

    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> h) noexcept;
    io_result await_resume() const noexcept;

private:
    std::unique_ptr< impl > _impl;
};

net_op async_recv(socket_handle& sock, std::span< uint8_t > buf);

// Fire-and-forget launcher for callers that are not themselves coroutines -- same semantics as
// iomgr::detach(io_op, ...): launches `op` and invokes `on_done(result)` when it completes. `on_done` runs
// on sock's owning reactor thread. Deliberately no sync_wait(net_op) -- see iomgr_net_op_design.md point 5.
void detach(net_op op, std::function< void(io_result) > on_done);

} // namespace iomgr

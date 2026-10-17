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
#include <expected>
#include <functional>
#include <memory>
#include <span>
#include <system_error>

#include <sisl/async/light_task.hpp>

#include <iomgr/io_op.hpp> // io_result
#include <iomgr/iomgr_types.hpp> // thread_specifier

namespace iomgr {

class net_op;
class accept_op;

// Owns one fd's reactor registration for its whole life; construct once per socket, never per operation.
// Holds at most one parked recv waiter and one parked send waiter -- serialize your own recv loop and your
// own send loop. Every net_op call on a handle must run on the reactor it was constructed on, for the
// handle's whole life, except construction and destruction: those may run on any thread (they hop onto the
// target/owning reactor themselves), since teardown can legitimately be triggered from another thread.
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
    friend net_op async_send(socket_handle& sock, std::span< uint8_t const > buf);
    std::unique_ptr< impl > _impl;
};

// Opaque awaitable for a single asynchronous socket recv or send. `co_await` it to get the io_result,
// exactly like iomgr::io_op:
//
//     iomgr::io_result r = co_await iomgr::async_recv(sock, buf);
//
// `await_ready()` retries the syscall itself before touching any parked-waiter state, so an already-ready
// fd never suspends. A recv returning zero bytes is the peer's orderly shutdown, an ordinary io_result{0}
// -- distinct from EAGAIN, which suspends and never reaches await_resume() as a value. May complete having
// moved fewer bytes than requested, same as a raw non-blocking socket always can. Must be driven entirely
// on the handle's owning reactor thread.
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
net_op async_send(socket_handle& sock, std::span< uint8_t const > buf);

// Fire-and-forget launcher for non-coroutine callers: launches `op` and invokes `on_done(result)` on sock's
// owning reactor thread when it completes. No sync_wait(net_op): a socket_handle is pinned to one reactor
// for its life, so there's no second reactor to block-wait through without risking deadlocking the one
// thread that has to make the operation progress.
void detach(net_op op, std::function< void(io_result) > on_done);

// Loops async_recv/async_send until buf is fully consumed. A short read (peer closed before buf filled) or
// a short write (send() returning zero for a non-empty buffer) surfaces as connection_aborted rather than
// a silently-partial success -- callers that need the partial count should loop on async_recv/async_send
// directly instead.
sisl::async::light_task< io_result > recv_exact(socket_handle& sock, std::span< uint8_t > buf);
sisl::async::light_task< io_result > send_all(socket_handle& sock, std::span< uint8_t const > buf);

// Owns one listening fd's reactor registration for its whole life, mirroring socket_handle. Holds at most
// one parked accept waiter; construction and destruction may run on any thread, same as socket_handle.
class listener_handle {
public:
    struct impl;

    listener_handle(int listen_fd, thread_specifier const& reactor);
    ~listener_handle();
    listener_handle(listener_handle&&) noexcept;
    listener_handle& operator=(listener_handle&&) noexcept;
    listener_handle(listener_handle const&) = delete;
    listener_handle& operator=(listener_handle const&) = delete;

    int fd() const noexcept;

private:
    friend accept_op async_accept(listener_handle& listener);
    std::unique_ptr< impl > _impl;
};

// accept has no partial-transfer case, so it gets its own result type rather than overloading io_result's
// byte count to mean "the accepted fd". The returned fd is unregistered: wrap it in its own socket_handle
// on whichever reactor it should be placed on.
using accept_result = std::expected< int, std::error_condition >;

class accept_op {
public:
    struct impl;

    explicit accept_op(std::unique_ptr< impl > p) noexcept;
    accept_op(accept_op&&) noexcept;
    accept_op& operator=(accept_op&&) noexcept;
    accept_op(accept_op const&) = delete;
    accept_op& operator=(accept_op const&) = delete;
    ~accept_op();

    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> h) noexcept;
    accept_result await_resume() const noexcept;

private:
    std::unique_ptr< impl > _impl;
};

accept_op async_accept(listener_handle& listener);

} // namespace iomgr

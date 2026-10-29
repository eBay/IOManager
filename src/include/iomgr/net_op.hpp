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

#include <sisl/async/light_task.hpp>

#include <iomgr/io_op.hpp>       // io_result
#include <iomgr/iomgr_types.hpp> // thread_specifier

// Socket recv/send (this file). See accept_op.hpp for listener/accept, connect_op.hpp for outbound
// connect -- each is an independent awaitable with its own result type and lifetime story.

namespace iomgr {

class net_op;

// Owns one fd's reactor registration for its whole life; construct once per socket, never per operation.
// Holds at most one parked recv waiter and one parked send waiter, so callers must serialize their own
// recv and send loops. All net_op calls on a handle must run on the reactor it was constructed with;
// construction and destruction may run on any thread and hop onto that reactor internally.
//
// Must be destroyed before its owning reactor stops: destruction also hops onto that reactor, and once
// the reactor has stopped there is nothing to hop onto. Doing so is undefined behavior -- it can corrupt
// the reactor object mid-teardown (typically a pure-virtual-call crash), not just leak or hang. Callers
// holding many handles must release them before IOManager::stop(), as with timers and other per-reactor
// resources.
class socket_handle {
public:
    struct impl;

    // Registers fd on the given reactor. Safe to call from any thread.
    socket_handle(int fd, thread_specifier const& reactor);
    // Requests cancellation of any parked waiter and returns without waiting: resolution (connection_aborted)
    // and closing fd both happen asynchronously. Safe to call from any thread, but only while the owning
    // reactor is still running; see the class comment above.
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
// fd never suspends. A recv returning zero bytes is the peer's orderly shutdown (io_result{0}), distinct
// from EAGAIN, which suspends instead of reaching await_resume(). May complete with fewer bytes than
// requested, same as a raw non-blocking socket. Must be driven entirely on the handle's owning reactor
// thread.
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
// owning reactor thread when it completes. No sync_wait(net_op): blocking the owning reactor's thread
// would deadlock the only thread that can make the operation progress.
void detach(net_op op, std::function< void(io_result) > on_done);

// Loops async_recv/async_send until buf is fully consumed. A short read (peer closed before buf filled) or
// a short write (send() returning zero for a non-empty buffer) surfaces as connection_aborted rather than
// a silently-partial success -- callers that need the partial count should loop on async_recv/async_send
// directly instead.
sisl::async::light_task< io_result > recv_exact(socket_handle& sock, std::span< uint8_t > buf);
sisl::async::light_task< io_result > send_all(socket_handle& sock, std::span< uint8_t const > buf);

} // namespace iomgr

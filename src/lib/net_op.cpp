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

// Implementation of the public socket_handle/net_op API (iomgr/net_op.hpp) over GenericIOInterface's
// epoll-based callback registration. This is the ONLY place the public handle/awaiter meet IODevice /
// IOInterface; nothing leaks into the public header.

#include <cerrno>
#include <cstring>
#include <system_error>
#include <utility>

extern "C" {
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
}

#include <sisl/logging/logging.h>
#include <iomgr/iomgr.hpp>
#include <iomgr/net_op.hpp>
#include "io_device.hpp"
#include "io_interface.hpp"

namespace iomgr {

namespace {

io_result try_recv(int fd, std::span< uint8_t > buf) {
    for (;;) {
        ssize_t const n = ::recv(fd, buf.data(), buf.size(), 0);
        if (n >= 0) { return io_result{static_cast< size_t >(n)}; }
        if (errno == EINTR) { continue; } // interrupted by a signal, not "no data" -- retry transparently
        return io_result{std::unexpected(std::error_code{errno, std::system_category()}.default_error_condition())};
    }
}

bool is_retryable(io_result const& r) {
    return !r.has_value() && (r.error() == std::errc::resource_unavailable_try_again ||
                              r.error() == std::errc::operation_would_block);
}

void set_nonblocking(int fd) {
    int const flags = ::fcntl(fd, F_GETFL, 0);
    DEBUG_ASSERT_GE(flags, 0, "fcntl(F_GETFL) failed on fd {}: {}", fd, strerror(errno));
    int const r = ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    DEBUG_ASSERT_EQ(r, 0, "fcntl(F_SETFL, O_NONBLOCK) failed on fd {}: {}", fd, strerror(errno));
}

// Runs fn() on `reactor` -- inline if the calling thread is already that reactor, else dispatched via
// run_on_wait (blocks the calling thread until fn() has run there). socket_handle's construction and
// destruction use this so they're safe to call from any thread; the per-operation net_op calls are not
// (see the caller contract in net_op.hpp) -- marshaling every recv/send would defeat the point of this API.
void run_on_reactor(IOReactor* reactor, const auto& fn) {
    if (iomanager.this_reactor() == reactor) {
        fn();
    } else {
        iomanager.run_on_wait(reactor, fn);
    }
}

} // namespace

// ----- socket_handle --------------------------------------------------------------------------------
struct socket_handle::impl {
    io_device_ptr iodev;
    net_op::impl* parked_recv{nullptr};
};

// ----- net_op: opaque awaitable wrapping a single recv -----------------------------------------------
struct net_op::impl {
    socket_handle::impl* owner{nullptr};
    std::span< uint8_t > buf;
    io_result result{0};
    bool done{false};
    std::coroutine_handle<> waiter{};

    io_result try_once() const { return try_recv(owner->iodev->fd(), buf); }

    ~impl() {
        DEBUG_ASSERT(owner->parked_recv != this,
                     "net_op destroyed while still parked waiting on its socket_handle -- would leave a "
                     "dangling pointer the next ev_callback fire would touch");
    }
};

namespace {

// Clears the parked slot and resumes its waiter with `r`. The resume happens on whatever thread calls
// this -- for ev_callback that's always the owning reactor (by the caller contract); for teardown,
// run_on_reactor above already ensures the same.
void complete_parked(net_op::impl*& slot, io_result r) {
    auto* const op = std::exchange(slot, nullptr);
    op->result = std::move(r);
    op->done = true;
    if (auto w = op->waiter) { w.resume(); }
}

void retry_parked(net_op::impl*& slot) {
    auto r = slot->try_once();
    if (is_retryable(r)) { return; } // still not ready; stays parked for the next edge
    complete_parked(slot, std::move(r));
}

void fail_parked(net_op::impl*& slot, std::error_condition const& ec) {
    if (slot) { complete_parked(slot, io_result{std::unexpected(ec)}); }
}

void socket_ev_callback(IODevice*, void* cookie, int events) {
    auto* const self = static_cast< socket_handle::impl* >(cookie);
    if (self->parked_recv && (events & (EPOLLIN | EPOLLHUP | EPOLLERR))) { retry_parked(self->parked_recv); }
}

void teardown(socket_handle::impl& s) {
    fail_parked(s.parked_recv, std::make_error_condition(std::errc::connection_aborted));
    s.iodev->io_interface->close_dev(s.iodev);
}

} // namespace

socket_handle::socket_handle(int fd, thread_specifier const& reactor) : _impl{std::make_unique< impl >()} {
    DEBUG_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                 "socket_handle must be pinned to a specific reactor, not a reactor_regex");
    auto* const target = std::get< IOReactor* >(reactor);
    set_nonblocking(fd);
    impl* const p = _impl.get();
    run_on_reactor(target, [fd, target, p]() {
        p->iodev = iomanager.generic_interface()->make_io_device(fd, EPOLLIN, 1 /* pri */, p,
                                                                  thread_specifier{target}, socket_ev_callback);
    });
}

socket_handle::~socket_handle() {
    if (!_impl) { return; } // moved-from
    impl* const p = _impl.get();
    run_on_reactor(p->iodev->reactor_scope(), [p]() { teardown(*p); });
}

socket_handle::socket_handle(socket_handle&&) noexcept = default;

socket_handle& socket_handle::operator=(socket_handle&& o) noexcept {
    if (this != &o) {
        if (_impl) {
            impl* const p = _impl.get();
            run_on_reactor(p->iodev->reactor_scope(), [p]() { teardown(*p); });
        }
        _impl = std::move(o._impl);
    }
    return *this;
}

int socket_handle::fd() const noexcept { return _impl->iodev->fd(); }

// ----- net_op -------------------------------------------------------------------------------------
net_op::net_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
net_op::net_op(net_op&&) noexcept = default;
net_op& net_op::operator=(net_op&&) noexcept = default;
net_op::~net_op() = default;

bool net_op::await_ready() const noexcept {
    impl* const p = _impl.get();
    DEBUG_ASSERT(iomanager.this_reactor() == p->owner->iodev->reactor_scope(),
                 "net_op driven from a thread other than its socket_handle's owning reactor");
    auto r = p->try_once();
    if (is_retryable(r)) { return false; } // park in await_suspend
    p->result = std::move(r);
    p->done = true;
    return true;
}

void net_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    DEBUG_ASSERT(p->owner->parked_recv == nullptr,
                 "a second recv is already parked on this socket_handle -- callers must serialize their own "
                 "recv loop");
    p->waiter = h;
    p->owner->parked_recv = p;
}

io_result net_op::await_resume() const noexcept { return _impl->result; }

net_op async_recv(socket_handle& sock, std::span< uint8_t > buf) {
    DEBUG_ASSERT(!buf.empty(),
                 "async_recv called with an empty buffer -- a zero-byte recv() return is indistinguishable "
                 "from EOF");
    auto p = std::make_unique< net_op::impl >();
    p->owner = sock._impl.get();
    p->buf = buf;
    return net_op{std::move(p)};
}

// ----- fire-and-forget launcher for non-coroutine callers --------------------------------------------
namespace {
struct ff_task {
    struct promise_type {
        ff_task get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { std::terminate(); }
    };
};
// op and on_done are coroutine PARAMETERS (copied into the self-owning frame), so nothing dangles.
ff_task run_detached(net_op op, std::function< void(io_result) > on_done) { on_done(co_await std::move(op)); }
} // namespace

void detach(net_op op, std::function< void(io_result) > on_done) { run_detached(std::move(op), std::move(on_done)); }

} // namespace iomgr

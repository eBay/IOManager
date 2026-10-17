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

io_result try_send(int fd, std::span< uint8_t const > buf) {
    for (;;) {
        // MSG_NOSIGNAL: a peer that already RST/closed its read side must surface as an error result, not
        // SIGPIPE taking the process down.
        ssize_t const n = ::send(fd, buf.data(), buf.size(), MSG_NOSIGNAL);
        if (n >= 0) { return io_result{static_cast< size_t >(n)}; }
        if (errno == EINTR) { continue; }
        return io_result{std::unexpected(std::error_code{errno, std::system_category()}.default_error_condition())};
    }
}

accept_result try_accept(int fd) {
    for (;;) {
        int const r = ::accept(fd, nullptr, nullptr);
        if (r >= 0) { return accept_result{r}; }
        // ECONNABORTED: the peer reset the connection between its SYN completing and accept() picking it
        // up -- retry rather than fail the whole listener over one aborted connection.
        if (errno == EINTR || errno == ECONNABORTED) { continue; }
        return accept_result{std::unexpected(std::error_code{errno, std::system_category()}.default_error_condition())};
    }
}

template < typename T >
bool is_retryable(std::expected< T, std::error_condition > const& r) {
    return !r.has_value() && (r.error() == std::errc::resource_unavailable_try_again ||
                              r.error() == std::errc::operation_would_block);
}

void set_nonblocking(int fd) {
    int const flags = ::fcntl(fd, F_GETFL, 0);
    DEBUG_ASSERT_GE(flags, 0, "fcntl(F_GETFL) failed on fd {}: {}", fd, strerror(errno));
    int const r = ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    DEBUG_ASSERT_EQ(r, 0, "fcntl(F_SETFL, O_NONBLOCK) failed on fd {}: {}", fd, strerror(errno));
}

// Runs fn() on `reactor` -- inline if already there, else dispatched via run_on_wait (blocking). Lets
// socket_handle's construction/destruction be called from any thread without making every recv/send pay
// for a dispatch too.
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
    net_op::impl* parked_send{nullptr};
};

// ----- net_op: opaque awaitable wrapping a single recv or send ----------------------------------------
struct net_op::impl {
    socket_handle::impl* owner{nullptr};
    bool is_send{false};
    std::span< uint8_t > recv_buf;       // valid when !is_send
    std::span< uint8_t const > send_buf; // valid when is_send
    io_result result{0};
    bool done{false};
    std::coroutine_handle<> waiter{};

    io_result try_once() const {
        return is_send ? try_send(owner->iodev->fd(), send_buf) : try_recv(owner->iodev->fd(), recv_buf);
    }

    ~impl() {
        DEBUG_ASSERT(owner->parked_recv != this && owner->parked_send != this,
                     "net_op destroyed while still parked waiting on its socket_handle -- would leave a "
                     "dangling pointer the next ev_callback fire would touch");
    }
};

// ----- listener_handle -------------------------------------------------------------------------------
struct listener_handle::impl {
    io_device_ptr iodev;
    accept_op::impl* parked_accept{nullptr};
};

// ----- accept_op: opaque awaitable wrapping a single accept -------------------------------------------
struct accept_op::impl {
    listener_handle::impl* owner{nullptr};
    accept_result result{-1};
    bool done{false};
    std::coroutine_handle<> waiter{};

    accept_result try_once() const { return try_accept(owner->iodev->fd()); }

    ~impl() {
        DEBUG_ASSERT(owner->parked_accept != this,
                     "accept_op destroyed while still parked waiting on its listener_handle -- would leave "
                     "a dangling pointer the next ev_callback fire would touch");
    }
};

namespace {

// Clears the parked slot and resumes its waiter with `r`.
template < typename Impl, typename Result >
void complete_parked(Impl*& slot, Result r) {
    auto* const op = std::exchange(slot, nullptr);
    op->result = std::move(r);
    op->done = true;
    if (auto w = op->waiter) { w.resume(); }
}

template < typename Impl >
void retry_parked(Impl*& slot) {
    auto r = slot->try_once();
    if (is_retryable(r)) { return; } // still not ready; stays parked for the next edge
    complete_parked(slot, std::move(r));
}

void fail_parked(net_op::impl*& slot, std::error_condition const& ec) {
    if (slot) { complete_parked(slot, io_result{std::unexpected(ec)}); }
}

void fail_parked(accept_op::impl*& slot, std::error_condition const& ec) {
    if (slot) { complete_parked(slot, accept_result{std::unexpected(ec)}); }
}

void socket_ev_callback(IODevice*, void* cookie, int events) {
    auto* const self = static_cast< socket_handle::impl* >(cookie);
    if (self->parked_recv && (events & (EPOLLIN | EPOLLHUP | EPOLLERR))) { retry_parked(self->parked_recv); }
    if (self->parked_send && (events & (EPOLLOUT | EPOLLHUP | EPOLLERR))) { retry_parked(self->parked_send); }
}

void listener_ev_callback(IODevice*, void* cookie, int events) {
    auto* const self = static_cast< listener_handle::impl* >(cookie);
    if (self->parked_accept && (events & (EPOLLIN | EPOLLHUP | EPOLLERR))) { retry_parked(self->parked_accept); }
}

void teardown(socket_handle::impl& s) {
    auto const ec = std::make_error_condition(std::errc::connection_aborted);
    fail_parked(s.parked_recv, ec);
    fail_parked(s.parked_send, ec);
    s.iodev->io_interface->close_dev(s.iodev);
}

void teardown(listener_handle::impl& s) {
    fail_parked(s.parked_accept, std::make_error_condition(std::errc::connection_aborted));
    s.iodev->io_interface->close_dev(s.iodev);
}

} // namespace

// Must run before the owning reactor stops: run_on_reactor's dispatch has nothing to deliver the message
// to once that reactor's thread has exited, and would block forever.
socket_handle::socket_handle(int fd, thread_specifier const& reactor) : _impl{std::make_unique< impl >()} {
    DEBUG_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                 "socket_handle must be pinned to a specific reactor, not a reactor_regex");
    auto* const target = std::get< IOReactor* >(reactor);
    set_nonblocking(fd);
    impl* const p = _impl.get();
    run_on_reactor(target, [fd, target, p]() {
        p->iodev = iomanager.generic_interface()->make_io_device(fd, EPOLLIN | EPOLLOUT, 1 /* pri */, p,
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

listener_handle::listener_handle(int listen_fd, thread_specifier const& reactor) :
        _impl{std::make_unique< impl >()} {
    DEBUG_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                 "listener_handle must be pinned to a specific reactor, not a reactor_regex");
    auto* const target = std::get< IOReactor* >(reactor);
    set_nonblocking(listen_fd);
    impl* const p = _impl.get();
    run_on_reactor(target, [listen_fd, target, p]() {
        p->iodev = iomanager.generic_interface()->make_io_device(listen_fd, EPOLLIN, 1 /* pri */, p,
                                                                  thread_specifier{target}, listener_ev_callback);
    });
}

listener_handle::~listener_handle() {
    if (!_impl) { return; } // moved-from
    impl* const p = _impl.get();
    run_on_reactor(p->iodev->reactor_scope(), [p]() { teardown(*p); });
}

listener_handle::listener_handle(listener_handle&&) noexcept = default;

listener_handle& listener_handle::operator=(listener_handle&& o) noexcept {
    if (this != &o) {
        if (_impl) {
            impl* const p = _impl.get();
            run_on_reactor(p->iodev->reactor_scope(), [p]() { teardown(*p); });
        }
        _impl = std::move(o._impl);
    }
    return *this;
}

int listener_handle::fd() const noexcept { return _impl->iodev->fd(); }

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
    net_op::impl*& slot = p->is_send ? p->owner->parked_send : p->owner->parked_recv;
    DEBUG_ASSERT(slot == nullptr,
                 "a second {} is already parked on this socket_handle -- callers must serialize their own "
                 "{} loop",
                 p->is_send ? "send" : "recv", p->is_send ? "send" : "recv");
    p->waiter = h;
    slot = p;
}

io_result net_op::await_resume() const noexcept { return _impl->result; }

net_op async_recv(socket_handle& sock, std::span< uint8_t > buf) {
    DEBUG_ASSERT(!buf.empty(),
                 "async_recv called with an empty buffer -- a zero-byte recv() return is indistinguishable "
                 "from EOF");
    auto p = std::make_unique< net_op::impl >();
    p->owner = sock._impl.get();
    p->recv_buf = buf;
    return net_op{std::move(p)};
}

net_op async_send(socket_handle& sock, std::span< uint8_t const > buf) {
    DEBUG_ASSERT(!buf.empty(), "async_send called with an empty buffer");
    auto p = std::make_unique< net_op::impl >();
    p->owner = sock._impl.get();
    p->is_send = true;
    p->send_buf = buf;
    return net_op{std::move(p)};
}

// ----- accept_op -----------------------------------------------------------------------------------
accept_op::accept_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
accept_op::accept_op(accept_op&&) noexcept = default;
accept_op& accept_op::operator=(accept_op&&) noexcept = default;
accept_op::~accept_op() = default;

bool accept_op::await_ready() const noexcept {
    impl* const p = _impl.get();
    DEBUG_ASSERT(iomanager.this_reactor() == p->owner->iodev->reactor_scope(),
                 "accept_op driven from a thread other than its listener_handle's owning reactor");
    auto r = p->try_once();
    if (is_retryable(r)) { return false; }
    p->result = std::move(r);
    p->done = true;
    return true;
}

void accept_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    DEBUG_ASSERT(p->owner->parked_accept == nullptr,
                 "a second accept is already parked on this listener_handle -- callers must serialize "
                 "their own accept loop");
    p->waiter = h;
    p->owner->parked_accept = p;
}

accept_result accept_op::await_resume() const noexcept { return _impl->result; }

accept_op async_accept(listener_handle& listener) {
    auto p = std::make_unique< accept_op::impl >();
    p->owner = listener._impl.get();
    return accept_op{std::move(p)};
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

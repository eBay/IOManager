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

// Implementation of the public outbound-connect API (iomgr/connect_op.hpp) over the per-reactor
// io_uring channel (interfaces/uring_net_interface.hpp). See net_op.cpp for socket recv/send,
// accept_op.cpp for listener/accept.

#include <chrono>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>

extern "C" {
#include <sys/socket.h>
}

#include <exec/async_scope.hpp>
#include <exec/task.hpp>
#include <sisl/logging/logging.h>
#include <iomgr/connect_op.hpp>
#include <iomgr/iomgr.hpp>

#include "net_op_common.hpp"

namespace iomgr {

// ----- connect_op: opaque awaitable wrapping a single outbound connect ---------------------------------
struct connect_op::impl {
    using connect_result = std::expected< void, std::error_condition >;

    int fd{-1};
    sockaddr_storage addr{};
    socklen_t addrlen{0};
    std::optional< std::chrono::milliseconds > timeout;

    connect_result result{};
    bool done{false};
    std::coroutine_handle<> waiter{};
};

namespace {

// One-shot IORING_OP_CONNECT, optionally linked (IOSQE_IO_LINK) to an IORING_OP_LINK_TIMEOUT so the
// kernel cancels the connect if it fires first, rather than racing a second sender against `self` after
// its connect_op may already be gone. The timeout SQE's user_data is left 0 ("unmanaged"), same convention
// as elsewhere, so poll_once's reap loop ignores its CQE.
//
// addr/ts are coroutine-frame locals, not references to caller-owned storage: the kernel reads them at
// actual submission time (deferred to the next poll_once), and the frame stays alive across the suspend.
exec::task< void > run_uring_connect(connect_op::impl* self, int fd, sockaddr_storage addr, socklen_t addrlen,
                                     std::optional< std::chrono::milliseconds > timeout) {
    net_io_scope io_scope;
    // A cancelled connect can't tell from its CQE alone whether its own link-timeout fired or something
    // else cancelled it (e.g. drain_outstanding_ops's bulk cancel) -- both give -ECANCELED. A wall-clock
    // deadline check sidesteps that ambiguity without depending on CQE reap order.
    auto const deadline = timeout ? std::optional{std::chrono::steady_clock::now() + *timeout} : std::nullopt;
    struct cancel_guard {
        connect_op::impl* self;
        std::optional< std::chrono::steady_clock::time_point > deadline;
        bool completed{false};
        ~cancel_guard() {
            if (completed) { return; }
            bool const timed_out = deadline.has_value() && (std::chrono::steady_clock::now() >= *deadline);
            self->result = std::unexpected(
                std::make_error_condition(timed_out ? std::errc::timed_out : std::errc::connection_aborted));
            self->done = true;
            if (auto w = self->waiter) { w.resume(); }
        }
    } guard{self, deadline};

    auto* const ch = UringNetInterface::this_channel();
    __kernel_timespec ts{};
    if (timeout) {
        auto const ns = std::chrono::duration_cast< std::chrono::nanoseconds >(*timeout).count();
        ts.tv_sec = static_cast< __kernel_time64_t >(ns / 1'000'000'000);
        ts.tv_nsec = static_cast< long long >(ns % 1'000'000'000);

        // Needs room for both the connect SQE and its linked timeout, or the timeout can't be armed
        // below -- flush to free already-queued SQEs' slots and retry once, matching the courtesy the
        // connect SQE's own exhaustion already gets inside async_submit, then fail closed. Must happen
        // before io_uring_prep_connect runs: once the kernel has the connect SQE there is no way to
        // stop it going out unbounded.
        if (::io_uring_sq_space_left(ch->m_sched.ring()) < 2) { ::io_uring_submit(ch->m_sched.ring()); }
        if (::io_uring_sq_space_left(ch->m_sched.ring()) < 2) {
            self->result = std::unexpected(std::make_error_condition(std::errc::not_enough_memory));
            self->done = true;
            if (auto w = self->waiter) { w.resume(); }
            guard.completed = true;
            co_return;
        }
    }
    int res = 0;
    try {
        res = co_await ch->m_sched.async_submit([&](::io_uring_sqe* sqe) {
            ::io_uring_prep_connect(sqe, fd, reinterpret_cast< sockaddr const* >(&addr), addrlen);
            if (timeout) {
                // Space was already reserved above, synchronously, with nothing able to consume it
                // before this prep lambda runs (single reactor thread, no intervening suspension).
                auto* const timeout_sqe = ::io_uring_get_sqe(ch->m_sched.ring());
                RELEASE_ASSERT(timeout_sqe != nullptr, "connect timeout SQE unavailable despite reserved space");
                sqe->flags |= IOSQE_IO_LINK;
                ::io_uring_prep_link_timeout(timeout_sqe, &ts, 0);
                ::io_uring_sqe_set_data64(timeout_sqe, 0);
            }
        });
    } catch (std::exception const& e) {
        // Not retried: a second connect() on an already-mid-handshake fd risks EALREADY; see
        // connect_op::await_ready below for why there's no raw-syscall fast path either.
        if (!self->done) {
            self->result = std::unexpected(exception_to_error_condition(e));
            self->done = true;
            if (auto w = self->waiter) { w.resume(); }
        }
        guard.completed = true;
        co_return;
    }

    if (!self->done) {
        self->result = (res < 0)
            ? std::unexpected(std::error_code{-res, std::system_category()}.default_error_condition())
            : connect_op::impl::connect_result{};
        self->done = true;
        if (auto w = self->waiter) { w.resume(); }
    }
    guard.completed = true;
}

} // namespace

connect_op::connect_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
connect_op::connect_op(connect_op&&) noexcept = default;
connect_op& connect_op::operator=(connect_op&&) noexcept = default;
connect_op::~connect_op() = default;

// Unlike recv/send/accept, connect has no safe fast path: a raw non-blocking ::connect() would itself
// start the handshake (EINPROGRESS), and a second connect() on an already-connecting fd -- including
// IORING_OP_CONNECT's own internal one -- is rejected with EALREADY. So IORING_OP_CONNECT must be the
// only connect() call ever made on this fd; go straight to uring submission every time.
bool connect_op::await_ready() const noexcept { return false; }

void connect_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    p->waiter = h;
    UringNetInterface::this_channel()->m_scope.spawn(run_uring_connect(p, p->fd, p->addr, p->addrlen, p->timeout));
}

std::expected< void, std::error_condition > connect_op::await_resume() const noexcept { return _impl->result; }

connect_op async_connect(int fd, sockaddr const* addr, socklen_t addrlen, thread_specifier const& reactor,
                         std::optional< std::chrono::milliseconds > timeout) {
    RELEASE_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                   "async_connect must be called with a specific reactor, not a reactor_regex");
    RELEASE_ASSERT(iomanager.this_reactor() == std::get< IOReactor* >(reactor),
                   "async_connect driven from a thread other than the reactor it was called with");
    RELEASE_ASSERT(static_cast< size_t >(addrlen) <= sizeof(sockaddr_storage),
                   "addrlen {} exceeds sizeof(sockaddr_storage)", addrlen);
    set_nonblocking(fd);
    auto p = std::make_unique< connect_op::impl >();
    p->fd = fd;
    std::memcpy(&p->addr, addr, addrlen);
    p->addrlen = addrlen;
    p->timeout = timeout;
    return connect_op{std::move(p)};
}

} // namespace iomgr

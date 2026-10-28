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

// Implementation of the public listener/accept API (iomgr/accept_op.hpp) over the per-reactor io_uring
// channel (interfaces/uring_net_interface.hpp). See net_op.cpp for socket recv/send, connect_op.cpp for
// outbound connect.

#include <cerrno>
#include <memory>
#include <utility>

extern "C" {
#include <sys/socket.h>
}

#include <exec/async_scope.hpp>
#include <exec/task.hpp>
#include <sisl/logging/logging.h>
#include <iomgr/accept_op.hpp>
#include <iomgr/iomgr.hpp>

#include "net_op_common.hpp"

namespace iomgr {

namespace {

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

} // namespace

// ----- listener_handle -------------------------------------------------------------------------------
struct listener_handle::impl {
    int fd{-1};
    IOReactor* reactor{nullptr};
    exec::async_scope scope;
    accept_op::impl* parked_accept{nullptr};
};

// ----- accept_op: opaque awaitable wrapping a single accept -------------------------------------------
struct accept_op::impl {
    listener_handle::impl* owner{nullptr};
    accept_result result{-1};
    bool done{false};
    std::coroutine_handle<> waiter{};

    accept_result try_once() const { return try_accept(owner->fd); }

    // See net_op::impl::~impl for why done is checked before owner is ever dereferenced.
    ~impl() {
        RELEASE_ASSERT(done || owner->parked_accept != this,
                       "accept_op destroyed while still parked waiting on its listener_handle -- would leave "
                       "a dangling pointer the next completion would touch");
    }
};

namespace {

// Mirrors net_op.cpp's run_uring_op for a single one-shot IORING_OP_ACCEPT.
exec::task< void > run_uring_accept(accept_op::impl* self) {
    net_io_scope io_scope;
    struct cancel_guard {
        accept_op::impl* self;
        bool completed{false};
        ~cancel_guard() {
            if (completed) { return; }
            accept_op::impl*& slot = self->owner->parked_accept;
            if (slot == self) {
                complete_parked(
                    slot, accept_result{std::unexpected(std::make_error_condition(std::errc::connection_aborted))});
            }
        }
    } guard{self};

    auto& sched = UringNetInterface::this_channel()->m_sched;
    int res = 0;
    for (;;) {
        try {
            res = co_await sched.async_submit(
                [self](::io_uring_sqe* sqe) { ::io_uring_prep_accept(sqe, self->owner->fd, nullptr, nullptr, 0); });
        } catch (std::exception const& e) {
            accept_op::impl*& slot = self->owner->parked_accept;
            if (slot == self) {
                complete_parked(slot, accept_result{std::unexpected(exception_to_error_condition(e))});
            }
            guard.completed = true;
            co_return;
        }
        if (res != -EINTR && res != -ECONNABORTED) { break; } // see try_accept for why ECONNABORTED retries
    }

    accept_op::impl*& slot = self->owner->parked_accept;
    if (slot == self) {
        if (res < 0) {
            complete_parked(slot,
                            accept_result{std::unexpected(
                                std::error_code{-res, std::system_category()}.default_error_condition())});
        } else {
            complete_parked(slot, accept_result{res});
        }
    }
    guard.completed = true;
}

} // namespace

listener_handle::listener_handle(int listen_fd, thread_specifier const& reactor) : _impl{std::make_unique< impl >()} {
    RELEASE_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                   "listener_handle must be pinned to a specific reactor, not a reactor_regex");
    auto* const target = std::get< IOReactor* >(reactor);
    set_nonblocking(listen_fd);
    impl* const p = _impl.get();
    run_on_reactor(target, [listen_fd, target, p]() {
        p->fd = listen_fd;
        p->reactor = target;
    });
}

// Same "must run before the owning reactor stops" requirement as socket_handle::~socket_handle(); see
// that destructor's comment.
listener_handle::~listener_handle() {
    if (!_impl) { return; } // moved-from
    impl* const p = _impl.release();
    run_on_reactor(p->reactor, [p]() { teardown(p); });
}

listener_handle::listener_handle(listener_handle&&) noexcept = default;

listener_handle& listener_handle::operator=(listener_handle&& o) noexcept {
    if (this != &o) {
        if (_impl) {
            impl* const p = _impl.release();
            run_on_reactor(p->reactor, [p]() { teardown(p); });
        }
        _impl = std::move(o._impl);
    }
    return *this;
}

int listener_handle::fd() const noexcept { return _impl->fd; }

accept_op::accept_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
accept_op::accept_op(accept_op&&) noexcept = default;
accept_op& accept_op::operator=(accept_op&&) noexcept = default;
accept_op::~accept_op() = default;

bool accept_op::await_ready() const noexcept {
    impl* const p = _impl.get();
    RELEASE_ASSERT(iomanager.this_reactor() == p->owner->reactor,
                   "accept_op driven from a thread other than its listener_handle's owning reactor");
    auto r = p->try_once();
    if (is_retryable(r)) { return false; }
    p->result = std::move(r);
    p->done = true;
    return true;
}

void accept_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    RELEASE_ASSERT(p->owner->parked_accept == nullptr,
                   "a second accept is already parked on this listener_handle -- callers must serialize "
                   "their own accept loop");
    p->waiter = h;
    p->owner->parked_accept = p;
    p->owner->scope.spawn(run_uring_accept(p));
}

accept_result accept_op::await_resume() const noexcept { return _impl->result; }

accept_op async_accept(listener_handle& listener) {
    auto p = std::make_unique< accept_op::impl >();
    p->owner = listener._impl.get();
    return accept_op{std::move(p)};
}

} // namespace iomgr

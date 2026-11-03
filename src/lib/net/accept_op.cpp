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
#include <poll.h>
#include <sys/socket.h>
}

#include <exec/async_scope.hpp>
#include <exec/task.hpp>
#include <sisl/logging/logging.h>
#include <iomgr/accept_op.hpp>
#include <iomgr/iomgr.hpp>
#include <iomgr/iomgr_flip.hpp>

#include "net_op_common.hpp"

namespace iomgr {

namespace {

accept_result try_accept(int fd) {
    for (;;) {
        int const r = ::accept4(fd, nullptr, nullptr, SOCK_CLOEXEC);
        if (r >= 0) { return accept_result{r}; }
        // ECONNABORTED: the peer reset the connection between its SYN completing and accept() picking it
        // up -- retry rather than fail the whole listener over one aborted connection.
        if (errno == EINTR || errno == ECONNABORTED) { continue; }
        return accept_result{std::unexpected(std::error_code{errno, std::system_category()}.default_error_condition())};
    }
}

accept_result res_to_accept_result(int res) {
    if (res < 0) {
        return accept_result{std::unexpected(std::error_code{-res, std::system_category()}.default_error_condition())};
    }
    return accept_result{res};
}

bool inject_uring_accept_eagain() {
#ifdef _PRERELEASE
    return iomgr_flip::instance()->test_flip("net_accept_uring_eagain");
#else
    return false;
#endif
}

bool inject_uring_accept_connaborted() {
#ifdef _PRERELEASE
    return iomgr_flip::instance()->test_flip("net_accept_uring_connaborted");
#else
    return false;
#endif
}

} // namespace

// ----- listener_handle -------------------------------------------------------------------------------
struct listener_handle::impl {
    int fd{-1};
    IOReactor* reactor{nullptr};
    exec::async_scope scope;
    accept_op::impl* parked_accept{nullptr};

    bool has_parked() const { return parked_accept != nullptr; }
};

// ----- accept_op: opaque awaitable wrapping a single accept -------------------------------------------
struct accept_op::impl {
    listener_handle::impl* owner{nullptr};
    accept_result result{-1};
    bool parked{false};
    std::coroutine_handle<> waiter{};

    accept_result try_once() const { return try_accept(owner->fd); }

    // See net_op::impl::~impl for why owner is never dereferenced here.
    ~impl() {
        RELEASE_ASSERT(!parked,
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
                auto const ec = net_stopping() ? std::errc::operation_canceled : std::errc::connection_aborted;
                complete_parked(slot, accept_result{std::unexpected(std::make_error_condition(ec))});
            }
        }
    } guard{self};

    auto& sched = UringNetInterface::this_channel()->m_sched;
    int const fd = self->owner->fd;
    accept_result result{std::unexpected(std::make_error_condition(std::errc::io_error))};
    // Mainline 5.15-5.17 completes IORING_OP_ACCEPT on an O_NONBLOCK listener with -EAGAIN instead of arming
    // poll, and a resubmit fails the same way. Once seen, wait for POLLIN ourselves and accept4() directly.
    bool poll_then_accept{false};
    for (;;) {
        // A resubmit after the drain's one-shot bulk cancel would never be cancelled.
        if (net_stopping()) {
            result = accept_result{std::unexpected(std::make_error_condition(std::errc::operation_canceled))};
            break;
        }
        int res = 0;
        try {
            if (poll_then_accept) {
                res = co_await sched.async_submit(
                    [fd](::io_uring_sqe* sqe) { ::io_uring_prep_poll_add(sqe, fd, POLLIN); });
            } else if (inject_uring_accept_eagain()) {
                res = -EAGAIN;
            } else {
                res = co_await sched.async_submit(
                    [fd](::io_uring_sqe* sqe) { ::io_uring_prep_accept(sqe, fd, nullptr, nullptr, SOCK_CLOEXEC); });
                if ((res >= 0) && inject_uring_accept_connaborted()) {
                    ::close(res);
                    res = -ECONNABORTED;
                }
            }
        } catch (std::exception const& e) {
            accept_op::impl*& slot = self->owner->parked_accept;
            if (slot == self) {
                complete_parked(slot, accept_result{std::unexpected(exception_to_error_condition(e))});
            }
            guard.completed = true;
            co_return;
        }

        if (poll_then_accept) {
            if (res == -EINTR) { continue; }
            if (res < 0) {
                result = res_to_accept_result(res);
                break;
            }
            result = try_accept(fd);
            if (is_retryable(result)) { continue; } // lost the race for that connection
            break;
        }
        if (res == -EAGAIN) {
            poll_then_accept = true;
            continue;
        }
        if (res == -EINTR || res == -ECONNABORTED) { continue; } // see try_accept for why ECONNABORTED retries
        result = res_to_accept_result(res);
        break;
    }

    accept_op::impl*& slot = self->owner->parked_accept;
    if (slot == self) {
        complete_parked(slot, std::move(result));
    } else {
        DEBUG_ASSERT(false, "accept completed but its op is no longer parked on the listener");
        if (result.has_value()) { ::close(*result); }
    }
    guard.completed = true;
}

} // namespace

listener_handle::listener_handle(int listen_fd, thread_specifier const& reactor) : _impl{std::make_unique< impl >()} {
    RELEASE_ASSERT(std::holds_alternative< IOReactor* >(reactor),
                   "listener_handle must be pinned to a specific reactor, not a reactor_regex");
    auto* const target = std::get< IOReactor* >(reactor);
    set_nonblocking(listen_fd);
    _impl->fd = listen_fd;
    _impl->reactor = target;
}

// Same "must run before the owning reactor stops" requirement as socket_handle::~socket_handle(); see
// that destructor's comment.
listener_handle::~listener_handle() {
    if (!_impl) { return; } // moved-from
    teardown_on_owner(_impl.release());
}

listener_handle::listener_handle(listener_handle&&) noexcept = default;

listener_handle& listener_handle::operator=(listener_handle&& o) noexcept {
    if (this != &o) {
        if (_impl) { teardown_on_owner(_impl.release()); }
        _impl = std::move(o._impl);
    }
    return *this;
}

int listener_handle::fd() const noexcept {
    DEBUG_ASSERT(_impl, "fd() called on a moved-from listener_handle");
    return _impl->fd;
}

accept_op::accept_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
accept_op::accept_op(accept_op&&) noexcept = default;
accept_op& accept_op::operator=(accept_op&&) noexcept = default;
accept_op::~accept_op() = default;

bool accept_op::await_ready() const noexcept {
    impl* const p = _impl.get();
    RELEASE_ASSERT(iomanager.this_reactor() == p->owner->reactor,
                   "accept_op driven from a thread other than its listener_handle's owning reactor");
    auto r = p->try_once();
    if (is_retryable(r)) {
        if (net_stopping()) {
            p->result = accept_result{std::unexpected(std::make_error_condition(std::errc::operation_canceled))};
            return true;
        }
        return false;
    }
    p->result = std::move(r);
    return true;
}

void accept_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    RELEASE_ASSERT(p->owner->parked_accept == nullptr,
                   "a second accept is already parked on this listener_handle -- callers must serialize "
                   "their own accept loop");
    p->waiter = h;
    p->parked = true;
    p->owner->parked_accept = p;
    p->owner->scope.spawn(run_uring_accept(p));
}

accept_result accept_op::await_resume() const noexcept { return _impl->result; }

accept_op async_accept(listener_handle& listener) {
    DEBUG_ASSERT(listener._impl, "async_accept called on a moved-from listener_handle");
    auto p = std::make_unique< accept_op::impl >();
    p->owner = listener._impl.get();
    return accept_op{std::move(p)};
}

} // namespace iomgr

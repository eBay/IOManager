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

// Internal helpers shared by net_op.cpp, accept_op.cpp, and connect_op.cpp. Never included outside
// src/lib/net/.

#include <cerrno>
#include <cstring>
#include <expected>
#include <system_error>
#include <utility>

extern "C" {
#include <fcntl.h>
#include <unistd.h>
}

#include <exec/async_scope.hpp>
#include <exec/task.hpp>
#include <sisl/logging/logging.h>
#include <iomgr/iomgr.hpp>
#include "interfaces/uring_net_interface.hpp"
#include "reactor/reactor.hpp"

namespace iomgr {

template < typename T >
bool is_retryable(std::expected< T, std::error_condition > const& r) {
    return !r.has_value() &&
        (r.error() == std::errc::resource_unavailable_try_again || r.error() == std::errc::operation_would_block);
}

inline void set_nonblocking(int fd) {
    int const flags = ::fcntl(fd, F_GETFL, 0);
    RELEASE_ASSERT_GE(flags, 0, "fcntl(F_GETFL) failed on fd {}: {}", fd, strerror(errno));
    int const r = ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    RELEASE_ASSERT_EQ(r, 0, "fcntl(F_SETFL, O_NONBLOCK) failed on fd {}: {}", fd, strerror(errno));
}

// io_uring_scheduler::async_submit's sender completes via set_error(exception_ptr), not a negative `res`,
// when the ring is exhausted even after its internal flush-and-retry. exec::async_scope::spawn isn't
// built to handle a spawned sender that fails this way, so every submission coroutine wraps its co_await
// in try/catch(std::exception const&) and converts through this instead of letting it propagate.
inline std::error_condition exception_to_error_condition(std::exception const& e) {
    if (auto const* se = dynamic_cast< std::system_error const* >(&e)) { return se->code().default_error_condition(); }
    return std::make_error_condition(std::errc::not_enough_memory);
}

// True once this reactor's net channel has begun its stop drain, or is already gone. Ops failed or cancelled
// then report operation_canceled, so a caller can tell reactor shutdown apart from a peer error.
inline bool net_stopping() {
    auto* const ch = UringNetInterface::this_channel();
    return (ch == nullptr) || ch->m_stopping;
}

// No tight-poll here unlike UringDriveInterface's io_started()/io_finished(): a net op can sit parked
// on peer behavior indefinitely, so tight-polling for its whole life would pin poll_interval at 0
// while anything waits on the network.
inline void net_io_started() {
    auto* const ch = UringNetInterface::this_channel();
    ++ch->m_outstanding;
}

inline void net_io_finished() {
    auto* const ch = UringNetInterface::this_channel();
    --ch->m_outstanding;
}

// Constructed first (incremented first) / destroyed last (decremented last) relative to each coroutine's
// own cancel_guard, so m_outstanding only reaches zero once the op is fully resolved -- including the
// cancellation-path resolution a cancel_guard's own destructor may still need to run.
struct net_io_scope {
    net_io_scope() { net_io_started(); }
    net_io_scope(net_io_scope const&) = delete;
    ~net_io_scope() { net_io_finished(); }
};

// Clears the parked slot and resumes its waiter with `r`. Shared by net_op::impl (io_result) and
// accept_op::impl (accept_result) -- both have the same owner/result/parked/waiter shape.
template < typename Impl, typename Result >
void complete_parked(Impl*& slot, Result r) {
    auto* const op = std::exchange(slot, nullptr);
    op->result = std::move(r);
    op->parked = false;
    if (auto w = op->waiter) { w.resume(); }
}

// Frees p only once p->scope (request_stop()'d by teardown()) is actually empty -- fires immediately
// if nothing was parked. net_io_scope folds this into m_outstanding so drain_outstanding_ops's own
// wait-at-reactor-stop covers it too.
template < typename ImplT >
exec::task< void > close_once_drained(ImplT* p) {
    net_io_scope io_scope;
    co_await p->scope.on_empty();
    ::close(p->fd);
    delete p;
}

// request_stop() alone drives kernel-level cancellation (each nested op's own submit_op::on_stop
// submits it). close()+free is deferred to close_once_drained rather than awaited here: teardown can
// run nested inside the completion that's resolving s's last op, and waiting in a loop here would
// re-enter that same reap.
template < typename ImplT >
void teardown(ImplT* s) {
    s->scope.request_stop();
    auto* const ch = UringNetInterface::this_channel();
    if (ch != nullptr) {
        ch->m_teardown_scope.spawn(close_once_drained(s));
        return;
    }
    // Net already stopped on this reactor. A successful drain completed every op, so nothing can still be
    // parked unless the drain timed out and leaked the ring -- then leak this too, since its ops never resolve.
    if (s->has_parked()) {
        LOGERRORMOD(iomgr, "Leaking fd {}: torn down after a timed-out net drain with an op still parked", s->fd);
        return;
    }
    ::close(s->fd);
    delete s;
}

// Off the owning reactor this only queues the teardown: blocking here would deadlock two reactors that drop
// each other's handles.
template < typename ImplT >
void teardown_on_owner(ImplT* s) {
    if (iomanager.this_reactor() == s->reactor) {
        teardown(s);
    } else {
        iomanager.run_on_forget(s->reactor, [s]() { teardown(s); });
    }
}

} // namespace iomgr

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

#include <cstdint>
#include <string>

/// NOTE: These are defined here to prevent inclusion of liburing/compat.h which conflicts
/// with system header /usr/include/futex.h
#define LIBURING_COMPAT_H
#define BLOCK_URING_CMD_DISCARD _IO(0x12, 0)
///

#include "open_how_compat.hpp" // complete struct open_how before <liburing.h> (Ubuntu 22.04 / gcc 13)
#include <liburing.h>
#include <sys/eventfd.h>

#include <exec/async_scope.hpp>
#include <sisl/async/io_uring_scheduler.hpp>

#include "io_interface.hpp" // IOInterface (internal)
#include <iomgr/iomgr_types.hpp>

namespace iomgr {

// Per-reactor io_uring channel dedicated to net_op's socket ops (recv/send/accept/connect). Kept
// separate from UringDriveInterface's own per-reactor ring: a socket_handle can be pinned to a reactor
// with no drive interface attached at all, and disk I/O (few, large ops) and socket I/O (many, small,
// short-lived ops) want independently sized queues rather than sharing one ring's SQE/CQE budget.
class UringNetInterface;
struct uring_net_channel {
    ::io_uring m_ring{};
    sisl::async::io_uring_scheduler m_sched; // wraps &m_ring; non-movable
    io_device_ptr m_ring_ev_iodev;
    uint32_t m_sentinel_idx{0};
    uint64_t m_outstanding{0}; // in-flight net ops on this reactor
    // connect_op has no persistent owner to host its own per-call scope, so run_uring_connect spawns into
    // this long-lived, channel-level scope instead -- avoids racing a connect's resume (which may
    // immediately destroy the connect_op) against the scope's own destruction.
    exec::async_scope m_scope;

    uring_net_channel(UringNetInterface* iface);
    ~uring_net_channel();
};

class UringNetInterface : public IOInterface {
public:
    UringNetInterface() = default;
    ~UringNetInterface() override = default;
    std::string name() const override { return "uring_net_interface"; }

    void on_event_notification(IODevice* iodev, void* cookie, int event);

    // Reactor sentinel: flush queued SQEs + reap completions (resuming suspended coroutines).
    void poll_completions();

    static uring_net_channel* this_channel() { return t_uring_net_ch; }

private:
    void init_iface_reactor_context(IOReactor*) override;
    void clear_iface_reactor_context(IOReactor*) override;
    // Reactor stop: actively cancels everything still in flight, then polls until cancellation lands.
    // Unlike drive's passive "wait for outstanding == 0", a parked accept/recv/connect can wait forever
    // for an external event that will never come once the reactor is stopping. Returns false if ops are
    // still outstanding when the bound is hit.
    bool drain_outstanding_ops();

private:
    static thread_local uring_net_channel* t_uring_net_ch;
};
} // namespace iomgr

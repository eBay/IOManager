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

#include <chrono>
#include <coroutine>
#include <expected>
#include <memory>
#include <optional>
#include <system_error>

extern "C" {
#include <sys/socket.h> // sockaddr, socklen_t
}

#include <iomgr/iomgr_types.hpp> // thread_specifier

namespace iomgr {

// Bare, handle-less awaitable for a single outbound connect: a connect happens once per fd, so unlike
// socket_handle/listener_handle (see net_op.hpp/accept_op.hpp) there's no persistent owner and nothing to
// serialize or tear down. Same opaque PIMPL shape as net_op/accept_op otherwise.
class connect_op {
public:
    struct impl;

    explicit connect_op(std::unique_ptr< impl > p) noexcept;
    connect_op(connect_op&&) noexcept;
    connect_op& operator=(connect_op&&) noexcept;
    connect_op(connect_op const&) = delete;
    connect_op& operator=(connect_op const&) = delete;
    ~connect_op();

    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> h) noexcept;
    std::expected< void, std::error_condition > await_resume() const noexcept;

private:
    std::unique_ptr< impl > _impl;
};

// Connects `fd` (already created via ::socket(), not yet connected) to addr/addrlen on `reactor`, backed
// by a one-shot IORING_OP_CONNECT. Async because a blocking ::connect() to a stalled peer can hang for
// Linux's default connect timeout (100+ seconds), stalling every other coroutine on that reactor thread.
//
// Unlike recv/send/accept, there's no owning handle whose teardown can cancel this, so `timeout` is the
// only way to bound the wait: it arms a linked IORING_OP_LINK_TIMEOUT, surfacing as std::errc::timed_out
// if it fires first. Omitting it waits indefinitely.
connect_op async_connect(int fd, sockaddr const* addr, socklen_t addrlen, thread_specifier const& reactor,
                         std::optional< std::chrono::milliseconds > timeout = std::nullopt);

} // namespace iomgr

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
#include <expected>
#include <memory>
#include <system_error>

#include <iomgr/iomgr_types.hpp> // thread_specifier

namespace iomgr {

class accept_op;

// Owns one listening fd's reactor registration for its whole life, mirroring socket_handle (see
// net_op.hpp), including the "destroy before the owning reactor stops" requirement; see socket_handle's
// comment.
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

// accept has no partial-transfer case, so it gets its own result type instead of overloading io_result's
// byte count as "the accepted fd". The returned fd is unregistered; wrap it in its own socket_handle.
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

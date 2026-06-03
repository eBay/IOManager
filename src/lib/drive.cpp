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

// Implementation of the reduced public drive API (iomgr/drive.hpp + iomgr/io_op.hpp) over the internal
// io_uring backend. This is the ONLY place the public handle/awaitable meet DriveInterface / IODevice /
// exec::task; nothing leaks into the public headers.

#include <cassert>
#include <coroutine>
#include <exception>
#include <future>

#include <iomgr/drive.hpp>
#include <iomgr/io_op.hpp>
#include <iomgr/iomgr.hpp>
#include "io_device.hpp"
#include "interfaces/drive_interface.hpp"
#include "io_launch.hpp" // iomgr::detail::launch_drive_task (internal; keeps stdexec out of public headers)

namespace iomgr {

// ----- opaque drive handle ------------------------------------------------------------------------
class drive {
public:
    explicit drive(io_device_ptr iodev) : m_iodev{std::move(iodev)} {}
    drive(const drive&) = delete;
    // RAII: dropping the last drive_handle closes the device (a drive device is not registered
    // per-reactor, so close just removes any registration, closes the fd and clears the iodev). The
    // handle must be released before iomanager::stop().
    ~drive() {
        if (m_iodev) { m_iodev->drive_interface()->close_dev(m_iodev); }
    }
    IODevice* dev() const { return m_iodev.get(); }
    const io_device_ptr& iodev() const { return m_iodev; }
    DriveInterface* iface() const { return m_iodev->drive_interface(); }

private:
    io_device_ptr m_iodev;
};

// ----- io_op: opaque awaitable wrapping an internal exec::task -------------------------------------
struct io_op::impl {
    exec::task< std::error_code > task;
    std::size_t nbytes{0}; // success value (bytes transferred); the retry loop runs IO to completion
    io_result result{0};
    std::coroutine_handle<> waiter{};
    bool done{false};
};

io_op::io_op(std::unique_ptr< impl > p) noexcept : _impl{std::move(p)} {}
io_op::io_op(io_op&&) noexcept = default;
io_op& io_op::operator=(io_op&&) noexcept = default;
io_op::~io_op() = default;

bool io_op::await_ready() const noexcept { return _impl->done; }
io_result io_op::await_resume() const noexcept { return _impl->result; }

void io_op::await_suspend(std::coroutine_handle<> h) noexcept {
    impl* const p = _impl.get();
    p->waiter = h;
    // launch_drive_task() runs the task on a reactor (marshalling if the caller is off-reactor) and
    // invokes the callback on the completing reactor thread; that resumes our awaiting coroutine.
    detail::launch_drive_task(std::move(p->task), [p](std::error_code ec) {
        p->result = ec ? io_result{std::unexpected(ec.default_error_condition())} : io_result{p->nbytes};
        p->done = true;
        if (auto w = p->waiter) w.resume();
    });
}

static io_op make_io_op(exec::task< std::error_code > t, std::size_t nbytes) {
    return io_op{std::make_unique< io_op::impl >(io_op::impl{std::move(t), nbytes})};
}

// ----- fire-and-forget launcher for non-coroutine callers (e.g. a folly::Future bridge) ------------
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
ff_task run_detached(io_op op, std::function< void(io_result) > on_done) { on_done(co_await std::move(op)); }
} // namespace

void detach(io_op op, std::function< void(io_result) > on_done) { run_detached(std::move(op), std::move(on_done)); }

// ----- blocking sync I/O: run the op on the dedicated sync reactor and wait for it -----------------
io_result sync_wait(io_op op) {
    io_op::impl* const p = op._impl.get();
    IOReactor* const sync_reactor = iomanager.sync_io_reactor();
    assert(sync_reactor != nullptr && "iomgr::sync_wait called before IOManager::start() or after stop()");
    // The sync reactor cannot block-wait on itself: it must stay free to issue + reap the op. Callers must
    // never be the sync reactor (nothing dispatched to it -- start_drive_task / io completions -- re-enters
    // sync_wait, so this only fires on genuine misuse).
    assert(iomanager.this_reactor() != sync_reactor && "iomgr::sync_wait must not be called on the sync reactor");

    std::promise< void > comp;
    auto fut = comp.get_future();
    // Issue + reap the op on the dedicated sync reactor (never the caller). We block on `fut` until on_done has
    // run, so the by-reference capture of `comp` (and the raw `p`, which lives in `op` on this frame) stays
    // valid throughout. The op completing on the sync reactor -- which is never itself a sync_wait caller -- is
    // exactly what makes blocking the caller here deadlock-free, even when the caller is another reactor.
    iomanager.run_on_forget(sync_reactor, [p, &comp]() {
        detail::start_drive_task(std::move(p->task), [p, &comp](std::error_code ec) {
            p->result = ec ? io_result{std::unexpected(ec.default_error_condition())} : io_result{p->nbytes};
            comp.set_value();
        });
    });
    fut.get();
    return p->result;
}

// ----- open / query -------------------------------------------------------------------------------
std::expected< drive_handle, std::error_condition > open_drive(const std::string& dev_name, int oflags) noexcept {
    try {
        return std::make_shared< drive >(DriveInterface::open_dev(dev_name, oflags));
    } catch (const std::system_error& e) { return std::unexpected(e.code().default_error_condition()); }
}
drive_attributes attributes_of(const std::string& dev_name) { return DriveInterface::get_attributes(dev_name); }
drive_type type_of(const std::string& dev_name) { return DriveInterface::get_drive_type(dev_name); }
size_t size_of(const drive_handle& d) { return DriveInterface::get_size(d->dev()); }

// ----- async I/O ----------------------------------------------------------------------------------
io_op async_write(const drive_handle& d, const char* data, uint32_t size, uint64_t offset) {
    return make_io_op(d->iface()->async_write(d->dev(), data, size, offset), size);
}
io_op async_writev(const drive_handle& d, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    return make_io_op(d->iface()->async_writev(d->dev(), iov, iovcnt, size, offset), size);
}
io_op async_read(const drive_handle& d, char* data, uint32_t size, uint64_t offset) {
    return make_io_op(d->iface()->async_read(d->dev(), data, size, offset), size);
}
io_op async_readv(const drive_handle& d, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    return make_io_op(d->iface()->async_readv(d->dev(), iov, iovcnt, size, offset), size);
}
io_op async_unmap(const drive_handle& d, uint32_t size, uint64_t offset) {
    return make_io_op(d->iface()->async_unmap(d->dev(), size, offset), size);
}
io_op async_write_zero(const drive_handle& d, uint64_t size, uint64_t offset) {
    return make_io_op(d->iface()->async_write_zero(d->dev(), size, offset), size);
}
io_op queue_fsync(const drive_handle& d) { return make_io_op(d->iface()->queue_fsync(d->dev()), 0); }

} // namespace iomgr

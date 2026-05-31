/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Harihara Kadayam, Yaming Kuang
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
#include "interfaces/uring_drive_interface.hpp"
#include <iomgr/iomgr.hpp>

#include <memory>
#include <system_error>
#include <vector>
#include "iomgr_config.hpp"

#ifdef __linux__
#include <sys/epoll.h>
#include <linux/version.h>
#endif

#include <sisl/fds/utils.hpp>
#include <sisl/logging/logging.h>
#include "epoll/reactor_epoll.hpp"

namespace iomgr {
thread_local uring_drive_channel* UringDriveInterface::t_uring_ch{nullptr};

using namespace std::chrono;

uring_drive_channel::uring_drive_channel(UringDriveInterface* iface) : m_sched{&m_ring} {
    int ret = io_uring_queue_init(IM_DYNAMIC_CONFIG(drive.uring_per_thread_qdepth), &m_ring, 0);
    if (ret) {
        throw std::system_error{errno, std::system_category(),
                                fmt::format("Unable to create uring queue created ret={}", ret)};
    }

    int ev_fd = eventfd(0, EFD_NONBLOCK);
    if (ev_fd == -1) {
        throw std::system_error{errno, std::system_category(),
                                "Unable to create eventfd to listen for uring queue events"};
    }

    ret = io_uring_register_eventfd(&m_ring, ev_fd);
    if (ret == -1) {
        throw std::system_error{errno, std::system_category(), "Unable to register event fd to uring queue"};
    }

    // Create io device and add it to the local thread; the sentinel reaps completions every loop.
    using namespace std::placeholders;
    m_ring_ev_iodev = iomanager.generic_interface()->make_io_device(
        ev_fd, EPOLLIN, 0, nullptr, true, std::bind(&UringDriveInterface::on_event_notification, iface, _1, _2, _3));
    iomanager.this_reactor()->attach_iomgr_sentinel_cb([iface]() { iface->poll_completions(); });
}

uring_drive_channel::~uring_drive_channel() {
    io_uring_queue_exit(&m_ring);
    if (m_ring_ev_iodev != nullptr) {
        iomanager.this_reactor()->detach_iomgr_sentinel_cb();
        iomanager.generic_interface()->remove_io_device(m_ring_ev_iodev);
        close(m_ring_ev_iodev->fd());
    }
}

///////////////////////////// UringDriveInterface /////////////////////////////////////////
UringDriveInterface::UringDriveInterface(const bool new_interface_supported) :
        KernelDriveInterface(), m_new_intfc(new_interface_supported) {}

void UringDriveInterface::init_iface_reactor_context(IOReactor*) {
    if (t_uring_ch == nullptr) { t_uring_ch = new uring_drive_channel(this); }
}

void UringDriveInterface::clear_iface_reactor_context(IOReactor*) {
    if (t_uring_ch != nullptr) {
        drain_outstanding_ios();
        delete t_uring_ch;
        t_uring_ch = nullptr;
    }
}

void UringDriveInterface::poll_completions() {
    // Flush any SQEs queued since the last poll and reap available completions, resuming the suspended
    // coroutines inline on this reactor thread. Non-blocking: the reactor's epoll_wait (woken by the
    // ring eventfd) provides the actual wait.
    t_uring_ch->m_sched.poll_once(nanoseconds{0});
}

void UringDriveInterface::on_event_notification(IODevice* iodev, [[maybe_unused]] void* cookie,
                                                [[maybe_unused]] int event) {
    uint64_t temp = 0;
    [[maybe_unused]] auto rsize = read(iodev->fd(), &temp, sizeof(uint64_t));
    poll_completions();
}

void UringDriveInterface::drain_outstanding_ios() {
    // Runs on the owning reactor thread during reactor stop; the ring is still valid. Keep flushing +
    // reaping until every in-flight drive coroutine has resumed and completed (freeing its iocb), so
    // nothing is stranded when the ring is torn down. Bounded so a stuck device can't hang stop.
    auto deadline = steady_clock::now() + seconds{5};
    while (t_uring_ch->m_outstanding > 0) {
        t_uring_ch->m_sched.poll_once(milliseconds{1});
        if (steady_clock::now() > deadline) {
            LOGERRORMOD(iomgr, "Timed out draining {} outstanding uring IOs at reactor stop",
                        t_uring_ch->m_outstanding);
            break;
        }
    }
}

void UringDriveInterface::submit_batch() {
    // No-op: the scheduler defers + batches submission in poll_once. Retained for API compatibility.
}

// --------- outstanding accounting + tight-poll while IO is in flight -----------------------
namespace {
inline void io_started() {
    auto* const ch = UringDriveInterface::this_channel();
    if (ch->m_outstanding++ == 0) {
        // tight-poll so queued SQEs flush promptly; restore the reactor's prior interval when drained.
        ch->m_saved_poll_interval = iomanager.this_reactor()->get_poll_interval();
        iomanager.this_reactor()->set_poll_interval(0);
    }
}
inline void io_finished() {
    auto* const ch = UringDriveInterface::this_channel();
    if (--ch->m_outstanding == 0) { iomanager.this_reactor()->set_poll_interval(ch->m_saved_poll_interval); }
}
// Advance an iovec list past `done` consumed bytes (drops fully-consumed entries, trims the first
// partially-consumed one). Used for partial readv/writev re-issue.
inline void advance_iovecs(std::vector< iovec >& iovs, size_t done) {
    size_t i = 0;
    while (i < iovs.size() && done > 0) {
        if (iovs[i].iov_len <= done) {
            done -= iovs[i].iov_len;
            ++i;
        } else {
            iovs[i].iov_base = static_cast< uint8_t* >(iovs[i].iov_base) + done;
            iovs[i].iov_len -= done;
            done = 0;
        }
    }
    if (i > 0) { iovs.erase(iovs.begin(), iovs.begin() + i); }
}
} // namespace

// RAII for the per-op metrics + outstanding accounting. Fires on normal co_return AND on a
// cancellation/exception unwind of the coroutine frame, so the counters never leak.
namespace {
struct io_op_scope {
    drive_iocb* iocb;
    explicit io_op_scope(drive_iocb* cb) : iocb{cb} {
        increment_outstanding_counter(iocb);
        io_started();
    }
    io_op_scope(const io_op_scope&) = delete;
    ~io_op_scope() {
        iocb->iodev->observe_metrics(iocb);
        decrement_outstanding_counter(iocb);
        io_finished();
    }
};
} // namespace

static std::error_code res_to_ec(int res) {
    return res >= 0 ? std::error_code{} : std::error_code{-res, std::system_category()};
}

io_device_ptr UringDriveInterface::open_dev(const std::string& devname, drive_type dev_type, int oflags) {
    LOGMSG_ASSERT(((dev_type == drive_type::block_nvme) || (dev_type == drive_type::block_hdd) ||
                   (dev_type == drive_type::file_on_hdd) || (dev_type == drive_type::file_on_nvme)),
                  "Unexpected dev type to open {}", dev_type);

    auto fd = open(devname.c_str(), oflags, 0640);
    if (fd == -1) {
        throw std::system_error{errno, std::system_category(),
                                fmt::format("Unable to open the device={} dev_type={}, errno={} strerror={}", devname,
                                            dev_type, errno, strerror(errno))};
    }

    auto iodev = alloc_io_device(fd, 9 /* pri */, reactor_regex::all_io);
    iodev->devname = devname;
    iodev->dtype = dev_type;
    iodev->enable_metrics(devname);

    LOGINFOMOD(iomgr, "Device={} of type={} opened with flags={} successfully, fd={}", devname, dev_type, oflags, fd);
    return iodev;
}

void UringDriveInterface::close_dev(const io_device_ptr& iodev) {
    IOInterface::close_dev(iodev);
    LOGINFOMOD(iomgr, "Device {} close device", iodev->devname);
    close(iodev->fd());
    iodev->clear();
}

// ----------------------------------- async ops -----------------------------------------------
// Each op runs ON a reactor thread (off-reactor callers are marshalled by the launcher), reads the
// reactor-local channel, and co_awaits the scheduler's async_submit sender in a retry loop that folds
// in EAGAIN/transient resubmit and partial-read remainder re-issue (the policy handle_completions used
// to own). The scheduler defers+batches submission and reaps in poll_completions().

exec::task< std::error_code > UringDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size,
                                                               uint64_t offset, bool) {
    auto& sched = t_uring_ch->m_sched;
    auto iocb = std::make_unique< drive_iocb >(this, iodev, DriveOpType::WRITE, size, offset);
    io_op_scope scope{iocb.get()};

    const char* buf = data;
    uint32_t remaining = size;
    uint64_t off = offset;
    uint32_t resub = 0;
    // Coroutine-local iovec for the legacy (non-new_intfc) path: io_uring_prep_writev stores &iov and
    // submission is DEFERRED to poll_once, so the iovec must outlive the prep lambda and the suspend.
    iovec iov{};
    for (;;) {
        const int res = co_await sched.async_submit([&](::io_uring_sqe* sqe) {
            if (m_new_intfc) {
                ::io_uring_prep_write(sqe, iodev->fd(), buf, remaining, off);
            } else {
                iov = iovec{const_cast< char* >(buf), remaining};
                ::io_uring_prep_writev(sqe, iodev->fd(), &iov, 1, off);
            }
        });
        if (res < 0) {
            if (res == -EAGAIN) continue;
            if (resub++ > IM_DYNAMIC_CONFIG(drive.max_resubmit_cnt)) co_return res_to_ec(res);
            continue;
        }
        if (static_cast< uint32_t >(res) == remaining) co_return std::error_code{};
        buf += res;
        off += res;
        remaining -= res;
    }
}

exec::task< std::error_code > UringDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt,
                                                                uint32_t size, uint64_t offset, bool) {
    auto& sched = t_uring_ch->m_sched;
    auto iocb = std::make_unique< drive_iocb >(this, iodev, DriveOpType::WRITE, size, offset);
    io_op_scope scope{iocb.get()};

    std::vector< iovec > iovs(iov, iov + iovcnt);
    uint64_t off = offset;
    uint32_t remaining = size, resub = 0;
    for (;;) {
        const int res = co_await sched.async_submit(
            [&](::io_uring_sqe* sqe) { ::io_uring_prep_writev(sqe, iodev->fd(), iovs.data(), iovs.size(), off); });
        if (res < 0) {
            if (res == -EAGAIN) continue;
            if (resub++ > IM_DYNAMIC_CONFIG(drive.max_resubmit_cnt)) co_return res_to_ec(res);
            continue;
        }
        if (static_cast< uint32_t >(res) == remaining) co_return std::error_code{};
        advance_iovecs(iovs, res);
        off += res;
        remaining -= res;
    }
}

exec::task< std::error_code > UringDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size,
                                                              uint64_t offset, bool) {
    auto& sched = t_uring_ch->m_sched;
    auto iocb = std::make_unique< drive_iocb >(this, iodev, DriveOpType::READ, size, offset);
    io_op_scope scope{iocb.get()};

    char* buf = data;
    uint32_t remaining = size, partial = 0, resub = 0;
    uint64_t off = offset;
    // Coroutine-local iovec for the legacy path (see async_write): must outlive the deferred submit.
    iovec iov{};
    for (;;) {
        const int res = co_await sched.async_submit([&](::io_uring_sqe* sqe) {
            if (m_new_intfc) {
                ::io_uring_prep_read(sqe, iodev->fd(), buf, remaining, off);
            } else {
                iov = iovec{buf, remaining};
                ::io_uring_prep_readv(sqe, iodev->fd(), &iov, 1, off);
            }
        });
        if (res < 0) {
            if (res == -EAGAIN) continue;
            if (resub++ > IM_DYNAMIC_CONFIG(drive.max_resubmit_cnt)) co_return res_to_ec(res);
            continue;
        }
        if (static_cast< uint32_t >(res) == remaining) co_return std::error_code{};
        if (partial++ > IM_DYNAMIC_CONFIG(drive.partial_read_max_resubmit_cnt))
            co_return std::make_error_code(std::errc::io_error); // never filled the tail: surface a failure
        COUNTER_INCREMENT(m_metrics, retry_on_partial_read, 1);
        buf += res;
        off += res;
        remaining -= res;
    }
}

exec::task< std::error_code > UringDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt,
                                                               uint32_t size, uint64_t offset, bool) {
    auto& sched = t_uring_ch->m_sched;
    auto iocb = std::make_unique< drive_iocb >(this, iodev, DriveOpType::READ, size, offset);
    io_op_scope scope{iocb.get()};

    std::vector< iovec > iovs(iov, iov + iovcnt);
    uint64_t off = offset;
    uint32_t remaining = size, partial = 0, resub = 0;
    for (;;) {
        const int res = co_await sched.async_submit(
            [&](::io_uring_sqe* sqe) { ::io_uring_prep_readv(sqe, iodev->fd(), iovs.data(), iovs.size(), off); });
        if (res < 0) {
            if (res == -EAGAIN) continue;
            if (resub++ > IM_DYNAMIC_CONFIG(drive.max_resubmit_cnt)) co_return res_to_ec(res);
            continue;
        }
        if (static_cast< uint32_t >(res) == remaining) co_return std::error_code{};
        if (partial++ > IM_DYNAMIC_CONFIG(drive.partial_read_max_resubmit_cnt))
            co_return std::make_error_code(std::errc::io_error); // never filled the tail: surface a failure
        COUNTER_INCREMENT(m_metrics, retry_on_partial_read, 1);
        advance_iovecs(iovs, res);
        off += res;
        remaining -= res;
    }
}

exec::task< std::error_code > UringDriveInterface::async_unmap(IODevice*, uint32_t, uint64_t, bool) {
    co_return std::make_error_code(std::errc::not_supported);
}

exec::task< std::error_code > UringDriveInterface::async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) {
    uint64_t remain = size, cur_offset = offset;
    while (remain > 0) {
        const uint64_t write_size = std::min(remain, k_write_zero_chunk);
        // Shared, process-wide, read-only zero buffer; the kernel only reads it for the write.
        auto ec = co_await async_write(iodev, reinterpret_cast< const char* >(zero_buffer(write_size)),
                                       static_cast< uint32_t >(write_size), cur_offset);
        if (ec) co_return ec;
        remain -= write_size;
        cur_offset += write_size;
    }
    co_return std::error_code{};
}

exec::task< std::error_code > UringDriveInterface::queue_fsync(IODevice* iodev) {
    auto& sched = t_uring_ch->m_sched;
    auto iocb = std::make_unique< drive_iocb >(this, iodev, DriveOpType::FSYNC, 0, 0);
    io_op_scope scope{iocb.get()};
    const int res = co_await sched.async_submit(
        [&](::io_uring_sqe* sqe) { ::io_uring_prep_fsync(sqe, iodev->fd(), IORING_FSYNC_DATASYNC); });
    co_return res_to_ec(res);
}
} // namespace iomgr

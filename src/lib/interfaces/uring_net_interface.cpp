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
#include "interfaces/uring_net_interface.hpp"
#include <iomgr/iomgr.hpp>

#include <cerrno>
#include <chrono>
#include <system_error>

#ifdef __linux__
#include <sys/epoll.h>
#endif

extern "C" {
#include <unistd.h>
}

#include <sisl/logging/logging.h>
#include "iomgr_config.hpp"
#include "reactor/reactor.hpp"

namespace iomgr {
thread_local uring_net_channel* UringNetInterface::t_uring_net_ch{nullptr};

uring_net_channel::uring_net_channel(UringNetInterface* iface) : m_sched{&m_ring} {
    // iomgr_params::net_uring_qdepth (0 = unset) takes precedence over the process-wide dynamic config
    // default.
    auto const qdepth = (iomanager.m_net_uring_qdepth != 0) ? iomanager.m_net_uring_qdepth
                                                            : IM_DYNAMIC_CONFIG(net.uring_per_thread_qdepth);

    // See uring_drive_channel's constructor for the rationale on these flags -- same per-reactor,
    // single-submitter story applies here.
    io_uring_params params{};
#ifdef IORING_SETUP_SINGLE_ISSUER
    if (IM_DYNAMIC_CONFIG(net.uring_single_issuer) != 0) { params.flags |= IORING_SETUP_SINGLE_ISSUER; }
#endif
#ifdef IORING_SETUP_CQSIZE
    if (auto const cq_mult = IM_DYNAMIC_CONFIG(net.uring_cq_size_multiple); cq_mult > 0) {
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = qdepth * cq_mult;
    }
#endif

    int ret = io_uring_queue_init_params(qdepth, &m_ring, &params);
    if ((ret == -EINVAL) && (params.flags != 0)) {
        LOGWARN("io_uring setup flags {:#x} unsupported by this kernel; retrying net ring with defaults", params.flags);
        params = io_uring_params{};
        ret = io_uring_queue_init_params(qdepth, &m_ring, &params);
    }
    if (ret) {
        throw std::system_error{-ret, std::system_category(),
                                fmt::format("Unable to create net uring queue ret={}", ret)};
    }

    // From here, anything that throws must still release m_ring: the destructor never runs for a
    // constructor that didn't finish.
    struct ring_guard {
        ::io_uring* ring;
        bool dismissed{false};
        ~ring_guard() {
            if (!dismissed) { io_uring_queue_exit(ring); }
        }
    } ring_cleanup{&m_ring};

    // drain_outstanding_ops needs kernel 5.19+ for IORING_ASYNC_CANCEL_ANY/ALL; older kernels fail that
    // cancel SQE with -EINVAL, invisible through the normal reap loop since its CQE is unmanaged
    // (user_data=0), same as this probe's. Submit the identical SQE now, on an empty ring, and wait on it
    // directly -- surfaces the gap once at startup instead of as an unexplained stall+leak at reactor stop.
    if (auto* const sqe = ::io_uring_get_sqe(&m_ring)) {
        ::io_uring_prep_cancel64(sqe, 0, IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL);
        ::io_uring_sqe_set_data64(sqe, 0);
        ::io_uring_submit(&m_ring);
        if (::io_uring_cqe* cqe = nullptr; ::io_uring_wait_cqe(&m_ring, &cqe) == 0) {
            if (cqe->res == -EINVAL) {
                LOGWARNMOD(iomgr, "kernel does not support io_uring cancel-any/all (needs 5.19+) -- a socket "
                                 "still in flight at reactor stop can stall for the drain deadline and leak "
                                 "this reactor's net channel instead of being cancelled");
            }
            ::io_uring_cqe_seen(&m_ring, cqe);
        }
    }

    int ev_fd = eventfd(0, EFD_NONBLOCK);
    if (ev_fd == -1) {
        throw std::system_error{errno, std::system_category(),
                                "Unable to create eventfd to listen for net uring queue events"};
    }
    struct fd_guard {
        int fd;
        bool dismissed{false};
        ~fd_guard() {
            if (!dismissed) { ::close(fd); }
        }
    } fd_cleanup{ev_fd};

    // io_uring_register_eventfd() is a liburing wrapper: returns 0 or -errno, not -1/errno like a raw
    // syscall.
    ret = io_uring_register_eventfd(&m_ring, ev_fd);
    if (ret) {
        throw std::system_error{-ret, std::system_category(), "Unable to register event fd to net uring queue"};
    }

    // Create io device and add it to the local thread; the sentinel reaps completions every loop.
    using namespace std::placeholders;
    m_ring_ev_iodev = iomanager.generic_interface()->make_io_device(
        ev_fd, EPOLLIN, 0, nullptr, true, std::bind(&UringNetInterface::on_event_notification, iface, _1, _2, _3));
    m_sentinel_idx = iomanager.this_reactor()->attach_iomgr_sentinel_cb([iface]() { iface->poll_completions(); });

    fd_cleanup.dismissed = true;
    ring_cleanup.dismissed = true;
}

uring_net_channel::~uring_net_channel() {
    io_uring_queue_exit(&m_ring);
    if (m_ring_ev_iodev != nullptr) {
        iomanager.this_reactor()->detach_iomgr_sentinel_cb(m_sentinel_idx);
        iomanager.generic_interface()->remove_io_device(m_ring_ev_iodev, true /* wait_to_remove */);
    }
}

void UringNetInterface::init_iface_reactor_context(IOReactor*) {
    if (t_uring_net_ch == nullptr) { t_uring_net_ch = new uring_net_channel(this); }
}

void UringNetInterface::clear_iface_reactor_context(IOReactor*) {
    if (t_uring_net_ch != nullptr) {
        // m_scope asserts nothing is outstanding at destruction; only delete once drain confirms that.
        if (drain_outstanding_ops()) {
            delete t_uring_net_ch;
            t_uring_net_ch = nullptr;
        }
    }
}

bool UringNetInterface::drain_outstanding_ops() {
    auto* const ch = t_uring_net_ch;
    ch->m_scope.request_stop(); // propagates to run_uring_connect / close_once_drained / anything else nested in it

    if (ch->m_outstanding == 0) { return true; }

    // Cancel everything still in flight on this ring in one shot: IORING_ASYNC_CANCEL_ANY matches any
    // request regardless of key, and IORING_ASYNC_CANCEL_ALL cancels all of them, not just the first. The
    // cancel SQE's own user_data is left 0 ("unmanaged") so poll_once's reap loop ignores its CQE; each
    // cancelled op's own CQE still arrives as -ECANCELED and completes normally.
    if (auto* const sqe = ::io_uring_get_sqe(ch->m_sched.ring())) {
        ::io_uring_prep_cancel64(sqe, 0, IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL);
        ::io_uring_sqe_set_data64(sqe, 0);
        ::io_uring_submit(ch->m_sched.ring()); // don't wait for poll_once's own first iteration to flush this
    }

    using namespace std::chrono;
    auto const deadline = steady_clock::now() + seconds{5};
    while (ch->m_outstanding > 0) {
        ch->m_sched.poll_once(milliseconds{1});
        if (steady_clock::now() > deadline) {
            LOGERRORMOD(iomgr,
                        "Timed out cancelling {} outstanding net uring ops at reactor stop -- leaking this "
                        "reactor's net channel (ring + scope) rather than risk tearing it down while still "
                        "referenced",
                        ch->m_outstanding);
            return false;
        }
    }
    return true;
}

void UringNetInterface::poll_completions() {
    // Flush any SQEs queued since the last poll and reap available completions, resuming the suspended
    // coroutines inline on this reactor thread. Non-blocking: the reactor's epoll_wait (woken by the
    // ring eventfd) provides the actual wait.
    auto& sched = t_uring_net_ch->m_sched;
    sched.poll_once(std::chrono::nanoseconds{0});

    // Coroutines resumed above may queue new SQEs of their own (e.g. the next recv in a read loop).
    // poll_once only flushes SQEs queued before it ran, so those need an explicit submit here.
    if (::io_uring_sq_ready(sched.ring()) > 0) { ::io_uring_submit(sched.ring()); }
}

void UringNetInterface::on_event_notification(IODevice* iodev, [[maybe_unused]] void* cookie,
                                              [[maybe_unused]] int event) {
    uint64_t temp = 0;
    [[maybe_unused]] auto rsize = read(iodev->fd(), &temp, sizeof(uint64_t));
    poll_completions();
}
} // namespace iomgr

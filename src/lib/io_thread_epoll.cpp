/**
 * Copyright eBay Corporation 2018
 */

extern "C" {
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <time.h>
}

#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/io_thread_epoll.hpp"
#include <fds/obj_allocator.hpp>

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {

using Clock = std::chrono::steady_clock;

uint64_t get_elapsed_time_ns(Clock::time_point startTime) {
    std::chrono::nanoseconds ns = std::chrono::duration_cast< std::chrono::nanoseconds >(Clock::now() - startTime);
    return ns.count();
}

#define MAX_EVENTS 20
#define ESTIMATED_MSGS_PER_THREAD 128

static bool compare_priority(const epoll_event& ev1, const epoll_event& ev2) {
    io_device_t* iodev1 = (io_device_t*)ev1.data.ptr;
    io_device_t* iodev2 = (io_device_t*)ev2.data.ptr;

    // In case of equal priority, pick global fd which could get rescheduled
    if (iodev1->pri == iodev2->pri) { return iodev1->is_global; }
    return (iodev1->pri > iodev2->pri);
}

IOReactorEPoll::IOReactorEPoll() : m_msg_q(ESTIMATED_MSGS_PER_THREAD) {}

bool IOReactorEPoll::iocontext_init() {
    // Create a epollset for one per thread
    m_epollfd = epoll_create1(0);
    if (m_epollfd < 1) {
        assert(0);
        LOGERROR("epoll_create failed: {}", strerror(errno));
        goto error;
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    m_is_io_thread = true;

    LOGTRACEMOD(iomgr, "EPoll created: {}", m_epollfd);

    // Create a message fd and add it to tht epollset
    m_msg_iodev = std::make_shared< io_device_t >();
    m_msg_iodev->dev = backing_dev_t(eventfd(0, EFD_NONBLOCK));
    if (m_msg_iodev->fd() == -1) {
        assert(0);
        LOGERROR("Unable to open the eventfd, marking this as non-io thread");
        goto error;
    }
    m_msg_iodev->ev = EPOLLIN;
    m_msg_iodev->pri = 1; // Set message fd as high priority. TODO: Make multiple messages fd for various priority
    LOGINFO("Creating a message event fd {} and add to this thread epoll fd {}", m_msg_iodev->fd(), m_epollfd)
    if (add_iodev_to_reactor(m_msg_iodev) == -1) { goto error; }

    // Create a per thread timer
    m_thread_timer = std::make_unique< timer_epoll >(true /* is_per_thread */);
    return true;

error:
    if (m_epollfd > 0) {
        close(m_epollfd);
        m_epollfd = -1;
    }

    if (m_msg_iodev) {
        if (m_msg_iodev->fd() > 0) { close(m_msg_iodev->fd()); }
        m_msg_iodev = nullptr;
    }
    return false;
}

void IOReactorEPoll::iocontext_exit() {
    if (m_msg_iodev && (m_msg_iodev->fd() != -1)) {
        remove_iodev_from_reactor(m_msg_iodev);
        close(m_msg_iodev->fd());
    }

    m_thread_timer->stop();
    if (m_epollfd != -1) { close(m_epollfd); }

    m_metrics = nullptr;
}

void IOReactorEPoll::listen() {
    std::array< struct epoll_event, MAX_EVENTS > events;

    int num_fds = 0;
    do {
        num_fds = epoll_wait(m_epollfd, &events[0], MAX_EVENTS, iomanager.idle_timeout_interval_usec());
    } while (num_fds < 0 && errno == EINTR);

    if (num_fds == 0) {
        iomanager.idle_timeout_expired();
        return;
    } else if (num_fds < 0) {
        LOGERROR("epoll wait failed: {} strerror {}", errno, strerror(errno));
        return;
    }

    // Next sort the events based on priority and handle them in that order
    std::sort(events.begin(), (events.begin() + num_fds), compare_priority);
    for (auto i = 0; i < num_fds; ++i) {
        auto& e = events[i];
        if (e.data.ptr == (void*)m_msg_iodev.get()) {
            LOGTRACEMOD(iomgr, "Processing event on msg fd: {}", m_msg_iodev->fd());
            ++m_metrics->msg_recvd_count;
            on_msg_fd_notification();

            // It is possible for io thread status by the msg processor. Catch at the exit and return
            if (!m_is_io_thread) {
                LOGINFO("listen will exit because this is no longer an iothread");
                return;
            }
        } else {
            io_device_t* iodev = (io_device_t*)e.data.ptr;
            if (iodev->tinfo) {
                timer_epoll::on_timer_fd_notification(iodev);
            } else {
                on_user_iodev_notification(iodev, e.events);
            }
        }
    }
}

int IOReactorEPoll::add_iodev_to_reactor(const io_device_ptr& iodev) {
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLEXCLUSIVE | iodev->ev;
    ev.data.ptr = (void*)iodev.get();
    if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, iodev->fd(), &ev) == -1) {
        LOGDFATAL("Adding fd {} to this thread's epoll fd {} failed, error = {}", iodev->fd(), m_epollfd,
                  strerror(errno));
        return -1;
    }
    LOGDEBUGMOD(iomgr, "Added fd {} to this io thread's epoll fd {}, data.ptr={}", iodev->fd(), m_epollfd,
                (void*)ev.data.ptr);
    ++m_n_iodevices;
    iodev->io_interface->on_add_iodev_to_reactor(this, iodev);
    return 0;
}

int IOReactorEPoll::remove_iodev_from_reactor(const io_device_ptr& iodev) {
    if (epoll_ctl(m_epollfd, EPOLL_CTL_DEL, iodev->fd(), nullptr) == -1) {
        LOGDFATAL("Removing fd {} to this thread's epoll fd {} failed, error = {}", iodev->fd(), m_epollfd,
                  strerror(errno));
        return -1;
    }
    LOGDEBUGMOD(iomgr, "Removed fd {} from this io thread's epoll fd {}", iodev->fd(), m_epollfd);
    --m_n_iodevices;
    iodev->io_interface->on_remove_iodev_from_reactor(this, iodev);
    return 0;
}

bool IOReactorEPoll::send_msg(const iomgr_msg& msg) {
    if (!m_msg_iodev) return false;

    if (msg.is_sync_msg) { sync_iomgr_msg::to_sync_msg(msg).pending(); }
    LOGTRACEMOD(iomgr, "Sending msg of type {} to local thread msg fd = {}, ptr = {}", msg.m_type, m_msg_iodev->fd(),
                (void*)m_msg_iodev.get());
    put_msg(msg);
    uint64_t temp = 1;
    while (0 > write(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    return true;
}

void IOReactorEPoll::put_msg(const iomgr_msg& msg) { m_msg_q.blockingWrite(msg); }

void IOReactorEPoll::on_msg_fd_notification() {
    uint64_t temp;
    while (0 > read(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    // Start pulling all the messages and handle them.
    while (true) {
        iomgr_msg msg;
        if (!m_msg_q.read(msg)) { break; }

        ++m_metrics->msg_recvd_count;
        iomanager.get_msg_module(msg.m_dest_module)(msg);
    }
}
} // namespace iomgr

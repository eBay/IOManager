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
#include "include/reactor_epoll.hpp"
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
#define ESTIMATED_MSGS_PER_THREAD 10000

static bool compare_priority(const epoll_event& ev1, const epoll_event& ev2) {
    IODevice* iodev1 = (IODevice*)ev1.data.ptr;
    IODevice* iodev2 = (IODevice*)ev2.data.ptr;

    // In case of equal priority, pick global fd which could get rescheduled
    if (iodev1->pri == iodev2->pri) { return iodev1->is_global(); }
    return (iodev1->pri > iodev2->pri);
}

IOReactorEPoll::IOReactorEPoll() : m_msg_q(ESTIMATED_MSGS_PER_THREAD) {}

bool IOReactorEPoll::reactor_specific_init_thread(const io_thread_t& thr) {
    // Create a epollset for one per thread
    m_epollfd = epoll_create1(0);
    if (m_epollfd < 1) {
        assert(0);
        REACTOR_LOG(ERROR, base, thr->thread_addr, "epoll_create failed: {}", strerror(errno));
        goto error;
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    thr->thread_impl = m_reactor_num;

    REACTOR_LOG(TRACE, iomgr, thr->thread_addr, "EPoll created: {}", m_epollfd);

    // Create a message fd and add it to tht epollset
    m_msg_iodev = std::make_shared< IODevice >();
    m_msg_iodev->dev = backing_dev_t(eventfd(0, EFD_NONBLOCK));
    if (m_msg_iodev->fd() == -1) {
        assert(0);
        REACTOR_LOG(ERROR, base, thr->thread_addr, "Unable to open the eventfd, marking this as non-io reactor");
        goto error;
    }
    m_msg_iodev->ev = EPOLLIN;
    m_msg_iodev->pri = 1; // Set message fd as high priority. TODO: Make multiple messages fd for various priority
    REACTOR_LOG(INFO, base, thr->thread_addr, "Creating a message event fd {} and add to this thread epoll fd {}",
                m_msg_iodev->fd(), m_epollfd)
    if (add_iodev_to_thread(m_msg_iodev, thr) == -1) { goto error; }

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

void IOReactorEPoll::reactor_specific_exit_thread(const io_thread_t& thr) {
    if (m_msg_iodev && (m_msg_iodev->fd() != -1)) {
        remove_iodev_from_thread(m_msg_iodev, thr);
        close(m_msg_iodev->fd());
    }

    m_thread_timer->stop();
    if (m_epollfd != -1) { close(m_epollfd); }

    // Drain the message q and drop the message.
    auto dropped = 0u;
    iomgr_msg* msg;
    while (m_msg_q.read(msg)) {
        if (msg->has_sem_block()) { msg->m_msg_sem->done(); }
        iomgr_msg::free(msg);
    }
    if (dropped) { LOGINFO("Exiting the reactor with {} messages yet to handle, dropping them", dropped); }
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
        REACTOR_LOG(ERROR, base, , "epoll wait failed: {} strerror {}", errno, strerror(errno));
        return;
    }

    // Next sort the events based on priority and handle them in that order
    std::sort(events.begin(), (events.begin() + num_fds), compare_priority);
    for (auto i = 0; i < num_fds; ++i) {
        auto& e = events[i];
        if (e.data.ptr == (void*)m_msg_iodev.get()) {
            REACTOR_LOG(TRACE, iomgr, , "Processing event on msg fd: {}", m_msg_iodev->fd());
            ++m_io_threads[0]->m_metrics->msg_recvd_count;
            on_msg_fd_notification();

            // It is possible for io thread status by the msg processor. Catch at the exit and return
            if (!is_io_reactor()) {
                REACTOR_LOG(INFO, base, , "listen will exit because this is no longer an io reactor");
                return;
            }
        } else {
            IODevice* iodev = (IODevice*)e.data.ptr;
            if (iodev->tinfo) {
                timer_epoll::on_timer_fd_notification(iodev);
            } else {
                on_user_iodev_notification(iodev, e.events);
            }
        }
    }
}

int IOReactorEPoll::_add_iodev_to_thread(const io_device_ptr& iodev, [[maybe_unused]] const io_thread_t& thr) {
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLEXCLUSIVE | iodev->ev;
    ev.data.ptr = (void*)iodev.get();
    if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, iodev->fd(), &ev) == -1) {
        LOGDFATAL("Adding fd {} to this thread's epoll fd {} failed, error = {}", iodev->fd(), m_epollfd,
                  strerror(errno));
        return -1;
    }
    REACTOR_LOG(DEBUG, iomgr, thr->thread_addr, "Added fd {} to this io thread's epoll fd {}, data.ptr={}", iodev->fd(),
                m_epollfd, (void*)ev.data.ptr);
    return 0;
}

int IOReactorEPoll::_remove_iodev_from_thread(const io_device_ptr& iodev, [[maybe_unused]] const io_thread_t& thr) {
    if (epoll_ctl(m_epollfd, EPOLL_CTL_DEL, iodev->fd(), nullptr) == -1) {
        LOGDFATAL("Removing fd {} to this thread's epoll fd {} failed, error = {}", iodev->fd(), m_epollfd,
                  strerror(errno));
        return -1;
    }
    REACTOR_LOG(DEBUG, iomgr, thr->thread_addr, "Removed fd {} from this io thread's epoll fd {}", iodev->fd(),
                m_epollfd);
    return 0;
}

bool IOReactorEPoll::put_msg(iomgr_msg* msg) {
    if (!m_msg_iodev) return false;

    REACTOR_LOG(DEBUG, iomgr, msg->m_dest_thread, "Put msg of type {} to its msg fd = {}, ptr = {}", msg->m_type,
                m_reactor_num, msg->m_dest_thread, m_msg_iodev->fd(), (void*)m_msg_iodev.get());

    m_msg_q.blockingWrite(msg);
    uint64_t temp = 1;
    while (0 > write(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    return true;
}

void IOReactorEPoll::on_msg_fd_notification() {
    uint64_t temp;
    while (0 > read(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    // Start pulling all the messages and handle them.
    while (true) {
        iomgr_msg* msg;
        if (!m_msg_q.read(msg)) { break; }
        handle_msg(msg);
    }
}

void IOReactorEPoll::on_user_iodev_notification(IODevice* iodev, int event) {
    ++m_io_threads[0]->m_metrics->outstanding_ops;
    ++m_io_threads[0]->m_metrics->io_count;

    REACTOR_LOG(TRACE, iomgr, , "Processing event on user iodev: {}", iodev->dev_id());
    iodev->cb(iodev, iodev->cookie, event);

    --m_io_threads[0]->m_metrics->outstanding_ops;
}

bool IOReactorEPoll::is_iodev_addable(const io_device_ptr& iodev, const io_thread_t& thread) const {
    return (!iodev->is_spdk_dev() && IOReactor::is_iodev_addable(iodev, thread));
}
} // namespace iomgr

/**
 * Copyright eBay Corporation 2018
 */

#include "io_thread.hpp"

extern "C" {
#include <sys/eventfd.h>
#include <sys/epoll.h>
}

#include <sds_logging/logging.h>

#include "iomgr.hpp"

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

static bool compare_prioirty(const epoll_event& ev1, const epoll_event& ev2) {
    fd_info* info1 = (fd_info *)ev1.data.ptr;
    fd_info* info2 = (fd_info *)ev2.data.ptr;
    return (info1->pri > info2->pri);
}

ioMgrThreadContext::ioMgrThreadContext() :
        m_msg_q(ESTIMATED_MSGS_PER_THREAD) {
    m_thread_num = sisl::ThreadLocalContext::my_thread_num();
}

ioMgrThreadContext::~ioMgrThreadContext() {
    iomgr_instance.foreach_endpoint([&](EndPoint* ep) { ep->on_thread_exit(); });
    if (m_epollfd != -1) { close(m_epollfd); }
    if (m_msg_fd_info && (m_msg_fd_info->fd != -1)) { close(m_msg_fd_info->fd); }
}

void ioMgrThreadContext::run() {
    if (!m_is_io_thread) {
        iothread_init(true /* wait_till_ready */);
    }

    bool keep_running = true;
    while (keep_running) {
        listen();
    }
}

void ioMgrThreadContext::iothread_init(bool wait_till_ready) {
    if (!iomgr_instance.is_ready()) {
        if (!wait_till_ready) {
            LOGTRACEMOD(iomgr, "IOmanager is not ready yet, will init the thread context once it is ready later");
            return;
        }
        iomgr_instance.wait_to_be_ready();
    }

    LOGTRACEMOD(iomgr, "Initializing iomanager context for this thread");
    m_is_io_thread = true;

    // Create a epollset for one per thread
    m_epollfd = epoll_create1(0);
    if (m_epollfd < 1) {
        assert(0);
        LOGERROR("epoll_ctl failed: {}", strerror(errno));
        goto error;
    }

    LOGTRACEMOD(iomgr, "EPoll created: {}", m_epollfd);

    // Create a message fd and add it to tht epollset
    m_msg_fd_info = std::make_unique< fd_info >();
    m_msg_fd_info->fd = eventfd(0, EFD_NONBLOCK);
    if (m_msg_fd_info->fd == -1) {
        assert(0);
        LOGERROR("Unable to open the eventfd, marking this as non-io thread");
        goto error;
    }

    // Set message fd as high priority. TODO: Make multiple messages fd for various priority
    m_msg_fd_info->pri = 1;

    struct epoll_event msg_ev;
    msg_ev.events = EPOLLET | EPOLLIN;
    msg_ev.data.fd = m_msg_fd_info->fd;
    msg_ev.data.ptr = m_msg_fd_info.get();
    if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, m_msg_fd_info->fd, &msg_ev) == -1) {
        assert(0);
        goto error;
    }

    LOGDEBUGMOD(iomgr, "Epoll event added");

    // For every registered fds add my thread portion of event fd
    iomgr_instance.foreach_fd_info([&](fd_info* fdi) {
        struct epoll_event ev;
        ev.events = EPOLLET | EPOLLEXCLUSIVE | fdi->ev;
        ev.data.ptr = fdi;
        if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, fdi->fd, &ev) == -1) {
            assert(0);
            LOGERROR("epoll_ctl failed: {}", strerror(errno));
        }

        LOGTRACEMOD(iomgr, "registered event fd {} to this thread context", fdi->fd);
    });

    /* Notify all the end points about new thread */
    iomgr_instance.foreach_endpoint([&](EndPoint* ep) { ep->on_thread_start(); });
    return;

error:
    m_is_io_thread = false;

    if (m_epollfd > 0) {
        close(m_epollfd);
        m_epollfd = -1;
    }

    if (m_msg_fd_info) {
        if (m_msg_fd_info->fd > 0) { close(m_msg_fd_info->fd); }
        m_msg_fd_info = nullptr;
    }
}

bool ioMgrThreadContext::is_io_thread() const { return m_is_io_thread; }

void ioMgrThreadContext::listen() {
    std::array<struct epoll_event, MAX_EVENTS > events;

    int num_fds = epoll_wait(m_epollfd, &events[0], MAX_EVENTS, iomgr_instance.idle_timeout_interval_usec());
    if (num_fds == 0) {
        iomgr_instance.idle_timeout_expired();
        return;
    } else if (num_fds < 0) {
        LOGERROR("epoll wait failed: {}", errno);
        return;
    }
    // Next sort the events based on priority and handle them in that order
    std::sort(events.begin(), events.end(), compare_prioirty);
    for (auto &e : events) {
        if (e.data.fd == m_msg_fd_info->fd) {
            on_msg_fd_notification();
        } else {
            Clock::time_point write_startTime = Clock::now();
            ++m_count;
            LOGTRACEMOD(iomgr, "Processing event on fd: {}", e.data.fd);

            fd_info* info = (fd_info*)e.data.ptr;
            info->cb(info->fd, info->cookie, e.events);

            m_time_spent_ns += get_elapsed_time_ns(write_startTime);
            LOGTRACEMOD(iomgr, "Call took: {}ns", m_time_spent_ns);
        }
    }
}

fd_info* ioMgrThreadContext::add_fd_to_thread(EndPoint* ep, int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie) {
    // if (!m_is_io_thread) { iothread_init(true /* wait_till_ready */); }
    if (!m_is_io_thread) {
        LOGTRACEMOD(iomgr, "Ignoring to add fd {} to this non-io thread", fd);
        return nullptr;
    }

    fd_info* info = iomgr_instance.create_fd_info(ep, fd, cb, iomgr_ev, pri, cookie);

    struct epoll_event ev;
    ev.events = EPOLLET | iomgr_ev;
    ev.data.ptr = info;
    if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        assert(0);
    }
    LOGDEBUGMOD(iomgr, "Added FD: {} to this thread context", fd);
    return info;
}

void ioMgrThreadContext::put_msg(const iomgr_msg& msg) {
    m_msg_q.blockingWrite(msg);
}

void ioMgrThreadContext::put_msg(iomgr_msg_type type, fd_info *info, int event, void *buf, uint32_t size) {
    put_msg(iomgr_msg(type, info, event, buf, size));
}

void ioMgrThreadContext::on_msg_fd_notification() {
    uint64_t temp;
    while (0 > read(m_msg_fd_info->fd, &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    // Start pulling all the messages and handle them.
    while(true) {
        iomgr_msg msg;
        if (!m_msg_q.read(msg)) {
            break;
        }

        switch (msg.m_type) {
        case RESCHEDULE: {
            auto info = msg.m_fd_info;
            if (msg.m_event & EPOLLIN) { info->cb(info->fd, info->cookie, EPOLLIN); }
            if (msg.m_event & EPOLLOUT) { info->cb(info->fd, info->cookie, EPOLLOUT); }
            break;
        }

        case UNKNOWN:
        case WAKEUP:
        case SHUTDOWN:
        case DESIGNATE_IO_THREAD:
        case RELINQUISH_IO_THREAD:
        default:
            assert(0);
            break;
        }
    }
}
} // namespace iomgr

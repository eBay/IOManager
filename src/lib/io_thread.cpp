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
#include "iomgr_impl.hpp"

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {

using Clock = std::chrono::steady_clock;

uint64_t get_elapsed_time_ns(Clock::time_point startTime) {
    std::chrono::nanoseconds ns = std::chrono::duration_cast< std::chrono::nanoseconds >(Clock::now() - startTime);
    return ns.count();
}

#define MAX_EVENTS 20

ioMgrThreadContext::ioMgrThreadContext(iomgr::ioMgrImpl* iomgr) {
    m_iomgr = iomgr;
    m_thread_num = sisl::ThreadLocalContext::my_thread_num();
    // context_init(false /* wait_till_ready */);
}

void ioMgrThreadContext::context_init(bool wait_till_ready) {
    if (!m_iomgr->is_ready()) {
        if (!wait_till_ready) {
            LOGTRACEMOD(iomgr, "IOmanager is not ready yet, will init the thread context once it is ready later");
            return;
        }
        m_iomgr->wait_to_be_ready();
    }

    LOGTRACEMOD(iomgr, "Initializing iomanager context for this thread");
    m_inited = true;

    m_epollfd = epoll_create1(0);
    if (m_epollfd < 1) {
        assert(0);
        LOGERROR("epoll_ctl failed: {}", strerror(errno));
    }
    LOGTRACEMOD(iomgr, "EPoll created: {}", m_epollfd);

    // Create parent epollset and mutliple priority based children epollsets.
    struct epoll_event ev;
    for (auto i = 0ul; i < MAX_PRI; ++i) {
        m_epollfd_pri[i] = epoll_create1(0);
        ev.events = EPOLLET | EPOLLIN | EPOLLOUT;
        ev.data.fd = m_epollfd_pri[i];
        if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, m_epollfd_pri[i], &ev) == -1) {
            LOGERROR("epoll_ctl failed: {}", strerror(errno));
            assert(0);
        }
    }

    // For every registered fds add my thread portion of event fd
    m_iomgr->foreach_fd_info([&](fd_info* fdi) {
        /* We cannot use EPOLLEXCLUSIVE flag here. otherwise
         * some events can be missed.
         */
        ev.events = EPOLLET | fdi->ev;
        ev.data.ptr = fdi;
        if (epoll_ctl(m_epollfd_pri[fdi->pri], EPOLL_CTL_ADD, fdi->fd, &ev) == -1) {
            assert(0);
            LOGERROR("epoll_ctl failed: {}", strerror(errno));
        }

        // Create an event fd and add it to this thread representation of infos.ev_fd array
        auto my_event_fd = eventfd(0, EFD_NONBLOCK);
        fdi->ev_fd[m_thread_num] = my_event_fd;
        add_fd_to_thread(
            my_event_fd,
            [this](int fd, void* cookie, uint32_t events) { this->m_iomgr->process_evfd(fd, cookie, events); }, EPOLLIN,
            1, fdi);

        LOGTRACEMOD(iomgr, "registered event fd {} to this thread context", my_event_fd);
    });

    /* initialize all the thread local variables in end point */
    m_iomgr->foreach_endpoint([&](EndPoint* ep) { ep->init_local(); });
}

void ioMgrThreadContext::run() {
    if (!m_inited) {
        context_init(true /* wait_till_ready */);
    }

    bool keep_running = true;
    while (keep_running) {
        listen();
    }
}

bool ioMgrThreadContext::is_io_thread() const { return m_inited; }

void ioMgrThreadContext::listen() {
    struct epoll_event fd_events[MAX_PRI];
    struct epoll_event events[MAX_EVENTS];
    int                num_fds;

    num_fds = epoll_wait(m_epollfd, fd_events, MAX_PRI, -1);
    for (auto i = 0ul; i < MAX_PRI; ++i) {
        /* XXX: should it be  go through only those fds which has the events. */
        num_fds = epoll_wait(m_epollfd_pri[i], events, MAX_EVENTS, 0);
        if (num_fds < 0) {
            LOGERROR("epoll wait failed: {}", errno);
            continue;
        }

        for (auto j = 0; j < num_fds; ++j) {
            LOGTRACEMOD(iomgr, "Checking: {}", j);

            if (m_iomgr->can_process(events[j].data.ptr, events[j].events)) {
                Clock::time_point write_startTime = Clock::now();
                ++m_count;
                LOGTRACEMOD(iomgr, "Processing event on: {}", j);
                m_iomgr->callback(events[j].data.ptr, events[j].events);
                m_time_spent_ns += get_elapsed_time_ns(write_startTime);
                LOGTRACEMOD(iomgr, "Call took: {}ns", m_time_spent_ns);
            } else {
            }
        }
    }
}

fd_info* ioMgrThreadContext::add_fd_to_thread(int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie) {
    // if (!m_inited) { context_init(true /* wait_till_ready */); }
    if (!m_inited) {
        LOGTRACEMOD(iomgr, "Ignoring to add fd {} to this non-io thread", fd);
        return nullptr;
    }

    fd_info* info = m_iomgr->create_fd_info(fd, cb, iomgr_ev, pri, cookie);

    struct epoll_event ev;
    ev.events = EPOLLET | iomgr_ev;
    ev.data.ptr = info;
    if (epoll_ctl(m_epollfd_pri[pri], EPOLL_CTL_ADD, fd, &ev) == -1) {
        assert(0);
    }
    LOGDEBUGMOD(iomgr, "Added FD: {} to this thread context", fd);
    return info;
}
} // namespace iomgr

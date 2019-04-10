//
// Created by Rishabh Mittal on 04/20/2018
//

#include "iomgr_impl.hpp"

extern "C" {
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/types.h>
}

#include <cerrno>
#include <chrono>
#include <ctime>
#include <functional>
#include <vector>
#include <thread>

#include <sds_logging/logging.h>

#include "io_thread.hpp"
#include "iomgr.hpp"
#include <utility/thread_factory.hpp>

namespace iomgr {

ioMgrImpl::ioMgrImpl(size_t const expected_eps, size_t const num_threads) :
        m_expected_eps(expected_eps),
        m_num_threads(num_threads),
        m_ready((expected_eps == 0)),
        m_thread_ctx(this) {
    m_fd_infos.wlock()->reserve(expected_eps * 10);
    m_ep_list.wlock()->reserve(expected_eps);
    LOGINFO("Starting ioManager");
}

ioMgrImpl::~ioMgrImpl() = default;

void ioMgrImpl::start() {
    for (auto i = 0u; i < m_num_threads; i++) {
        auto t = sisl::thread_factory("io_thread", &ioMgrImpl::run_io_loop, this);
        LOGTRACEMOD(iomgr, "Created iomanager thread...", i);
        t.detach();
    }
}

void ioMgrImpl::run_io_loop() { m_thread_ctx->run(); }

bool ioMgrImpl::is_ready() const { return m_ready.load(std::memory_order_acquire); }

void ioMgrImpl::wait_to_be_ready() {
    std::unique_lock< std::mutex > lck(m_cv_mtx);
    m_cv.wait(lck, [this] { return m_ready.load(std::memory_order_acquire); });
}

void ioMgrImpl::add_ep(EndPoint* ep) {
    m_ep_list.wlock()->push_back(ep);

    if (m_ep_list.rlock()->size() == m_expected_eps) {
        /* allow threads to run */
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_ready.store(true, std::memory_order_release);
    }
}

fd_info* ioMgrImpl::create_fd_info(int fd, const iomgr::ev_callback& cb, int ev, int pri, void* cookie) {
    fd_info* info = new fd_info;

    info->cb = cb;
    info->is_outstanding[fd_info::READ] = 0;
    info->is_outstanding[fd_info::WRITE] = 0;
    info->fd = fd;
    info->ev = ev;
    info->is_global = false;
    info->pri = pri;
    info->cookie = cookie;

    m_fd_info_map.wlock()->insert(std::pair< int, fd_info* >(fd, info));
    return info;
}

fd_info* ioMgrImpl::add_fd(int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie) {
    fd_info* info = create_fd_info(fd, cb, iomgr_ev, pri, cookie);
    info->is_global = true;

    m_thread_ctx.access_all_threads([this, info, pri](ioMgrThreadContext* ctx) {
        if (!ctx->is_io_thread()) {
            LOGTRACEMOD(iomgr, "Ignoring to add fd {} to this non-io thread", info->fd);
            return true;
        }
        // Create a new event fd for this thread and add it to list of event fd for this data fd
        auto my_event_fd = eventfd(0, EFD_NONBLOCK);
        info->ev_fd[ctx->m_thread_num] = my_event_fd;
        info->event[ctx->m_thread_num] = 0;
        ctx->add_fd_to_thread(my_event_fd,
                              [this](int fd, void* cookie, uint32_t events) { this->process_evfd(fd, cookie, events); },
                              EPOLLIN, 1, info);

        // Register our thread to appropriate epoll priority thread context
        struct epoll_event ev;
        ev.events = EPOLLET | info->ev;
        ev.data.ptr = info;
        if (epoll_ctl(ctx->m_epollfd_pri[pri], EPOLL_CTL_ADD, info->fd, &ev) == -1) {
            assert(0);
        }
        return true;
    });

    m_fd_infos.wlock()->push_back(info);
    return info;
}

fd_info* ioMgrImpl::add_local_fd(int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie) {
    return m_thread_ctx->add_fd_to_thread(fd, cb, iomgr_ev, pri, cookie);
}

void ioMgrImpl::callback(void* data, uint32_t event) {
    fd_info* info = (fd_info*)data;
    info->cb(info->fd, info->cookie, event);
}

bool ioMgrImpl::can_process(void* data, uint32_t ev) {
    fd_info* info = (fd_info*)data;
    int      expected = 0;
    int      desired = 1;
    bool     ret = false;
    if (ev & EPOLLIN) {
        ret = info->is_outstanding[fd_info::READ].compare_exchange_strong(expected, desired, std::memory_order_acquire,
                                                                          std::memory_order_acquire);
    } else if (ev & EPOLLOUT) {
        ret = info->is_outstanding[fd_info::WRITE].compare_exchange_strong(expected, desired, std::memory_order_acquire,
                                                                           std::memory_order_acquire);
    } else if (ev & EPOLLERR || ev & EPOLLHUP) {
        LOGCRITICAL("Received EPOLLERR or EPOLLHUP without other event: {}!", ev);
    } else {
        LOGCRITICAL("Unknown event: {}", ev);
        assert(0);
    }
    if (ret) {
        //		LOG(INFO) << "running for fd" << info->fd;
    } else {
        //		LOG(INFO) << "not allowed running for fd" << info->fd;
    }
    return ret;
}

void ioMgrImpl::fd_reschedule(int fd, uint32_t event) { fd_reschedule(fd_to_info(fd), event); }

void ioMgrImpl::fd_reschedule(fd_info* info, uint32_t event) {
    uint64_t min_cnt = UINTMAX_MAX;
    int      min_id = 0;

    m_thread_ctx.access_all_threads([&min_id, &min_cnt](ioMgrThreadContext* ctx) {
        if (ctx->m_count < min_cnt) {
            min_id = ctx->m_thread_num;
            min_cnt = ctx->m_count;
        }
        return true;
    });
    info->event[min_id] |= event;

    uint64_t temp = 1;
    while (0 > write(info->ev_fd[min_id], &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;
}

void ioMgrImpl::process_evfd(int evfd, void* data, uint32_t event) {
    uint64_t temp;
    while (0 > read(evfd, &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    fd_info* base_info = (fd_info*)data;

    if (base_info->event[sisl::ThreadLocalContext::my_thread_num()] & EPOLLIN && can_process(base_info, event)) {
        base_info->cb(base_info->fd, base_info->cookie, EPOLLIN);
    }

    if (base_info->event[sisl::ThreadLocalContext::my_thread_num()] & EPOLLOUT && can_process(base_info, event)) {
        base_info->cb(base_info->fd, base_info->cookie, EPOLLOUT);
    }
    base_info->event[sisl::ThreadLocalContext::my_thread_num()] = 0;

    process_done(evfd, event);
}

void ioMgrImpl::process_done(int fd, int ev) { process_done(fd_to_info(fd), ev); }

void ioMgrImpl::process_done(fd_info* info, int ev) {
    if (ev & EPOLLIN) {
        info->is_outstanding[fd_info::READ].fetch_sub(1, std::memory_order_release);
    } else if (ev & EPOLLOUT) {
        info->is_outstanding[fd_info::WRITE].fetch_sub(1, std::memory_order_release);
    } else {
        assert(0);
    }
}

void ioMgrImpl::print_perf_cntrs() {
    m_thread_ctx.access_all_threads([](ioMgrThreadContext* ctx) {
        LOGINFO("\n\tthread {} counters.\n\tnumber of times {} it run\n\ttotal time spent {}ms", ctx->m_thread_num,
                ctx->m_count, (ctx->m_time_spent_ns / (1000 * 1000)));
        return true;
    });

    foreach_endpoint([](EndPoint* ep) { ep->print_perf(); });
}

fd_info* ioMgrImpl::fd_to_info(int fd) {
    auto it = m_fd_info_map.rlock()->find(fd);
    assert(it->first == fd);
    fd_info* info = it->second;

    return info;
}

void ioMgrImpl::set_my_evfd(fd_info* fdi, int ev_fd) { fdi->ev_fd[sisl::ThreadLocalContext::my_thread_num()] = ev_fd; }

int ioMgrImpl::get_my_evfd(fd_info* fdi) const { return fdi->ev_fd[sisl::ThreadLocalContext::my_thread_num()]; }

void ioMgrImpl::foreach_fd_info(std::function< void(fd_info*) > fd_cb) {
    m_fd_infos.withRLock([&](auto& fd_infos) {
        for (auto fdi : fd_infos) {
            fd_cb(fdi);
        }
    });
}

void ioMgrImpl::foreach_endpoint(std::function< void(EndPoint*) > ep_cb) {
    m_ep_list.withRLock([&](auto& ep_list) {
        for (auto ep : ep_list) {
            ep_cb(ep);
        }
    });
}
} // namespace iomgr

//
// Created by Rishabh Mittal on 04/20/2018
//

#include "iomgr.hpp"

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

#include "include/default_endpoint.hpp"
#include "include/aio_drive_endpoint.hpp"
#include "include/iomgr.hpp"
#include "include/io_thread.hpp"
#include <utility/thread_factory.hpp>

namespace iomgr {

IOManager::IOManager() : m_expected_eps(INBUILT_ENDPOINTS_COUNT) {
    m_ep_list.wlock()->reserve(INBUILT_ENDPOINTS_COUNT + 5);
}

IOManager::~IOManager() = default;

void IOManager::start(size_t const expected_custom_eps, size_t const num_threads,
                      const io_thread_state_notifier& notifier) {
    LOGINFO("Starting IOManager");
    m_expected_eps += expected_custom_eps;
    m_yet_to_start_nthreads.set(num_threads);
    m_thread_state_notifier = notifier;

    set_state(iomgr_state::waiting_for_endpoints);

    /* Create all in-built endpoints */
    m_default_ep = std::make_shared< DefaultEndPoint >();
    add_ep(std::dynamic_pointer_cast< EndPoint >(m_default_ep));
}

void IOManager::stop() {
    iomgr_msg msg(iomgr_msg_type::RELINQUISH_IO_THREAD);
    send_msg(-1, std::move(msg));
    for (auto& t : m_iomgr_threads) {
        t.join();
    }
}

void IOManager::add_ep(std::shared_ptr< EndPoint > ep) {
    m_ep_list.wlock()->push_back(ep);

    auto ep_count = m_ep_list.rlock()->size();
    if (ep_count == m_expected_eps) {
        LOGINFO("Registered expected {} endpoints, marking iomanager waiting for threads", ep_count);

        auto nthreads = m_yet_to_start_nthreads.get();
        if (nthreads) {
            set_state_and_notify(iomgr_state::waiting_for_threads);
            LOGINFO("IOManager is asked to start {} number of threads, starting them", nthreads);
            for (auto i = 0; i < nthreads; i++) {
                m_iomgr_threads.push_back(
                    std::move(sisl::thread_factory("io_thread", &IOManager::run_io_loop, this, true)));
                LOGTRACEMOD(iomgr, "Created iomanager thread...", i);
                // t.detach();
            }
        } else {
            set_state_and_notify(iomgr_state::running);
        }
    } else if (ep_count < m_expected_eps) {
        LOGINFO("Only added {} end points, need to wait till we get {} endpoints registered", ep_count, m_expected_eps);
    }
}

void IOManager::run_io_loop(bool is_iomgr_thread) { m_thread_ctx->run(is_iomgr_thread); }

void IOManager::iomgr_thread_ready() {
    if (m_yet_to_start_nthreads.decrement_testz()) { set_state_and_notify(iomgr_state::running); }
}

fd_info* IOManager::_add_fd(EndPoint* ep, int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie,
                            bool is_per_thread_fd) {
    // We can add per thread fd even when iomanager is not ready. However, global fds need IOManager
    // to be initialized, since it has to maintain global map
    if (!is_per_thread_fd && !is_ready()) {
        LOGINFO("IOManager is not ready to add fd {}, will wait for it to be ready", fd);
        wait_to_be_ready();
        LOGINFO("IOManager is ready now, proceed to add fd to the list");
    }

    LOGTRACEMOD(iomgr, "fd {} is requested to add to IOManager, will add it to {} thread(s)", fd,
                (is_per_thread_fd ? "this" : "all"));

    fd_info* info = create_fd_info(ep, fd, cb, iomgr_ev, pri, cookie);
    info->is_global = !is_per_thread_fd;

    if (is_per_thread_fd) {
        m_thread_ctx->add_fd_to_thread(info);
    } else {
        m_thread_ctx.access_all_threads([info](ioMgrThreadContext* ctx) { ctx->add_fd_to_thread(info); });
        m_fd_info_map.wlock()->insert(std::pair< int, fd_info* >(fd, info));
    }
    return info;
}

void IOManager::remove_fd(EndPoint* ep, fd_info* info, ioMgrThreadContext* iomgr_ctx) {
    (void)ep;
    if (!is_ready()) {
        LOGDFATAL("Expected IOManager to be ready before we receive _remove_fd");
        return;
    }

    if (info->is_global) {
        m_thread_ctx.access_all_threads([info](ioMgrThreadContext* ctx) { ctx->remove_fd_from_thread(info); });
        m_fd_info_map.wlock()->erase(info->fd);
    } else {
        iomgr_ctx ? iomgr_ctx->remove_fd_from_thread(info) : m_thread_ctx->remove_fd_from_thread(info);
    }
    delete (info);
}

#if 0
void IOManager::callback(void* data, uint32_t event) {
    fd_info* info = (fd_info*)data;
    info->cb(info->fd, info->cookie, event);
}

bool IOManager::can_process(void* data, uint32_t ev) {
    fd_info* info = (fd_info*)data;
    int      expected = 0;
    int      desired = 1;
    bool     ret = false;
    if (ev & EPOLLIN) {
        ret = info->is_processing[fd_info::READ].compare_exchange_strong(expected, desired, std::memory_order_acquire,
                                                                          std::memory_order_acquire);
    } else if (ev & EPOLLOUT) {
        ret = info->is_processing[fd_info::WRITE].compare_exchange_strong(expected, desired, std::memory_order_acquire,
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
#endif

void IOManager::fd_reschedule(int fd, uint32_t event) { fd_reschedule(fd_to_info(fd), event); }

void IOManager::fd_reschedule(fd_info* info, uint32_t event) {
    uint64_t min_cnt = UINTMAX_MAX;
    int      min_id = 0;
    bool     rescheduled = false;

    iomgr_msg msg(iomgr_msg_type::RESCHEDULE, info, event);
    do {
        m_thread_ctx.access_all_threads([&min_id, &min_cnt](ioMgrThreadContext* ctx) {
            if (!ctx->is_io_thread()) { return; }
            if (ctx->m_count < min_cnt) {
                min_id = ctx->m_thread_num;
                min_cnt = ctx->m_count;
            }
        });

        // Try to send msg to the thread. send_msg could fail if thread is not alive (i,e between access_all_threads)
        // and next method, thread exits.
        rescheduled = (send_msg(min_id, msg) == 1);
    } while (!rescheduled);
}

uint32_t IOManager::send_msg(int thread_num, const iomgr_msg& msg) {
    uint32_t msg_sent_count = 0;
    if (thread_num == -1) {
        m_thread_ctx.access_all_threads([msg, &msg_sent_count](ioMgrThreadContext* ctx) {
            if (!ctx->m_msg_fd_info || !ctx->m_is_io_thread) return;

            LOGTRACEMOD(iomgr, "Sending msg of type {} to local thread msg fd = {}, ptr = {}", msg.m_type,
                        ctx->m_msg_fd_info->fd, (void*)ctx->m_msg_fd_info.get());
            ctx->put_msg(std::move(msg));
            uint64_t temp = 1;
            while (0 > write(ctx->m_msg_fd_info->fd, &temp, sizeof(uint64_t)) && errno == EAGAIN)
                ;
            msg_sent_count++;
        });
    } else {
        m_thread_ctx.access_specific_thread(thread_num, [msg, &msg_sent_count](ioMgrThreadContext* ctx) {
            if (!ctx->m_msg_fd_info || !ctx->m_is_io_thread) return;

            ctx->put_msg(std::move(msg));
            uint64_t temp = 1;
            while (0 > write(ctx->m_msg_fd_info->fd, &temp, sizeof(uint64_t)) && errno == EAGAIN)
                ;
            msg_sent_count++;
        });
    }
    return msg_sent_count;
}

#if 0
void IOManager::process_evfd(int evfd, void* data, uint32_t event) {
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

void IOManager::process_done(int fd, int ev) { process_done(fd_to_info(fd), ev); }

void IOManager::process_done(fd_info* info, int ev) {
    if (ev & EPOLLIN) {
        info->is_processing[fd_info::READ].fetch_sub(1, std::memory_order_release);
    } else if (ev & EPOLLOUT) {
        info->is_processing[fd_info::WRITE].fetch_sub(1, std::memory_order_release);
    } else {
        assert(0);
    }
}

void IOManager::print_perf_cntrs() {
    m_thread_ctx.access_all_threads([](ioMgrThreadContext* ctx) {
        if (!ctx->is_io_thread()) { return true; }
        LOGINFO("\n\tthread {} counters.\n\tnumber of times {} it run\n\ttotal time spent {}ms", ctx->m_thread_num,
                ctx->m_count, (ctx->m_time_spent_ns / (1000 * 1000)));
    });

    foreach_endpoint([](EndPoint* ep) { ep->print_perf(); });
}
#endif

fd_info* IOManager::create_fd_info(EndPoint* ep, int fd, const iomgr::ev_callback& cb, int ev, int pri, void* cookie) {
    fd_info* info = new fd_info;

    info->cb = cb;
    info->is_processing[fd_info::READ] = 0;
    info->is_processing[fd_info::WRITE] = 0;
    info->fd = fd;
    info->ev = ev;
    info->is_global = false;
    info->pri = pri;
    info->cookie = cookie;
    info->endpoint = ep;
    return info;
}

fd_info* IOManager::fd_to_info(int fd) {
    auto it = m_fd_info_map.rlock()->find(fd);
    assert(it->first == fd);
    fd_info* info = it->second;

    return info;
}

void IOManager::foreach_fd_info(std::function< void(fd_info*) > fd_cb) {
    m_fd_info_map.withRLock([&](auto& fd_infos) {
        for (auto& fdi : fd_infos) {
            fd_cb(fdi.second);
        }
    });
}

void IOManager::foreach_endpoint(std::function< void(EndPoint*) > ep_cb) {
    m_ep_list.withRLock([&](auto& ep_list) {
        for (auto ep : ep_list) {
            ep_cb(ep.get());
        }
    });
}
} // namespace iomgr

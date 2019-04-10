/**
 * Copyright eBay Corporation 2018
 */

#pragma once

extern "C" {
#include <event.h>
#include <sys/time.h>
}
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <vector>
#include <utility/thread_buffer.hpp>
#include <fds/sparse_vector.hpp>
#include <folly/Synchronized.h>
#include "io_thread.hpp"

namespace iomgr {

using ev_callback = std::function< void(int fd, void* cookie, uint32_t events) >;

struct fd_info {
    enum { READ = 0, WRITE };

    ev_callback                cb;
    int                        fd;
    std::atomic< int >         is_outstanding[2];
    int                        ev;
    bool                       is_global;
    int                        pri;
    sisl::sparse_vector< int > ev_fd; // ev_fd and its event for each thread context
    sisl::sparse_vector< int > event;
    void*                      cookie;
};

enum iomgr_event_type { RESCHEDULE = 1, SHUTDOWN };

#if 0
struct thread_info {
    pthread_t tid;
    uint64_t  count;
    uint64_t  time_spent_ns;
    int       id;
    int       ev_fd;
    int       inited;
    int*      epollfd_pri;
};
#endif

class EndPoint;

struct ioMgrImpl {
    // static thread_local int epollfd_pri[MAX_PRI];
    // static thread_local int epollfd;

    ioMgrImpl(size_t num_ep, size_t num_threads);
    ~ioMgrImpl();
    void     start();
    void     run_io_loop();
    fd_info* create_fd_info(int fd, const ev_callback& cb, int ev, int pri, void* cookie);

    void     add_ep(EndPoint* ep);
    fd_info* add_fd(int fd, ev_callback cb, int ev, int pri, void* cookie);
    fd_info* add_local_fd(int fd, ev_callback cb, int ev, int pri, void* cookie);
    void     callback(void* data, uint32_t ev);
    void     print_perf_cntrs();
    bool     can_process(void* data, uint32_t event);
    void     fd_reschedule(int fd, uint32_t event);
    void     fd_reschedule(fd_info* info, uint32_t event);
    void     process_evfd(int fd, void* data, uint32_t event);
    void     process_done(int fd, int ev);
    void     process_done(fd_info* info, int ev);
    bool     is_ready() const;
    void     set_my_evfd(fd_info* fdi, int ev_fd);
    int      get_my_evfd(fd_info* fdi) const;

    void foreach_fd_info(std::function< void(fd_info*) > fd_cb);
    void foreach_endpoint(std::function< void(EndPoint*) > ep_cb);

    void wait_to_be_ready();

private:
    fd_info* fd_to_info(int fd);

private:
    size_t           m_expected_eps;
    size_t           m_num_threads;
    std::atomic_bool m_ready = false;

    folly::Synchronized< std::vector< fd_info* > >   m_fd_infos; /* fds shared between the threads */
    folly::Synchronized< std::vector< EndPoint* > >  m_ep_list;
    folly::Synchronized< std::map< int, fd_info* > > m_fd_info_map;

    sisl::ThreadBuffer< ioMgrThreadContext, ioMgrImpl* > m_thread_ctx;

    std::mutex              m_cv_mtx;
    std::condition_variable m_cv;
};

} // namespace iomgr

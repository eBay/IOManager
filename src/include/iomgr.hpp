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
#include "iomgr_msg.hpp"
#include "impl/io_thread.hpp"
#include "impl/endpoints/drive_endpoint.hpp"
#include "impl/endpoints/default_endpoint.hpp"

namespace iomgr {

using ev_callback = std::function< void(int fd, void* cookie, uint32_t events) >;

struct fd_info {
    enum { READ = 0, WRITE };

    ev_callback                cb;
    int                        fd;
    std::atomic< int >         is_processing[2];
    int                        ev;
    bool                       is_global;
    int                        pri;
    void*                      cookie;
    EndPoint*                  endpoint;
};

#if 0
// This is some of the critical thread stats like count and any other info that needs to be shared
// across the instances. It is not wise to add this one to ThreadBuffer because we end up having to
// access all these threads
struct thread_stats {
    uint64_t    io_count;    // Total number of
};
#endif

#define iomgr_instance IOManager::instance()

// Only Default endpoints
// TODO: Make this part of an enum, to force add count upon adding new inbuilt endpoint.
#define INBUILT_ENDPOINTS_COUNT 1

class IOManager {
public:
    static IOManager& instance() {
        static IOManager inst;
        return inst;
    }

    IOManager();
    ~IOManager();
    void     start(size_t num_ep, size_t num_threads);
    void     run_io_loop();

    std::shared_ptr< DriveEndPoint > create_add_drive_endpoint(drive_comp_callback cb);
    fd_info* create_fd_info(EndPoint* ep, int fd, const ev_callback& cb, int ev, int pri, void* cookie);

    void     add_ep(std::shared_ptr< EndPoint > ep);
    fd_info* add_fd(EndPoint* ep, int fd, ev_callback cb, int ev, int pri, void* cookie);
    fd_info* add_per_thread_fd(EndPoint* ep, int fd, ev_callback cb, int ev, int pri, void* cookie);
    void     print_perf_cntrs();
    void     fd_reschedule(int fd, uint32_t event);
    void     fd_reschedule(fd_info* info, uint32_t event);
    bool     is_ready() const;

    //bool     can_process(void* data, uint32_t event);
    //void     process_evfd(int fd, void* data, uint32_t event);
    //void     process_done(int fd, int ev);
    //void     process_done(fd_info* info, int ev);

    void foreach_fd_info(std::function< void(fd_info*) > fd_cb);
    void foreach_endpoint(std::function< void(EndPoint*) > ep_cb);

    void wait_to_be_ready();

    uint32_t send_msg(int thread_num, const iomgr_msg& msg);

    DefaultEndPoint* default_endpoint() { return m_default_ep.get(); }
    DriveEndPoint* drive_endpoint() { return m_drive_ep.get(); }

    int64_t idle_timeout_interval_usec() const { return -1; };
    void idle_timeout_expired() {
        if (m_idle_timeout_expired_cb) { m_idle_timeout_expired_cb(); }
    }
private:
    fd_info* fd_to_info(int fd);

private:
    size_t           m_expected_eps;
    std::atomic_bool m_ready = false;

    folly::Synchronized< std::vector< fd_info* > >   m_fd_infos; /* fds shared between the threads */
    folly::Synchronized< std::vector< std::shared_ptr < EndPoint > > >  m_ep_list;
    folly::Synchronized< std::map< int, fd_info* > > m_fd_info_map;

    folly::Synchronized< std::vector< uint64_t > > m_global_thread_contexts;

    sisl::ActiveOnlyThreadBuffer< ioMgrThreadContext > m_thread_ctx;

    std::mutex              m_cv_mtx;
    std::condition_variable m_cv;
    std::function< void() > m_idle_timeout_expired_cb = nullptr;

    std::shared_ptr< DriveEndPoint >   m_drive_ep;
    std::shared_ptr< DefaultEndPoint > m_default_ep;
};

} // namespace iomgr

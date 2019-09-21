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
#include <utility/atomic_counter.hpp>
#include <fds/sparse_vector.hpp>
#include <folly/Synchronized.h>
#include "iomgr_msg.hpp"
#include "io_thread.hpp"
#include "endpoint.hpp"

namespace iomgr {

using ev_callback = std::function< void(int fd, void* cookie, uint32_t events) >;

struct fd_info {
    enum { READ = 0, WRITE };

    ev_callback        cb;
    int                fd;
    std::atomic< int > is_processing[2];
    int                ev;
    bool               is_global;
    int                pri;
    void*              cookie;
    EndPoint*          endpoint;
};

#if 0
// This is some of the critical thread stats like count and any other info that needs to be shared
// across the instances. It is not wise to add this one to ThreadBuffer because we end up having to
// access all these threads
struct thread_stats {
    uint64_t    io_count;    // Total number of
};
#endif

// Only Default endpoints
// TODO: Make this part of an enum, to force add count upon adding new inbuilt endpoint.
#define INBUILT_ENDPOINTS_COUNT 1
class AioDriveEndPoint;
class DefaultEndPoint;

enum class iomgr_state : uint16_t {
    stopped = 0,
    waiting_for_endpoints = 1,
    waiting_for_threads = 2,
    running = 3,
};

typedef std::function< void(bool is_started) > io_thread_state_notifier;

class IOManager {
public:
    friend class ioMgrThreadContext; // TODO: Remove this, temporary
    static IOManager& instance() {
        static IOManager inst;
        return inst;
    }

    IOManager();
    ~IOManager();
    void start(size_t num_ep, size_t num_threads = 0, const io_thread_state_notifier& notifier = nullptr);
    void run_io_loop(bool is_iomgr_thread = false);
    void stop();

    fd_info* create_fd_info(EndPoint* ep, int fd, const ev_callback& cb, int ev, int pri, void* cookie);

    void     add_ep(std::shared_ptr< EndPoint > ep);
    fd_info* add_fd(EndPoint* ep, int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie) {
        return _add_fd(ep, fd, cb, iomgr_ev, pri, cookie, false /* is_per_thread_fd */);
    }
    fd_info* add_per_thread_fd(EndPoint* ep, int fd, ev_callback cb, int iomgr_ev, int pri, void* cookie) {
        return _add_fd(ep, fd, cb, iomgr_ev, pri, cookie, true /* is_per_thread_fd */);
    }
    fd_info* _add_fd(EndPoint* ep, int fd, ev_callback cb, int ev, int pri, void* cookie, bool is_per_thread_fd);
    void     remove_fd(EndPoint* ep, fd_info* info, ioMgrThreadContext* iomgr_ctx = nullptr);

    void print_perf_cntrs();
    void fd_reschedule(int fd, uint32_t event);
    void fd_reschedule(fd_info* info, uint32_t event);

    // bool     can_process(void* data, uint32_t event);
    // void     process_evfd(int fd, void* data, uint32_t event);
    // void     process_done(int fd, int ev);
    // void     process_done(fd_info* info, int ev);

    void foreach_fd_info(std::function< void(fd_info*) > fd_cb);
    void foreach_endpoint(std::function< void(EndPoint*) > ep_cb);

    bool is_ready() const { return (get_state() == iomgr_state::running); }
    bool is_endpoint_registered() const {
        return ((uint16_t)get_state() > (uint16_t)iomgr_state::waiting_for_endpoints);
    }

    void wait_to_be_ready() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_cv.wait(lck, [this] { return is_ready(); });
    }

    void wait_for_ep_registration() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_cv.wait(lck, [this] { return is_endpoint_registered(); });
    }

    // Notification that iomanager thread is ready to serve
    void iomgr_thread_ready();
    void notify_thread_state(bool is_started) {
        if (m_thread_state_notifier) m_thread_state_notifier(is_started);
    }

    uint32_t send_msg(int thread_num, const iomgr_msg& msg);

    std::shared_ptr< DefaultEndPoint > default_endpoint() { return m_default_ep; }
    // std::shared_ptr< AioDriveEndPoint > drive_endpoint() { return m_drive_ep; }

    int64_t idle_timeout_interval_usec() const { return -1; };
    void    idle_timeout_expired() {
        if (m_idle_timeout_expired_cb) { m_idle_timeout_expired_cb(); }
    }

private:
    fd_info*    fd_to_info(int fd);
    void        set_state(iomgr_state state) { m_state.store(state, std::memory_order_release); }
    iomgr_state get_state() const { return m_state.load(std::memory_order_acquire); }
    void        set_state_and_notify(iomgr_state state) {
        set_state(state);
        m_cv.notify_all();
    }

private:
    size_t                          m_expected_eps;                 // Total number of endpoints expected
    std::atomic< iomgr_state >      m_state = iomgr_state::stopped; // Current state of IOManager
    sisl::atomic_counter< int16_t > m_yet_to_start_nthreads = 0;    // Total number of iomanager threads yet to start

    folly::Synchronized< std::vector< fd_info* > >                    m_fd_infos; /* fds shared between the threads */
    folly::Synchronized< std::vector< std::shared_ptr< EndPoint > > > m_ep_list;
    folly::Synchronized< std::unordered_map< int, fd_info* > >        m_fd_info_map;

    folly::Synchronized< std::vector< uint64_t > > m_global_thread_contexts;

    sisl::ActiveOnlyThreadBuffer< ioMgrThreadContext > m_thread_ctx;

    std::mutex              m_cv_mtx;
    std::condition_variable m_cv;
    std::function< void() > m_idle_timeout_expired_cb = nullptr;

    std::shared_ptr< DefaultEndPoint > m_default_ep;
    std::vector< std::thread >         m_iomgr_threads;
    io_thread_state_notifier           m_thread_state_notifier = nullptr;
};

#define iomanager IOManager::instance()
} // namespace iomgr

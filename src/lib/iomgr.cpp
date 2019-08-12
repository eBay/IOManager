//
// Created by Rishabh Mittal on 04/20/2018
//

#include "iomgr_impl.hpp"

extern "C" {
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/types.h>
}

#include <cerrno>
#include <ctime>
#include <chrono>
#include <functional>
#include <vector>
#include <thread>

#include <sds_logging/logging.h>

#include "io_thread.hpp"

namespace iomgr
{

thread_local int ioMgrImpl::epollfd = 0;
thread_local int ioMgrImpl::epollfd_pri[iomgr::MAX_PRI] = {};

struct fd_info {
   enum {READ = 0, WRITE};

   ev_callback cb;
   int fd;
   std::atomic<int> is_running[2];
   int ev;
   bool is_global;
   int pri;
   std::vector<int> ev_fd;
   std::vector<int> event;
   void *cookie;
};

ioMgrImpl::ioMgrImpl(size_t const num_ep, size_t const num_threads) :
    threads(num_threads),
    num_ep(num_ep),
    running(false)
{
   ready = num_ep == 0;
   global_fd.reserve(num_ep * 10);
   LOGDEBUG("Starting ready: {}", ready);
}

ioMgrImpl::~ioMgrImpl() {
    // free the memory of fd_info
    for (auto i = 0u; i < ep_list.size(); ++i) {
        delete ep_list[i];
    }
    for (auto& x : fd_info_map)  {
        delete x.second;
    }
}

// 
// Stop iomgr procedure
// 1. set thread running to false;
// 2. trigger the event to wake up iothread and call shutdown_local of each ep
// 3. join all the i/o threads;
// 
void 
ioMgrImpl::stop() {
    stop_running();
    uint64_t temp = 1;

    for (auto& x : global_fd) {
        // 
        // Currently one event will wake up all the i/o threads.
        // When change back to EPOLLEXCLUSIVE, we need to send multiple times to 
        // wake up all the threads to stop them from running;
        //
        while (0 > write(x->fd, &temp, sizeof(uint64_t)) && errno == EAGAIN);
        // wait for all the threads to wake up;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if(close(x->fd)) {
            LOGERROR("Failed to close epoll fd: {}", x->fd);
            return;
        }
        LOGDEBUG("close epoll fd: {}", x->fd);
    }

    for (auto& x : threads) {
        for (uint32_t i = 0; i < MAX_PRI; i++) {
            auto y = x.epollfd_pri[i];
            if(close(y)) {
                LOGERROR("{}, Failed to close epollfd_pri[{}]: {}", __FUNCTION__, i, y);
                return;
            }
            LOGDEBUG("{}, close epollfd_pri[{}]: {}", __FUNCTION__, i, y);
        }
    }

    for (auto& x : threads) {
        if(close(x.ev_fd)) {
            LOGERROR("Failed to close epoll fd: {}", x.ev_fd);
            return;
        }
        LOGDEBUG("close epoll fd: {}", x.ev_fd);
    }

    void *res;
    for (auto& x : threads) {
        int s = pthread_join(x.tid, &res);
        if (s != 0) {
            // handle error here;
            LOGERROR("{}, error joining with thread {}; returned value: {}", __FUNCTION__, x.id, (char*)res);
            return;
        }
        LOGDEBUG("{}, successfully joined with thread: {}", __FUNCTION__, x.id);
    }
}

// 
// Stop the running threads;
//
void 
ioMgrImpl::stop_running() {
    running.store(false, std::memory_order_relaxed);   
}

void
ioMgrImpl::start() {
   running.store(true, std::memory_order_relaxed);
   for (auto i = 0u; threads.size() > i; ++i) {
      auto& t_info = threads[i];
      auto iomgr_copy = new std::shared_ptr<ioMgrImpl>(shared_from_this());
      int rc = pthread_create(&(t_info.tid), nullptr, iothread, iomgr_copy);
      assert(!rc);
      if (rc) {
         LOGCRITICAL("Failed to create thread: {}", rc);
         continue;
      }
      LOGTRACEMOD(iomgr, "Created thread...", i);
      t_info.id = i;
      t_info.inited = false;
   }
}

void
ioMgrImpl::local_init() {
   {
      std::unique_lock<std::mutex> lck(cv_mtx);
      cv.wait(lck, [this] { return ready; });
   }
   if (!is_running()) return;
   LOGTRACEMOD(iomgr, "Initializing locals.");

   struct epoll_event ev;
   pthread_t t = pthread_self();
   thread_info *info = get_tid_info(t);

   epollfd = epoll_create1(0);
   LOGTRACEMOD(iomgr, "EPoll created: {}", epollfd);

   if (epollfd < 1) {
      assert(0);
      LOGERROR("epoll_ctl failed: {}", strerror(errno));
   }

   for (auto i = 0ul; i < MAX_PRI; ++i) {
      epollfd_pri[i] = epoll_create1(0);
      ev.events = EPOLLET | EPOLLIN | EPOLLOUT;
      ev.data.fd = epollfd_pri[i];
      if (epoll_ctl(epollfd, EPOLL_CTL_ADD,
                    epollfd_pri[i], &ev) == -1) {
         assert(0);
         LOGERROR("epoll_ctl failed: {}", strerror(errno));
      }
   }

   // add event fd to each thread
   info->inited = true;
   info->ev_fd = epollfd;
   info->epollfd_pri = epollfd_pri;

   for(auto i = 0u; i < global_fd.size(); ++i) {
      /* We cannot use EPOLLEXCLUSIVE flag here. otherwise
       * some events can be missed.
       */
      ev.events = EPOLLET | global_fd[i]->ev;
      ev.data.ptr = global_fd[i];
      if (epoll_ctl(epollfd_pri[global_fd[i]->pri], EPOLL_CTL_ADD,
                    global_fd[i]->fd, &ev) == -1) {
         assert(0);
         LOGERROR("epoll_ctl failed: {}", strerror(errno));
      }

      add_local_fd(global_fd[i]->ev_fd[info->id],
                   [this] (int fd, void* cookie, uint32_t events)
                   { process_evfd(fd, cookie, events); },
                   EPOLLIN, 1, global_fd[i]);

      LOGDEBUGMOD(iomgr, "registered global fds");
   }

   assert(num_ep == ep_list.size());
   /* initialize all the thread local variables in end point */
   for (auto i = 0u; i < num_ep; ++i) {
      ep_list[i]->init_local();
   }
}

bool
ioMgrImpl::is_running() const {
    return running.load(std::memory_order_relaxed);
}

void
ioMgrImpl::add_ep(class EndPoint *ep) {
   ep_list.push_back(ep);
   if (ep_list.size() == num_ep) {
      /* allow threads to run */
      std::lock_guard<std::mutex> lck(cv_mtx);
      ready = true;
   }
   cv.notify_all();
   LOGTRACEMOD(iomgr, "Added Endpoint.");
}

void
ioMgrImpl::add_fd(int fd, ev_callback cb, int iomgr_ev, int pri, void *cookie) {
   auto info = new struct fd_info;
   {  std::unique_lock<mutex_t> lck(map_mtx);
      fd_info_map.insert(std::pair<int, fd_info*>(fd, info));
      info->cb = cb;
      info->is_running[fd_info::READ] = 0;
      info->is_running[fd_info::WRITE] = 0;
      info->fd = fd;
      info->ev = iomgr_ev;
      info->is_global = true;
      info->pri = pri;
      info->cookie = cookie;
      info->ev_fd.resize(threads.size());
      info->event.resize(threads.size());

      for (auto i = 0u; i < threads.size(); ++i) {
         info->ev_fd[i] = eventfd(0, EFD_NONBLOCK);
         info->event[i] = 0;
      }
   }

   global_fd.push_back(info);

   struct epoll_event ev;
   /* add it to all the threads */
   for (auto i = 0u; threads.size() > i; ++i) {
      auto& t_info = threads[i];
      ev.events = EPOLLET | info->ev;
      ev.data.ptr = info;
      if (!t_info.inited) {
         continue;
      }
      if (epoll_ctl(t_info.epollfd_pri[pri], EPOLL_CTL_ADD,
                    fd, &ev) == -1) {
         assert(0);
      }
      add_fd_to_thread(t_info, info->ev_fd[i],
                       [this] (int fd, void* cookie, uint32_t events)
                       { process_evfd(fd, cookie, events); },
                       EPOLLIN, 1, info);
   }
}

void
ioMgrImpl::add_local_fd(int fd, ev_callback cb, int iomgr_ev, int pri, void *cookie) {
   /* get local id */
   pthread_t t = pthread_self();
   thread_info *info = get_tid_info(t);

   add_fd_to_thread(*info, fd, cb, iomgr_ev, pri, cookie);
}

void
ioMgrImpl::add_fd_to_thread(thread_info& t_info, int fd, ev_callback cb,
                        int iomgr_ev, int pri, void *cookie) {
   struct fd_info*  info = nullptr;
   {
       std::unique_lock<mutex_t> lck(map_mtx);

       auto it = fd_info_map.end();
       bool happened {false};
       std::tie(it, happened) = fd_info_map.emplace(std::make_pair(fd, nullptr));
       if (it != fd_info_map.end() && !happened) {
           info = it->second;
       } else {
           info = new struct fd_info;
           it->second = info;
       }

       info->cb = cb;
       info->is_running[fd_info::READ] = 0;
       info->is_running[fd_info::WRITE] = 0;
       info->fd = fd;
       info->ev = iomgr_ev;
       info->is_global = false;
       info->pri = pri;
       info->cookie = cookie;
   }

   epoll_event ev;
   ev.events = EPOLLET | iomgr_ev;
   ev.data.ptr = info;
   if (epoll_ctl(t_info.epollfd_pri[pri], EPOLL_CTL_ADD,
                 fd, &ev) == -1) {
      assert(0);
   }
   LOGDEBUGMOD(iomgr, "Added FD: {}", fd);
   return;
}

void
ioMgrImpl::callback(void *data, uint32_t event) {
   struct fd_info *info = (struct fd_info *)data;
   info->cb(info->fd, info->cookie, event);
}

bool
ioMgrImpl::can_process(void *data, uint32_t ev) {
   struct fd_info *info = (struct fd_info *)data;
   int expected = 0;
   int desired = 1;
   bool ret = false;
   if (ev & EPOLLIN) {
      ret = info->is_running[fd_info::READ].compare_exchange_strong(expected, desired,
                                                           std::memory_order_acquire,
                                                           std::memory_order_acquire);
   } else if (ev & EPOLLOUT) {
      ret = info->is_running[fd_info::WRITE].compare_exchange_strong(expected, desired,
                                                            std::memory_order_acquire,
                                                            std::memory_order_acquire);
   } else if (ev & EPOLLERR || ev & EPOLLHUP) {
      LOGCRITICAL("Received EPOLLERR or EPOLLHUP without other event: {}!", ev);
   } else {
      LOGCRITICAL("Unknown event: {}", ev);
      assert(0);
   }
   return ret;
}

void
ioMgrImpl::fd_reschedule(int fd, uint32_t event) {
   std::map<int, fd_info*>::iterator it;
   fd_info* info {nullptr};
   {  std::shared_lock<mutex_t> lck(map_mtx);
      if (auto it = fd_info_map.find(fd); fd_info_map.end() != it) {
         assert(it->first == fd);
         info = it->second;
      }
   }
   if (!info) return;

   uint64_t min_cnt = UINTMAX_MAX;
   int min_id = 0;

   for(auto i = 0u; threads.size() > i; ++i) {
      if (threads[i].count < min_cnt) {
         min_id = i;
         min_cnt = threads[i].count;
      }
   }
   info->event[min_id] |= event;
   uint64_t temp = 1;
   while (0 > write(info->ev_fd[min_id], &temp, sizeof(uint64_t)) && errno == EAGAIN);
}

void
ioMgrImpl::process_evfd(int fd, void *data, uint32_t event) {
   struct fd_info *info = (struct fd_info *)data;
   uint64_t temp;
   pthread_t t = pthread_self();
   thread_info *tinfo = get_tid_info(t);

   if (info->event[tinfo->id] & EPOLLIN && can_process(info, event)) {
      info->cb(info->fd, info->cookie, EPOLLIN);
   }

   if (info->event[tinfo->id] & EPOLLOUT && can_process(info, event)) {
      info->cb(info->fd, info->cookie, EPOLLOUT);
   }
   info->event[tinfo->id] = 0;
   process_done(fd, event);
   while (0 > read(fd, &temp, sizeof(uint64_t)) && errno == EAGAIN);
}

void
ioMgrImpl::process_done(int fd, int ev) {
   std::map<int, fd_info*>::iterator it;
   fd_info* info {nullptr};
   {  std::shared_lock<mutex_t> lck(map_mtx);
      if (auto it = fd_info_map.find(fd); fd_info_map.end() != it) {
         assert(it->first == fd);
         info = it->second;
      }
   }
   if (info) {
      if (ev & EPOLLIN) {
         info->is_running[fd_info::READ].fetch_sub(1, std::memory_order_release);
      } else if (ev & EPOLLOUT) {
         info->is_running[fd_info::WRITE].fetch_sub(1, std::memory_order_release);
      } else {
         assert(0);
      }
   }
}

struct thread_info *
ioMgrImpl::get_tid_info(pthread_t &tid) {
   for (auto& t_info: threads) {
      if (t_info.tid == tid) {
         return &t_info;
      }
   }
   assert(0);
   return nullptr;
}

void
ioMgrImpl::print_perf_cntrs() {
   for(auto i = 0u; threads.size() > i; ++i) {
      LOGINFO("\n\tthread {} counters.\n\tnumber of times {} it run\n\ttotal time spent {}ms",
              i,
              threads[i].count,
              (threads[i].time_spent_ns/(1000 * 1000)));
   }
   for(auto const& ep : ep_list) {
      ep->print_perf();
   }
}

} /* iomgr */

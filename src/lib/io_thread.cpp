/**
 * Copyright eBay Corporation 2018
 */

#include "io_thread.hpp"

extern "C" {
#include <sys/eventfd.h>
#include <sys/epoll.h>
}

#include <sds_logging/logging.h>

#include "iomgr_impl.hpp"

#define likely(x)     __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

namespace iomgr
{

using Clock = std::chrono::steady_clock;
   
uint64_t 
get_elapsed_time_ns(Clock::time_point startTime) {
   std::chrono::nanoseconds ns = std::chrono::duration_cast
       < std::chrono::nanoseconds >(Clock::now() - startTime);
   return ns.count();
}

#define MAX_EVENTS 20
void* iothread(void *obj) {
    pthread_t t = pthread_self();
    auto iomgr_ptr = static_cast<std::shared_ptr<ioMgrImpl>*>(obj);
    {
        auto iomgr = *iomgr_ptr;
        thread_info *info = iomgr->get_tid_info(t);
        struct epoll_event fd_events[MAX_PRI];
        struct epoll_event events[MAX_EVENTS];
        int num_fds;

        if (likely(iomgr->is_running())) {
            /* initialize the variables local to a thread */
            LOGTRACEMOD(iomgr, "Becoming ready.");
            iomgr->local_init();
            info->count = 0;
            info->time_spent_ns = 0;
        }
        while (likely(iomgr->is_running())) {
            LOGTRACEMOD(iomgr, "Waiting");
            num_fds = epoll_wait(iomgr->epollfd, fd_events, MAX_PRI, -1);
            if (unlikely(!iomgr->is_running())) { 
                break;
            }
            for (auto i = 0ul; i < MAX_PRI; ++i) {
                /* XXX: should it be  go through only
                 * those fds which has the events.
                 */
                num_fds = epoll_wait(iomgr->epollfd_pri[i], events, 
                        MAX_EVENTS, 0);
                if (num_fds < 0) {
                    LOGERROR("epoll wait failed: {}", errno);
                    continue;
                }
                for (auto i = 0; i < num_fds; ++i) {
                    LOGTRACEMOD(iomgr, "Checking: {}", i);
                    if (iomgr->can_process(events[i].data.ptr, events[i].events)) {
                        Clock::time_point write_startTime = Clock::now();
                        ++info->count;
                        LOGTRACEMOD(iomgr, "Processing event on: {}", i);
                        iomgr->callback(events[i].data.ptr, 
                                events[i].events);
                        info->time_spent_ns += get_elapsed_time_ns(write_startTime);
                        LOGTRACEMOD(iomgr, "Call took: {}ns", info->time_spent_ns);
                    } else {
                    }
                }
            }
        }
        
        for (auto& ep : iomgr->ep_list) {
            ep->shutdown_local();
        } 
    } // de-ref of iomgr use_count;

    /* close the global epoll fd */
    for (uint32_t i = 0; i < MAX_PRI; i++) {
        auto iomgr = *iomgr_ptr;
        auto y = iomgr->epollfd_pri[i];
        if(close(y)) {
            LOGERROR("{}, Failed to close iomgr->epollfd_pri[{}]: {}", __FUNCTION__, i, y); 
        } else {
            LOGDEBUG("{}, close iomgr->epollfd_pri[{}]: {}", __FUNCTION__, i, y); 
        }
    }

    delete iomgr_ptr;
    return nullptr;
}

} /* iomgr */ 

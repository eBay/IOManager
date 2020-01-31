/**
 * Copyright eBay Corporation 2018
 */

extern "C" {
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <time.h>
}

#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/io_thread.hpp"

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {

using Clock = std::chrono::steady_clock;
template < class... Ts >
struct overloaded : Ts... {
    using Ts::operator()...;
};
template < class... Ts >
overloaded(Ts...)->overloaded< Ts... >;

uint64_t get_elapsed_time_ns(Clock::time_point startTime) {
    std::chrono::nanoseconds ns = std::chrono::duration_cast< std::chrono::nanoseconds >(Clock::now() - startTime);
    return ns.count();
}

#define MAX_EVENTS 20
#define ESTIMATED_MSGS_PER_THREAD 128

static bool compare_priority(const epoll_event& ev1, const epoll_event& ev2) {
    fd_info* info1 = (fd_info*)ev1.data.ptr;
    fd_info* info2 = (fd_info*)ev2.data.ptr;

    // In case of equal priority, pick global fd which could get rescheduled
    if (info1->pri == info2->pri) { return info1->is_global; }
    return (info1->pri > info2->pri);
}

ioMgrThreadContext::ioMgrThreadContext() : m_msg_q(ESTIMATED_MSGS_PER_THREAD) {}

ioMgrThreadContext::~ioMgrThreadContext() {
    if (m_is_io_thread) { iothread_stop(); }
}

void ioMgrThreadContext::run(bool is_iomgr_thread) {
    if (!m_is_io_thread) {
        m_is_iomgr_thread = is_iomgr_thread;

        m_thread_num = sisl::ThreadLocalContext::my_thread_num();
        LOGINFO("IOThread is assigned thread num {}", m_thread_num);

        iothread_init(true /* wait_for_iface_register */);
        if (is_iomgr_thread) { iomanager.iomgr_thread_ready(); }

        LOGINFO("IOThread is ready to go to listen loop");
    }

    while (m_keep_running) {
        listen();
    }
}

void ioMgrThreadContext::iothread_init(bool wait_for_iface_register) {
    if (!iomanager.is_interface_registered()) {
        if (!wait_for_iface_register) {
            LOGINFO("IOmanager interfaces are not registered yet and wait is off, it will not be an iothread");
            return;
        }
        LOGINFO("IOManager interfaces are not registered yet, waiting for interfaces to get registered");
        iomanager.wait_for_interface_registration();
        LOGTRACEMOD(iomgr, "All endponts are registered to IOManager, can proceed with this thread initialization");
    }

    LOGTRACEMOD(iomgr, "Initializing iomanager context for this thread, thread_num= {}", m_thread_num);
    m_is_io_thread = true;

    assert(m_metrics == nullptr);
    m_metrics = std::make_unique< ioMgrThreadMetrics >(m_thread_num);

    // Create a epollset for one per thread
    m_epollfd = epoll_create1(0);
    if (m_epollfd < 1) {
        assert(0);
        LOGERROR("epoll_create failed: {}", strerror(errno));
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
    m_msg_fd_info->ev = EPOLLIN;
    m_msg_fd_info->pri = 1; // Set message fd as high priority. TODO: Make multiple messages fd for various priority
    LOGINFO("Creating a message event fd {} and add to this thread epoll fd {}", m_msg_fd_info->fd, m_epollfd)
    if (add_fd_to_thread(m_msg_fd_info.get()) == -1) { goto error; }

    m_common_timer_fd_info = std::make_unique< fd_info >();
    if (!setup_timer_fd(m_common_timer_fd_info.get())) { goto error; }

    // Add all iomanager existing fds to be added to this thread epoll
    iomanager.foreach_fd_info([&](fd_info* fdi) { add_fd_to_thread(fdi); });

    // Notify all the end points about new thread
    iomanager.foreach_interface([&](IOInterface* iface) { iface->on_io_thread_start(this); });

    // Notify the caller registered to iomanager for it
    iomanager.notify_thread_state(true /* started */);
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

void ioMgrThreadContext::iothread_stop() {
    iomanager.foreach_interface([&](IOInterface* iface) { iface->on_io_thread_stopped(this); });
    iomanager.foreach_fd_info([&](fd_info* fdi) { remove_fd_from_thread(fdi); });

    // Notify the caller registered to iomanager for it
    iomanager.notify_thread_state(false /* started */);

    if (m_msg_fd_info && (m_msg_fd_info->fd != -1)) {
        remove_fd_from_thread(m_msg_fd_info.get());
        close(m_msg_fd_info->fd);
    }

    // Remove all timers in the non-recurring timer list
    while (!m_timer_list.empty()) {
        // auto& tinfo = m_timer_list.top(); // TODO: Check if we need to make upcall that timer is cancelled
        m_timer_list.pop();
    }

    // Now close the common timer
    if (m_common_timer_fd_info && (m_common_timer_fd_info->fd != -1)) {
        remove_fd_from_thread(m_common_timer_fd_info.get());
        close(m_common_timer_fd_info->fd);
    }

    // Remove all recurring timers and close their correpsonding fds
    auto it = m_recurring_timer_fds.begin();
    while (it != m_recurring_timer_fds.end()) {
        auto finfo = *it;
        remove_fd_from_thread(finfo.get());
        if (finfo->fd != -1) close(finfo->fd);
        it = m_recurring_timer_fds.erase(it);
    }

    if (m_epollfd != -1) { close(m_epollfd); }
}

bool ioMgrThreadContext::is_io_thread() const { return m_is_io_thread; }

void ioMgrThreadContext::listen() {
    std::array< struct epoll_event, MAX_EVENTS > events;

    int num_fds = epoll_wait(m_epollfd, &events[0], MAX_EVENTS, iomanager.idle_timeout_interval_usec());
    if (num_fds == 0) {
        iomanager.idle_timeout_expired();
        return;
    } else if (num_fds < 0) {
        LOGERROR("epoll wait failed: {} strerror {}", errno, strerror(errno));
        return;
    }

    // Next sort the events based on priority and handle them in that order
    std::sort(events.begin(), (events.begin() + num_fds), compare_priority);
    for (auto i = 0; i < num_fds; ++i) {
        auto& e = events[i];
        if (e.data.ptr == (void*)m_msg_fd_info.get()) {
            LOGTRACEMOD(iomgr, "Processing event on msg fd: {}", m_msg_fd_info->fd);
            ++m_metrics->msg_recvd_count;
            on_msg_fd_notification();

            // It is possible for io thread status by the msg processor. Catch at the exit and return
            if (!m_is_io_thread) {
                LOGINFO("listen will exit because this is no longer an iothread");
                return;
            }
        } else {
            fd_info* info = (fd_info*)e.data.ptr;
            if (info->is_timer_fd) {
                on_timer_fd_notification(info);
            } else {
                on_user_fd_notification(info, e.events);
            }
        }
    }
}

int ioMgrThreadContext::add_fd_to_thread(fd_info* info) {
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLEXCLUSIVE | info->ev;
    ev.data.ptr = (void*)info;
    if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, info->fd, &ev) == -1) {
        LOGDFATAL("Adding fd {} to this thread's epoll fd {} failed, error = {}", info->fd, m_epollfd, strerror(errno));
        return -1;
    }
    LOGDEBUGMOD(iomgr, "Added fd {} to this io thread's epoll fd {}, data.ptr={}", info->fd, m_epollfd,
                (void*)ev.data.ptr);
    return 0;
}

int ioMgrThreadContext::remove_fd_from_thread(fd_info* info) {
    if (epoll_ctl(m_epollfd, EPOLL_CTL_DEL, info->fd, nullptr) == -1) {
        LOGDFATAL("Removing fd {} to this thread's epoll fd {} failed, error = {}", info->fd, m_epollfd,
                  strerror(errno));
        return -1;
    }
    LOGDEBUGMOD(iomgr, "Removed fd {} from this io thread's epoll fd {}", info->fd, m_epollfd);
    return 0;
}

void ioMgrThreadContext::put_msg(const iomgr_msg& msg) { m_msg_q.blockingWrite(msg); }

void ioMgrThreadContext::put_msg(iomgr_msg_type type, fd_info* info, int event, void* buf, uint32_t size) {
    put_msg(iomgr_msg(type, info, event, buf, size));
}

void ioMgrThreadContext::on_msg_fd_notification() {
    uint64_t temp;
    while (0 > read(m_msg_fd_info->fd, &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    // Start pulling all the messages and handle them.
    while (true) {
        iomgr_msg msg;
        if (!m_msg_q.read(msg)) { break; }

        ++m_metrics->msg_recvd_count;
        switch (msg.m_type) {
        case RESCHEDULE: {
            auto info = msg.m_fd_info;
            ++m_metrics->rescheduled_in;
            if (msg.m_event & EPOLLIN) { info->cb(info->fd, info->cookie, EPOLLIN); }
            if (msg.m_event & EPOLLOUT) { info->cb(info->fd, info->cookie, EPOLLOUT); }
            break;
        }

        case RELINQUISH_IO_THREAD:
            LOGINFO("This thread is asked to be reliquished its status as io thread. Will exit io loop");
            m_keep_running = false;
            iothread_stop();
            m_is_io_thread = false;
            break;

        case DESIGNATE_IO_THREAD:
            LOGINFO("This thread is asked to be designated its status as io thread. Will start running io loop");
            m_keep_running = true;
            m_is_io_thread = true;
            break;

        case WAKEUP:
        case SHUTDOWN:
            if (iomanager.msg_notifier()) {
                iomanager.msg_notifier()(msg);
            } else {
                LOGINFO("Received a message, but no message handler registered. Ignoring this message");
            }
            break;

        case UNKNOWN:
        default: assert(0); break;
        }
    }
}

void ioMgrThreadContext::on_user_fd_notification(fd_info* info, uint32_t event) {
    Clock::time_point write_startTime = Clock::now();
    ++m_count;
    ++m_metrics->io_count;

    LOGTRACEMOD(iomgr, "Processing event on user fd: {}", info->fd);
    info->cb(info->fd, info->cookie, event);

    m_time_spent_ns += get_elapsed_time_ns(write_startTime);
    LOGTRACEMOD(iomgr, "Call took: {}ns", m_time_spent_ns);
}

void ioMgrThreadContext::on_timer_fd_notification(fd_info* info) {
    // Read the timer fd and see the number of completions
    uint64_t exp_count = 0;
    if ((read(info->fd, &exp_count, sizeof(uint64_t)) <= 0) || (exp_count == 0)) {
        return; // Nothing is expired. TODO: Update some spurious counter
    }

    if (info == m_common_timer_fd_info.get()) {
        // It is generic timer fd call, loop in for all timers which have expired
        while (!m_timer_list.empty()) {
            auto  time_now = std::chrono::steady_clock::now();
            auto& tinfo = m_timer_list.top();
            if (tinfo.expiry_time <= time_now) {
                tinfo.cb(tinfo.context);
                m_timer_list.pop();
            } else {
                break;
            }
        }
    } else {
        assert(info->recurring_timer_info);
        info->recurring_timer_info->cb(info->recurring_timer_info->context);
    }
}

timer_handle_t ioMgrThreadContext::schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                                         timer_callback_t&& timer_fn) {
    struct timespec   now;
    struct itimerspec tspec;
    timer_handle_t    thdl;
    fd_info*          raw_finfo = nullptr;

    if (recurring) {
        tspec.it_interval.tv_sec = nanos_after / 1000000000;
        tspec.it_interval.tv_nsec = nanos_after % 1000000000;

        // For a recurring timer, create a new timer fd and ask epoll to listen on them
        auto finfo = std::make_shared< fd_info >();
        raw_finfo = finfo.get();
        if (!setup_timer_fd(raw_finfo)) {
            throw std::system_error(errno, std::generic_category(), "Unable to add timer fd for recurring timer");
        }

        // Associate recurring timer to the fd since they have 1-1 relotionship for fd
        finfo->recurring_timer_info = std::make_unique< timer_info >(nanos_after, cookie, std::move(timer_fn));
        m_recurring_timer_fds.insert(finfo); // Add to list of recurring timer fd infos
        thdl = timer_handle_t(finfo);
    } else {
        tspec.it_interval.tv_sec = 0;
        tspec.it_interval.tv_nsec = 0;
        raw_finfo = m_common_timer_fd_info.get();

        // Create a timer_info and add it to the heap.
        thdl = timer_handle_t(m_timer_list.emplace(nanos_after, cookie, std::move(timer_fn)));
    }

    if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
        LOGDFATAL("Unable to get the current time, errno={}", errno);
        throw std::system_error(errno, std::generic_category(), "Unable to get cur time");
    }
    tspec.it_value.tv_sec = now.tv_sec + nanos_after / 1000000000;
    tspec.it_value.tv_nsec = now.tv_nsec + nanos_after % 1000000000;

    //    LOGTRACEMOD(iomgr, "Setting per thread timer with timeout: [sec={} nsec={}] cur_time: [sec={}, nsec={}]",
    //        tspec.it_value.tv_sec, tpsec.)

    if (!raw_finfo || (timerfd_settime(raw_finfo->fd, TFD_TIMER_ABSTIME, &tspec, NULL) == -1)) {
        LOGDFATAL("Unable to set a timer using timer fd = {}, errno={}", m_common_timer_fd_info->fd, errno);
        throw std::system_error(errno, std::generic_category(), "timer fd set time failed");
    }

    return thdl;
}

void ioMgrThreadContext::cancel_thread_timer(timer_handle_t thandle) {
    return std::visit(overloaded{[&](std::shared_ptr< fd_info > info) {
                                     if (info->fd != -1) {
                                         remove_fd_from_thread(info.get());
                                         close(info->fd);
                                     }
                                     m_recurring_timer_fds.erase(info);
                                 },
                                 [&](timer_heap_t::handle_type heap_hdl) { m_timer_list.erase(heap_hdl); }},
                      thandle);
}

bool ioMgrThreadContext::setup_timer_fd(fd_info* info) {
    // Create a timer fd
    info->fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    info->ev = EPOLLIN;
    info->pri = 1; // Set message fd as high priority.
    info->is_timer_fd = true;
    if (info->fd == -1) { throw ::std::system_error(errno, std::generic_category(), "timer_fd creation failed"); }

    LOGINFO("Creating timer fd {} and adding it into fd poll list", info->fd);
    if (add_fd_to_thread(info) == -1) {
        close(info->fd);
        return false;
    }
    return true;
}

} // namespace iomgr

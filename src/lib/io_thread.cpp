/**
 * Copyright eBay Corporation 2018
 */

extern "C" {
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <time.h>
}

#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/io_thread.hpp"
#include <fds/obj_allocator.hpp>

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

static bool compare_priority(const epoll_event& ev1, const epoll_event& ev2) {
    io_device_t* iodev1 = (io_device_t*)ev1.data.ptr;
    io_device_t* iodev2 = (io_device_t*)ev2.data.ptr;

    // In case of equal priority, pick global fd which could get rescheduled
    if (iodev1->pri == iodev2->pri) { return iodev1->is_global; }
    return (iodev1->pri > iodev2->pri);
}

IOThreadContextEPoll::IOThreadContextEPoll() : m_msg_q(ESTIMATED_MSGS_PER_THREAD) {}

IOThreadContextEPoll::~IOThreadContextEPoll() {
    if (m_is_io_thread) { iothread_stop(); }
}

void IOThreadContextEPoll::run(bool is_iomgr_thread, const iodev_selector_t& iodev_selector,
                               const io_thread_msg_handler& this_thread_msg_handler) {
    auto state = iomanager.get_state();
    if ((state == iomgr_state::stopping) || (state == iomgr_state::stopped)) {
        LOGINFO("Starting a new IOThreadContext while iomanager is stopping or stopped, not starting io loop");
        return;
    }

    if (!m_is_io_thread) {
        m_is_iomgr_thread = is_iomgr_thread;
        m_iodev_selector = iodev_selector;
        m_this_thread_msg_handler = this_thread_msg_handler;

        m_thread_num = sisl::ThreadLocalContext::my_thread_num();
        LOGINFO("IOThread is assigned thread num {}", m_thread_num);

        iothread_init(true /* wait_for_iface_register */);
        if (m_keep_running) LOGINFO("IOThread is ready to go to listen loop");
    }

    while (m_keep_running) {
        listen();
    }
}

void IOThreadContextEPoll::iothread_init(bool wait_for_iface_register) {
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

    assert(m_metrics == nullptr);
    m_metrics = std::make_unique< ioMgrThreadMetrics >(m_thread_num);

    // Create a epollset for one per thread
    m_epollfd = epoll_create1(0);
    if (m_epollfd < 1) {
        assert(0);
        LOGERROR("epoll_create failed: {}", strerror(errno));
        goto error;
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    m_is_io_thread = true;

    LOGTRACEMOD(iomgr, "EPoll created: {}", m_epollfd);

    // Create a message fd and add it to tht epollset
    m_msg_iodev = std::make_shared< io_device_t >();
    m_msg_iodev->dev = backing_dev_t(eventfd(0, EFD_NONBLOCK));
    if (m_msg_iodev->fd() == -1) {
        assert(0);
        LOGERROR("Unable to open the eventfd, marking this as non-io thread");
        goto error;
    }
    m_msg_iodev->ev = EPOLLIN;
    m_msg_iodev->pri = 1; // Set message fd as high priority. TODO: Make multiple messages fd for various priority
    LOGINFO("Creating a message event fd {} and add to this thread epoll fd {}", m_msg_iodev->fd(), m_epollfd)
    if (add_iodev_to_thread(m_msg_iodev) == -1) { goto error; }

    // Create a per thread timer
    m_thread_timer = std::make_unique< timer_epoll >(true /* is_per_thread */);

    // Add all iomanager existing fds to be added to this thread epoll
    iomanager.foreach_iodevice([&](const io_device_ptr& iodev) { add_iodev_to_thread(iodev); });

    // Notify all the end points about new thread
    iomanager.foreach_interface([&](IOInterface* iface) { iface->on_io_thread_start(this); });

    // Notify the caller registered to iomanager for it.
    iomanager.io_thread_started(m_is_iomgr_thread);

    // NOTE: This should be the last one before return, because notification might call iothread_stop() and thus need
    // to have clean exit in those cases.
    notify_thread_state(true /* started */);
    return;

error:
    m_is_io_thread = false;

    if (m_epollfd > 0) {
        close(m_epollfd);
        m_epollfd = -1;
    }

    if (m_msg_iodev) {
        if (m_msg_iodev->fd() > 0) { close(m_msg_iodev->fd()); }
        m_msg_iodev = nullptr;
    }
}

void IOThreadContextEPoll::iothread_stop() {
    m_keep_running = false;

    iomanager.foreach_interface([&](IOInterface* iface) { iface->on_io_thread_stopped(this); });
    iomanager.foreach_iodevice([&](const io_device_ptr& iodev) { remove_iodev_from_thread(iodev); });

    // Notify the caller registered to iomanager for it
    notify_thread_state(false /* started */);

    if (m_msg_iodev && (m_msg_iodev->fd() != -1)) {
        remove_iodev_from_thread(m_msg_iodev);
        close(m_msg_iodev->fd());
    }

    m_thread_timer->stop();
    m_is_io_thread = false;
    std::atomic_thread_fence(std::memory_order_acquire);
    if (m_epollfd != -1) { close(m_epollfd); }

    m_metrics = nullptr;
    iomanager.io_thread_stopped();
}

void IOThreadContextEPoll::listen() {
    std::array< struct epoll_event, MAX_EVENTS > events;

    int num_fds = 0;
    do {
        num_fds = epoll_wait(m_epollfd, &events[0], MAX_EVENTS, iomanager.idle_timeout_interval_usec());
    } while (num_fds < 0 && errno == EINTR);

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
        if (e.data.ptr == (void*)m_msg_iodev.get()) {
            LOGTRACEMOD(iomgr, "Processing event on msg fd: {}", m_msg_iodev->fd());
            ++m_metrics->msg_recvd_count;
            on_msg_fd_notification();

            // It is possible for io thread status by the msg processor. Catch at the exit and return
            if (!m_is_io_thread) {
                LOGINFO("listen will exit because this is no longer an iothread");
                return;
            }
        } else {
            io_device_t* iodev = (io_device_t*)e.data.ptr;
            if (iodev->tinfo) {
                timer_epoll::on_timer_fd_notification(iodev);
            } else {
                on_user_fd_notification(iodev, e.events);
            }
        }
    }
}

int IOThreadContextEPoll::add_iodev_to_thread(const io_device_ptr& iodev) {
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLEXCLUSIVE | iodev->ev;
    ev.data.ptr = (void*)iodev.get();
    if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, iodev->fd(), &ev) == -1) {
        LOGDFATAL("Adding fd {} to this thread's epoll fd {} failed, error = {}", iodev->fd(), m_epollfd,
                  strerror(errno));
        return -1;
    }
    LOGDEBUGMOD(iomgr, "Added fd {} to this io thread's epoll fd {}, data.ptr={}", iodev->fd(), m_epollfd,
                (void*)ev.data.ptr);
    return 0;
}

int IOThreadContextEPoll::remove_iodev_from_thread(const io_device_ptr& iodev) {
    if (epoll_ctl(m_epollfd, EPOLL_CTL_DEL, iodev->fd(), nullptr) == -1) {
        LOGDFATAL("Removing fd {} to this thread's epoll fd {} failed, error = {}", iodev->fd(), m_epollfd,
                  strerror(errno));
        return -1;
    }
    LOGDEBUGMOD(iomgr, "Removed fd {} from this io thread's epoll fd {}", iodev->fd(), m_epollfd);
    return 0;
}

bool IOThreadContextEPoll::send_msg(const iomgr_msg& msg) {
    if (!m_msg_iodev) return false;

    LOGTRACEMOD(iomgr, "Sending msg of type {} to local thread msg fd = {}, ptr = {}", msg.m_type, m_msg_iodev->fd(),
                (void*)m_msg_iodev.get());
    put_msg(std::move(msg));
    uint64_t temp = 1;
    while (0 > write(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    return true;
}

void IOThreadContextEPoll::put_msg(const iomgr_msg& msg) { m_msg_q.blockingWrite(msg); }

void IOThreadContextEPoll::on_msg_fd_notification() {
    uint64_t temp;
    while (0 > read(m_msg_iodev->fd(), &temp, sizeof(uint64_t)) && errno == EAGAIN)
        ;

    // Start pulling all the messages and handle them.
    while (true) {
        iomgr_msg msg;
        if (!m_msg_q.read(msg)) { break; }

        ++m_metrics->msg_recvd_count;
        switch (msg.m_type) {
        case RESCHEDULE: {
            auto iodev = msg.m_iodev;
            ++m_metrics->rescheduled_in;
            if (msg.m_event & EPOLLIN) { iodev->cb(iodev.get(), iodev->cookie, EPOLLIN); }
            if (msg.m_event & EPOLLOUT) { iodev->cb(iodev.get(), iodev->cookie, EPOLLOUT); }
            break;
        }

        case RELINQUISH_IO_THREAD:
            LOGINFO("This thread is asked to be reliquished its status as io thread. Will exit io loop");
            iothread_stop();
            break;

        case DESIGNATE_IO_THREAD:
            LOGINFO("This thread is asked to be designated its status as io thread. Will start running io loop");
            m_keep_running = true;
            m_is_io_thread = true;
            break;

        case RUN_METHOD: {
            LOGTRACE("We are picked the thread to run the method");
            auto method_to_run = (run_method_t*)msg.m_data_buf;
            (*method_to_run)();
            sisl::ObjectAllocator< run_method_t >::deallocate(method_to_run);
            break;
        }

        case WAKEUP:
        case SHUTDOWN:
        case CUSTOM_MSG: {
            auto& handler = msg_handler();
            if (handler) {
                handler(msg);
            } else {
                LOGINFO("Received a message, but no message handler registered. Ignoring this message");
            }
            break;
        }

        case UNKNOWN:
        default: assert(0); break;
        }
    }
}

void IOThreadContextEPoll::on_user_fd_notification(io_device_t* iodev, int event) {
    Clock::time_point write_startTime = Clock::now();
    ++m_count;
    ++m_metrics->io_count;

    LOGTRACEMOD(iomgr, "Processing event on user fd: {}", iodev->fd());
    iodev->cb(iodev, iodev->cookie, event);

    --m_count;
    LOGTRACEMOD(iomgr, "Call took: {}ns", get_elapsed_time_ns(write_startTime));
}

bool IOThreadContextEPoll::is_iodev_addable(const io_device_ptr& iodev) {
    return (!m_iodev_selector || m_iodev_selector(iodev));
}

void IOThreadContextEPoll::notify_thread_state(bool is_started) {
    iomgr_msg msg(is_started ? iomgr_msg_type::WAKEUP : iomgr_msg_type::SHUTDOWN);
    auto& handler = msg_handler();
    if (handler) { handler(msg); }
}

io_thread_msg_handler& IOThreadContextEPoll::msg_handler() {
    return (m_this_thread_msg_handler) ? m_this_thread_msg_handler : iomanager.m_common_thread_msg_handler;
}

} // namespace iomgr

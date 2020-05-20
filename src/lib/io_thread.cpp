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
#include "include/io_thread_epoll.hpp"
#include <fds/obj_allocator.hpp>

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {

IOReactor::~IOReactor() {
    if (m_is_io_thread.load()) { stop(); }
}

void IOReactor::run(bool is_iomgr_thread, const iodev_selector_t& iodev_selector,
                    const io_thread_msg_handler& this_thread_msg_handler) {
    auto state = iomanager.get_state();
    if ((state == iomgr_state::stopping) || (state == iomgr_state::stopped)) {
        LOGINFO("Starting a new IOReactor while iomanager is stopping or stopped, not starting io loop");
        return;
    }

    if (!m_is_io_thread) {
        m_is_iomgr_thread = is_iomgr_thread;
        m_iodev_selector = iodev_selector;
        m_this_thread_msg_handler = this_thread_msg_handler;

        m_thread_num = sisl::ThreadLocalContext::my_thread_num();
        LOGINFO("IOThread is assigned thread num {}", m_thread_num);

        init(true /* wait_for_iface_register */);
        if (m_keep_running) LOGINFO("IOThread is ready to go to listen loop");
    }

    while (m_keep_running) {
        listen();
    }
}

void IOReactor::init(bool wait_for_iface_register) {
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

    if (!iocontext_init()) { return; }

    m_is_io_thread.store(true);

    // Add all iomanager existing fds to be added to this thread epoll
    iomanager.foreach_iodevice([&](const io_device_ptr& iodev) { add_iodev_to_reactor(iodev); });

    // Notify all the end points about new thread
    iomanager.foreach_interface([&](IOInterface* iface) { iface->on_io_thread_start(this); });

    // Notify the caller registered to iomanager for it.
    iomanager.io_thread_started(m_is_iomgr_thread);

    // NOTE: This should be the last one before return, because notification might call stop() and thus need
    // to have clean exit in those cases.
    notify_thread_state(true /* started */);
}

void IOReactor::stop() {
    m_keep_running = false;

    iomanager.foreach_interface([&](IOInterface* iface) { iface->on_io_thread_stopped(this); });
    iomanager.foreach_iodevice([&](const io_device_ptr& iodev) { remove_iodev_from_reactor(iodev); });

    // Notify the caller registered to iomanager for it
    notify_thread_state(false /* started */);

    // Clear all the IO carrier specific context (epoll or spdk etc..)
    iocontext_exit();

    m_is_io_thread.store(false);
    iomanager.io_thread_stopped();
}

void IOReactor::handle_msg(const iomgr_msg& msg) {
    ++m_metrics->msg_recvd_count;
    switch (msg.get_type< iomgr_msg_type >()) {
    case iomgr_msg_type::RESCHEDULE: {
        auto iodev = msg.m_iodev;
        ++m_metrics->rescheduled_in;
        if (msg.m_event & EPOLLIN) { iodev->cb(iodev.get(), iodev->cookie, EPOLLIN); }
        if (msg.m_event & EPOLLOUT) { iodev->cb(iodev.get(), iodev->cookie, EPOLLOUT); }
        break;
    }

    case iomgr_msg_type::RELINQUISH_IO_THREAD:
        LOGINFO("This thread is asked to be reliquished its status as io thread. Will exit io loop");
        stop();
        break;

    case iomgr_msg_type::DESIGNATE_IO_THREAD:
        LOGINFO("This thread is asked to be designated its status as io thread. Will start running io loop");
        m_keep_running = true;
        m_is_io_thread = true;
        break;

    case iomgr_msg_type::RUN_METHOD: {
        LOGTRACE("We are picked the thread to run the method");
        auto method_to_run = (run_method_t*)msg.m_data_buf;
        (*method_to_run)();
        sisl::ObjectAllocator< run_method_t >::deallocate(method_to_run);
        break;
    }

    case iomgr_msg_type::ADD_DEVICE: {
        add_iodev_to_reactor(msg.m_iodev);
        sync_iomgr_msg::to_sync_msg(msg).done();
        break;
    }

    case iomgr_msg_type::REMOVE_DEVICE: {
        add_iodev_to_reactor(msg.m_iodev);
        sync_iomgr_msg::to_sync_msg(msg).done();
        break;
    }

    case iomgr_msg_type::WAKEUP:
    case iomgr_msg_type::SHUTDOWN:
    case iomgr_msg_type::CUSTOM_MSG: {
        auto& handler = msg_handler();
        if (handler) {
            handler(msg);
        } else {
            LOGINFO("Received a message, but no message handler registered. Ignoring this message");
        }
        break;
    }

    case iomgr_msg_type::UNKNOWN:
    default:
        assert(0);
        break;
    }
}

void IOReactor::on_user_iodev_notification(io_device_t* iodev, int event) {
    ++m_count;
    ++m_metrics->io_count;

    LOGTRACEMOD(iomgr, "Processing event on user iodev: {}", iodev->dev_id());
    iodev->cb(iodev, iodev->cookie, event);

    --m_count;
}

bool IOReactor::is_iodev_addable(const io_device_ptr& iodev) const {
    return (!m_iodev_selector || m_iodev_selector(iodev));
}

void IOReactor::notify_thread_state(bool is_started) {
    iomgr_msg msg(is_started ? (int)iomgr_msg_type::WAKEUP : (int)iomgr_msg_type::SHUTDOWN);
    auto& handler = msg_handler();
    if (handler) { handler(msg); }
}

io_thread_msg_handler& IOReactor::msg_handler() {
    return (m_this_thread_msg_handler) ? m_this_thread_msg_handler : iomanager.m_common_thread_msg_handler;
}

} // namespace iomgr

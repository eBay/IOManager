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
#include "include/reactor_epoll.hpp"
#include <fds/obj_allocator.hpp>

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

namespace iomgr {

io_thread::io_thread(IOReactor* reactor) : thread_addr(reactor->reactor_idx()), reactor(reactor) {}

IOReactor::~IOReactor() {
    if (is_io_reactor()) { stop(); }
}

void IOReactor::run(int worker_slot_num, const iodev_selector_t& iodev_selector,
                    const thread_state_notifier_t& thread_state_notifier) {
    auto state = iomanager.get_state();
    if ((state == iomgr_state::stopping) || (state == iomgr_state::stopped)) {
        LOGINFO("Starting a new IOReactor while iomanager is stopping or stopped, not starting io loop");
        return;
    }

    if (!is_io_reactor()) {
        m_worker_slot_num = worker_slot_num;
        m_iodev_selector = iodev_selector;
        m_this_thread_notifier = thread_state_notifier;

        m_reactor_num = sisl::ThreadLocalContext::my_thread_num();
        REACTOR_LOG(INFO, base, , "IOReactor started and assigned reactor id {}", m_reactor_num);

        init();
        if (m_keep_running) { REACTOR_LOG(INFO, base, , "IOReactor is ready to go to listen loop"); }
    }

    while (m_keep_running) {
        listen();
    }
}

void IOReactor::init() {
#if 0
    if (!iomanager.is_interface_registered()) {
        if (!wait_for_iface_register) {
            REACTOR_LOG(INFO, base, , "iomgr interfaces are not registered yet and not requested to wait, ",
                        "it will not be an IO Reactor");
            return;
        }
        REACTOR_LOG(INFO, base, ,
                    "IOManager interfaces are not registered yet, waiting for interfaces to get registered");
        iomanager.wait_for_interface_registration();
        REACTOR_LOG(TRACE, iomgr, ,
                    "All interfaces are registered to IOManager, can proceed with this thread initialization");
    }
#endif

    LOGTRACEMOD(iomgr, "Initializing iomanager context for this thread, reactor_id= {}", m_reactor_num);

    // Create a new IO lightweight thread (if need be) and add it to its list, notify everyone about the new thread
    start_io_thread(iomanager.make_io_thread(this));

    // Notify the caller registered to iomanager for it.
    iomanager.reactor_started(shared_from_this());

    // For IOMgr created reactors, we want the notification to go only after all reactors are started and system init.
    if (!is_worker()) { iomanager.this_reactor()->notify_thread_state(true); }
}

void IOReactor::stop() {
    m_keep_running = false;

    for (auto thr : m_io_threads) {
        stop_io_thread(thr);
    }
    iomanager.reactor_stopped();

    // Notify the caller registered to iomanager for it
    notify_thread_state(false /* started */);
}

bool IOReactor::can_add_iface(const std::shared_ptr< IOInterface >& iface) const {
    if (iface->scope() == thread_regex::all_io) { return true; }
    return is_worker() ? (iface->scope() == thread_regex::all_worker) : (iface->scope() == thread_regex::all_user);
}

void IOReactor::start_io_thread(const io_thread_t& thr) {
    m_io_threads.emplace_back(thr);
    thr->thread_addr = m_io_threads.size() - 1;

    thr->m_metrics =
        std::make_unique< IOThreadMetrics >(fmt::format("{}.{}-{}", reactor_idx(), thr->thread_addr, loop_type()));

    // Initialize any thing specific to specialized reactors
    if (!reactor_specific_init_thread(thr)) { return; }

    // Notify all the interfaces about new thread, which in turn will add all relevant devices to current reactor.
    {
        auto iface_list = iomanager.iface_rlock();
        for (auto& iface : *iface_list) {
            if (can_add_iface(iface)) { iface->on_io_thread_start(thr); }
        }
        m_io_thread_count.increment();
    }
}

void IOReactor::stop_io_thread(const io_thread_t& thr) {
    // LOGMSG_ASSERT_EQ(m_io_threads[thr->thread_addr].get(), thr.get(), "Expected io thread {} to present in the list",
    //                 *(thr.get()));
    // iomanager.foreach_interface([&](IOInterface* iface) { iface->on_io_thread_stopped(thr); });

    {
        auto iface_list = iomanager.iface_rlock();
        for (auto& iface : *iface_list) {
            iface->on_io_thread_stopped(thr);
        }
        m_io_thread_count.decrement();
    }

    // Clear all the IO carrier specific context (epoll or spdk etc..)
    reactor_specific_exit_thread(thr);

    thr->m_metrics = nullptr;
    m_io_threads[thr->thread_addr] = nullptr;
}

int IOReactor::add_iodev_to_thread(const io_device_ptr& iodev, const io_thread_t& thr) {
    auto ret = _add_iodev_to_thread(iodev, thr);
    if (ret == 0) { ++m_n_iodevices; }
    return ret;
}

int IOReactor::remove_iodev_from_thread(const io_device_ptr& iodev, const io_thread_t& thr) {
    auto ret = _remove_iodev_from_thread(iodev, thr);
    if (ret == 0) { --m_n_iodevices; }
    return ret;
}

// This method assumes that interface lock is already held by the caller or message passer
void IOReactor::start_interface(IOInterface* iface) {
    for (auto& thr : m_io_threads) {
        iface->on_io_thread_start(thr);
    }
}

const io_thread_t& IOReactor::iothread_self() const { return m_io_threads[0]; };

bool IOReactor::deliver_msg(io_thread_addr_t taddr, iomgr_msg* msg, IOReactor* sender_reactor) {
    msg->m_dest_thread = taddr;
    if (msg->has_sem_block()) { msg->m_msg_sem->pending(); }

    // If the sender and receiver are same thread, take a shortcut to directly handle the message. Of course, this
    // will cause out-of-order delivery of messages. However, there is no good way to prevent deadlock
    if (sender_reactor == this) {
        handle_msg(msg);
        return true;
    } else {
        return put_msg(msg);
    }
}

const io_thread_t& IOReactor::msg_thread(iomgr_msg* msg) { return addr_to_thread(msg->m_dest_thread); }

void IOReactor::handle_msg(iomgr_msg* msg) {
    ++msg_thread(msg)->m_metrics->msg_recvd_count;

    // If the message is for a different module, pass it on to their handler
    if (msg->m_dest_module != iomanager.m_internal_msg_module_id) {
        auto& handler = iomanager.get_msg_module(msg->m_dest_module);
        if (!handler) {
            REACTOR_LOG(INFO, base, msg->m_dest_module,
                        "Received a msg of type={}, but no handler registered. Ignoring this msg", msg->m_type);
        } else {
            handler(msg);
        }
    } else {
        switch (msg->m_type) {
        case iomgr_msg_type::RESCHEDULE: {
            auto iodev = msg->iodevice_data();
            //++m_metrics->rescheduled_in;
            if (msg->event() & EPOLLIN) { iodev->cb(iodev.get(), iodev->cookie, EPOLLIN); }
            if (msg->event() & EPOLLOUT) { iodev->cb(iodev.get(), iodev->cookie, EPOLLOUT); }
            break;
        }

        case iomgr_msg_type::RELINQUISH_IO_THREAD:
            REACTOR_LOG(INFO, base, ,
                        "This reactor is asked to be reliquished its status as io reactor. Will exit io loop");
            stop();
            break;

        case iomgr_msg_type::DESIGNATE_IO_THREAD:
            REACTOR_LOG(INFO, base, ,
                        "This reactor is asked to be designated its status as io reactor. Will start running io loop");
            m_keep_running = true;
            break;

        case iomgr_msg_type::RUN_METHOD: {
            msg->method_data()(msg->m_dest_thread);
            break;
        }

#if 0
        case iomgr_msg_type::ADD_DEVICE: {
            add_iodev_to_reactor(msg->iodevice_data());
            break;
        }

        case iomgr_msg_type::REMOVE_DEVICE: {
            remove_iodev_from_reactor(msg->iodevice_data());
            break;
        }
#endif

        case iomgr_msg_type::UNKNOWN:
        default:
            LOGMSG_ASSERT(0, "Received a unknown msg type={}, to internal message handler. Ignoring this message",
                          msg->m_type);
            break;
        }
    }

    if (msg->has_sem_block()) { msg->m_msg_sem->done(); }
    iomgr_msg::free(msg);
}

bool IOReactor::is_iodev_addable(const io_device_ptr& iodev, const io_thread_t& thread) const {
    return (!m_iodev_selector || m_iodev_selector(iodev));
}

void IOReactor::notify_thread_state(bool is_started) {
    if (m_this_thread_notifier) { m_this_thread_notifier(is_started); }
    if (iomanager.thread_state_notifier()) { iomanager.thread_state_notifier()(is_started); }
}

const io_thread_t& IOReactor::select_thread() { return m_io_threads[m_total_op++ % m_io_threads.size()]; }

const io_thread_t& IOReactor::addr_to_thread(io_thread_addr_t addr) {
    if (addr >= m_io_threads.size()) {
        LOGMSG_ASSERT(0, "Accessing invalid thread on reactor={} thread_addr={} num_of_threads_in_reactor={}",
                      m_reactor_num, addr, m_io_threads.size());
        return m_io_threads[0];
    }
    return m_io_threads[addr];
}

} // namespace iomgr

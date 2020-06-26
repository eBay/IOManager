/**
 * Copyright eBay Corporation 2018
 */

#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/iomgr_msg.hpp"
#include "include/reactor_spdk.hpp"
#include <fds/obj_allocator.hpp>
#include <spdk/log.h>
#include <spdk/env.h>
#include <spdk/thread.h>
#include "spdk/bdev.h"

namespace iomgr {
static void _handle_thread_msg(void* _msg) { iomanager.this_reactor()->handle_msg((iomgr_msg*)_msg); }

bool IOReactorSPDK::reactor_specific_init_thread(const io_thread_t& thr) {
    // Create SPDK LW thread for this io thread
    auto sthread = spdk_thread_create(NULL, NULL);
    if (sthread == nullptr) {
        throw std::runtime_error("SPDK Thread Create failed");
        return false;
    }
    spdk_set_thread(sthread);
    thr->thread_impl = sthread;

    m_thread_timer = std::make_unique< timer_spdk >(true /* is_per_thread */);

    // TODO: Add per thread timer object.
    return true;
}

void IOReactorSPDK::listen() {
    for (auto& thr : m_io_threads) {
        spdk_thread_poll(thr->spdk_thread_impl(), 0, 0);
    }
}

void IOReactorSPDK::reactor_specific_exit_thread(const io_thread_t& thr) {
    if (thr->thread_addr) {
        spdk_thread_exit(thr->spdk_thread_impl());
        spdk_thread_destroy(thr->spdk_thread_impl());
        thr->thread_impl = nullptr;
    }
}

int IOReactorSPDK::_add_iodev_to_thread(const io_device_ptr& iodev, const io_thread_t& thr) { return 0; }
int IOReactorSPDK::_remove_iodev_from_thread(const io_device_ptr& iodev, const io_thread_t& thr) { return 0; }

bool IOReactorSPDK::put_msg(iomgr_msg* msg) {
    spdk_thread_send_msg(addr_to_thread(msg->m_dest_thread)->spdk_thread_impl(), _handle_thread_msg, msg);
    return true;
}

bool IOReactorSPDK::is_iodev_addable(const io_device_ptr& iodev, const io_thread_t& thread) const {
    return (iodev->is_spdk_dev() && IOReactor::is_iodev_addable(iodev, thread));
}

void IOReactorSPDK::deliver_msg_direct(spdk_thread* to_thread, iomgr_msg* msg) {
    if (msg->has_sem_block()) { msg->m_msg_sem->pending(); }
    spdk_thread_send_msg(to_thread, _handle_thread_msg, msg);
}
} // namespace iomgr
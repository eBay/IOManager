/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Harihara Kadayam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **************************************************************************/
#include <cstring>

#include <sisl/logging/logging.h>
#include <sisl/fds/obj_allocator.hpp>
#include <spdk/log.h>
#include <spdk/env.h>
#include <spdk/thread.h>
#include <spdk/bdev.h>

#include <iomgr/iomgr.hpp>
#include <iomgr/iomgr_msg.hpp>
#include "reactor_spdk.hpp"

namespace iomgr {
static std::string s_spdk_thread_name_prefix = "iomgr_reactor_io_thread_";

static void _handle_thread_msg(void* _msg) {
    iomgr_msg* msg = r_cast< iomgr_msg* >(_msg);

    auto orig_thread = spdk_get_thread();
    spdk_set_thread(msg->m_dest_fiber->spdk_thr);
    iomanager.this_reactor()->handle_msg((iomgr_msg*)_msg);
    spdk_set_thread(orig_thread);
}

int IOReactorSPDK::event_about_spdk_thread(struct spdk_thread* sthread, enum spdk_thread_op op) {
    switch (op) {
    case spdk_thread_op::SPDK_THREAD_OP_NEW: {
        // Since we get a callback for even the thread this reactor created, we check for this and return rightaway.
        if (is_iomgr_created_spdk_thread(sthread)) { return 0; }

        auto reactor = static_cast< IOReactorSPDK* >(iomanager.round_robin_reactor());
        iomanager.run_on_forget(
            reactor->pick_fiber(fiber_regex::main_only),
            [](void* arg) {
                auto sthread = reinterpret_cast< spdk_thread* >(arg);
                static_cast< IOReactorSPDK* >(iomanager.this_reactor())->add_external_spdk_thread(sthread);
            },
            (void*)sthread);
        return 0;
    }
    case spdk_thread_op::SPDK_THREAD_OP_RESCHED:
    default:
        return -ENOTSUP;
    }
}

bool IOReactorSPDK::reactor_thread_op_supported(enum spdk_thread_op op) {
    switch (op) {
    case SPDK_THREAD_OP_NEW:
        return true;
    case SPDK_THREAD_OP_RESCHED:
    default:
        return false;
    }
}

std::string IOReactorSPDK::gen_spdk_thread_name() {
    static uint32_t s_sthread_idx{0};
    return s_spdk_thread_name_prefix + std::to_string(s_sthread_idx++);
}

bool IOReactorSPDK::is_iomgr_created_spdk_thread(const spdk_thread* sthread) {
    return (std::strncmp(spdk_thread_get_name(sthread), s_spdk_thread_name_prefix.c_str(),
                         s_spdk_thread_name_prefix.size()) == 0);
}

spdk_thread* IOReactorSPDK::create_spdk_thread() {
    struct spdk_cpuset cpu_mask;
    struct spdk_cpuset* pcpu_mask{nullptr};

    const auto lcore = spdk_env_get_current_core();
    if (lcore != std::numeric_limits< uint32_t >::max()) {
        pcpu_mask = &cpu_mask;
        spdk_cpuset_zero(pcpu_mask);
        spdk_cpuset_set_cpu(pcpu_mask, lcore, true);
    }
    return spdk_thread_create(gen_spdk_thread_name().c_str(), pcpu_mask);
}

void IOReactorSPDK::init_impl() {
    for (auto& fiber : m_io_fibers) {
        auto sthread = create_spdk_thread();
        if (sthread == nullptr) { throw std::runtime_error("SPDK Thread Create failed"); }
        spdk_set_thread(sthread);
        fiber->spdk_thr = sthread;
    }
    m_thread_timer = std::make_unique< timer_spdk >(m_io_fibers[0].get());
}

void IOReactorSPDK::listen() {
    for (auto& fiber : m_io_fibers) {
        if (!m_keep_running) { break; }
        spdk_thread_poll(fiber->spdk_thr, 0, 0);
    }

    for (auto& thr : m_external_spdk_threads) {
        spdk_thread_poll(thr, 0, 0);
    }
}

void IOReactorSPDK::stop_impl() {
    for (auto& fiber : m_io_fibers) {
        spdk_set_thread(fiber->spdk_thr);
        spdk_thread_exit(fiber->spdk_thr);
        while (!spdk_thread_is_exited(fiber->spdk_thr)) {
            spdk_thread_poll(fiber->spdk_thr, 0, 0);
        }
        spdk_thread_destroy(fiber->spdk_thr);
        fiber->spdk_thr = nullptr;
    }
}

void IOReactorSPDK::add_external_spdk_thread(struct spdk_thread* sthread) {
    m_external_spdk_threads.push_back(sthread);
    REACTOR_LOG(INFO, "Added External SPDK Thread {} to this reactor", spdk_thread_get_name(sthread));
}

int IOReactorSPDK::add_iodev_impl(const io_device_ptr& iodev) {
    iodev->io_interface->init_iodev_reactor_context(iodev, this);
    return 0;
}

int IOReactorSPDK::remove_iodev_impl(const io_device_ptr& iodev) {
    iodev->io_interface->clear_iodev_reactor_context(iodev, this);
    return 0;
}

void IOReactorSPDK::put_msg(iomgr_msg* msg) {
    spdk_thread_send_msg(msg->m_dest_fiber->spdk_thr, _handle_thread_msg, msg);
}

bool IOReactorSPDK::is_iodev_addable(const io_device_const_ptr& iodev) const {
    return (iodev->is_spdk_dev() && IOReactor::is_iodev_addable(iodev));
}
} // namespace iomgr

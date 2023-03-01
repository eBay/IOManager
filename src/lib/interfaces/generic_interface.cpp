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
#include <sisl/logging/logging.h>
#include <iomgr/iomgr.hpp>
#include <iomgr/iomgr_msg.hpp>
#include <iomgr/io_interface.hpp>
#include "reactor.hpp"

namespace iomgr {

IOInterface::IOInterface() { m_iface_thread_ctx.reserve(IOManager::max_io_threads); }
IOInterface::~IOInterface() = default;

void IOInterface::close_dev(const io_device_ptr& iodev) {
    if (iodev->ready) { remove_io_device(iodev); }
}

void IOInterface::add_io_device(const io_device_ptr& iodev, const wait_type_t wtype,
                                const run_on_closure_t& add_done_cb) {
    if (iodev->is_my_thread_scope()) {
        auto r = iomanager.this_reactor();
        if (r) {
            // Select one thread among reactor in possible round robin fashion and add it there.
            auto& thr = r->select_thread();
            add_to_my_reactor(iodev, thr);
            iodev->ready = true;
        } else {
            LOGDFATAL("IOManager does not support adding local iodevices through non-io threads yet. Send a message to "
                      "an io thread");
        }
        if (add_done_cb) { add_done_cb(); }
    } else {
        {
            std::unique_lock lg(m_mtx);
            m_iodev_map.insert(std::pair< backing_dev_t, io_device_ptr >(iodev->dev, iodev));
        }

        // Need a callback to mark device ready, so run as callback version
        const auto run_wtype{(wtype == wait_type_t::no_wait) ? wait_type_t::callback : wtype};
        iomanager.run_on(
            iodev->global_scope(),
            [this, iodev](io_thread_addr_t taddr) {
                LOGDEBUGMOD(iomgr, "IODev {} is being added to thread {}.{}", iodev->dev_id(),
                            iomanager.this_reactor()->reactor_idx(), taddr);
                add_to_my_reactor(iodev, iomanager.this_reactor()->addr_to_thread(taddr));
            },
            run_wtype,
            [iodev, add_done_cb]() {
                LOGDEBUGMOD(iomgr, "IODev {} added to all threads, marking it as ready", iodev->dev_id());
                iodev->ready = true;
                if (add_done_cb) { add_done_cb(); }
            });
    }
}

void IOInterface::remove_io_device(const io_device_ptr& iodev, const wait_type_t wtype,
                                   const run_on_closure_t& remove_done_cb) {
    if (!iodev->ready) {
        LOGINFO("Device {} is not added to IOManager. Ignoring this request", iodev->dev_id());
        return;
    }

    auto state = iomanager.get_state();
    if ((state != iomgr_state::running) && (state != iomgr_state::stopping)) {
        LOGDFATAL("Expected IOManager to be running or stopping state before we receive remove io device");
        return;
    }

    if (iodev->is_my_thread_scope()) {
        auto r = iomanager.this_reactor();
        if (r) {
            remove_from_my_reactor(iodev, iodev->per_thread_scope());
        } else {
            LOGDFATAL("IOManager does not support removing local iodevices through non-io threads yet. Send a "
                      "message to an io thread");
        }
        if (remove_done_cb) { remove_done_cb(); }
    } else {
        const auto run_wtype{(wtype == wait_type_t::no_wait) ? wait_type_t::callback : wtype};

        {
            std::unique_lock lg(m_mtx);
            m_iodev_map.erase(iodev->dev);
        }
        iomanager.run_on(
            iodev->global_scope(),
            [this, iodev](io_thread_addr_t taddr) {
                LOGDEBUGMOD(iomgr, "IODev {} is being removed from thread {}.{}", iodev->dev_id(),
                            iomanager.this_reactor()->reactor_idx(), taddr);
                remove_from_my_reactor(iodev, iomanager.this_reactor()->addr_to_thread(taddr));
            },
            run_wtype,
            [iodev, remove_done_cb]() {
                LOGDEBUGMOD(iomgr, "IODev {} removed from all threads, marking it NOTREADY", iodev->dev_id());
                iodev->ready = false;
                if (remove_done_cb) { remove_done_cb(); }
            });
    }
}

void IOInterface::on_io_thread_start(const io_thread_t& thr) {
    IOInterfaceThreadContext* iface_thread_ctx{nullptr};
    {
        // This step ensures that sparse vector if need be expands under lock.
        // Once a slot is assigned, there is no contention
        std::unique_lock lk(m_ctx_init_mtx);
        iface_thread_ctx = m_iface_thread_ctx[thr->thread_idx].get();
    }

    if (iface_thread_ctx == nullptr) {
        init_iface_thread_ctx(thr);

        // Add all devices part of this interface to this thread
        uint32_t added_count{0};
        {
            std::shared_lock lg(m_mtx);
            for (auto& iodev : m_iodev_map) {
                if (add_to_my_reactor(iodev.second, thr)) { ++added_count; }
            }
        }

        // If the derived class iface_thread_ctx is still not written, create one for ourselves
        if (m_iface_thread_ctx[thr->thread_idx] == nullptr) {
            m_iface_thread_ctx[thr->thread_idx] = std::make_unique< IOInterfaceThreadContext >();
        }
        LOGINFOMOD(iomgr, "Added {} [scope={}] and iodevices [{} out of {}] to io_thread [idx={},addr={}]", name(),
                   scope(), added_count, m_iodev_map.size(), thr->thread_idx, thr->thread_addr);
    } else {
        LOGINFO("IO Thread is already started, duplicate request to init thread context, ignoring");
    }
}

void IOInterface::on_io_thread_stopped(const io_thread_t& thr) {
    if (m_iface_thread_ctx[thr->thread_idx] != nullptr) {
        uint32_t removed_count{0};
        {
            std::shared_lock lg(m_mtx);
            for (auto& iodev : m_iodev_map) {
                if (remove_from_my_reactor(iodev.second, thr)) { ++removed_count; }
            }
        }

        clear_iface_thread_ctx(thr);
        m_iface_thread_ctx[thr->thread_idx].reset();
        LOGINFOMOD(iomgr, "Removed {} [scope={}] and iodevices [{} out of {}] from io_thread [idx={},addr={}]", name(),
                   scope(), removed_count, m_iodev_map.size(), thr->thread_idx, thr->thread_addr);
    }
}

bool IOInterface::add_to_my_reactor(const io_device_ptr& iodev, const io_thread_t& thr) {
    bool added{false};
    IODeviceThreadContext* iodev_thread_ctx{nullptr};
    {
        // This step ensures that sparse vector if need be expands under lock.
        // Once a slot is assigned, there is no contention
        std::unique_lock lk(iodev->m_ctx_init_mtx);
        iodev_thread_ctx = iodev->m_iodev_thread_ctx[thr->thread_idx].get();
    }

    if (iodev_thread_ctx == nullptr) {
        if (thr->reactor->is_iodev_addable(iodev, thr)) {
            thr->reactor->add_iodev(iodev, thr);
            added = true;
        }
        init_iodev_thread_ctx(iodev, thr);
        if (iodev->m_iodev_thread_ctx[thr->thread_idx] == nullptr) {
            iodev->m_iodev_thread_ctx[thr->thread_idx] = std::make_unique< IODeviceThreadContext >();
        }
    }
    return added;
}

bool IOInterface::remove_from_my_reactor(const io_device_ptr& iodev, const io_thread_t& thr) {
    bool removed{false};
    if (iodev->m_iodev_thread_ctx[thr->thread_idx]) {
        if (thr->reactor->is_iodev_addable(iodev, thr)) {
            thr->reactor->remove_iodev(iodev, thr);
            removed = true;
        }
        clear_iodev_thread_ctx(iodev, thr);
        iodev->m_iodev_thread_ctx[thr->thread_idx].reset();
    }
    return removed;
}

io_device_ptr IOInterface::alloc_io_device(const backing_dev_t dev, const int events_interested, const int pri,
                                           void* cookie, const thread_specifier& scope, const ev_callback& cb) {
    auto iodev = std::make_shared< IODevice >(pri, scope);
    iodev->dev = dev;
    iodev->cb = cb;
    iodev->cookie = cookie;
    iodev->ev = events_interested;
    iodev->io_interface = this;

    return iodev;
}

///////////////////////////////////////// GenericIOInterface Section ////////////////////////////////////////////
io_device_ptr GenericIOInterface::make_io_device(const backing_dev_t dev, const int events_interested, const int pri,
                                                 void* cookie, const bool is_per_thread_dev, const ev_callback& cb) {
    return make_io_device(dev, events_interested, pri, cookie,
                          is_per_thread_dev ? thread_specifier{iomanager.iothread_self()}
                                            : thread_specifier{thread_regex::all_io},
                          std::move(cb));
}

io_device_ptr GenericIOInterface::make_io_device(const backing_dev_t dev, const int events_interested, const int pri,
                                                 void* cookie, const thread_specifier& scope, const ev_callback& cb) {
    auto iodev = alloc_io_device(dev, events_interested, pri, cookie, scope, cb);
    add_io_device(iodev);
    return iodev;
}

void GenericIOInterface::init_iface_thread_ctx(const io_thread_t& thr) {
    auto ctx = std::make_unique< GenericInterfaceThreadContext >();
    ctx->listen_sentinel_cb = m_listen_sentinel_cb;
    m_iface_thread_ctx[thr->thread_idx] = std::move(ctx);
}

void GenericIOInterface::clear_iface_thread_ctx(const io_thread_t& thr) { m_iface_thread_ctx[thr->thread_idx].reset(); }

void GenericIOInterface::attach_listen_sentinel_cb(const listen_sentinel_cb_t& cb, const wait_type_t wtype) {
    {
        std::unique_lock lg(m_mtx);
        m_listen_sentinel_cb = cb;
    }
    iomanager.run_on(
        thread_regex::all_io,
        [this, cb]([[maybe_unused]] io_thread_addr_t taddr) { thread_ctx()->listen_sentinel_cb = cb; }, wtype);
}

void GenericIOInterface::detach_listen_sentinel_cb(const wait_type_t wtype) {
    {
        std::unique_lock lg(m_mtx);
        m_listen_sentinel_cb = nullptr;
    }
    iomanager.run_on(
        thread_regex::all_io,
        [this]([[maybe_unused]] io_thread_addr_t taddr) { thread_ctx()->listen_sentinel_cb = nullptr; }, wtype);
}

listen_sentinel_cb_t& GenericIOInterface::get_listen_sentinel_cb() { return thread_ctx()->listen_sentinel_cb; }

GenericInterfaceThreadContext* GenericIOInterface::thread_ctx() {
    return static_cast< GenericInterfaceThreadContext* >(
        m_iface_thread_ctx[IOReactor::this_reactor->default_thread_idx()].get());
}

} // namespace iomgr

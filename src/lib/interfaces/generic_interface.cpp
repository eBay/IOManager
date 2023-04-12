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
#include "reactor/reactor.hpp"

namespace iomgr {

IOInterface::IOInterface() = default;
IOInterface::~IOInterface() = default;

void IOInterface::close_dev(const io_device_ptr& iodev) {
    if (iodev->ready) { remove_io_device(iodev, true /* wait_to_remove */); }
}

int IOInterface::add_io_device(const io_device_ptr& iodev, bool wait_to_add) {
    auto add_to_reactor = [this, iodev]() {
        auto reactor = iomanager.this_reactor();
        if (reactor) {
            LOGDEBUGMOD(iomgr, "IODev {} is being added to reactor {}", iodev->dev_id(), reactor->reactor_idx());
            reactor->add_iodev(iodev);
        } else {
            LOGDFATAL("IOManager does not support adding local iodevices through non-io threads yet. Send a message to "
                      "an io thread");
        }
        iodev->decrement_pending();
    };

    auto post_add = [](IODevice* iodev) {
        LOGDEBUGMOD(iomgr, "IODev {} added to all threads, marking it as ready", iodev->dev_id());
        iodev->ready = true;
    };

    int added_count{0};
    if (iodev->is_my_thread_scope()) {
        add_to_reactor();
        ++added_count;
        post_add(iodev.get());
    } else {
        {
            std::unique_lock lg(m_mtx);
            m_iodev_map.insert(std::pair< backing_dev_t, io_device_ptr >(iodev->dev, iodev));
        }
        if (wait_to_add) {
            added_count = iomanager.run_on_wait(iodev->global_scope(), add_to_reactor);
            post_add(iodev.get());
        } else {
            iodev->post_add_remove_cb = post_add;
            added_count = iomanager.run_on_forget(iodev->global_scope(), add_to_reactor);
            iodev->increment_pending(added_count);
        }
    }

    return added_count;
}

int IOInterface::remove_io_device(const io_device_ptr& iodev, bool wait_to_remove) {
    if (!iodev->ready) {
        LOGINFO("Device {} is not added to IOManager. Ignoring this request", iodev->dev_id());
        return 0;
    }

    auto state = iomanager.get_state();
    if ((state != iomgr_state::running) && (state != iomgr_state::stopping)) {
        LOGDFATAL("Expected IOManager to be running or stopping state before we receive remove io device");
        return 0;
    }

    auto remove_from_reactor = [this, iodev]() {
        auto reactor = iomanager.this_reactor();
        if (reactor) {
            LOGDEBUGMOD(iomgr, "IODev {} is being removed from reactor {}", iodev->dev_id(), reactor->reactor_idx());
            reactor->remove_iodev(iodev);
        } else {
            LOGDFATAL("IOManager does not support removing local iodevices through non-io threads yet. Send a "
                      "message to an io thread");
        }
        iodev->decrement_pending();
    };

    auto post_remove = [](IODevice* iodev) {
        LOGDEBUGMOD(iomgr, "IODev {} removed from all threads, marking it NOTREADY", iodev->dev_id());
        iodev->close();
        iodev->ready = false;
    };

    int removed_count{0};
    if (iodev->is_my_thread_scope()) {
        remove_from_reactor();
        ++removed_count;
        post_remove(iodev.get());
    } else {
        {
            std::unique_lock lg(m_mtx);
            m_iodev_map.erase(iodev->dev);
        }

        if (wait_to_remove) {
            removed_count = iomanager.run_on_wait(iodev->global_scope(), remove_from_reactor);
            post_remove(iodev.get());
        } else {
            iodev->post_add_remove_cb = post_remove;
            removed_count = iomanager.run_on_forget(iodev->global_scope(), remove_from_reactor);
            iodev->increment_pending(removed_count);
        }
    }
    return removed_count;
}

void IOInterface::on_reactor_start(IOReactor* reactor) {
    init_iface_reactor_context(reactor);
    uint32_t added_count{0};
    {
        std::shared_lock lg(m_mtx);
        for (auto& iodev : m_iodev_map) {
            if (reactor->is_iodev_addable(iodev.second)) {
                reactor->add_iodev(iodev.second);
                ++added_count;
            }
        }
    }
    LOGINFOMOD(iomgr, "Added {} [scope={}] and iodevices [{} out of {}] to reactor [{}]", name(), scope(), added_count,
               m_iodev_map.size(), reactor->reactor_idx());
}

void IOInterface::on_reactor_stop(IOReactor* reactor) {
    clear_iface_reactor_context(reactor);
    uint32_t removed_count{0};
    {
        std::shared_lock lg(m_mtx);
        for (auto& iodev : m_iodev_map) {
            if (reactor->is_iodev_addable(iodev.second)) {
                reactor->remove_iodev(iodev.second);
                ++removed_count;
            }
        }

        LOGINFOMOD(iomgr, "Removed {} [scope={}] and iodevices [{} out of {}] from reactor [{}]", name(), scope(),
                   removed_count, m_iodev_map.size(), reactor->reactor_idx());
    }
}

io_device_ptr IOInterface::alloc_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                           const thread_specifier& scope, const ev_callback& cb) {
    auto iodev = std::make_shared< IODevice >(pri, scope);
    iodev->dev = dev;
    iodev->cb = cb;
    iodev->cookie = cookie;
    iodev->ev = events_interested;
    iodev->io_interface = this;

    return iodev;
}

///////////////////////////////////////// GenericIOInterface Section ////////////////////////////////////////////
thread_local listen_sentinel_cb_t GenericIOInterface::t_listen_sentinel_cb;
io_device_ptr GenericIOInterface::make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                                 bool is_per_thread_dev, const ev_callback& cb) {
    return make_io_device(dev, events_interested, pri, cookie,
                          is_per_thread_dev ? thread_specifier{iomanager.this_reactor()->main_fiber()}
                                            : thread_specifier{reactor_regex::all_io},
                          std::move(cb));
}

io_device_ptr GenericIOInterface::make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                                 const thread_specifier& scope, const ev_callback& cb) {
    auto iodev = alloc_io_device(dev, events_interested, pri, cookie, scope, cb);
    add_io_device(iodev, true /* wait_to_add */);
    return iodev;
}

void GenericIOInterface::init_iface_reactor_context(IOReactor* reactor) { t_listen_sentinel_cb = m_listen_sentinel_cb; }

void GenericIOInterface::clear_iface_reactor_context(IOReactor* reactor) { t_listen_sentinel_cb = nullptr; }

void GenericIOInterface::attach_listen_sentinel_cb(const listen_sentinel_cb_t& cb) {
    {
        std::unique_lock lg(m_mtx);
        m_listen_sentinel_cb = cb;
    }
    iomanager.run_on_wait(reactor_regex::all_io, [this]() { t_listen_sentinel_cb = m_listen_sentinel_cb; });
}

void GenericIOInterface::detach_listen_sentinel_cb() {
    {
        std::unique_lock lg(m_mtx);
        m_listen_sentinel_cb = nullptr;
    }
    iomanager.run_on_wait(reactor_regex::all_io, [this]() { t_listen_sentinel_cb = nullptr; });
}

listen_sentinel_cb_t& GenericIOInterface::get_listen_sentinel_cb() { return t_listen_sentinel_cb; }

} // namespace iomgr

//
// Created by Kadayam, Hari on 02/04/18.
//
#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/iomgr_msg.hpp"
#include "include/io_interface.hpp"

namespace iomgr {

IOInterface::IOInterface() { m_thread_local_ctx.reserve(IOManager::max_io_threads); }
IOInterface::~IOInterface() = default;

void IOInterface::close_dev(const io_device_ptr& iodev) {
    if (iodev->ready) { remove_io_device(iodev); }
}

void IOInterface::add_io_device(const io_device_ptr& iodev, bool wait_to_add,
                                const std::function< void(io_device_ptr) >& add_comp_cb) {
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
        if (add_comp_cb) add_comp_cb(iodev);
    } else {
        int sent_count = 0;
        {
            auto lock = iomanager.iface_wlock();
            sent_count = iomanager.run_on(
                iodev->global_scope(),
                [this, iodev, add_comp_cb](io_thread_addr_t taddr) {
                    LOGDEBUGMOD(iomgr, "IODev {} is being added to thread {}.{}", iodev->dev_id(),
                                iomanager.this_reactor()->reactor_idx(), taddr);
                    add_to_my_reactor(iodev, iomanager.this_reactor()->addr_to_thread(taddr));

                    // If this thread is the last one to finish adding to reactors, then mark it ready and call the cb
                    if (iodev->thread_op_pending_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                        LOGDEBUGMOD(iomgr, "IODev {} added to all threads, marking it as ready", iodev->dev_id());
                        iodev->ready = true;
                        if (add_comp_cb) add_comp_cb(iodev);
                    }
                },
                wait_to_add ? wait_type_t::sleep : wait_type_t::no_wait);
            m_iodev_map.insert(std::pair< backing_dev_t, io_device_ptr >(iodev->dev, iodev));
        }

        LOGDEBUGMOD(iomgr, "IODev {} add message sent to {} threads", iodev->dev_id(), sent_count);
        if (iodev->thread_op_pending_count.fetch_add(sent_count, std::memory_order_acq_rel) == -sent_count) {
            LOGDEBUGMOD(iomgr, "IODev {} added to all threads, marking it as ready", iodev->dev_id());
            iodev->ready = true;
            if (add_comp_cb) add_comp_cb(iodev);
        }
    }
}

void IOInterface::remove_io_device(const io_device_ptr& iodev, bool wait_to_remove,
                                   const std::function< void(io_device_ptr) >& remove_comp_cb) {
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
        if (remove_comp_cb) remove_comp_cb(iodev);
    } else {
        int sent_count = 0;
        {
            auto lock = iomanager.iface_wlock();
            sent_count = iomanager.run_on(
                iodev->global_scope(),
                [this, iodev, remove_comp_cb](io_thread_addr_t taddr) {
                    LOGDEBUGMOD(iomgr, "IODev {} is being removed from thread {}.{}", iodev->dev_id(),
                                iomanager.this_reactor()->reactor_idx(), taddr);
                    remove_from_my_reactor(iodev, iomanager.this_reactor()->addr_to_thread(taddr));
                    if (iodev->thread_op_pending_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                        LOGDEBUGMOD(iomgr, "IODev {} removed from all threads, marking it NOTREADY", iodev->dev_id());
                        iodev->ready = false;
                        if (remove_comp_cb) remove_comp_cb(iodev);
                    }
                },
                wait_to_remove ? wait_type_t::sleep : wait_type_t::no_wait);
            m_iodev_map.erase(iodev->dev);
        }

        LOGDEBUGMOD(iomgr, "IODev {} remove message sent to {} threads", iodev->dev_id(), sent_count);
        if (iodev->thread_op_pending_count.fetch_add(sent_count, std::memory_order_acq_rel) == -sent_count) {
            LOGDEBUGMOD(iomgr, "IODev {} removed from all threads, marking it NOTREADY", iodev->dev_id());
            iodev->ready = false;
            if (remove_comp_cb) remove_comp_cb(iodev);
        }
    }
}

/* This method is expected to be called with interface lock held always */
void IOInterface::on_io_thread_start(const io_thread_t& thr) {
    init_iface_thread_ctx(thr);

    // Add all devices part of this interface to this thread
    uint32_t added_count{0};
    for (auto& iodev : m_iodev_map) {
        if (add_to_my_reactor(iodev.second, thr)) { ++added_count; }
    }
    LOGINFOMOD(iomgr, "Added IOInterface [scope={}] and iodevices [{} out of {}] to io_thread [idx={},addr={}]",
               scope(), added_count, m_iodev_map.size(), thr->thread_idx, thr->thread_addr);
}

/* This method is expected to be called with interface lock held always */
void IOInterface::on_io_thread_stopped(const io_thread_t& thr) {
    uint32_t removed_count{0};
    for (auto& iodev : m_iodev_map) {
        if (remove_from_my_reactor(iodev.second, thr)) { ++removed_count; }
    }
    clear_iface_thread_ctx(thr);
    LOGINFOMOD(iomgr, "Removed IOInterface [scope={}] and iodevices [{} out of {}] from io_thread [idx={},addr={}]",
               scope(), removed_count, m_iodev_map.size(), thr->thread_idx, thr->thread_addr);
}

bool IOInterface::add_to_my_reactor(const io_device_const_ptr& iodev, const io_thread_t& thr) {
    if (thr->reactor->is_iodev_addable(iodev, thr)) {
        thr->reactor->add_iodev(iodev, thr);
        init_iodev_thread_ctx(iodev, thr);
        return true;
    } else {
        return false;
    }
}

bool IOInterface::remove_from_my_reactor(const io_device_const_ptr& iodev, const io_thread_t& thr) {
    if (thr->reactor->is_iodev_addable(iodev, thr)) {
        clear_iodev_thread_ctx(iodev, thr);
        thr->reactor->remove_iodev(iodev, thr);
        return true;
    } else {
        return false;
    }
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
    m_thread_local_ctx[thr->thread_idx] = std::move(ctx);
}

void GenericIOInterface::clear_iface_thread_ctx(const io_thread_t& thr) { m_thread_local_ctx[thr->thread_idx].reset(); }

void GenericIOInterface::attach_listen_sentinel_cb(const listen_sentinel_cb_t& cb,
                                                   const run_method_t& on_attach_closure) {
    auto closure_in_origin = [this, origin_thr = iomanager.iothread_self(), on_attach_closure]() {
        iomanager.run_on(origin_thr, on_attach_closure);
    };

    {
        auto lock = iomanager.iface_wlock();
        m_listen_sentinel_cb = cb;
    }
    iomanager.run_on(
        thread_regex::all_io,
        [this, cb]([[maybe_unused]] io_thread_addr_t taddr) { thread_ctx()->listen_sentinel_cb = cb; },
        on_attach_closure ? closure_in_origin : run_on_closure_t{nullptr});
}

void GenericIOInterface::detach_listen_sentinel_cb(const run_method_t& on_detach_closure) {
    auto closure_in_origin = [this, origin_thr = iomanager.iothread_self(), on_detach_closure]() {
        iomanager.run_on(origin_thr, on_detach_closure);
    };

    {
        auto lock = iomanager.iface_wlock();
        m_listen_sentinel_cb = nullptr;
    }
    iomanager.run_on(
        thread_regex::all_io,
        [this]([[maybe_unused]] io_thread_addr_t taddr) { thread_ctx()->listen_sentinel_cb = nullptr; },
        on_detach_closure ? closure_in_origin : run_on_closure_t{nullptr});
}

listen_sentinel_cb_t& GenericIOInterface::get_listen_sentinel_cb() { return thread_ctx()->listen_sentinel_cb; }

GenericInterfaceThreadContext* GenericIOInterface::thread_ctx() {
    return static_cast< GenericInterfaceThreadContext* >(
        m_thread_local_ctx[IOReactor::this_reactor->default_thread_idx()].get());
}

} // namespace iomgr

//
// Created by Kadayam, Hari on 02/04/18.
//
#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/io_interface.hpp"

namespace iomgr {

IOInterface::IOInterface() { m_thread_local_ctx.reserve(IOManager::max_io_threads); }
IOInterface::~IOInterface() = default;

void IOInterface::close_dev(const io_device_ptr& iodev) {
    if (iodev->added_to_listen) { remove_io_device(iodev); }
}

void IOInterface::add_io_device(const io_device_ptr& iodev) {
    if (iodev->is_global()) {
        // Ensure the iomanager is running or wait until it is so.
        iomanager.ensure_running();

        {
            std::unique_lock lk(m_mtx);
            iomanager.run_on(
                thread_regex::all_io,
                [this, iodev](io_thread_addr_t taddr) {
                    LOGDEBUGMOD(iomgr, "IODev {} is being added to thread {}.{}", iodev->dev_id(),
                                iomanager.this_reactor()->reactor_idx(), taddr);
                    _add_to_thread(iodev, iomanager.this_reactor()->addr_to_thread(taddr));
                },
                true /* wait_for_completion */);

            m_iodev_map.insert(std::pair< backing_dev_t, io_device_ptr >(iodev->dev, iodev));
        }
    } else {
        auto r = iomanager.this_reactor();
        if (r) {
            // Select one thread among reactor in possible round robin fashion and add it there.
            auto& thr = r->select_thread();
            _add_to_thread(iodev, thr);
        } else {
            LOGDFATAL("IOManager does not support adding local iodevices through non-io threads yet. Send a message to "
                      "an io thread");
        }
    }
    iodev->added_to_listen = true;
}

void IOInterface::remove_io_device(const io_device_ptr& iodev) {
    if (!iodev->added_to_listen) {
        LOGINFO("Device {} is not added to IOManager. Ignoring this request", iodev->dev_id());
        return;
    }

    auto state = iomanager.get_state();
    if ((state != iomgr_state::running) && (state != iomgr_state::stopping)) {
        LOGDFATAL("Expected IOManager to be running or stopping state before we receive remove io device");
        return;
    }

    if (iodev->is_global()) {
        std::unique_lock lk(m_mtx);

        // Send a sync message to add device to all io threads
        iomanager.run_on(
            thread_regex::all_io,
            [this, iodev](io_thread_addr_t taddr) {
                _remove_from_thread(iodev, iomanager.this_reactor()->addr_to_thread(taddr));
            },
            true /* wait_for_completion */);

        m_iodev_map.erase(iodev->dev);
    } else {
        auto r = iomanager.this_reactor();
        if (r) {
            _remove_from_thread(iodev, std::get< io_thread_t >(iodev->owner_thread));
        } else {
            LOGDFATAL("IOManager does not support removing local iodevices through non-io threads yet. Send a "
                      "message to an io thread");
        }
    }
    iodev->added_to_listen = false;
}

void IOInterface::on_io_thread_start(const io_thread_t& thr) {
    init_iface_thread_ctx(thr);

    // Add all devices part of this interface to this thread
    std::shared_lock lk(m_mtx);
    for (auto& iodev : m_iodev_map) {
        _add_to_thread(iodev.second, thr);
    }
}

void IOInterface::on_io_thread_stopped(const io_thread_t& thr) {
    std::shared_lock lk(m_mtx);
    for (auto& iodev : m_iodev_map) {
        _remove_from_thread(iodev.second, thr);
    }

    clear_iface_thread_ctx(thr);
}

void IOInterface::_add_to_thread(const io_device_ptr& iodev, const io_thread_t& thr) {
    if (thr->reactor->is_iodev_addable(iodev, thr)) {
        thr->reactor->add_iodev_to_thread(iodev, thr);
        init_iodev_thread_ctx(iodev, thr);
        if (!iodev->is_global()) iodev->owner_thread = thr;
    }
}

void IOInterface::_remove_from_thread(const io_device_ptr& iodev, const io_thread_t& thr) {
    if (thr->reactor->is_iodev_addable(iodev, thr)) {
        clear_iodev_thread_ctx(iodev, thr);
        thr->reactor->remove_iodev_from_thread(iodev, thr);
    }
}

/*************************** GenericIOInterface ******************************/
io_device_ptr GenericIOInterface::make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                                 bool is_per_thread_dev, const ev_callback& cb) {
    auto iodev = std::make_shared< IODevice >();
    iodev->dev = dev;
    iodev->cb = cb;
    if (is_per_thread_dev) {
        iodev->owner_thread = iomanager.iothread_self();
    } else {
        iodev->owner_thread = thread_regex::all_io;
    }
    iodev->pri = pri;
    iodev->cookie = cookie;
    iodev->ev = events_interested;
    iodev->io_interface = this;

    add_io_device(iodev);
    return iodev;
}
} // namespace iomgr

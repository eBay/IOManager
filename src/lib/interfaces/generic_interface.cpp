//
// Created by Kadayam, Hari on 02/04/18.
//
#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/io_interface.hpp"

namespace iomgr {

io_device_ptr GenericIOInterface::make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                                 bool is_per_thread_dev, const ev_callback& cb) {
    auto iodev = std::make_shared< io_device_t >();
    iodev->dev = dev;
    iodev->cb = cb;
    iodev->is_global = !is_per_thread_dev;
    iodev->pri = pri;
    iodev->cookie = cookie;
    iodev->ev = events_interested;
    iodev->io_interface = this;

    iomanager.add_io_device(iodev);
    return iodev;
}

void GenericIOInterface::remove_io_device(const io_device_ptr& iodev) { iomanager.remove_io_device(iodev); }
void GenericIOInterface::on_io_thread_start(__attribute__((unused)) IOThreadContext* ctx) {}
void GenericIOInterface::on_io_thread_stopped(__attribute__((unused)) IOThreadContext* ctx) {
    // TODO: See if we need to remove_io_device from per thread
}
} // namespace iomgr

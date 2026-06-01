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
#pragma once

#include <unistd.h>
#include <cstdint>
#include <string>

#include <fcntl.h>

/// NOTE: These are defined here to prevent inclusion of liburing/compat.h which conflicts
/// with system header /usr/include/futex.h
#define LIBURING_COMPAT_H
#define BLOCK_URING_CMD_DISCARD _IO(0x12, 0)
///

#include "open_how_compat.hpp" // complete struct open_how before <liburing.h> (Ubuntu 22.04 / gcc 13)
#include <liburing.h>
#include <sys/eventfd.h>

#include <sisl/metrics/metrics.hpp>
#include <sisl/fds/buffer.hpp>
#include <sisl/async/io_uring_scheduler.hpp>

#include "interfaces/kernel_drive_interface.hpp"
#include "interfaces/drive_iocb.hpp"
#include "drive_interface.hpp" // exec::task return type
#include <iomgr/iomgr_types.hpp>

namespace iomgr {
class UringDriveInterfaceMetrics : public DriveInterfaceMetrics {
public:
    explicit UringDriveInterfaceMetrics(const char* inst_name = "UringDriveInterface") :
            DriveInterfaceMetrics("UringDriveInterface", inst_name) {
        REGISTER_COUNTER(retry_io_eagain_error, "Retry IOs count because of kernel eagain");
        REGISTER_COUNTER(total_io_callbacks, "Number of times poll returned io events");
        REGISTER_COUNTER(retry_on_partial_read, "number of times ios are retried on partial read");
        REGISTER_COUNTER(overflow_errors, "number of CQ overflow occurrences");
        REGISTER_COUNTER(num_of_drops, "number of dropped ios due to CQ overflow");
        register_me_to_farm();
    }

    ~UringDriveInterfaceMetrics() = default;
};

// Per-thread io_uring channel. The ring's completion delivery + SQE batching is owned by the
// sisl::async::io_uring_scheduler; this struct just holds the ring, the scheduler, the wakeup
// eventfd device, and the outstanding-op count used to drive tight-polling and shutdown drain.
class UringDriveInterface;
struct uring_drive_channel {
    ::io_uring m_ring{};
    sisl::async::io_uring_scheduler m_sched; // wraps &m_ring; non-movable
    io_device_ptr m_ring_ev_iodev;
    uint64_t m_outstanding{0};     // in-flight drive coroutines on this reactor
    int m_saved_poll_interval{-1}; // reactor poll interval captured while tight-polling outstanding IO

    uring_drive_channel(UringDriveInterface* iface);
    ~uring_drive_channel();
};

class UringDriveInterface : public KernelDriveInterface {
public:
    UringDriveInterface(const bool new_interface_supported);
    virtual ~UringDriveInterface() = default;
    drive_interface_type interface_type() const override { return drive_interface_type::uring; }
    std::string name() const override { return "uring_drive_interface"; }

    io_device_ptr open_dev(const std::string& devname, drive_type dev_type, int oflags) override;
    void close_dev(const io_device_ptr& iodev) override;

    exec::task< std::error_code > async_write(IODevice* iodev, const char* data, uint32_t size,
                                              uint64_t offset) override;
    exec::task< std::error_code > async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                               uint64_t offset) override;
    exec::task< std::error_code > async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) override;
    exec::task< std::error_code > async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                              uint64_t offset) override;
    exec::task< std::error_code > async_unmap(IODevice* iodev, uint32_t size, uint64_t offset) override;
    exec::task< std::error_code > async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) override;
    exec::task< std::error_code > queue_fsync(IODevice* iodev) override;

    void on_event_notification(IODevice* iodev, void* cookie, int event);

    // Reactor sentinel: flush queued SQEs + reap completions (resuming suspended coroutines).
    void poll_completions();

    static uring_drive_channel* this_channel() { return t_uring_ch; }

protected:
    DriveInterfaceMetrics& get_metrics() override { return m_metrics; }

private:
    void init_iface_reactor_context(IOReactor*) override;
    void clear_iface_reactor_context(IOReactor*) override;
    // Reactor stop: drive poll_once until every in-flight coroutine has completed and freed.
    void drain_outstanding_ios();

private:
    static thread_local uring_drive_channel* t_uring_ch;
    UringDriveInterfaceMetrics m_metrics;
    bool m_new_intfc;
};
} // namespace iomgr

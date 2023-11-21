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
#include <string>
#include <stack>
#include <queue>
#include <atomic>
#include <mutex>

#include <fcntl.h>
#include <liburing.h>
#include <sys/eventfd.h>


#include <sisl/metrics/metrics.hpp>
#include <sisl/fds/buffer.hpp>

#include "interfaces/kernel_drive_interface.hpp"
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

// Per thread structure which has all details for uring
class UringDriveInterface;
struct uring_drive_channel {
    struct io_uring m_ring;
    std::queue< drive_iocb* > m_iocb_waitq;
    io_device_ptr m_ring_ev_iodev;
    // prepared_ios are IOs sent to uring but not submitted yet
    uint32_t m_prepared_ios{0};
    // in_flight_ios are IOs submitted to uring, but not completed yet
    uint32_t m_in_flight_ios{0};

    uring_drive_channel(UringDriveInterface* iface);
    ~uring_drive_channel();
    drive_iocb* pop_waitq() {
        if (m_iocb_waitq.size() == 0) { return nullptr; }
        drive_iocb* iocb = m_iocb_waitq.front();
        m_iocb_waitq.pop();
        return iocb;
    }

    size_t waitq_size() const { return m_iocb_waitq.size(); }
    struct io_uring_sqe* get_sqe_or_enqueue(drive_iocb* iocb);
    void submit_ios();
    // It assumes SQ size is same as CQ size, so we check the counters to make sure CQ doesn't overflow.
    bool can_submit() const;
    void submit_if_needed(drive_iocb* iocb, struct io_uring_sqe*, bool part_of_batch);
    void drain_waitq();
};

class UringDriveInterface : public KernelDriveInterface {
public:
    static constexpr uint32_t per_thread_qdepth = 256;

    UringDriveInterface(const bool new_interface_supported, const io_interface_comp_cb_t& cb = nullptr);
    virtual ~UringDriveInterface() = default;
    drive_interface_type interface_type() const override { return drive_interface_type::uring; }
    std::string name() const override { return "uring_drive_interface"; }

    io_device_ptr open_dev(const std::string& devname, drive_type dev_type, int oflags) override;
    void close_dev(const io_device_ptr& iodev) override;
    folly::Future< std::error_code > async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset,
                                                 bool part_of_batch = false) override;
    folly::Future< std::error_code > async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                  uint64_t offset, bool part_of_batch = false) override;
    folly::Future< std::error_code > async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset,
                                                bool part_of_batch = false) override;
    folly::Future< std::error_code > async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                 uint64_t offset, bool part_of_batch = false) override;
    folly::Future< std::error_code > async_unmap(IODevice* iodev, uint32_t size, uint64_t offset,
                                                 bool part_of_batch = false) override;
    folly::Future< std::error_code > async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) override;
    folly::Future< std::error_code > queue_fsync(IODevice* iodev) override;

    std::error_code sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) override;
    std::error_code sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    std::error_code sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) override;
    std::error_code sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;

    void on_event_notification(IODevice* iodev, void* cookie, int event);
    void handle_completions();
    void submit_batch() override;
    DriveInterfaceMetrics& get_metrics() override { return m_metrics; }

private:
    void init_iface_reactor_context(IOReactor*) override;
    void clear_iface_reactor_context(IOReactor*) override;

    void complete_io(drive_iocb* iocb);

private:
    static thread_local uring_drive_channel* t_uring_ch;
    UringDriveInterfaceMetrics m_metrics;
    bool m_new_intfc;
};
} // namespace iomgr

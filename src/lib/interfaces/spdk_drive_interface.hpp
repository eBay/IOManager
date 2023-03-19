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

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include <sisl/fds/buffer.hpp>
#include <sisl/fds/vector_pool.hpp>
#include <sisl/metrics/metrics.hpp>
#include <sisl/utility/enum.hpp>

#include <spdk/bdev.h>

#include <iomgr/drive_interface.hpp>
#include <iomgr/iomgr_msg.hpp>
#include "iomgr_config.hpp"

struct spdk_io_channel;
struct spdk_thread;

namespace iomgr {
struct SpdkDriveDeviceContext : public IODeviceThreadContext {
    ~SpdkDriveDeviceContext() = default;
    spdk_io_channel* channel{nullptr};
};

class SpdkDriveInterfaceMetrics : public DriveInterfaceMetrics {
public:
    explicit SpdkDriveInterfaceMetrics(const char* inst_name = "SpdkDriveInterface") :
            DriveInterfaceMetrics("SpdkDriveInterface", inst_name) {
        REGISTER_COUNTER(num_async_io_non_spdk_thread, "Count of async ios issued from non-spdk threads");
        REGISTER_COUNTER(force_sync_io_non_spdk_thread,
                         "Count of async ios converted to sync ios because of non-spdk threads");
        REGISTER_COUNTER(queued_ios_for_memory_pressure, "Count of times drive queued ios because of lack of memory");

        register_me_to_farm();
    }

    ~SpdkDriveInterfaceMetrics() = default;
};

struct SpdkIocb;

// static constexpr uint32_t SPDK_BATCH_IO_NUM{2};

// static_assert(SPDK_BATCH_IO_NUM > 1);

class SpdkDriveInterface : public DriveInterface {
    friend struct SpdkIocb;

public:
    SpdkDriveInterface(const io_interface_comp_cb_t& cb = nullptr);
    drive_interface_type interface_type() const override { return drive_interface_type::spdk; }
    std::string name() const override { return "spdk_drive_interface"; }

    io_device_ptr open_dev(const std::string& devname, drive_type dev_type, int oflags) override;
    void close_dev(const io_device_ptr& iodev) override;

    size_t get_dev_size(IODevice* iodev) override;

    folly::Future< bool > async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset,
                                      bool part_of_batch = false) override;
    folly::Future< bool > async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                       bool part_of_batch = false) override;
    folly::Future< bool > async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset,
                                     bool part_of_batch = false) override;
    folly::Future< bool > async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      bool part_of_batch = false) override;
    folly::Future< bool > async_unmap(IODevice* iodev, uint32_t size, uint64_t offset,
                                      bool part_of_batch = false) override;
    folly::Future< bool > async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) override;

    void sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) override;
    void sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    void sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) override;
    void sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    void sync_write_zero(IODevice* iodev, uint64_t size, uint64_t offset);

    folly::Future< bool > queue_fsync(IODevice* iodev) override {
        LOGWARN("fsync on spdk drive interface is not supported");
        return folly::makeFuture< bool >(false);
    }
    void submit_batch() override;

    io_interface_comp_cb_t& get_completion_cb() { return m_comp_cb; }

    DriveInterfaceMetrics& get_metrics() override { return m_metrics; }
    drive_attributes get_attributes(const std::string& devname, const drive_type drive_type) override;

    bool is_spdk_interface() const override { return true; }

    static drive_type detect_drive_type(const std::string& devname);

private:
    drive_attributes get_attributes(const io_device_ptr& dev) const;
    io_device_ptr create_open_dev_internal(const std::string& devname, drive_type drive_type);
    void open_dev_internal(const io_device_ptr& iodev);
    void init_iface_reactor_context(IOReactor*) override;
    void clear_iface_reactor_context(IOReactor*) override {}

    void init_iodev_reactor_context(const io_device_ptr& iodev, IOReactor* reactor) override;
    void clear_iodev_reactor_context(const io_device_ptr& iodev, IOReactor* reactor) override;

    void submit_async_io(SpdkIocb* iocb, bool part_of_batch);
    void submit_sync_io(SpdkIocb* iocb);

private:
    std::mutex m_sync_cv_mutex;
    std::condition_variable m_sync_cv;
    SpdkDriveInterfaceMetrics m_metrics;
    std::mutex m_opened_dev_mtx;
    std::unordered_map< std::string, io_device_ptr > m_opened_device;
    std::atomic< size_t > m_outstanding_async_ios;
};

struct SpdkBatchIocb {
    SpdkBatchIocb() {
        batch_io = sisl::VectorPool< SpdkIocb* >::alloc();
        num_io_comp = 0;
    }

    ~SpdkBatchIocb() {
        batch_io->clear();
        sisl::VectorPool< SpdkIocb* >::free(batch_io);
        batch_io = nullptr;
    }

    uint32_t num_io_comp{0};
    std::vector< SpdkIocb* >* batch_io{nullptr};
};

struct SpdkIocb : public drive_iocb {
    spdk_bdev_io_wait_entry io_wait_entry;
    SpdkBatchIocb* batch_info_ptr{nullptr};
    bool owns_by_spdk{false};

    SpdkIocb(DriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset) :
            drive_iocb{iface, iodev, op_type, size, offset} {
        io_wait_entry.bdev = iodev->bdev();
        io_wait_entry.cb_arg = (void*)this;
    }

    std::string to_string() const {
        std::string str;
#ifndef NDEBUG
        str = fmt::format("id={} ", iocb_id);
#endif
        str += fmt::format("addr={}, op_type={}, size={}, offset={}, iovcnt={}, batch_sz={}, "
                           "resubmit_cnt={}, unique_id={}, elapsed_time_us(op_start_time)={}, with_spdk={}, ",
                           (void*)this, enum_name(op_type), size, offset, iovcnt,
                           batch_info_ptr ? batch_info_ptr->batch_io->size() : 0, resubmit_cnt, unique_id,
                           get_elapsed_time_us(op_start_time), owns_by_spdk);

        if (has_iovs()) {
            const auto* ivs{get_iovs()};
            for (decltype(iovcnt) i{0}; i < iovcnt; ++i) {
                str += fmt::format("iov[{}]=<base={},len={}>", i, ivs[i].iov_base, ivs[i].iov_len);
            }
        } else {
            str += fmt::format("buf={}", static_cast< void* >(get_data()));
        }
        return str;
    }
};
} // namespace iomgr

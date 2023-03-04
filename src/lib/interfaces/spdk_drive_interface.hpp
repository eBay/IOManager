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

#ifdef WITH_SPDK
#include <spdk/bdev.h>
#endif

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

struct spdk_msg_type {
    static constexpr int QUEUE_IO{100};
    static constexpr int ASYNC_IO_DONE{101};
    static constexpr int QUEUE_BATCH_IO{102};
    static constexpr int ASYNC_BATCH_IO_DONE{103};
};

class SpdkDriveInterfaceMetrics : public sisl::MetricsGroup {
public:
    explicit SpdkDriveInterfaceMetrics(const char* inst_name = "SpdkDriveInterface") :
            sisl::MetricsGroup("SpdkDriveInterface", inst_name) {
        REGISTER_COUNTER(num_async_io_non_spdk_thread, "Count of async ios issued from non-spdk threads");
        REGISTER_COUNTER(force_sync_io_non_spdk_thread,
                         "Count of async ios converted to sync ios because of non-spdk threads");
        REGISTER_COUNTER(queued_ios_for_memory_pressure, "Count of times drive queued ios because of lack of memory");
        REGISTER_COUNTER(completion_errors, "Spdk Drive Completion errors");
        REGISTER_COUNTER(resubmit_io_on_err, "number of times ios are resubmitted");

        REGISTER_COUNTER(outstanding_write_cnt, "outstanding write cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_read_cnt, "outstanding read cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_unmap_cnt, "outstanding unmap cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_write_zero_cnt, "outstanding write zero cnt", sisl::_publish_as::publish_as_gauge);

        register_me_to_farm();
    }

    ~SpdkDriveInterfaceMetrics() { deregister_me_from_farm(); }
};

struct SpdkIocb;

// static constexpr uint32_t SPDK_BATCH_IO_NUM{2};

// static_assert(SPDK_BATCH_IO_NUM > 1);

class IOWatchDog {
    typedef SpdkIocb* io_wd_ptr_t;

public:
    IOWatchDog();
    ~IOWatchDog();

    void add_io(const io_wd_ptr_t& iocb);
    void complete_io(const io_wd_ptr_t& iocb);

    void io_timer();

    bool is_on();

    IOWatchDog(const IOWatchDog&) = delete;
    IOWatchDog(IOWatchDog&&) noexcept = delete;
    IOWatchDog& operator=(const IOWatchDog&) = delete;
    IOWatchDog& operator=(IOWatchDog&&) noexcept = delete;

private:
    bool m_wd_on{false};
    iomgr::timer_handle_t m_timer_hdl;
    std::mutex m_mtx;
    std::map< uint64_t, SpdkIocb* > m_outstanding_ios;
    uint64_t m_wd_pass_cnt{0}; // total watchdog check passed count
    uint64_t m_unique_id{0};
};

class SpdkDriveInterface : public DriveInterface {
    friend struct SpdkIocb;

public:
    SpdkDriveInterface(const io_interface_comp_cb_t& cb = nullptr);
    drive_interface_type interface_type() const override { return drive_interface_type::spdk; }
    std::string name() const override { return "spdk_drive_interface"; }

    io_device_ptr open_dev(const std::string& devname, drive_type dev_type, int oflags) override;
    void close_dev(const io_device_ptr& iodev) override;

    size_t get_dev_size(IODevice* iodev) override;
    virtual void submit_batch();

    ssize_t sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    ssize_t sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    void async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                      bool part_of_batch = false) override;
    void async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                    bool part_of_batch = false) override;
    void async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) override;
    void fsync(IODevice* iodev, uint8_t* cookie) override {
        // LOGMSG_ASSERT(false, "fsync on spdk drive interface is not supported");
        if (m_comp_cb) m_comp_cb(0, cookie);
    }

    io_interface_comp_cb_t& get_completion_cb() { return m_comp_cb; }

    SpdkDriveInterfaceMetrics& get_metrics() { return m_metrics; }
    drive_attributes get_attributes(const std::string& devname, const drive_type drive_type) override;

    [[nodiscard]] bool is_spdk_interface() const override { return true; }

    static drive_type detect_drive_type(const std::string& devname);
    static constexpr std::chrono::microseconds max_sync_io_poll_freq_us{5};

    static void increment_outstanding_counter(const SpdkIocb* iocb);
    static void decrement_outstanding_counter(const SpdkIocb* iocb);
    static uint64_t increment_outstanding_asyncios(const SpdkIocb* iocb, uint64_t count = 1);
    static uint64_t decrement_outstanding_asyncios(const SpdkIocb* iocb, uint64_t count = 1);

private:
    drive_attributes get_attributes(const io_device_ptr& dev) const;
    io_device_ptr create_open_dev_internal(const std::string& devname, drive_type drive_type);
    void open_dev_internal(const io_device_ptr& iodev);
    void init_iface_thread_ctx(const io_thread_t& thr) override;
    void clear_iface_thread_ctx(const io_thread_t& thr) override {}

    void init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override;
    void clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override;

    bool try_submit_io(SpdkIocb* iocb, bool part_of_batch);
    void submit_async_io_to_tloop_thread(SpdkIocb* iocb, bool part_of_batch);
    void handle_msg(iomgr_msg* msg);
    ssize_t do_sync_io(SpdkIocb* iocb, const io_interface_comp_cb_t& comp_cb);
    void submit_sync_io_to_tloop_thread(SpdkIocb* iocb);
    void submit_sync_io_in_this_thread(SpdkIocb* iocb);

private:
    msg_module_id_t m_my_msg_modid;
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
    SpdkDriveInterface* iface;
    io_thread_t owner_thread{nullptr}; // Owner thread (nullptr if same owner as processor)
    io_interface_comp_cb_t comp_cb{nullptr};
    spdk_bdev_io_wait_entry io_wait_entry;
    SpdkBatchIocb* batch_info_ptr{nullptr};
    bool owns_by_spdk{false};
    // used by io watchdog
    uint64_t unique_id{0};

    SpdkIocb(SpdkDriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset,
             void* cookie) :
            drive_iocb{iodev, op_type, size, offset, cookie}, iface{iface} {
        io_wait_entry.bdev = iodev->bdev();
        io_wait_entry.cb_arg = (void*)this;
        comp_cb = reinterpret_cast< SpdkDriveInterface* >(iodev->io_interface)->m_comp_cb;
    }

    std::string to_string() const {
        std::string str;
#ifndef NDEBUG
        str = fmt::format("id={} ", iocb_id);
#endif
        str += fmt::format("spdk={}", owns_by_spdk);

        str += fmt::format("addr={}, op_type={}, size={}, offset={}, iovcnt={}, owner_thread={}, batch_sz={}, "
                           "resubmit_cnt={}, unique_id={}, elapsed_time_us(op_start_time)={}",
                           (void*)this, enum_name(op_type), size, offset, iovcnt, owner_thread,
                           batch_info_ptr ? batch_info_ptr->batch_io->size() : 0, resubmit_cnt, unique_id,
                           get_elapsed_time_us(op_start_time));

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

#if 0
struct SpdkIocb {
#ifndef NDEBUG
    static std::atomic< uint64_t > _iocb_id_counter;
#endif

    static constexpr int inlined_iov_count = 4;
    typedef std::array< iovec, inlined_iov_count > inline_iov_array;
    typedef std::unique_ptr< iovec[] > large_iov_array;

    SpdkIocb(SpdkDriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset,
             void* cookie) :
            iodev(iodev), iface(iface), op_type(op_type), size(size), offset(offset), user_cookie(cookie) {
        io_wait_entry.bdev = iodev->bdev();
        io_wait_entry.cb_arg = (void*)this;
        comp_cb = ((SpdkDriveInterface*)iodev->io_interface)->m_comp_cb;
#ifndef NDEBUG
        iocb_id = _iocb_id_counter.fetch_add(1, std::memory_order_relaxed);
#endif
        user_data.emplace< 0 >();
    }

    ~SpdkIocb() = default;

    void set_iovs(const iovec* iovs, const int count) {
        iovcnt = count;
        if (count > inlined_iov_count) { user_data = std::unique_ptr< iovec[] >(new iovec[count]); }
        std::memcpy(reinterpret_cast< void* >(get_iovs()), reinterpret_cast< const void* >(iovs),
                    count * sizeof(iovec));
    }

    void set_data(char* data) { user_data = data; }

    iovec* get_iovs() const {
        if (std::holds_alternative< inline_iov_array >(user_data)) {
            return const_cast< iovec* >(&(std::get< inline_iov_array >(user_data)[0]));
        } else if (std::holds_alternative< large_iov_array >(user_data)) {
            return std::get< large_iov_array >(user_data).get();
        } else {
            assert(0);
            return nullptr;
        }
    }

    char* get_data() const { return std::get< char* >(user_data); }
    bool has_iovs() const { return !std::holds_alternative< char* >(user_data); }

    std::string to_string() const {
        std::string str;
#ifndef NDEBUG
        str = fmt::format("id={} ", iocb_id);
#endif
        str += fmt::format(
            "addr={}, op_type={}, size={}, offset={}, iovcnt={}, owner_thread={}, batch_sz={}, resubmit_cnt={} ",
            (void*)this, enum_name(op_type), size, offset, iovcnt, owner_thread,
            batch_info_ptr ? batch_info_ptr->batch_io->size() : 0, resubmit_cnt);

        if (has_iovs()) {
            auto ivs = get_iovs();
            for (auto i = 0; i < iovcnt; ++i) {
                str += fmt::format("iov[{}]=<base={},len={}>", i, ivs[i].iov_base, ivs[i].iov_len);
            }
        } else {
            str += fmt::format("buf={}", (void*)get_data());
        }
        return str;
    }

    IODevice* iodev;
    SpdkDriveInterface* iface;
    DriveOpType op_type;
    uint64_t size;
    uint64_t offset;
    void* user_cookie = nullptr;
    int iovcnt = 0;
    std::optional< int > result;
    io_thread_t owner_thread = nullptr; // Owner thread (nullptr if same owner as processor)
    io_interface_comp_cb_t comp_cb = nullptr;
    spdk_bdev_io_wait_entry io_wait_entry;
    SpdkBatchIocb* batch_info_ptr = nullptr;
    uint32_t resubmit_cnt = 0;
#ifndef NDEBUG
    uint64_t iocb_id;
    bool owns_by_spdk{false};
#endif

private:
    // Inline or additional memory
    std::variant< inline_iov_array, large_iov_array, char* > user_data;
};
#endif
} // namespace iomgr

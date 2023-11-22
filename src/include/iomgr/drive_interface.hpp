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
#ifndef IOMGR_DRIVE_INTERFACE_HPP
#define IOMGR_DRIVE_INTERFACE_HPP

#include <fcntl.h>
#include <cstdint>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <mutex>
#include <system_error>

#include <nlohmann/json.hpp>
#define __kernel_timespec linux_timespec
#include <folly/futures/Future.h>
#undef __kernel_timespec
#include <iomgr/io_interface.hpp>
#include <iomgr/iomgr_types.hpp>
#include <iomgr/fiber_lib.hpp>

namespace iomgr {
ENUM(drive_interface_type, uint8_t, aio, spdk, uring)
ENUM(DriveOpType, uint8_t, WRITE, READ, UNMAP, WRITE_ZERO, FSYNC)

struct drive_attributes {
    uint32_t phys_page_size{4096};        // Physical page size of flash ssd/nvme. This is optimal size to do IO
    uint32_t align_size{0};               // size alignment supported by drives/kernel
    uint32_t atomic_phys_page_size{4096}; // atomic page size of the drive
    uint32_t num_streams{1};              // Total number of independent streams supported on Drive

    bool is_valid() const { return (align_size != 0); }
    bool operator==(const drive_attributes& other) const {
        return ((phys_page_size == other.phys_page_size) && (align_size == other.align_size) &&
                (atomic_phys_page_size == other.atomic_phys_page_size) && (num_streams == other.num_streams));
    }
    bool operator!=(const drive_attributes& other) const { return !(*this == other); }

    nlohmann::json to_json() const {
        nlohmann::json json;
        json["phys_page_size"] = phys_page_size;
        json["align_size"] = align_size;
        json["atomic_phys_page_size"] = atomic_phys_page_size;
        json["num_streams"] = num_streams;
        return json;
    }
};

class DriveInterfaceMetrics : public sisl::MetricsGroup {
public:
    explicit DriveInterfaceMetrics(const char* group_name, const char* inst_name) :
            sisl::MetricsGroup(group_name, inst_name) {
        REGISTER_COUNTER(completion_errors, "IO Completion errors");
        REGISTER_COUNTER(write_io_submission_errors, "write submission errors", "io_submission_errors",
                         {"io_direction", "write"});
        REGISTER_COUNTER(read_io_submission_errors, "read submission errors", "io_submission_errors",
                         {"io_direction", "read"});
        REGISTER_COUNTER(resubmit_io_on_err, "number of times ios are resubmitted");

        REGISTER_COUNTER(outstanding_write_cnt, "outstanding write cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_read_cnt, "outstanding read cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_unmap_cnt, "outstanding unmap cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_fsync_cnt, "outstanding fsync cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_write_zero_cnt, "outstanding write zero cnt", sisl::_publish_as::publish_as_gauge);
    }

    virtual ~DriveInterfaceMetrics() { deregister_me_from_farm(); }
};

class DriveInterface;
struct drive_iocb {
#ifndef NDEBUG
    static std::atomic< uint64_t > _iocb_id_counter;
#endif
    static constexpr int inlined_iov_count = 4;
    typedef std::array< iovec, inlined_iov_count > inline_iov_array;
    typedef std::unique_ptr< iovec[] > large_iov_array;

    IODevice* iodev;
    DriveInterface* iface;
    DriveOpType op_type;
    uint64_t size;
    uint64_t offset;
    uint64_t unique_id{0}; // used by io watchdog
    int iovcnt = 0;
    int64_t result{-1};
    std::variant< io_interface_comp_cb_t, folly::Promise< std::error_code >,
                  FiberManagerLib::Promise< std::error_code > >
        completion{nullptr};
    uint32_t resubmit_cnt{0};
    uint32_t part_read_resubmit_cnt{0}; // only valid for uring interface
    IOReactor* initiating_reactor;
#ifndef NDEBUG
    uint64_t iocb_id;
#endif
    Clock::time_point op_start_time;
    Clock::time_point op_submit_time;

private:
    // Inline or additional memory
    std::variant< inline_iov_array, large_iov_array, char* > user_data;

public:
    drive_iocb(DriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset);
    virtual ~drive_iocb() = default;

    void set_iovs(const iovec* iovs, const int count);
    void set_data(char* data);

    iovec* get_iovs() const;
    void update_iovs_on_partial_result();

    char* get_data() const { return std::get< char* >(user_data); }
    bool has_iovs() const { return !std::holds_alternative< char* >(user_data); }

    io_interface_comp_cb_t& cb_comp_promise() { return std::get< io_interface_comp_cb_t >(completion); }
    folly::Promise< std::error_code >& folly_comp_promise() {
        return std::get< folly::Promise< std::error_code > >(completion);
    }
    FiberManagerLib::Promise< std::error_code >& fiber_comp_promise() {
        return std::get< FiberManagerLib::Promise< std::error_code > >(completion);
    }

    std::string to_string() const;
};

class IOWatchDog;

class DriveInterface : public IOInterface {
public:
    DriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb) {}
    virtual drive_interface_type interface_type() const = 0;
    virtual void close_dev(const io_device_ptr& iodev) = 0;

    virtual folly::Future< std::error_code > async_write(IODevice* iodev, const char* data, uint32_t size,
                                                         uint64_t offset, bool part_of_batch = false) = 0;
    virtual folly::Future< std::error_code > async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                          uint64_t offset, bool part_of_batch = false) = 0;
    virtual folly::Future< std::error_code > async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset,
                                                        bool part_of_batch = false) = 0;
    virtual folly::Future< std::error_code > async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                         uint64_t offset, bool part_of_batch = false) = 0;
    virtual folly::Future< std::error_code > async_unmap(IODevice* iodev, uint32_t size, uint64_t offset,
                                                         bool part_of_batch = false) = 0;
    virtual folly::Future< std::error_code > async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) = 0;
    virtual folly::Future< std::error_code > queue_fsync(IODevice* iodev) = 0;
    virtual void submit_batch() = 0;

    virtual std::error_code sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) = 0;
    virtual std::error_code sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                        uint64_t offset) = 0;
    virtual std::error_code sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) = 0;
    virtual std::error_code sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                       uint64_t offset) = 0;
    virtual std::error_code sync_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) = 0;

    virtual void attach_completion_cb(const io_interface_comp_cb_t& cb) { m_comp_cb = cb; }
    virtual DriveInterfaceMetrics& get_metrics() = 0;

    static drive_attributes get_attributes(const std::string& dev_name);
    static drive_type get_drive_type(const std::string& dev_name);
    static void emulate_drive_type(const std::string& dev_name, const drive_type dtype);
    static void emulate_drive_attributes(const std::string& dev_name, const drive_attributes& attr);
    static io_device_ptr open_dev(const std::string& dev_name, int oflags);
    static std::shared_ptr< DriveInterface > get_iface_for_drive(const std::string& dev_name, const drive_type dtype);
    static size_t get_size(IODevice* iodev);
    static void increment_outstanding_counter(drive_iocb* iocb);
    static void decrement_outstanding_counter(drive_iocb* iocb);

#ifdef _PRERELEASE
    static bool inject_delay_if_needed(drive_iocb* iocb, std::function< void(drive_iocb*) > delayed_cb);
#endif

protected:
    virtual size_t get_dev_size(IODevice* iodev) = 0;
    virtual drive_attributes get_attributes(const std::string& devname, const drive_type drive_type) = 0;
    virtual io_device_ptr open_dev(const std::string& dev_name, drive_type dev_type, int oflags) = 0;

    io_interface_comp_cb_t m_comp_cb;

private:
    static drive_type detect_drive_type(const std::string& dev_name);

private:
    static std::unordered_map< std::string, drive_type > s_dev_type;
    static std::mutex s_dev_type_lookup_mtx;
    static std::unordered_map< std::string, drive_attributes > s_dev_attrs;
    static std::mutex s_dev_attrs_lookup_mtx;
};
} // namespace iomgr
#endif // IOMGR_DEFAULT_INTERFACE_HPP

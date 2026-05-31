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

#include <fcntl.h>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <mutex>
#include <system_error>

#include <nlohmann/json.hpp>
#include <exec/task.hpp>
#include <iomgr/io_interface.hpp>
#include <iomgr/iomgr_types.hpp>

namespace iomgr {

ENUM(drive_interface_type, uint8_t, uring)

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

class DriveInterfaceMetrics; // defined in drive_iocb.hpp (internal)

class DriveInterface : public IOInterface {
public:
    DriveInterface() = default;
    virtual drive_interface_type interface_type() const = 0;
    virtual void close_dev(const io_device_ptr& iodev) = 0;

    virtual exec::task< std::error_code > async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset,
                                                      bool part_of_batch = false) = 0;
    virtual exec::task< std::error_code > async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                       uint64_t offset, bool part_of_batch = false) = 0;
    virtual exec::task< std::error_code > async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset,
                                                     bool part_of_batch = false) = 0;
    virtual exec::task< std::error_code > async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                      uint64_t offset, bool part_of_batch = false) = 0;
    virtual exec::task< std::error_code > async_unmap(IODevice* iodev, uint32_t size, uint64_t offset,
                                                      bool part_of_batch = false) = 0;
    virtual exec::task< std::error_code > async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) = 0;
    virtual exec::task< std::error_code > queue_fsync(IODevice* iodev) = 0;
    virtual void submit_batch() = 0;
    virtual class DriveInterfaceMetrics& get_metrics() = 0;

    static drive_attributes get_attributes(const std::string& dev_name);
    static drive_type get_drive_type(const std::string& dev_name);
    static void emulate_drive_type(const std::string& dev_name, const drive_type dtype);
    static void emulate_drive_attributes(const std::string& dev_name, const drive_attributes& attr);
    static io_device_ptr open_dev(const std::string& dev_name, int oflags);
    static std::shared_ptr< DriveInterface > get_iface_for_drive(const std::string& dev_name, const drive_type dtype);
    static size_t get_size(IODevice* iodev);

protected:
    virtual size_t get_dev_size(IODevice* iodev) = 0;
    virtual drive_attributes get_attributes(const std::string& devname, const drive_type drive_type) = 0;
    virtual io_device_ptr open_dev(const std::string& dev_name, drive_type dev_type, int oflags) = 0;

private:
    static drive_type detect_drive_type(const std::string& dev_name);

private:
    static std::unordered_map< std::string, drive_type > s_dev_type;
    static std::mutex s_dev_type_lookup_mtx;
    static std::unordered_map< std::string, drive_attributes > s_dev_attrs;
    static std::mutex s_dev_attrs_lookup_mtx;
};
} // namespace iomgr

//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_DRIVE_INTERFACE_HPP
#define IOMGR_DRIVE_INTERFACE_HPP

#include <fcntl.h>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <string>

#include "io_interface.hpp"
#include "iomgr_types.hpp"

namespace iomgr {
enum class drive_interface_type { aio, spdk, uioring };

struct drive_attributes {
    uint32_t phys_page_size = 4096;        // Physical page size of flash ssd/nvme. This is optimal size to do IO
    uint32_t align_size = 0;               // size alignment supported by drives/kernel
    uint32_t atomic_phys_page_size = 4096; // atomic page size of the drive

    bool is_valid() const { return (align_size != 0); }
    bool operator==(const drive_attributes& other) const {
        return ((phys_page_size == other.phys_page_size) && (align_size == other.align_size) &&
                (atomic_phys_page_size == other.atomic_phys_page_size));
    }
    bool operator!=(const drive_attributes& other) const { return !(*this == other); }

    nlohmann::json to_json() const {
        nlohmann::json json;
        json["phys_page_size"] = phys_page_size;
        json["align_size"] = align_size;
        json["atomic_phys_page_size"] = atomic_phys_page_size;
        return json;
    }
};

class DriveInterface : public IOInterface {
public:
    virtual drive_interface_type interface_type() const = 0;

    virtual void attach_completion_cb(const io_interface_comp_cb_t& cb) = 0;
    virtual io_device_ptr open_dev(const std::string& devname, iomgr_drive_type dev_type, int oflags) = 0;
    virtual void close_dev(const io_device_ptr& iodev) = 0;

    virtual ssize_t sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) = 0;
    virtual void async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                             bool part_of_batch = false) = 0;
    virtual void async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                              uint8_t* cookie, bool part_of_batch = false) = 0;
    virtual void async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                            bool part_of_batch = false) = 0;
    virtual void async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                             uint8_t* cookie, bool part_of_batch = false) = 0;
    virtual void async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                             bool part_of_batch = false) = 0;
    virtual void write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) = 0;
    virtual size_t get_size(IODevice* iodev) = 0;
    virtual void submit_batch() = 0;
    virtual drive_attributes get_attributes(const std::string& devname, const iomgr_drive_type drive_type) = 0;

    virtual iomgr_drive_type get_drive_type(const std::string& devname) const {
        if (std::filesystem::is_regular_file(std::filesystem::status(devname))) {
            return iomgr_drive_type::file;
        } else if (std::filesystem::is_block_file(std::filesystem::status(devname))) {
            return iomgr_drive_type::block;
        } else {
            return iomgr_drive_type::unknown;
        }
    }
};
} // namespace iomgr
#endif // IOMGR_DEFAULT_INTERFACE_HPP

//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_DRIVE_INTERFACE_HPP
#define IOMGR_DRIVE_INTERFACE_HPP

#include <fcntl.h>
#include "io_interface.hpp"

namespace iomgr {
enum class drive_interface_type { aio, spdk, uioring };

class DriveInterface : public IOInterface {
public:
    virtual drive_interface_type interface_type() const = 0;

    virtual void attach_completion_cb(const io_interface_comp_cb_t& cb) = 0;
    virtual void attach_end_of_batch_cb(const io_interface_end_of_batch_cb_t& cb) = 0;
    virtual void detach_end_of_batch_cb() = 0;
    virtual io_device_ptr open_dev(const std::string& devname, int oflags) = 0;
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
    virtual void async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie, bool part_of_batch = false);
    virtual size_t get_size(IODevice* iodev) = 0;
    virtual void submit_batch() = 0;
};
} // namespace iomgr
#endif // IOMGR_DEFAULT_INTERFACE_HPP

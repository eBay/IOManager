//
// Created by Kadayam, Hari on 2021-10-04.
//
#pragma once

#include <string>

#include "drive_interface.hpp"
#include "iomgr_types.hpp"

namespace iomgr {
static constexpr uint32_t max_buf_size = 1 * 1024 * 1024ul;             // 1 MB
static constexpr uint32_t max_zero_write_size = max_buf_size * IOV_MAX; // 1 GB

class KernelDriveInterface : public DriveInterface {
public:
    KernelDriveInterface(const io_interface_comp_cb_t& cb) : DriveInterface(cb) {}
    virtual ssize_t sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) override;
    virtual ssize_t sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    virtual ssize_t sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) override;
    virtual ssize_t sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    virtual void write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) override;

protected:
    virtual void init_write_zero_buf(const std::string& devname, const drive_type dev_type);
    virtual size_t get_dev_size(IODevice* iodev) override;
    virtual drive_attributes get_attributes(const std::string& devname, const drive_type drive_type) override;

private:
    void write_zero_ioctl(const IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie);
    void write_zero_writev(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie);

private:
    std::unique_ptr< uint8_t, std::function< void(uint8_t* const) > > m_zero_buf{};
    uint64_t m_max_write_zeros{std::numeric_limits< uint64_t >::max()};
};
} // namespace iomgr

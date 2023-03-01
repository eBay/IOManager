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

#include <string>

#include <iomgr/drive_interface.hpp>
#include <iomgr/iomgr_types.hpp>
#include "reactor.hpp"

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

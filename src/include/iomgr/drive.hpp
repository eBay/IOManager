/************************************************************************
 * Copyright 2026 eBay Inc.
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

// Public drive I/O API. A drive is opened via a factory into an opaque handle; I/O is issued through
// free functions that return an awaitable io_op. None of the backend types (DriveInterface, IODevice,
// the io_uring scheduler, exec::task) are exposed here.

#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <system_error>

#include <sys/uio.h> // iovec

#include <nlohmann/json.hpp>

#include <iomgr/iomgr_types.hpp> // drive_type
#include <iomgr/io_op.hpp>

namespace iomgr {

struct drive_attributes {
    uint32_t phys_page_size{4096};        // Physical page size of flash ssd/nvme; optimal IO size
    uint32_t align_size{0};               // size alignment supported by drives/kernel
    uint32_t atomic_phys_page_size{4096}; // atomic page size of the drive
    uint32_t num_streams{1};              // total number of independent streams supported on the drive

    bool is_valid() const { return (align_size != 0); }
    bool operator==(const drive_attributes& o) const {
        return phys_page_size == o.phys_page_size && align_size == o.align_size &&
            atomic_phys_page_size == o.atomic_phys_page_size && num_streams == o.num_streams;
    }
    bool operator!=(const drive_attributes& o) const { return !(*this == o); }

    nlohmann::json to_json() const {
        nlohmann::json j;
        j["phys_page_size"] = phys_page_size;
        j["align_size"] = align_size;
        j["atomic_phys_page_size"] = atomic_phys_page_size;
        j["num_streams"] = num_streams;
        return j;
    }
};

// Opaque handle to an open drive (PIMPL over the io_uring backend + device).
class drive;
using drive_handle = std::shared_ptr< drive >;

// ----- open / query --------------------------------------------------------------------------------
// open_drive returns an owning handle; the drive is closed when the last handle is released (RAII).
// Operational failures (device missing, permission) return std::unexpected
std::expected< drive_handle, std::error_condition > open_drive(const std::string& dev_name, int oflags) noexcept;
drive_attributes attributes_of(const std::string& dev_name);
drive_type type_of(const std::string& dev_name);
size_t size_of(const drive_handle& d);

// ----- async I/O (awaitable io_op; the scheduler batches submissions automatically) -----------------
io_op async_write(const drive_handle& d, const char* data, uint32_t size, uint64_t offset);
io_op async_writev(const drive_handle& d, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset);
io_op async_read(const drive_handle& d, char* data, uint32_t size, uint64_t offset);
io_op async_readv(const drive_handle& d, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset);
io_op async_unmap(const drive_handle& d, uint32_t size, uint64_t offset);
io_op async_write_zero(const drive_handle& d, uint64_t size, uint64_t offset);
io_op queue_fsync(const drive_handle& d);

} // namespace iomgr

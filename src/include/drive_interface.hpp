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
#include <nlohmann/json.hpp>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <mutex>

#include "io_interface.hpp"
#include "iomgr_types.hpp"

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

struct drive_iocb {
#ifndef NDEBUG
    static std::atomic< uint64_t > _iocb_id_counter;
#endif
    static constexpr int inlined_iov_count = 4;
    typedef std::array< iovec, inlined_iov_count > inline_iov_array;
    typedef std::unique_ptr< iovec[] > large_iov_array;

    drive_iocb(IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset, void* cookie) :
            iodev(iodev), op_type(op_type), size(size), offset(offset), user_cookie(cookie) {
#ifndef NDEBUG
        iocb_id = _iocb_id_counter.fetch_add(1, std::memory_order_relaxed);
#endif
        user_data.emplace< 0 >();
        op_start_time = Clock::now();
    }

    virtual ~drive_iocb() = default;

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

    void update_iovs_on_partial_result() {
        DEBUG_ASSERT_EQ(op_type, DriveOpType::READ, "Only expecting READ op for be returned with partial results.");

        const auto iovs = get_iovs();
        uint32_t num_iovs_unset{1};
        uint64_t remaining_iov_len{0}, size_unset{size - result};
        uint64_t iov_len_part{0};
        // count remaining size from last iov
        for (auto i{iovcnt - 1}; i >= 0; --i, ++num_iovs_unset) {
            remaining_iov_len += iovs[i].iov_len;
            if (remaining_iov_len == size_unset) {
                break;
            } else if (remaining_iov_len > size_unset) {
                // we've had some left over within a single iov, calculate the size needs to be read;
                iov_len_part = (iovs[i].iov_len - (remaining_iov_len - size_unset));
                break;
            }

            // keep visiting next iov;
        }

        DEBUG_ASSERT_GE(remaining_iov_len, size_unset);

        std::vector< iovec > iovs_unset;
        iovs_unset.reserve(num_iovs_unset);

        // if a single iov entry is partial read, we need remember the size and resume read from there;
        uint32_t start_idx{0};
        if (iov_len_part > 0) {
            iovs_unset[start_idx].iov_len = iov_len_part;
            iovs_unset[start_idx].iov_base = reinterpret_cast< uint8_t* >(iovs[iovcnt - num_iovs_unset].iov_base) +
                (iovs[iovcnt - num_iovs_unset].iov_len - iov_len_part);
            ++start_idx;
        }

        // copy the unfilled iovs to iovs_unset;
        for (auto i{start_idx}; i < num_iovs_unset; ++i) {
            iovs_unset[i].iov_len = iovs[iovcnt - num_iovs_unset + i].iov_len;
            iovs_unset[i].iov_base = iovs[iovcnt - num_iovs_unset + i].iov_base;
        }

        set_iovs(iovs_unset.data(), num_iovs_unset);
        size = size_unset;
        offset += result;
    }

    std::string to_string() const {
        std::string str;
#ifndef NDEBUG
        str = fmt::format("id={} ", iocb_id);
#endif
        str += fmt::format("addr={}, op_type={}, size={}, offset={}, iovcnt={} ", (void*)this, enum_name(op_type), size,
                           offset, iovcnt);

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
    DriveOpType op_type;
    uint64_t size;
    uint64_t offset;
    void* user_cookie = nullptr;
    int iovcnt = 0;
    int64_t result{-1};
    bool sync_io_completed{false};
    uint32_t resubmit_cnt{0};
    uint32_t part_read_resubmit_cnt{0}; // only valid for uring interface
#ifndef NDEBUG
    uint64_t iocb_id;
#endif
    Clock::time_point op_start_time;
    Clock::time_point op_submit_time;

private:
    // Inline or additional memory
    std::variant< inline_iov_array, large_iov_array, char* > user_data;
};

class IOWatchDog;

class DriveInterface : public IOInterface {
public:
    DriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb) {}
    virtual drive_interface_type interface_type() const = 0;
    virtual void close_dev(const io_device_ptr& iodev) = 0;

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
    virtual void submit_batch() = 0;
    virtual ssize_t sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) = 0;
    virtual void write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) = 0;
    virtual void fsync(IODevice* iodev, uint8_t* cookie) = 0;

    virtual void attach_completion_cb(const io_interface_comp_cb_t& cb) { m_comp_cb = cb; }

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

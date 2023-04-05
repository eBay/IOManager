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

#include <iomgr/iomgr.hpp>
#include <iomgr/drive_interface.hpp>

namespace iomgr {

drive_iocb::drive_iocb(DriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset) :
        iodev(iodev), iface{iface}, op_type(op_type), size(size), offset(offset) {
#ifndef NDEBUG
    iocb_id = _iocb_id_counter.fetch_add(1, std::memory_order_relaxed);
#endif
    initiating_reactor = iomanager.this_reactor();
    user_data.emplace< 0 >();
    op_start_time = Clock::now();
}

void drive_iocb::set_iovs(const iovec* iovs, const int count) {
    iovcnt = count;
    if (count > inlined_iov_count) { user_data = std::unique_ptr< iovec[] >(new iovec[count]); }
    std::memcpy(reinterpret_cast< void* >(get_iovs()), reinterpret_cast< const void* >(iovs), count * sizeof(iovec));
}

void drive_iocb::set_data(char* data) { user_data = data; }

iovec* drive_iocb::get_iovs() const {
    if (std::holds_alternative< inline_iov_array >(user_data)) {
        return const_cast< iovec* >(&(std::get< inline_iov_array >(user_data)[0]));
    } else if (std::holds_alternative< large_iov_array >(user_data)) {
        return std::get< large_iov_array >(user_data).get();
    } else {
        assert(0);
        return nullptr;
    }
}

void drive_iocb::update_iovs_on_partial_result() {
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

std::string drive_iocb::to_string() const {
    std::string str;
#ifndef NDEBUG
    str = fmt::format("id={} ", iocb_id);
#endif
    str += fmt::format("addr={}, op_type={}, size={}, offset={}, iovcnt={}, unique_id={},", (void*)this,
                       enum_name(op_type), size, offset, iovcnt, unique_id);

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
} // namespace iomgr
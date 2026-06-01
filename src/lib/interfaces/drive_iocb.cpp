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
#include "drive_interface.hpp"
#include "interfaces/drive_iocb.hpp"

namespace iomgr {

drive_iocb::drive_iocb(DriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset) :
        iodev(iodev), iface{iface}, op_type(op_type), size(size), offset(offset) {
#ifndef NDEBUG
    iocb_id = _iocb_id_counter.fetch_add(1, std::memory_order_relaxed);
#endif
    initiating_reactor = iomanager.this_reactor();
    op_start_time = Clock::now();
}

std::string drive_iocb::to_string() const {
    std::string str;
#ifndef NDEBUG
    str = fmt::format("id={} ", iocb_id);
#endif
    str += fmt::format("addr={}, op_type={}, size={}, offset={}", (void*)this, enum_name(op_type), size, offset);
    return str;
}

const uint8_t* zero_buffer(size_t size) {
    static const std::vector< uint8_t > s_zeros(k_write_zero_chunk, 0);
    DEBUG_ASSERT_LE(size, s_zeros.size(), "zero_buffer request {} exceeds chunk size {}", size, s_zeros.size());
    return s_zeros.data();
}

void IODevice::close() {
    m_metrics.reset();
    ::close(fd());
}

void IODevice::enable_metrics(std::string group_name) {
    m_metrics = std::make_unique< IODeviceMetrics >(std::move(group_name));
}

void IODevice::observe_metrics(drive_iocb* iocb) {
    if (!m_metrics) { return; }
    auto dur = get_elapsed_time_us(iocb->op_start_time);
    switch (iocb->op_type) {
    case DriveOpType::WRITE:
        HISTOGRAM_OBSERVE(*m_metrics, write_lat, dur);
        HISTOGRAM_OBSERVE(*m_metrics, write_size, iocb->size);
        break;
    case DriveOpType::READ:
        HISTOGRAM_OBSERVE(*m_metrics, read_lat, dur);
        HISTOGRAM_OBSERVE(*m_metrics, read_size, iocb->size);
        break;
    case DriveOpType::FSYNC:
        HISTOGRAM_OBSERVE(*m_metrics, fsync_lat, dur);
        HISTOGRAM_OBSERVE(*m_metrics, fsync_size, iocb->size);
        break;
    default:
        break;
    }
}

} // namespace iomgr
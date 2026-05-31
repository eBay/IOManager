#pragma once

// Internal header — not part of the installed public API.
// Included only by drive interface implementations (.cpp files).

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

#include <sisl/metrics/metrics.hpp>
#include <sisl/utility/enum.hpp>

#include <iomgr/drive_interface.hpp>

namespace iomgr {
using Clock = std::chrono::steady_clock;

ENUM(DriveOpType, uint8_t, WRITE, READ, UNMAP, WRITE_ZERO, FSYNC)

// IODeviceMetrics is forward-declared in io_device.hpp; full definition here so any
// TU that includes drive_iocb.hpp can destroy unique_ptr<IODeviceMetrics>.
class IODeviceMetrics : public sisl::MetricsGroup {
public:
    explicit IODeviceMetrics(std::string devname) : sisl::MetricsGroup("IODeviceMetrics", devname) {
        REGISTER_HISTOGRAM(read_size, "Read IO Size", "io_size", {"op", "read"}, HistogramBucketsType(OpSizeBuckets));
        REGISTER_HISTOGRAM(write_size, "Write IO size", "io_size", {"op", "write"},
                           HistogramBucketsType(OpSizeBuckets));
        REGISTER_HISTOGRAM(fsync_size, "Fsync IO size", "io_size", {"op", "fsync"},
                           HistogramBucketsType(OpSizeBuckets));
        REGISTER_HISTOGRAM(read_lat, "Read IO Lat", "io_lat_us", {"op", "read"}, HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(write_lat, "Write IO Lat", "io_lat_us", {"op", "write"},
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(fsync_lat, "Fsync IO Lat", "io_lat_us", {"op", "fsync"},
                           HistogramBucketsType(OpLatecyBuckets));
        register_me_to_farm();
    }
    ~IODeviceMetrics() { deregister_me_from_farm(); }
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

    ~DriveInterfaceMetrics() { deregister_me_from_farm(); }
};

// Lightweight per-op record kept only for metrics/accounting now that the io_uring scheduler owns the
// completion path (buffers, iovecs and partial-result bookkeeping live in the drive coroutine frames).
class DriveInterface;
struct drive_iocb {
#ifndef NDEBUG
    static std::atomic< uint64_t > _iocb_id_counter;
    uint64_t iocb_id;
#endif
    IODevice* iodev;
    DriveInterface* iface;
    DriveOpType op_type;
    uint64_t size;
    uint64_t offset;
    uint64_t unique_id{0};         // assigned by IOWatchDog for outstanding-IO tracking
    IOReactor* initiating_reactor; // used by _PRERELEASE inject_delay_if_needed
    Clock::time_point op_start_time;

    drive_iocb(DriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset);
    virtual ~drive_iocb() = default;

    std::string to_string() const;
};

// Free functions used by drive backends — not part of the public API.
// Defined in drive_interface.cpp alongside their backing static data.
void increment_outstanding_counter(drive_iocb* iocb);
void decrement_outstanding_counter(drive_iocb* iocb);

// Largest single write_zero chunk and a shared, process-wide, read-only buffer of that many zero
// bytes. The kernel only reads it for the write, so one allocation safely serves every reactor
// thread (vs. a per-thread 1 MB buffer, which scaled resident memory with thread count).
constexpr uint64_t k_write_zero_chunk = 1ull * 1024 * 1024;
const uint8_t* zero_buffer(size_t size);
#ifdef _PRERELEASE
bool inject_delay_if_needed(drive_iocb* iocb, std::function< void(drive_iocb*) > delayed_cb);
#endif

} // namespace iomgr

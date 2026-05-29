#pragma once

// Internal header — not part of the installed public API.
// Included only by drive interface implementations (.cpp files).

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <variant>
#include <memory>

#include <sisl/metrics/metrics.hpp>
#include <sisl/async/cqe_state.hpp>
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

class DriveInterface;
struct drive_iocb {
#ifndef NDEBUG
    static std::atomic< uint64_t > _iocb_id_counter;
#endif
    static constexpr int inlined_iov_count = 4;
    typedef std::array< iovec, inlined_iov_count > inline_iov_array;
    typedef std::unique_ptr< iovec[] > large_iov_array;

    IODevice* iodev;
    DriveInterface* iface;
    DriveOpType op_type;
    uint64_t size;
    uint64_t offset;
    uint64_t unique_id{0};
    int iovcnt = 0;
    int64_t result{-1};
    sisl::async::cqe_awaitable completion{};
    uint32_t resubmit_cnt{0};
    uint32_t part_read_resubmit_cnt{0};
    IOReactor* initiating_reactor;
#ifndef NDEBUG
    uint64_t iocb_id;
#endif
    Clock::time_point op_start_time;
    Clock::time_point op_submit_time;

private:
    std::variant< inline_iov_array, large_iov_array, char* > user_data;

public:
    drive_iocb(DriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset);
    virtual ~drive_iocb() = default;

    void set_iovs(const iovec* iovs, const int count);
    void set_data(char* data);

    iovec* get_iovs() const;
    void update_iovs_on_partial_result();

    char* get_data() const { return std::get< char* >(user_data); }
    bool has_iovs() const { return !std::holds_alternative< char* >(user_data); }

    std::string to_string() const;
};

// Free functions used by drive backends — not part of the public API.
// Defined in drive_interface.cpp alongside their backing static data.
void increment_outstanding_counter(drive_iocb* iocb);
void decrement_outstanding_counter(drive_iocb* iocb);
#ifdef _PRERELEASE
bool inject_delay_if_needed(drive_iocb* iocb, std::function< void(drive_iocb*) > delayed_cb);
#endif

} // namespace iomgr

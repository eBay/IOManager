#pragma once

#include <string>
#include <mutex>

#include <sisl/fds/sparse_vector.hpp>
#include <iomgr/iomgr.hpp>
#include <iomgr/iomgr_timer.hpp>

struct spdk_nvmf_qpair;
struct spdk_bdev;

namespace iomgr {
class IOInterface;
class DriveInterface;

inline backing_dev_t null_backing_dev() { return backing_dev_t{std::in_place_type< spdk_bdev_desc* >, nullptr}; }

struct IODeviceThreadContext {
    virtual ~IODeviceThreadContext() = default;
};

class IODeviceMetrics : public sisl::MetricsGroup {
public:
    IODeviceMetrics(std::string devname) : sisl::MetricsGroup("IODeviceMetrics", devname) {

        REGISTER_HISTOGRAM(read_size, "Read IO Size", "io_size", {"op", "read"}, HistogramBucketsType(OpSizeBuckets));
        REGISTER_HISTOGRAM(write_size, "Write IO size", "io_size", {"op", "write"},
                           HistogramBucketsType(OpSizeBuckets));
        REGISTER_HISTOGRAM(fsync_size, "Fsync IO size", "io_size", {"op", "fsync"},
                           HistogramBucketsType(OpSizeBuckets));
        // FixMe: The OpLatecyBuckets might not friendly for HDD
        REGISTER_HISTOGRAM(read_lat, "Read IO Lat", "io_lat_us", {"op", "read"}, HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(write_lat, "Write IO Lat", "io_lat_us", {"op", "write"},
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(fsync_lat, "Fsync IO Lat", "io_lat_us", {"op", "fsync"},
                           HistogramBucketsType(OpLatecyBuckets));
        register_me_to_farm();
    }
    IODeviceMetrics(const IODeviceMetrics&) = delete;
    IODeviceMetrics(IODeviceMetrics&&) noexcept = delete;
    IODeviceMetrics& operator=(const IODeviceMetrics&) = delete;
    IODeviceMetrics& operator=(IODeviceMetrics&&) noexcept = delete;
    ~IODeviceMetrics() { deregister_me_from_farm(); }
};

class IODevice {
public:
    IODevice(const int pri, const thread_specifier scope);
    virtual ~IODevice() = default;

public:
    ev_callback cb{nullptr};
    std::string devname;
    std::string alias_name;
    backing_dev_t dev;
    int ev{0};
    io_fiber_t creator;
    void* cookie{nullptr};
    std::unique_ptr< timer_info > tinfo;
    IOInterface* io_interface{nullptr};
    std::mutex m_ctx_init_mtx; // Mutex to protect iodev thread ctx
    sisl::sparse_vector< std::unique_ptr< IODeviceThreadContext > > m_iodev_fiber_ctx;
    bool ready{false};
    sisl::atomic_counter< int32_t > thread_op_pending_count{0}; // Number of add/remove of iodev to thread pending
    drive_type dtype{drive_type::unknown};
    std::function< void(IODevice*) > post_add_remove_cb{nullptr};

#ifdef REFCOUNTED_OPEN_DEV
    sisl::atomic_counter< int > opened_count{0};
#endif

private:
    thread_specifier thread_scope{reactor_regex::all_io};
    int pri{1};
    std::unique_ptr< IODeviceMetrics > m_metrics;

public:
    int fd() const { return std::get< int >(dev); }
    spdk_bdev_desc* bdev_desc() const;
    spdk_bdev* bdev() const;
    bool is_spdk_dev() const {
        return (std::holds_alternative< spdk_bdev_desc* >(dev) || std::holds_alternative< spdk_nvmf_qpair* >(dev));
    }
    spdk_nvmf_qpair* nvmf_qp() const;

    bool is_global() const;
    bool is_my_thread_scope() const;
    io_fiber_t fiber_scope() const;
    reactor_regex global_scope() const;
    IOReactor* reactor_scope() const;

    inline int priority() const { return pri; }
    std::string dev_id() const;
    void clear();
    DriveInterface* drive_interface();

    void observe_metrics(drive_iocb* iocb) {
        if (!m_metrics.get()) { return; }
        auto dur = get_elapsed_time_us(iocb->op_start_time);
        switch (iocb->op_type) {
        case DriveOpType::WRITE:
            HISTOGRAM_OBSERVE(*m_metrics, write_lat, dur);
            HISTOGRAM_OBSERVE(*m_metrics, write_size, iocb->size);
            LOGINFO("write, size {}, lat {}", iocb->size, dur);
            break;
        case DriveOpType::READ:
            HISTOGRAM_OBSERVE(*m_metrics, read_lat, dur);
            HISTOGRAM_OBSERVE(*m_metrics, read_size, iocb->size);
            LOGINFO("read, size {}, lat {}", iocb->size, dur);
            break;
        case DriveOpType::FSYNC:
            HISTOGRAM_OBSERVE(*m_metrics, fsync_lat, dur);
            HISTOGRAM_OBSERVE(*m_metrics, fsync_size, iocb->size);
            LOGINFO("fsync, size {}, lat {}", iocb->size, dur);
            break;

        default:
            break;
        }
    }

    void decrement_pending(int32_t count = 1) {
        if ((post_add_remove_cb != nullptr) && thread_op_pending_count.decrement_testz(count)) {
            post_add_remove_cb(this);
        }
    }

    void increment_pending(int32_t count = 1) {
        if ((post_add_remove_cb != nullptr) && thread_op_pending_count.increment_test_eq(count, 0)) {
            post_add_remove_cb(this);
        }
    }

    void enable_metrics(std::string group_name) { m_metrics = std::make_unique< IODeviceMetrics >(group_name); }

    void close() {
        m_metrics.release();
        if (!is_spdk_dev()) { ::close(fd()); }
    }
};

} // namespace iomgr

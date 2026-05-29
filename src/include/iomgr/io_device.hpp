#pragma once

#include <string>
#include <mutex>

#include <iomgr/iomgr_types.hpp>
#include <sisl/utility/atomic_counter.hpp>

namespace iomgr {
class IOInterface;
class DriveInterface;
class IODeviceMetrics;        // defined in drive_iocb.hpp (internal)
struct IODeviceThreadContext; // extension point for per-reactor device state
struct timer_info;            // defined in iomgr_timer_impl.hpp (internal)

class IODevice {
public:
    IODevice(const int pri, const thread_specifier scope);
    virtual ~IODevice(); // defined in iomgr.cpp where timer_info and IODeviceMetrics are complete

public:
    ev_callback cb{nullptr};
    std::string devname;
    std::string alias_name;
    int dev{-1}; // file descriptor
    int ev{0};
    void* cookie{nullptr};
    std::unique_ptr< timer_info > tinfo;
    IOInterface* io_interface{nullptr};
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
    std::unique_ptr< IODeviceMetrics > m_metrics; // forward-declared above

public:
    int fd() const { return dev; }

    bool is_global() const;
    bool is_my_thread_scope() const;
    reactor_regex global_scope() const;
    IOReactor* reactor_scope() const;

    inline int priority() const { return pri; }
    std::string dev_id() const;
    void clear();
    DriveInterface* drive_interface();

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

    void enable_metrics(std::string group_name);   // defined in drive_iocb.cpp
    void observe_metrics(struct drive_iocb* iocb); // defined in drive_iocb.cpp

    void close(); // defined in drive_iocb.cpp where IODeviceMetrics is complete
};

} // namespace iomgr

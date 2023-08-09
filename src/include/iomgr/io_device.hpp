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

    void close() {
        if (!is_spdk_dev()) { ::close(fd()); }
    }
};

} // namespace iomgr
//
// Created by Kadayam, Hari on 2021-08-16.
//
#pragma once

#include <unistd.h>
#include <string>
#include <stack>
#include <queue>
#include <atomic>
#include <mutex>

#include <fcntl.h>
#include <liburing.h>
#include <sys/eventfd.h>

#include <sisl/metrics/metrics.hpp>
#include <sisl/fds/buffer.hpp>
#include <sisl/fds/obj_allocator.hpp>

#include "drive_interface.hpp"
#include "iomgr_types.hpp"

namespace iomgr {
static constexpr int max_batch_iocb_count = 4;
static constexpr int max_batch_iov_cnt = IOV_MAX;
static constexpr uint32_t max_buf_size = 1 * 1024 * 1024ul;             // 1 MB
static constexpr uint32_t max_zero_write_size = max_buf_size * IOV_MAX; // 1 GB

// inline iocb_info_t* to_iocb_info(user_io_info_t* p) { return container_of(p, iocb_info_t, user_io_info); }
struct iocb_batch_t {
    std::array< iocb_info_t*, max_batch_iocb_count > iocb_info;
    int n_iocbs = 0;

    iocb_batch_t() = default;

    void reset() { n_iocbs = 0; }

    std::string to_string() const {
        std::stringstream ss;
        ss << "Batch of " << n_iocbs << " : ";
        for (auto i = 0; i < n_iocbs; ++i) {
            auto i_info = iocb_info[i];
            ss << "{(" << i << ") -> " << i_info->to_string() << " } ";
        }
        return ss.str();
    }

    struct iocb** get_iocb_list() {
        return (struct iocb**)&iocb_info[0];
    }
};

struct IODevice;
class IOReactor;
struct aio_thread_context {
    struct io_event events[MAX_COMPLETIONS] = {{}};
    int ev_fd = 0;
    io_context_t ioctx = 0;
    std::stack< iocb_info_t* > iocb_free_list;
    std::queue< iocb_info_t* > iocb_retry_list;
    iocb_batch_t cur_iocb_batch;
    bool timer_set = false;
    uint64_t post_alloc_iocb = 0;
    uint64_t submitted_aio = 0;
    uint64_t max_submitted_aio;
    std::shared_ptr< IODevice > ev_io_dev = nullptr; // fd info after registering with IOManager
    poll_cb_idx_t poll_cb_idx;

    ~aio_thread_context() {
        if (ev_fd) { close(ev_fd); }
        io_destroy(ioctx);

        while (!iocb_retry_list.empty()) {
            auto info = iocb_retry_list.front();
            iocb_retry_list.pop();
            free_iocb((struct iocb*)info);
        }

        while (!iocb_free_list.empty()) {
            auto info = iocb_free_list.top();
            delete info;
            iocb_free_list.pop();
        }
    }

    void iocb_info_prealloc(uint32_t count) {
        for (auto i = 0u; i < count; ++i) {
            iocb_free_list.push(new iocb_info_t());
        }
        max_submitted_aio = count;
    }

    bool can_be_batched(int iovcnt) {
        return ((iovcnt <= max_batch_iov_cnt) && (cur_iocb_batch.n_iocbs < max_batch_iocb_count));
    }

    bool can_submit_aio() { return submitted_aio < max_submitted_aio ? true : false; }

    iocb_info_t* alloc_iocb(uint32_t iovcnt = 0) {
        iocb_info_t* info;
        if (!iocb_free_list.empty()) {
            info = iocb_free_list.top();
            iocb_free_list.pop();
        } else {
            info = new iocb_info_t();
            ++post_alloc_iocb;
        }
        if (iovcnt > max_batch_iov_cnt) {
            info->iov_ptr = new iovec[iovcnt];
        } else {
            info->iov_ptr = info->iovs;
        }
        return info;
    }

    void dec_submitted_aio();

    void inc_submitted_aio(int count);

    void push_retry_list(struct iocb* iocb) { iocb_retry_list.push(static_cast< iocb_info_t* >(iocb)); }

    struct iocb* pop_retry_list() {
        if (!iocb_retry_list.empty()) {
            auto info = iocb_retry_list.front();
            iocb_retry_list.pop();
            return (static_cast< iocb* >(info));
        }
        return nullptr;
    }

    void free_iocb(struct iocb* iocb) {
        auto info = static_cast< iocb_info_t* >(iocb);
        if (info->iov_ptr != info->iovs) { delete (info->iov_ptr); }
        info->iov_ptr = nullptr;
        if (post_alloc_iocb == 0) {
            iocb_free_list.push(info);
        } else {
            --post_alloc_iocb;
            delete info;
        }
    }

    void prep_iocb_for_resubmit(struct iocb* iocb) {
        auto info = static_cast< iocb_info_t* >(iocb);
        auto cookie = iocb->data;
        if (info->is_read) {
            if (info->user_data) {
                io_prep_pread(iocb, info->fd, info->user_data, info->size, info->offset);
            } else {
                io_prep_preadv(iocb, info->fd, info->iov_ptr, info->iovcnt, info->offset);
            }
        } else {
            if (info->user_data) {
                io_prep_pwrite(iocb, info->fd, info->user_data, info->size, info->offset);
            } else {
                io_prep_pwritev(iocb, info->fd, info->iov_ptr, info->iovcnt, info->offset);
            }
        }
        io_set_eventfd(iocb, ev_fd);
        iocb->data = cookie;
    }

    struct iocb* prep_iocb(bool batch_io, int fd, bool is_read, const char* data, uint32_t size, uint64_t offset,
                           void* cookie) {
        auto i_info = alloc_iocb();
        i_info->is_read = is_read;
        i_info->user_data = (char*)data;
        i_info->size = size;
        i_info->offset = offset;
        i_info->fd = fd;
        i_info->iovcnt = 0;

        struct iocb* iocb = static_cast< struct iocb* >(i_info);
        (is_read) ? io_prep_pread(iocb, fd, (void*)data, size, offset)
                  : io_prep_pwrite(iocb, fd, (void*)data, size, offset);
        io_set_eventfd(iocb, ev_fd);
        iocb->data = cookie;

        LOGTRACE("Issuing IO info: {}, batch? = {}", i_info->to_string(), batch_io);
        if (batch_io) {
            assert(can_be_batched(0));
            cur_iocb_batch.iocb_info[cur_iocb_batch.n_iocbs++] = i_info;
        }
        return iocb;
    }

    struct iocb* prep_iocb_v(bool batch_io, int fd, bool is_read, const iovec* iov, int iovcnt, uint32_t size,
                             uint64_t offset, uint8_t* cookie) {
        auto i_info = alloc_iocb(iovcnt);

        i_info->is_read = is_read;
        i_info->user_data = nullptr;
        i_info->size = size;
        i_info->offset = offset;
        i_info->fd = fd;
        i_info->iovcnt = iovcnt;
        memcpy(&i_info->iov_ptr[0], iov, sizeof(iovec) * iovcnt);
        iov = i_info->iov_ptr;

        struct iocb* iocb = static_cast< struct iocb* >(i_info);
        if (batch_io) {
            // In case of batch io we need to copy the iovec because caller might free the iovec resuling in
            // corrupted data
            cur_iocb_batch.iocb_info[cur_iocb_batch.n_iocbs++] = i_info;
            LOGTRACE("cur_iocb_batch.n_iocbs = {} ", cur_iocb_batch.n_iocbs);
        }
        (is_read) ? io_prep_preadv(iocb, fd, iov, iovcnt, offset) : io_prep_pwritev(iocb, fd, iov, iovcnt, offset);
        io_set_eventfd(iocb, ev_fd);
        iocb->data = cookie;

        LOGTRACE("Issuing IO info: {}, batch? = {}", i_info->to_string(), batch_io);
        return iocb;
    }

    iocb_batch_t move_cur_batch() {
        auto ret = cur_iocb_batch;
        cur_iocb_batch.reset();
        return ret;
    }
};

struct UringThreadContext {};

class UringDriveInterfaceMetrics : public sisl::MetricsGroup {
public:
    explicit UringDriveInterfaceMetrics(const char* inst_name = "UringDriveInterface") :
            sisl::MetricsGroup("UringDriveInterface", inst_name) {
        REGISTER_COUNTER(completion_errors, "Uring Completion errors");
        REGISTER_COUNTER(write_io_submission_errors, "Uring write submission errors", "io_submission_errors",
                         {"io_direction", "write"});
        REGISTER_COUNTER(read_io_submission_errors, "Uring read submission errors", "io_submission_errors",
                         {"io_direction", "read"});
        REGISTER_COUNTER(retry_io_eagain_error, "Retry IOs count because of kernel eagain");

        REGISTER_COUNTER(total_io_callbacks, "Number of times aio returned io events");
        REGISTER_COUNTER(resubmit_io_on_err, "number of times ios are resubmitted");
        register_me_to_farm();
    }

    ~UringDriveInterfaceMetrics() { deregister_me_from_farm(); }
};

// Per thread structure which has all details for uring
struct uring_drive_channel {
    struct io_uring m_ring;
    std::queue< drive_iocb* > m_iocb_waitq;
    io_device_ptr m_ring_ev_iodev;
    uint32_t m_prepared_ios{0};

    drive_iocb* pop_waitq() {
        if (m_iocb_waitq.size() == 0) { return nullptr; }
        drive_iocb* iocb = m_iocb_waitq.front();
        m_iocb_waitq.pop();
        return iocb;
    }

    size_t waitq_size() const { return m_iocb_waitq.size(); }
    struct io_uring_sqe* get_sqe_or_enqueue(drive_iocb* iocb);
    void submit_ios();
    void submit_if_needed(drive_iocb* iocb, struct io_uring_sqe*, bool part_of_batch);
};

class UringDriveInterface : public DriveInterface {
public:
    static constexpr uint32_t per_thread_qdepth = 256;

    UringDriveInterface(const io_interface_comp_cb_t& cb = nullptr);
    virtual ~UringDriveInterface() = default;
    drive_interface_type interface_type() const override { return drive_interface_type::uring; }
    std::string name() const override { return "uring_drive_interface"; }

    void attach_completion_cb(const io_interface_comp_cb_t& cb) override { m_comp_cb = cb; }
    io_device_ptr open_dev(const std::string& devname, iomgr_drive_type dev_type, int oflags) override;
    void close_dev(const io_device_ptr& iodev) override;
    ssize_t sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    ssize_t sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    void async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                      bool part_of_batch = false) override;
    void async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                    bool part_of_batch = false) override;
    void async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    virtual void write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) override;
    void on_event_notification(IODevice* iodev, void* cookie, int event);

    size_t get_size(IODevice* iodev) override;
    virtual void submit_batch() override;
    drive_attributes get_attributes(const std::string& devname, const iomgr_drive_type drive_type) override;

    static std::vector< int > s_poll_interval_table;
    static void init_poll_interval_table();

private:
    void init_iface_thread_ctx(const io_thread_t& thr) override;
    void clear_iface_thread_ctx(const io_thread_t& thr) override;
    void init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override {}
    void clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override {}

    void complete_io(drive_iocb* iocb);
    ssize_t _sync_write(int fd, const char* data, uint32_t size, uint64_t offset);
    ssize_t _sync_writev(int fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset);
    ssize_t _sync_read(int fd, char* data, uint32_t size, uint64_t offset);
    ssize_t _sync_readv(int fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset);

    void write_zero_ioctl(const IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie);
    void write_zero_writev(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie);

private:
    static thread_local uring_drive_channel* t_uring_ch;
    uint8_t* m_zero_buf{nullptr};
    uint64_t m_max_write_zeros{0};
    UringDriveInterfaceMetrics m_metrics;
    io_interface_comp_cb_t m_comp_cb;
};
} // namespace iomgr
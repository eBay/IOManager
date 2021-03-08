//
// Created by Rishabh Mittal 04/20/2018
//
#pragma once

#include <unistd.h>
#include <string>
#include <stack>
#include <queue>
#include <atomic>
#include <mutex>
#include "drive_interface.hpp"
#include <metrics/metrics.hpp>
#include <fds/utils.hpp>

#ifdef linux
#include <fcntl.h>
#include <libaio.h>
#include <sys/eventfd.h>
#include <stdio.h>
#endif

using namespace std;
using Clock = std::chrono::steady_clock;

namespace iomgr {
#define MAX_OUTSTANDING_IO 200               // if max outstanding IO is more than 200 then io_submit will fail.
#define MAX_COMPLETIONS (MAX_OUTSTANDING_IO) // how many completions to process in one shot

static constexpr int max_batch_iocb_count = 4;
static constexpr int max_batch_iov_cnt = 4;

#ifdef linux
struct iocb_info_t : public iocb {
    bool is_read;
    char* user_data;
    uint32_t size;
    uint64_t offset;
    int fd;
    iovec iovs[max_batch_iov_cnt];
    int iovcnt;

    std::string to_string() const {
        return fmt::format("is_read={}, size={}, offset={}, fd={}, iovcnt={}", is_read, size, offset, fd, iovcnt);
    }
};

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

template < typename T, typename Container = std::deque< T > >
class iterable_stack : public std::stack< T, Container > {
    using std::stack< T, Container >::c;

public:
    // expose just the iterators of the underlying container
    auto begin() { return std::begin(c); }
    auto end() { return std::end(c); }

    auto begin() const { return std::begin(c); }
    auto end() const { return std::end(c); }
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
    uint64_t outstanding_aio = 0;
    uint64_t max_outstanding_aio;
    std::shared_ptr< IODevice > ev_io_dev = nullptr; // fd info after registering with IOManager

    ~aio_thread_context() {
        if (ev_fd) { close(ev_fd); }
        io_destroy(ioctx);

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
        max_outstanding_aio = count;
    }

    bool can_be_batched(int iovcnt) {
        return ((iovcnt <= max_batch_iov_cnt) && (cur_iocb_batch.n_iocbs < max_batch_iocb_count));
    }

    bool can_submit_aio() { return outstanding_aio < max_outstanding_aio ? true : false; }

    iocb_info_t* alloc_iocb() {
        iocb_info_t* info;
        ++outstanding_aio;
        if (can_submit_aio()) {
            info = iocb_free_list.top();
            iocb_free_list.pop();
            return info;
        } else {
            info = new iocb_info_t();
            ++post_alloc_iocb;
        }
        return info;
    }

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
        --outstanding_aio;
        if (post_alloc_iocb == 0) {
            iocb_free_list.push(info);
        } else {
            --post_alloc_iocb;
            delete info;
        }
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
        auto i_info = alloc_iocb();

        i_info->is_read = is_read;
        i_info->user_data = nullptr;
        i_info->size = size;
        i_info->offset = offset;
        i_info->fd = fd;

        struct iocb* iocb = static_cast< struct iocb* >(i_info);
        if (batch_io) {
            // In case of batch io we need to copy the iovec because caller might free the iovec resuling in
            // corrupted data
            assert(iovcnt <= max_batch_iov_cnt);
            memcpy(&i_info->iovs[0], iov, sizeof(iovec) * iovcnt);
            iov = &i_info->iovs[0];
            i_info->iovcnt = iovcnt;
            cur_iocb_batch.iocb_info[cur_iocb_batch.n_iocbs++] = i_info;
            LOGTRACE("cur_iocb_batch.n_iocbs = {} ", cur_iocb_batch.n_iocbs);
        } else {
            i_info->iovcnt = iovcnt;
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

class AioDriveInterfaceMetrics : public sisl::MetricsGroup {
public:
    explicit AioDriveInterfaceMetrics(const char* inst_name = "AioDriveInterface") :
            sisl::MetricsGroup("AioDriveInterface", inst_name) {
        REGISTER_COUNTER(spurious_events, "Spurious events count");
        REGISTER_COUNTER(completion_errors, "Aio Completion errors");
        REGISTER_COUNTER(write_io_submission_errors, "Aio write submission errors", "io_submission_errors",
                         {"io_direction", "write"});
        REGISTER_COUNTER(read_io_submission_errors, "Aio read submission errors", "io_submission_errors",
                         {"io_direction", "read"});
        REGISTER_COUNTER(force_sync_io_empty_iocb, "Forced sync io because of empty iocb");
        REGISTER_COUNTER(retry_io_eagain_error, "retrying sending IOs");

        REGISTER_COUNTER(total_io_submissions, "Number of times aio io_submit called");
        REGISTER_COUNTER(total_io_callbacks, "Number of times aio returned io events");
        register_me_to_farm();
    }

    ~AioDriveInterfaceMetrics() { deregister_me_from_farm(); }
};

class AioDriveInterface : public DriveInterface {
public:
    AioDriveInterface(const io_interface_comp_cb_t& cb = nullptr);
    drive_interface_type interface_type() const override { return drive_interface_type::aio; }

    void attach_completion_cb(const io_interface_comp_cb_t& cb) override { m_comp_cb = cb; }
    void attach_end_of_batch_cb(const io_interface_end_of_batch_cb_t& cb) override { m_io_end_of_batch_cb = cb; }
    void detach_end_of_batch_cb() override { m_io_end_of_batch_cb = nullptr; }
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
    void process_completions(IODevice* iodev, void* cookie, int event);
    size_t get_size(IODevice* iodev) override;
    virtual void submit_batch() override;
    drive_attributes get_attributes(const io_device_ptr& dev) const override;
    drive_attributes get_attributes(const std::string& devname, const iomgr_drive_type drive_type) override;

private:
    void init_iface_thread_ctx(const io_thread_t& thr) override;
    void clear_iface_thread_ctx(const io_thread_t& thr) override;
    void init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override {}
    void clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override {}

    /* return true if it queues io.
     * return false if it do completion callback for error.
     */
    bool handle_io_failure(struct iocb* iocb);
    void retry_io();
    void push_retry_list(struct iocb* iocb);
    ssize_t _sync_write(int fd, const char* data, uint32_t size, uint64_t offset);
    ssize_t _sync_writev(int fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset);
    ssize_t _sync_read(int fd, char* data, uint32_t size, uint64_t offset);
    ssize_t _sync_readv(int fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset);

private:
    static thread_local aio_thread_context* _aio_ctx;
    AioDriveInterfaceMetrics m_metrics;
    io_interface_comp_cb_t m_comp_cb;
    io_interface_end_of_batch_cb_t m_io_end_of_batch_cb;
};
#else
class AioDriveInterface : public DriveInterface {
public:
    AioDriveInterface(const io_interface_comp_cb_t& cb = nullptr) {}
    void init_iface_thread_ctx(const io_thread_t& thr) override;
    void clear_iface_thread_ctx(const io_thread_t& thr) override;
};
#endif
} // namespace iomgr

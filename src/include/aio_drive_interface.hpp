//
// Created by Rishabh Mittal 04/20/2018
//
#pragma once

#include <unistd.h>
#include <string>
#include <stack>
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
        std::stringstream ss;
        ss << "is_read = " << is_read << ", size = " << size << ", offset = " << offset << ", fd = " << fd
           << ", iovcnt = " << iovcnt;
        return ss.str();
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

struct fd_info;
class ioMgrThreadContext;
struct aio_thread_context {
    struct io_event events[MAX_COMPLETIONS] = {{}};
    int ev_fd = 0;
    io_context_t ioctx = 0;
    std::stack< iocb_info_t* > iocb_free_list;
    iocb_batch_t cur_iocb_batch;
    std::shared_ptr< fd_info > ev_fd_info = nullptr; // fd info after registering with IOManager

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
    }

    bool can_be_batched(int iovcnt) {
        return ((iovcnt <= max_batch_iov_cnt) && (cur_iocb_batch.n_iocbs < max_batch_iocb_count));
    }

    bool can_submit_aio() { return (!iocb_free_list.empty()); }

    iocb_info_t* alloc_iocb() {
        auto info = iocb_free_list.top();
        iocb_free_list.pop();
        return info;
    }

    void free_iocb(struct iocb* iocb) {
        auto info = static_cast< iocb_info_t* >(iocb);
        iocb_free_list.push(info);
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
        } else {
            i_info->iovcnt = iovcnt;
        }

        (is_read) ? io_prep_preadv(iocb, fd, iov, iovcnt, offset) : io_prep_pwritev(iocb, fd, iov, iovcnt, offset);
        io_set_eventfd(iocb, ev_fd);
        iocb->data = cookie;

        return iocb;
    }

    iocb_batch_t move_cur_batch() {
        auto ret = cur_iocb_batch;
        cur_iocb_batch.reset();
        return ret;
    }
};

class AioDriveInterfaceMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit AioDriveInterfaceMetrics(const char* inst_name = "AioDriveInterface") :
            sisl::MetricsGroupWrapper("AioDriveInterface", inst_name) {
        REGISTER_COUNTER(spurious_events, "Spurious events count");
        REGISTER_COUNTER(completion_errors, "Aio Completion errors");
        REGISTER_COUNTER(write_io_submission_errors, "Aio write submission errors", "io_submission_errors",
                         {"io_direction", "write"});
        REGISTER_COUNTER(read_io_submission_errors, "Aio read submission errors", "io_submission_errors",
                         {"io_direction", "read"});
        REGISTER_COUNTER(force_sync_io_empty_iocb, "Forced sync io because of empty iocb");
        REGISTER_COUNTER(force_sync_io_eagain_error, "Forced sync io because of EAGAIN error");
        REGISTER_COUNTER(async_write_count, "Aio Drive async write count", "io_count", {"io_direction", "write"});
        REGISTER_COUNTER(async_read_count, "Aio Drive async read count", "io_count", {"io_direction", "read"});
        REGISTER_COUNTER(sync_write_count, "Aio Drive sync write count", "io_count", {"io_direction", "write"});
        REGISTER_COUNTER(sync_read_count, "Aio Drive sync read count", "io_count", {"io_direction", "read"});
        REGISTER_COUNTER(total_io_submissions, "Number of times aio io_submit called");
        REGISTER_COUNTER(total_io_callbacks, "Number of times aio returned io events");

        REGISTER_HISTOGRAM(write_io_sizes, "Write IO Sizes", "io_sizes", {"io_direction", "write"},
                           HistogramBucketsType(ExponentialOfTwoBuckets));
        REGISTER_HISTOGRAM(read_io_sizes, "Read IO Sizes", "io_sizes", {"io_direction", "read"},
                           HistogramBucketsType(ExponentialOfTwoBuckets));
        register_me_to_farm();
    }
};

class AioDriveInterface : public DriveInterface {
public:
    AioDriveInterface(const io_interface_comp_cb_t& cb = nullptr);
    drive_interface_type interface_type() const override { return drive_interface_type::aio; }

    void attach_completion_cb(const io_interface_comp_cb_t& cb) override { m_comp_cb = cb; }
    void attach_end_of_batch_cb(const io_interface_end_of_batch_cb_t& cb) override {
        m_io_end_of_batch_cb_list.emplace_back(cb);
    }
    int open_dev(std::string devname, int oflags) override;
    void add_fd(int fd, int priority = 9) override;
    ssize_t sync_write(int data_fd, const char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_writev(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    ssize_t sync_read(int data_fd, char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_readv(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    void async_write(int data_fd, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void async_writev(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                      bool part_of_batch = false) override;
    void async_read(int data_fd, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                    bool part_of_batch = false) override;
    void async_readv(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void process_completions(int fd, void* cookie, int event);
    void on_io_thread_start(ioMgrThreadContext* iomgr_ctx) override;
    void on_io_thread_stopped(ioMgrThreadContext* iomgr_ctx) override;
    virtual void submit_batch() override;

private:
    void handle_io_failure(struct iocb* iocb);

private:
    static thread_local aio_thread_context* _aio_ctx;
    AioDriveInterfaceMetrics m_metrics;
    io_interface_comp_cb_t m_comp_cb;
    std::vector< io_interface_end_of_batch_cb_t > m_io_end_of_batch_cb_list;
};
#else
class AioDriveInterface : public DriveInterface {
public:
    AioDriveInterface(const io_interface_comp_cb_t& cb = nullptr) {}
    void on_io_thread_start(ioMgrThreadContext* iomgr_ctx) override {}
    void on_io_thread_stopped(ioMgrThreadContext* iomgr_ctx) override {}
};
#endif
} // namespace iomgr

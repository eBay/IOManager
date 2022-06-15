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

#include "kernel_drive_interface.hpp"
#include "iomgr_types.hpp"

namespace iomgr {
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
        REGISTER_COUNTER(retry_on_partial_read, "number of times ios are retried on partial read");

        REGISTER_COUNTER(outstanding_write_cnt, "outstanding write cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_read_cnt, "outstanding read cnt", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(outstanding_fsync_cnt, "outstanding fsync cnt", sisl::_publish_as::publish_as_gauge);

        register_me_to_farm();
    }

    ~UringDriveInterfaceMetrics() { deregister_me_from_farm(); }
};
// Per thread structure which has all details for uring
class UringDriveInterface;
struct uring_drive_channel {
    struct io_uring m_ring;
    std::queue< drive_iocb* > m_iocb_waitq;
    io_device_ptr m_ring_ev_iodev;
    uint32_t m_prepared_ios{0};

    uring_drive_channel(UringDriveInterface* iface);
    ~uring_drive_channel();
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
    void drain_waitq();
};

class UringDriveInterface : public KernelDriveInterface {
public:
    static constexpr uint32_t per_thread_qdepth = 256;

    UringDriveInterface(const io_interface_comp_cb_t& cb = nullptr);
    virtual ~UringDriveInterface() = default;
    drive_interface_type interface_type() const override { return drive_interface_type::uring; }
    std::string name() const override { return "uring_drive_interface"; }

    io_device_ptr open_dev(const std::string& devname, drive_type dev_type, int oflags) override;
    void close_dev(const io_device_ptr& iodev) override;
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
    void fsync(IODevice* iodev, uint8_t* cookie) override;

    void on_event_notification(IODevice* iodev, void* cookie, int event);
    virtual void submit_batch() override;
    static void increment_outstanding_counter(const drive_iocb* iocb, UringDriveInterface * iface);
    static void decrement_outstanding_counter(const drive_iocb* iocb, UringDriveInterface * iface);
    UringDriveInterfaceMetrics& get_metrics() { return m_metrics; }

private:
    void init_iface_thread_ctx(const io_thread_t& thr) override;
    void clear_iface_thread_ctx(const io_thread_t& thr) override;
    void init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override {}
    void clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override {}

    void complete_io(drive_iocb* iocb);

private:
    static thread_local uring_drive_channel* t_uring_ch;
    UringDriveInterfaceMetrics m_metrics;
};
} // namespace iomgr

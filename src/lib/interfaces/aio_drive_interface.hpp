/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Rishabh Mittal, Harihara Kadayam
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
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <stack>
#include <string>
#include <vector>

#ifdef __linux__
#include <fcntl.h>
#include <libaio.h>
#include <sys/eventfd.h>
#include <unistd.h>
#endif

#ifdef __APPLE__
#include <aio.h>
#include <sys/uio.h>
#endif

#include <sisl/fds/buffer.hpp>
#include <sisl/metrics/metrics.hpp>

#include "kernel_drive_interface.hpp"
#include <iomgr/iomgr_types.hpp>

namespace iomgr {
constexpr unsigned MAX_OUTSTANDING_IO{200}; // if max outstanding IO is more than 200 then io_submit will fail.
constexpr unsigned MAX_COMPLETIONS{MAX_OUTSTANDING_IO}; // how many completions to process in one shot

static constexpr int max_batch_iocb_count = 4;
static constexpr int max_batch_iov_cnt = IOV_MAX;

#ifdef __linux__
using kernel_iocb_t = struct iocb;
#elif defined(__APPLE__)
using kernel_iocb_t = aiocb_t;
#endif

struct drive_aio_iocb : public drive_iocb {
    drive_aio_iocb(DriveInterface* iface, IODevice* iodev, DriveOpType op_type, uint64_t size, uint64_t offset) :
            drive_iocb{iface, iodev, op_type, size, offset} {}
    kernel_iocb_t kernel_iocb;
};

struct IODevice;
class IOReactor;
struct aio_thread_context {
public:
    std::array< io_event, MAX_COMPLETIONS > m_events;
    int m_ev_fd{0};
    io_context_t m_ioctx{0};
    std::queue< drive_aio_iocb* > m_iocb_pending_list;

    std::array< kernel_iocb_t*, max_batch_iocb_count > m_iocb_batch;
    uint32_t m_cur_batch_size{0};

    shared< IODevice > m_ev_io_dev; // fd info after registering with IOManager
    poll_cb_idx_t m_poll_cb_idx;
    bool m_timer_set{false};

    uint64_t m_submitted_ios{0};

public:
    aio_thread_context();
    ~aio_thread_context();

    bool add_to_batch(drive_aio_iocb* diocb);
    bool can_submit_io() const;
    void inc_submitted_aio(int count);
    void dec_submitted_aio();
    void reset_batch();
    static drive_aio_iocb* to_drive_iocb(kernel_iocb_t* kiocb);
};

class AioDriveInterfaceMetrics : public DriveInterfaceMetrics {
public:
    explicit AioDriveInterfaceMetrics(const char* inst_name = "AioDriveInterface") :
            DriveInterfaceMetrics{"AioDriveInterface", inst_name} {
        REGISTER_COUNTER(retry_io_eagain_error, "Retry IOs count because of kernel eagain");
        REGISTER_COUNTER(queued_aio_slots_full, "Count of IOs queued because of aio slots full");

        // TODO: This shouldn't be a counter, but part of get_status(), but we haven't setup one for iomgr, so keeping
        // as a metric as of now. Once added, will remove this counter/gauge.
        REGISTER_COUNTER(retry_list_size, "Retry list size", sisl::_publish_as::publish_as_gauge);
        REGISTER_COUNTER(total_io_callbacks, "Number of times aio returned io events");
        register_me_to_farm();
    }

    ~AioDriveInterfaceMetrics() { deregister_me_from_farm(); }
};

class AioDriveInterface : public KernelDriveInterface {
public:
    AioDriveInterface(const io_interface_comp_cb_t& cb = nullptr);
    ~AioDriveInterface();
    drive_interface_type interface_type() const override { return drive_interface_type::aio; }
    std::string name() const override { return "aio_drive_interface"; }

    io_device_ptr open_dev(const std::string& devname, drive_type dev_type, int oflags) override;
    void close_dev(const io_device_ptr& iodev) override;

    folly::Future< bool > async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset,
                                      bool part_of_batch = false) override;
    folly::Future< bool > async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                       bool part_of_batch = false) override;
    folly::Future< bool > async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset,
                                     bool part_of_batch = false) override;
    folly::Future< bool > async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      bool part_of_batch = false) override;
    folly::Future< bool > async_unmap(IODevice* iodev, uint32_t size, uint64_t offset,
                                      bool part_of_batch = false) override;
    folly::Future< bool > async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset);
    folly::Future< bool > queue_fsync(IODevice* iodev) override {
        LOGWARN("fsync on aio drive interface is not supported");
        return folly::makeFuture< bool >(false);
    }

    virtual void submit_batch() override;

    void on_event_notification(IODevice* iodev, void* cookie, int event);
    DriveInterfaceMetrics& get_metrics() override { return m_metrics; }

    static std::vector< int > s_poll_interval_table;
    static void init_poll_interval_table();

private:
    void init_iface_reactor_context(IOReactor*) override;
    void clear_iface_reactor_context(IOReactor*) override;

    void handle_completions();

    // Returns true if it is able to submit, else false
    bool submit_io(drive_aio_iocb* diocb);
    void issue_pending_ios();
    void push_to_pending_list(drive_aio_iocb* diocb, bool because_no_slot);

    /* return true if it is requeued the io and it will process later
     * return false if given up and completed the io.
     */
    bool handle_io_failure(drive_aio_iocb* diocb, int error);
    void complete_io(drive_aio_iocb* diocb);

    static void submit_in_this_thread(AioDriveInterface* iface, drive_aio_iocb* diocb, bool part_of_batch);

private:
    static thread_local std::unique_ptr< aio_thread_context > t_aio_ctx;
    std::mutex m_open_mtx;
    AioDriveInterfaceMetrics m_metrics;
};
} // namespace iomgr

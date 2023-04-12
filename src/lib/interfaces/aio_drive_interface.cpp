/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Rishabh Mittal
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
#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>

#ifdef __linux__
#include <fcntl.h>
#include <fmt/format.h>
#include <linux/fs.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/sysmacros.h>
#endif

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif
#include <folly/Exception.h>
#include "iomgr_config.hpp"
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

#include <sisl/logging/logging.h>

// TODO: Remove this once the problem is fixed in flip
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <sisl/flip/flip.hpp>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

#include <iomgr/iomgr.hpp>
#include "interfaces/aio_drive_interface.hpp"
#include "iomgr_config.hpp"
#include "reactor/reactor.hpp"

namespace iomgr {
#ifdef __APPLE__

ssize_t preadv(int fd, const iovec* iov, int iovcnt, off_t offset) {
    lseek(fd, offset, SEEK_SET);
    return ::readv(fd, iov, iovcnt);
}

ssize_t pwritev(int fd, const iovec* iov, int iovcnt, off_t offset) {
    lseek(fd, offset, SEEK_SET);
    return ::writev(fd, iov, iovcnt);
}

#endif
using namespace std;

thread_local std::unique_ptr< aio_thread_context > AioDriveInterface::t_aio_ctx;
std::vector< int > AioDriveInterface::s_poll_interval_table;

AioDriveInterface::AioDriveInterface(const io_interface_comp_cb_t& cb) : KernelDriveInterface(cb) {
    init_poll_interval_table();
}

AioDriveInterface::~AioDriveInterface() {}

io_device_ptr AioDriveInterface::open_dev(const std::string& devname, drive_type dev_type, int oflags) {
    std::lock_guard lock{m_open_mtx};
    LOGMSG_ASSERT(((dev_type == drive_type::block_nvme) || (dev_type == drive_type::block_hdd) ||
                   (dev_type == drive_type::file_on_hdd) || (dev_type == drive_type::file_on_nvme)),
                  "Unexpected dev type to open {}", dev_type);
    init_write_zero_buf(devname, dev_type);

    auto fd = ::open(devname.c_str(), oflags, 0640);
    if (fd == -1) {
        folly::throwSystemError(fmt::format("Unable to open the device={} dev_type={}, errno={} strerror={}", devname,
                                            dev_type, errno, strerror(errno)));
        return nullptr;
    }

    auto iodev = alloc_io_device(backing_dev_t(fd), 9 /* pri */, reactor_regex::all_io);
    iodev->devname = devname;
    iodev->creator =
        iomanager.am_i_io_reactor() ? iomanager.this_reactor()->pick_fiber(fiber_regex::main_only) : nullptr;
    iodev->dtype = dev_type;

    // We don't need to add the device to each thread, because each AioInterface thread context add an
    // event fd and read/write use this device fd to control with iocb.
    LOGINFO("Device={} of type={} opened with flags={} successfully, fd={}", devname, dev_type, oflags, fd);
    return iodev;
}

void AioDriveInterface::close_dev(const io_device_ptr& iodev) {
    // TODO: This is where we would wait for any outstanding io's to complete

    IOInterface::close_dev(iodev);

    // AIO base devices are not added to any poll list, so it can be closed as is.
    ::close(iodev->fd());
    iodev->clear();
}

void AioDriveInterface::init_iface_reactor_context(IOReactor*) {
    t_aio_ctx = std::make_unique< aio_thread_context >();
    t_aio_ctx->m_ev_io_dev =
        iomanager.generic_interface()->make_io_device(backing_dev_t(t_aio_ctx->m_ev_fd), EPOLLIN, 0, nullptr, true,
                                                      bind_this(AioDriveInterface::on_event_notification, 3));
    t_aio_ctx->m_poll_cb_idx =
        iomanager.this_reactor()->register_poll_interval_cb(bind_this(AioDriveInterface::handle_completions, 0));
}

void AioDriveInterface::clear_iface_reactor_context(IOReactor*) {
    iomanager.this_reactor()->unregister_poll_interval_cb(t_aio_ctx->m_poll_cb_idx);
    iomanager.generic_interface()->remove_io_device(t_aio_ctx->m_ev_io_dev);
    t_aio_ctx.reset(nullptr);
}

void AioDriveInterface::on_event_notification(IODevice* iodev, [[maybe_unused]] void* cookie,
                                              [[maybe_unused]] int event) {
    assert(iodev->fd() == t_aio_ctx->m_ev_fd);

    uint64_t temp = 0;
    [[maybe_unused]] auto rsize = read(t_aio_ctx->m_ev_fd, &temp, sizeof(uint64_t));

    LOGTRACEMOD(iomgr, "Received completion on fd = {} ev_fd = {}", iodev->fd(), t_aio_ctx->m_ev_fd);
    handle_completions();
}

void AioDriveInterface::handle_completions() {
    auto& tmetrics = iomanager.this_thread_metrics();

    const int nevents =
        io_getevents(t_aio_ctx->m_ioctx, 0, t_aio_ctx->m_events.size(), t_aio_ctx->m_events.data(), NULL);
    ++tmetrics.io_callbacks;

    if (nevents == 0) {
        return;
    } else if (nevents < 0) {
        /* TODO: Handle error by reporting failures */
        LOGERROR("process_completions nevents is less then zero {}", nevents);
        COUNTER_INCREMENT(m_metrics, completion_errors, 1);
    } else {
        tmetrics.aio_events_in_callback += nevents;
    }

    for (int i = 0; i < nevents; ++i) {
        auto& e = t_aio_ctx->m_events[i];

        auto ret = static_cast< int64_t >(e.res);
        auto kiocb = r_cast< kernel_iocb_t* >(e.obj);
        auto diocb = aio_thread_context::to_drive_iocb(kiocb);

        t_aio_ctx->dec_submitted_aio();

        LOGTRACEMOD(iomgr, "Event[{}]: Result {} res2={}", i, e.res, e.res2);
        if (ret < 0) {
            COUNTER_INCREMENT(m_metrics, completion_errors, 1);
            LOGDFATAL("Error in completion of aio, result: {} diocb: {}", ret, diocb->to_string());
        } else if ((e.res != diocb->size) || e.res2) {
            COUNTER_INCREMENT(m_metrics, completion_errors, 1);
            LOGERROR("io is not completed properly. size read/written {} diocb {} error {}", e.res, diocb->to_string(),
                     e.res2);
            if (e.res2 == 0) { e.res2 = EIO; }
            if (handle_io_failure(diocb, e.res2)) { continue; }
        } else {
            diocb->result = 0;
            complete_io(diocb);
        }
        issue_pending_ios();
    }
}

bool AioDriveInterface::submit_io(drive_aio_iocb* diocb) {
    if (!t_aio_ctx->can_submit_io()) {
        push_to_pending_list(diocb, false);
        return false;
    }

    auto& metrics = iomanager.this_thread_metrics();
    ++metrics.iface_io_batch_count;
    ++metrics.iface_io_actual_count;

    auto kiocb = &diocb->kernel_iocb;
    auto ret = io_submit(t_aio_ctx->m_ioctx, 1, &kiocb);
    if (ret != 1) {
        handle_io_failure(diocb, errno);
        return false;
    }

    t_aio_ctx->inc_submitted_aio(ret);
    return true;
}

void AioDriveInterface::submit_batch() {
    auto n_issued = io_submit(t_aio_ctx->m_ioctx, t_aio_ctx->m_cur_batch_size, t_aio_ctx->m_iocb_batch.data());
    if (n_issued < 0) { n_issued = 0; }

    auto& metrics = iomanager.this_thread_metrics();
    ++metrics.iface_io_batch_count;
    metrics.iface_io_actual_count += n_issued;
    t_aio_ctx->inc_submitted_aio(n_issued);

    // For those which we are not able to issue, do failure handling
    for (auto i = n_issued; i < t_aio_ctx->m_cur_batch_size; ++i) {
        handle_io_failure(r_cast< drive_aio_iocb* >(t_aio_ctx->m_iocb_batch[i]->data), errno);
    }
    t_aio_ctx->reset_batch();
}

void AioDriveInterface::issue_pending_ios() {
    while (t_aio_ctx->can_submit_io() && !t_aio_ctx->m_iocb_pending_list.empty()) {
        COUNTER_DECREMENT(m_metrics, retry_list_size, 1);
        auto diocb = t_aio_ctx->m_iocb_pending_list.front();
        t_aio_ctx->m_iocb_pending_list.pop();
        if (!submit_io(diocb)) { break; }
    }
}

void AioDriveInterface::push_to_pending_list(drive_aio_iocb* diocb, bool because_no_slot) {
    COUNTER_INCREMENT_IF_ELSE(m_metrics, because_no_slot, queued_aio_slots_full, retry_io_eagain_error, 1);
    COUNTER_INCREMENT(m_metrics, retry_list_size, 1);

    LOGDEBUGMOD(iomgr, "adding io into retry list: {}", diocb->to_string());
    t_aio_ctx->m_iocb_pending_list.push(diocb);
    if (!t_aio_ctx->m_timer_set) {
        t_aio_ctx->m_timer_set = true;
        iomanager.schedule_thread_timer(IM_DYNAMIC_CONFIG(drive.retry_timeout_us) * 1000, false, nullptr,
                                        [this](void*) {
                                            t_aio_ctx->m_timer_set = false;
                                            issue_pending_ios();
                                        });
    }
}

bool AioDriveInterface::handle_io_failure(drive_aio_iocb* diocb, int error) {
    if (error == EAGAIN) {
        push_to_pending_list(diocb, false /* no_slot */);
        return true;
    } else if (diocb->resubmit_cnt > IM_DYNAMIC_CONFIG(drive.max_resubmit_cnt)) {
        ++diocb->resubmit_cnt;
        COUNTER_INCREMENT(m_metrics, resubmit_io_on_err, 1);
        push_to_pending_list(diocb, false /* no_slot */);
        return true;
    } else {
        LOGERROR("io submit fail: diocb: {}, errno: {}", diocb->to_string(), error);
        diocb->result = error;
        complete_io(diocb);
        return false;
    }
}

void AioDriveInterface::complete_io(drive_aio_iocb* diocb) {
    if (diocb->result == 0) {
        std::visit(overloaded{[&](folly::Promise< bool >& p) { p.setValue(true); },
                              [&](FiberManagerLib::Promise< bool >& p) { p.setValue(true); },
                              [&](io_interface_comp_cb_t& cb) { cb(diocb->result); }},
                   diocb->completion);
    } else {
        std::visit(overloaded{[&](folly::Promise< bool >& p) {
                                  p.setException(folly::makeSystemErrorExplicit(diocb->result, "Error in aio"));
                              },
                              [&](FiberManagerLib::Promise< bool >& p) {
                                  p.setException(std::make_exception_ptr(
                                      folly::makeSystemErrorExplicit(diocb->result, "Error in aio")));
                              },
                              [&](io_interface_comp_cb_t& cb) { cb(diocb->result > 0 ? 0 : diocb->result); }},
                   diocb->completion);
    }
    sisl::ObjectAllocator< drive_aio_iocb >::deallocate(diocb);
}

folly::Future< bool > AioDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset,
                                                     bool part_of_batch) {
    auto diocb = t_aio_ctx->prep_iocb(this, iodev, DriveOpType::WRITE, (char*)data, size, offset);
    diocb->completion = std::move(folly::Promise< bool >{});
    auto ret = diocb->folly_comp_promise().getFuture();

    if (part_of_batch) {
        if (t_aio_ctx->add_to_batch(diocb)) { submit_batch(); }
    } else {
        submit_io(diocb);
    }
    return ret;
}

folly::Future< bool > AioDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset,
                                                    bool part_of_batch) {
    auto diocb = t_aio_ctx->prep_iocb(this, iodev, DriveOpType::READ, data, size, offset);
    diocb->completion = std::move(folly::Promise< bool >{});
    auto ret = diocb->folly_comp_promise().getFuture();

    if (part_of_batch) {
        if (t_aio_ctx->add_to_batch(diocb)) { submit_batch(); }
    } else {
        submit_io(diocb);
    }
    return ret;
}

folly::Future< bool > AioDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                      uint64_t offset, bool part_of_batch) {
    auto diocb = t_aio_ctx->prep_iocb_v(this, iodev, DriveOpType::WRITE, iov, iovcnt, size, offset);
    diocb->completion = std::move(folly::Promise< bool >{});
    auto ret = diocb->folly_comp_promise().getFuture();

    if (part_of_batch) {
        if (t_aio_ctx->add_to_batch(diocb)) { submit_batch(); }
    } else {
        submit_io(diocb);
    }
    return ret;
}

folly::Future< bool > AioDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                     uint64_t offset, bool part_of_batch) {
    auto diocb = t_aio_ctx->prep_iocb_v(this, iodev, DriveOpType::READ, iov, iovcnt, size, offset);
    diocb->completion = std::move(folly::Promise< bool >{});
    auto ret = diocb->folly_comp_promise().getFuture();

    if (part_of_batch) {
        if (t_aio_ctx->add_to_batch(diocb)) { submit_batch(); }
    } else {
        submit_io(diocb);
    }
    return ret;
}

folly::Future< bool > AioDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset,
                                                     bool part_of_batch) {
    RELEASE_ASSERT(0, "async_unmap is not supported for aio yet");
    return folly::makeFuture< bool >(false);
}

folly::Future< bool > AioDriveInterface::async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) {
    RELEASE_ASSERT(0, "async_write_zero is not supported for aio yet");
    return folly::makeFuture< bool >(false);
}

void AioDriveInterface::init_poll_interval_table() {
    s_poll_interval_table.clear();
    s_poll_interval_table.push_back(IM_DYNAMIC_CONFIG(poll.force_wakeup_by_time_ms));
    auto tight_loop_after_ios{IM_DYNAMIC_CONFIG(poll.tight_loop_after_io_max)};

    // TODO: For now we are not putting the decay factor, but eventually timeout
    // will start high for lower outstanding ios and decay towards max ios
    int t{s_poll_interval_table[0]};
    for (uint32_t i{1}; i < tight_loop_after_ios; ++i) {
        s_poll_interval_table.push_back(t);
    }
    s_poll_interval_table.push_back(0); // Tight loop from here on
}

/////////////////////////// aio_thread_context /////////////////////////////////////////////////
aio_thread_context::aio_thread_context() {
#ifdef __linux__
    m_ev_fd = eventfd(0, EFD_NONBLOCK);

    int err = io_setup(MAX_OUTSTANDING_IO, &m_ioctx);
    if (err) {
        LOGCRITICAL("io_setup failed with ret status {} errno {}", err, errno);
        folly::throwSystemError(fmt::format("io_setup failed with ret status {} errno {}", err, errno));
    }
#elif defined(__APPLE__)
#endif
}

aio_thread_context::~aio_thread_context() {
    int err = io_destroy(m_ioctx);
    if (err) { LOGERROR("io_destroy failed with ret status={} errno={}", err, errno); }
    close(m_ev_fd);
}

bool aio_thread_context::can_submit_io() const { return (m_submitted_ios < MAX_OUTSTANDING_IO); }

drive_aio_iocb* aio_thread_context::prep_iocb(DriveInterface* iface, IODevice* iodev, DriveOpType op_type, char* data,
                                              uint64_t size, uint64_t offset) {
    auto diocb = sisl::ObjectAllocator< drive_aio_iocb >::make_object(iface, iodev, op_type, size, offset);
    diocb->set_data(data);
    auto* kernel_iocb = &diocb->kernel_iocb;

#ifdef __linux__
    if (op_type == DriveOpType::READ) {
        io_prep_pread(kernel_iocb, iodev->fd(), (void*)data, size, offset);
    } else if (op_type == DriveOpType::WRITE) {
        io_prep_pwrite(kernel_iocb, iodev->fd(), (void*)data, size, offset);
    }
    io_set_eventfd(kernel_iocb, m_ev_fd);
    kernel_iocb->data = diocb;
#elif defined(__APPLE__)
    kernel_iocb->aio_fildes = iodev->fd();
    kernel_iocb->aio_offset = offset;
    kernel_iocb->aio_buf = (void*)data;
    kernel_iocb->aio_nbytes = size;
#endif
    return diocb;
}

drive_aio_iocb* aio_thread_context::prep_iocb_v(DriveInterface* iface, IODevice* iodev, DriveOpType op_type,
                                                const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    auto diocb = sisl::ObjectAllocator< drive_aio_iocb >::make_object(iface, iodev, op_type, size, offset);
    diocb->set_iovs(iov, iovcnt);
    auto* kernel_iocb = &diocb->kernel_iocb;

    if (op_type == DriveOpType::READ) {
        io_prep_preadv(kernel_iocb, iodev->fd(), iov, iovcnt, offset);
    } else if (op_type == DriveOpType::WRITE) {
        io_prep_pwritev(kernel_iocb, iodev->fd(), iov, iovcnt, offset);
    }
    io_set_eventfd(kernel_iocb, m_ev_fd);
    kernel_iocb->data = diocb;
    return diocb;
}

bool aio_thread_context::add_to_batch(drive_aio_iocb* diocb) {
    m_iocb_batch[m_cur_batch_size++] = &diocb->kernel_iocb;
    return (m_cur_batch_size == max_batch_iocb_count);
}

void aio_thread_context::inc_submitted_aio(int count) {
    if (count < 0) { return; }
    m_submitted_ios += count;

    iomanager.this_reactor()->set_poll_interval(m_submitted_ios >= AioDriveInterface::s_poll_interval_table.size()
                                                    ? 0
                                                    : AioDriveInterface::s_poll_interval_table[m_submitted_ios]);
}

void aio_thread_context::dec_submitted_aio() {
    --m_submitted_ios;
    iomanager.this_reactor()->set_poll_interval((m_submitted_ios >= AioDriveInterface::s_poll_interval_table.size())
                                                    ? 0
                                                    : AioDriveInterface::s_poll_interval_table[m_submitted_ios]);
}

void aio_thread_context::reset_batch() { m_cur_batch_size = 0; }

drive_aio_iocb* aio_thread_context::to_drive_iocb(kernel_iocb_t* kiocb) {
#ifdef __linux__
    return r_cast< drive_aio_iocb* >(kiocb->data);
#else
#endif
}
} // namespace iomgr

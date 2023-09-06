/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Harihara Kadayam, Yaming Kuang
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
#include "interfaces/uring_drive_interface.hpp"
#include <iomgr/iomgr.hpp>

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif
#include <folly/Exception.h>
#include "iomgr_config.hpp"
#include <sisl/fds/obj_allocator.hpp>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

#ifdef __linux__
#include <sys/epoll.h>
#include <linux/version.h>
#endif

#include <sisl/fds/utils.hpp>
#include <sisl/logging/logging.h>
#include "epoll/reactor_epoll.hpp"

namespace iomgr {
thread_local uring_drive_channel* UringDriveInterface::t_uring_ch{nullptr};

uring_drive_channel::uring_drive_channel(UringDriveInterface* iface) {
    int ret = io_uring_queue_init(UringDriveInterface::per_thread_qdepth, &m_ring, 0);
    if (ret) { folly::throwSystemError(fmt::format("Unable to create uring queue created ret={}", ret)); }

    int ev_fd = eventfd(0, EFD_NONBLOCK);
    if (ev_fd == -1) { folly::throwSystemError("Unable to create eventfd to listen for uring queue events"); }

    ret = io_uring_register_eventfd(&m_ring, ev_fd);
    if (ret == -1) { folly::throwSystemError("Unable to register event fd to uring queue"); }

    // Create io device and add it local thread
    using namespace std::placeholders;
    m_ring_ev_iodev = iomanager.generic_interface()->make_io_device(
        backing_dev_t(ev_fd), EPOLLIN, 0, nullptr, true,
        std::bind(&UringDriveInterface::on_event_notification, iface, _1, _2, _3));
    iomanager.this_reactor()->attach_iomgr_sentinel_cb([iface]() { iface->handle_completions(); });
}

uring_drive_channel::~uring_drive_channel() {
    io_uring_queue_exit(&m_ring);
    if (m_ring_ev_iodev != nullptr) {
        iomanager.this_reactor()->detach_iomgr_sentinel_cb();
        iomanager.generic_interface()->remove_io_device(m_ring_ev_iodev);
        close(m_ring_ev_iodev->fd());
    }
}

struct io_uring_sqe* uring_drive_channel::get_sqe_or_enqueue(drive_iocb* iocb) {
    if (!can_submit()) {
        m_iocb_waitq.push(iocb);
        return nullptr;
    }
    struct io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
    if (!sqe) {
        // No available slots. Before enqueing we submit ios which were added as part of batch processing.
        submit_ios();
        m_iocb_waitq.push(iocb);
        return nullptr;
    }

    return sqe;
}

void uring_drive_channel::submit_ios() {
    if (m_prepared_ios != 0) {
        const auto ret = io_uring_submit(&m_ring);
        if (static_cast< int >(m_prepared_ios) < ret) {
            DEBUG_ASSERT(false, "prepared ios must be always equal or greater than just-submitted ios");
        }
        DEBUG_ASSERT_GT(ret, 0, "Facing an error in io_uring_submit");
        m_in_flight_ios += ret;

        m_prepared_ios -= ret;
    }
}

void uring_drive_channel::submit_if_needed(drive_iocb* iocb, struct io_uring_sqe* sqe, bool part_of_batch) {
    io_uring_sqe_set_data(sqe, (void*)iocb);
    ++m_prepared_ios;
    if (!part_of_batch) { submit_ios(); }
}

static void prep_sqe_from_iocb(drive_iocb* iocb, struct io_uring_sqe* sqe) {
    switch (iocb->op_type) {
    case DriveOpType::WRITE:
        if (iocb->has_iovs()) {
            io_uring_prep_writev(sqe, iocb->iodev->fd(), iocb->get_iovs(), iocb->iovcnt, iocb->offset);
        } else {
            io_uring_prep_write(sqe, iocb->iodev->fd(), (const void*)iocb->get_data(), iocb->size, iocb->offset);
        }
        break;

    case DriveOpType::READ:
        if (iocb->has_iovs()) {
            io_uring_prep_readv(sqe, iocb->iodev->fd(), iocb->get_iovs(), iocb->iovcnt, iocb->offset);
        } else {
            io_uring_prep_read(sqe, iocb->iodev->fd(), (void*)iocb->get_data(), iocb->size, iocb->offset);
        }
        break;

    case DriveOpType::FSYNC:
        io_uring_prep_fsync(sqe, iocb->iodev->fd(), IORING_FSYNC_DATASYNC);
        break;

    default:
        break;
    }
}

bool uring_drive_channel::can_submit() const {
    return (m_in_flight_ios + m_prepared_ios) <= UringDriveInterface::per_thread_qdepth;
}

void uring_drive_channel::drain_waitq() {
    while (m_iocb_waitq.size() != 0) {
        if (!can_submit()) { break; };
        struct io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
        if (sqe == nullptr) {
            DEBUG_ASSERT(false, "Don't expect sqe to be full or unavailable");
            return;
        };

        drive_iocb* iocb = pop_waitq();
        prep_sqe_from_iocb(iocb, sqe);
        submit_if_needed(iocb, sqe, false /* batch */);
    }
}

///////////////////////////// UringDriveInterface /////////////////////////////////////////
UringDriveInterface::UringDriveInterface(const bool new_interface_supported, const io_interface_comp_cb_t& cb) :
        KernelDriveInterface(cb), m_new_intfc(new_interface_supported) {}

void UringDriveInterface::init_iface_reactor_context(IOReactor*) {
    if (t_uring_ch == nullptr) { t_uring_ch = new uring_drive_channel(this); }
}

void UringDriveInterface::clear_iface_reactor_context(IOReactor*) {
    if (t_uring_ch != nullptr) {
        delete t_uring_ch;
        t_uring_ch = nullptr;
    }
}

io_device_ptr UringDriveInterface::open_dev(const std::string& devname, drive_type dev_type, int oflags) {
    LOGMSG_ASSERT(((dev_type == drive_type::block_nvme) || (dev_type == drive_type::block_hdd) ||
                   (dev_type == drive_type::file_on_hdd) || (dev_type == drive_type::file_on_nvme)),
                  "Unexpected dev type to open {}", dev_type);
    init_write_zero_buf(devname, dev_type);

    auto fd = open(devname.c_str(), oflags, 0640);
    if (fd == -1) {
        folly::throwSystemError(fmt::format("Unable to open the device={} dev_type={}, errno={} strerror={}", devname,
                                            dev_type, errno, strerror(errno)));
        return nullptr;
    }

    auto iodev = alloc_io_device(backing_dev_t(fd), 9 /* pri */, reactor_regex::all_io);
    iodev->devname = devname;
    iodev->creator = iomanager.am_i_io_reactor() ? iomanager.iofiber_self() : nullptr;
    iodev->dtype = dev_type;

    // We don't need to add the device to each thread, because each AioInterface thread context add an
    // event fd and read/write use this device fd to control with iocb.
    LOGINFOMOD(iomgr, "Device={} of type={} opened with flags={} successfully, fd={}", devname, dev_type, oflags, fd);
    return iodev;
}

void UringDriveInterface::close_dev(const io_device_ptr& iodev) {
    // TODO: This is where we would wait for any outstanding io's to complete

    IOInterface::close_dev(iodev);
    // reset counters
    LOGINFOMOD(iomgr, "Device {} close device", iodev->devname);
    // AIO base devices are not added to any poll list, so it can be closed as is.
    close(iodev->fd());
    iodev->clear();
}

folly::Future< std::error_code > UringDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size,
                                                                  uint64_t offset, bool part_of_batch) {
    if (!m_new_intfc) {
        std::array< iovec, 1 > iov;
        iov[0].iov_base = (void*)data;
        iov[0].iov_len = size;

        return async_writev(iodev, iov.data(), 1, size, offset, part_of_batch);
    } else {
        // io_uring_prep_write available starts from kernel 5.6
        auto iocb = new drive_iocb(this, iodev, DriveOpType::WRITE, size, offset);
        iocb->set_data((char*)data);
        iocb->completion = std::move(folly::Promise< std::error_code >{});
        auto ret = iocb->folly_comp_promise().getFuture();

        auto submit_in_this_thread = [this](drive_iocb* iocb, bool part_of_batch) {
            DriveInterface::increment_outstanding_counter(iocb);
            auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
            if (sqe == nullptr) { return; }

            io_uring_prep_write(sqe, iocb->iodev->fd(), (const void*)iocb->get_data(), iocb->size, iocb->offset);
            t_uring_ch->submit_if_needed(iocb, sqe, part_of_batch);
        };

        if (iomanager.this_reactor() != nullptr) {
            submit_in_this_thread(iocb, part_of_batch);
        } else {
            iomanager.run_on_forget(reactor_regex::random_worker,
                                    [=]() { submit_in_this_thread(iocb, part_of_batch); });
        }

        return ret;
    }
}

folly::Future< std::error_code > UringDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt,
                                                                   uint32_t size, uint64_t offset, bool part_of_batch) {
    auto iocb = new drive_iocb(this, iodev, DriveOpType::WRITE, size, offset);
    iocb->set_iovs(iov, iovcnt);
    iocb->completion = std::move(folly::Promise< std::error_code >{});
    auto ret = iocb->folly_comp_promise().getFuture();

    auto submit_in_this_thread = [this](drive_iocb* iocb, bool part_of_batch) {
        DriveInterface::increment_outstanding_counter(iocb);
        auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
        if (sqe == nullptr) { return; }

        io_uring_prep_writev(sqe, iocb->iodev->fd(), iocb->get_iovs(), iocb->iovcnt, iocb->offset);
        t_uring_ch->submit_if_needed(iocb, sqe, part_of_batch);
    };

    if (iomanager.this_reactor() != nullptr) {
        submit_in_this_thread(iocb, part_of_batch);
    } else {
        iomanager.run_on_forget(reactor_regex::random_worker, [=]() { submit_in_this_thread(iocb, part_of_batch); });
    }
    return ret;
}

folly::Future< std::error_code > UringDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size,
                                                                 uint64_t offset, bool part_of_batch) {
    if (!m_new_intfc) {
        std::array< iovec, 1 > iov;
        iov[0].iov_base = data;
        iov[0].iov_len = size;

        return async_readv(iodev, iov.data(), 1, size, offset, part_of_batch);
    } else {
        auto iocb = new drive_iocb(this, iodev, DriveOpType::READ, size, offset);
        iocb->set_data(data);
        iocb->completion = std::move(folly::Promise< std::error_code >{});
        auto ret = iocb->folly_comp_promise().getFuture();

        auto submit_in_this_thread = [this](drive_iocb* iocb, bool part_of_batch) {
            DriveInterface::increment_outstanding_counter(iocb);
            auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
            if (sqe == nullptr) { return; }

            io_uring_prep_read(sqe, iocb->iodev->fd(), (void*)iocb->get_data(), iocb->size, iocb->offset);
            t_uring_ch->submit_if_needed(iocb, sqe, part_of_batch);
        };

        if (iomanager.this_reactor() != nullptr) {
            submit_in_this_thread(iocb, part_of_batch);
        } else {
            iomanager.run_on_forget(reactor_regex::random_worker,
                                    [=]() { submit_in_this_thread(iocb, part_of_batch); });
        }
        return ret;
    }
}

folly::Future< std::error_code > UringDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt,
                                                                  uint32_t size, uint64_t offset, bool part_of_batch) {
    auto iocb = new drive_iocb(this, iodev, DriveOpType::READ, size, offset);
    iocb->set_iovs(iov, iovcnt);
    iocb->completion = std::move(folly::Promise< std::error_code >{});
    auto ret = iocb->folly_comp_promise().getFuture();

    auto submit_in_this_thread = [this](drive_iocb* iocb, bool part_of_batch) {
        DriveInterface::increment_outstanding_counter(iocb);
        auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
        if (sqe == nullptr) { return; }

        io_uring_prep_readv(sqe, iocb->iodev->fd(), iocb->get_iovs(), iocb->iovcnt, iocb->offset);
        t_uring_ch->submit_if_needed(iocb, sqe, part_of_batch);
    };

    if (iomanager.this_reactor() != nullptr) {
        submit_in_this_thread(iocb, part_of_batch);
    } else {
        iomanager.run_on_forget(reactor_regex::random_worker, [=]() { submit_in_this_thread(iocb, part_of_batch); });
    }
    return ret;
}

folly::Future< std::error_code > UringDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset,
                                                                  bool part_of_batch) {
    RELEASE_ASSERT(0, "async_unmap is not supported for uring yet");
    return folly::makeFuture< std::error_code >(std::error_code(ENOTSUP, std::system_category()));
}

folly::Future< std::error_code > UringDriveInterface::async_write_zero(IODevice* iodev, uint64_t size,
                                                                       uint64_t offset) {
    LOGWARN("Uring async_write_zero is implemented as sync write, need to have more intelligent implementation");
    auto ret = sync_write_zero(iodev, size, offset);
    return folly::makeFuture< std::error_code >(std::move(ret));
}

folly::Future< std::error_code > UringDriveInterface::queue_fsync(IODevice* iodev) {
    auto iocb = new drive_iocb(this, iodev, DriveOpType::FSYNC, 0, 0);
    iocb->completion = std::move(folly::Promise< std::error_code >{});
    auto ret = iocb->folly_comp_promise().getFuture();

    auto submit_in_this_thread = [this](drive_iocb* iocb) {
        DriveInterface::increment_outstanding_counter(iocb);
        auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
        if (sqe == nullptr) { return; }

        io_uring_prep_fsync(sqe, iocb->iodev->fd(), IORING_FSYNC_DATASYNC);
        t_uring_ch->submit_if_needed(iocb, sqe, false /* batching */);
    };

    if (iomanager.this_reactor() != nullptr) {
        submit_in_this_thread(iocb);
    } else {
        iomanager.run_on_forget(reactor_regex::random_worker, [=]() { submit_in_this_thread(iocb); });
    }
    return ret;
}

std::error_code UringDriveInterface::sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) {
    if (!iomanager.am_i_sync_io_capable() || (t_uring_ch == nullptr) || !t_uring_ch->can_submit()) {
        return KernelDriveInterface::sync_write(iodev, data, size, offset);
    }

    if (!m_new_intfc) {
        std::array< iovec, 1 > iov;
        iov[0].iov_base = (void*)data;
        iov[0].iov_len = size;

        return sync_writev(iodev, iov.data(), 1, size, offset);
    } else {
        auto iocb = new drive_iocb(this, iodev, DriveOpType::WRITE, size, offset);
        iocb->set_data((char*)data);
        iocb->completion = std::move(FiberManagerLib::Promise< std::error_code >{});
        auto f = iocb->fiber_comp_promise().getFuture();

        DriveInterface::increment_outstanding_counter(iocb);
        auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
        assert(sqe);

        io_uring_prep_write(sqe, iodev->fd(), (const void*)iocb->get_data(), iocb->size, offset);
        t_uring_ch->submit_if_needed(iocb, sqe, false);
        return f.get();
    }
}

std::error_code UringDriveInterface::sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                 uint64_t offset) {
    if (!iomanager.am_i_sync_io_capable() || (t_uring_ch == nullptr) || !t_uring_ch->can_submit()) {
        return KernelDriveInterface::sync_writev(iodev, iov, iovcnt, size, offset);
    }

    auto iocb = new drive_iocb(this, iodev, DriveOpType::WRITE, size, offset);
    iocb->set_iovs(iov, iovcnt);
    iocb->completion = std::move(FiberManagerLib::Promise< std::error_code >{});
    auto f = iocb->fiber_comp_promise().getFuture();

    DriveInterface::increment_outstanding_counter(iocb);
    auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
    assert(sqe);

    io_uring_prep_writev(sqe, iodev->fd(), iocb->get_iovs(), iocb->iovcnt, offset);
    t_uring_ch->submit_if_needed(iocb, sqe, false);
    return f.get();
}

std::error_code UringDriveInterface::sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) {
    if (!iomanager.am_i_sync_io_capable() || (t_uring_ch == nullptr) || !t_uring_ch->can_submit()) {
        return KernelDriveInterface::sync_read(iodev, data, size, offset);
    }

    if (!m_new_intfc) {
        std::array< iovec, 1 > iov;
        iov[0].iov_base = data;
        iov[0].iov_len = size;

        return sync_readv(iodev, iov.data(), 1, size, offset);
    } else {
        auto iocb = new drive_iocb(this, iodev, DriveOpType::READ, size, offset);
        iocb->set_data(data);
        iocb->completion = std::move(FiberManagerLib::Promise< std::error_code >{});
        auto f = iocb->fiber_comp_promise().getFuture();

        DriveInterface::increment_outstanding_counter(iocb);
        auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
        assert(sqe);

        io_uring_prep_read(sqe, iodev->fd(), (void*)iocb->get_data(), iocb->size, offset);
        t_uring_ch->submit_if_needed(iocb, sqe, false);
        return f.get();
    }
}

std::error_code UringDriveInterface::sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                uint64_t offset) {
    if (!iomanager.am_i_sync_io_capable() || (t_uring_ch == nullptr) || !t_uring_ch->can_submit()) {
        return KernelDriveInterface::sync_readv(iodev, iov, iovcnt, size, offset);
    }
    auto iocb = new drive_iocb(this, iodev, DriveOpType::READ, size, offset);
    iocb->set_iovs(iov, iovcnt);
    iocb->completion = std::move(FiberManagerLib::Promise< std::error_code >{});
    auto f = iocb->fiber_comp_promise().getFuture();

    DriveInterface::increment_outstanding_counter(iocb);
    auto sqe = t_uring_ch->get_sqe_or_enqueue(iocb);
    assert(sqe);

    io_uring_prep_readv(sqe, iodev->fd(), iocb->get_iovs(), iocb->iovcnt, offset);
    t_uring_ch->submit_if_needed(iocb, sqe, false);
    return f.get();
}

void UringDriveInterface::submit_batch() { t_uring_ch->submit_ios(); }

void UringDriveInterface::on_event_notification(IODevice* iodev, [[maybe_unused]] void* cookie,
                                                [[maybe_unused]] int event) {
    uint64_t temp = 0;
    [[maybe_unused]] auto rsize = read(iodev->fd(), &temp, sizeof(uint64_t));
    LOGTRACEMOD(iomgr, "Received completion on ev_fd = {}", iodev->fd());
    handle_completions();
}

void UringDriveInterface::handle_completions() {
    do {
        struct io_uring_cqe* cqe;
        int ret = io_uring_peek_cqe(&t_uring_ch->m_ring, &cqe);
        if (sisl_unlikely(*(t_uring_ch->m_ring.cq.koverflow))) {
            COUNTER_INCREMENT(m_metrics, overflow_errors, 1);
            COUNTER_INCREMENT(m_metrics, num_of_drops, *(t_uring_ch->m_ring.cq.koverflow));
            folly::throwSystemError(fmt::format("CQ overflow - number of dropped io requests : {} - {}",
                                                *(t_uring_ch->m_ring.cq.koverflow), strerror(errno)));
            break;
        }
        if (sisl_unlikely(ret < 0)) {
            if (ret != -EAGAIN) {
                COUNTER_INCREMENT(m_metrics, completion_errors, 1);
                folly::throwSystemError(fmt::format("io_uring_wait_cqe throw error={}", strerror(errno)));
            } else {
                LOGTRACEMOD(iomgr, "Received EAGAIN on uring peek cqe");
            }
            break;
        }
        if (cqe == nullptr) { break; }

        auto iocb = (drive_iocb*)io_uring_cqe_get_data(cqe);
        iocb->result = cqe->res;
        io_uring_cqe_seen(&t_uring_ch->m_ring, cqe);

        // Don't access cqe beyond this point.
        if (sisl_likely(iocb->result >= 0)) {
            if (sisl_likely(static_cast< uint64_t >(iocb->result) == iocb->size)) {
                // all read buffer is filled by uring;
                LOGDEBUGMOD(iomgr, "Received completion event, iocb={} Result={}", iocb->to_string(), iocb->result);
                complete_io(iocb);
            } else {
                // ***** Paritial Read Handling ******** //
                LOGDEBUGMOD(iomgr, "Received completion event with partial result, iocb={} size={} Result={}, retry={}",
                            (void*)iocb, iocb->size, iocb->result, iocb->resubmit_cnt);
                if (iocb->part_read_resubmit_cnt++ > IM_DYNAMIC_CONFIG(drive.partial_read_max_resubmit_cnt)) {
                    LOGMSG_ASSERT(false, "Don't expect partial read to exceed retry limit={}",
                                  IM_DYNAMIC_CONFIG(drive.partial_read_max_resubmit_cnt));
                    // in production, keep retrying until we get all the data;
                }

                COUNTER_INCREMENT(m_metrics, retry_on_partial_read, 1);
                iocb->update_iovs_on_partial_result();
                // retry I/O with remaining unset data;
                t_uring_ch->m_iocb_waitq.push(iocb);
                --(t_uring_ch->m_in_flight_ios);
            }
        } else {
            LOGERRORMOD(iomgr, "Error in completion of io, iocb={}, result={}, retry={}", (void*)iocb, iocb->result,
                        iocb->resubmit_cnt);
            if ((iocb->result != -EAGAIN) && iocb->resubmit_cnt++ > IM_DYNAMIC_CONFIG(drive.max_resubmit_cnt)) {
                // EAGAIN won't increase resubmit_cnt;
                DEBUG_ASSERT(false, "Don't expect op={} retry exceed limit={}", iocb->op_type,
                             IM_DYNAMIC_CONFIG(drive.max_resubmit_cnt));
                complete_io(iocb);
            } else {
                // if disk driver return EAGAIN, keep retrying unconditionally;
                // Retry IO by pushing it to waitq which will get scheduled later.
                t_uring_ch->m_iocb_waitq.push(iocb);
            }
        }
        t_uring_ch->drain_waitq();
    } while (true);
}

void UringDriveInterface::complete_io(drive_iocb* iocb) {
    --(t_uring_ch->m_in_flight_ios);

#ifdef _PRERELEASE
    if (DriveInterface::inject_delay_if_needed(iocb, [this](drive_iocb* iocb) { complete_io(iocb); })) { return; }
#endif

    if (sisl_likely(iocb->result >= 0)) {
        static std::error_code success;
        std::visit(overloaded{[&](folly::Promise< std::error_code >& p) { p.setValue(success); },
                              [&](FiberManagerLib::Promise< std::error_code >& p) { p.setValue(success); },
                              [&](io_interface_comp_cb_t& cb) { cb(iocb->result); }},
                   iocb->completion);
    } else {
        std::visit(overloaded{[&](folly::Promise< std::error_code >& p) {
                                  p.setValue(std::error_code{int_cast(-iocb->result), std::system_category()});
                              },
                              [&](FiberManagerLib::Promise< std::error_code >& p) {
                                  p.setValue(std::error_code{int_cast(-iocb->result), std::system_category()});
                              },
                              [&](io_interface_comp_cb_t& cb) { cb(iocb->result); }},
                   iocb->completion);
    }
    DriveInterface::decrement_outstanding_counter(iocb);
    delete iocb;
}
} // namespace iomgr

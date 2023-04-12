/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Harihara Kadayam
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
#include <filesystem>
#include <thread>

#include <sisl/fds/buffer.hpp>
#include <sisl/fds/obj_allocator.hpp>
#include <sisl/logging/logging.h>

// TODO: Remove this once the problem is fixed in folly
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattribute-warning"
#endif
#include <folly/Exception.h>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

// TODO: Remove this once the problem is fixed in flip
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <sisl/flip/flip.hpp>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

extern "C" {
#include <spdk/env.h>
#include <spdk/thread.h>
#include <spdk/string.h>
#ifdef SPDK_DRIVE_USE_URING
#include <spdk/module/bdev/uring/bdev_uring.h>
#else
#include <spdk/module/bdev/aio/bdev_aio.h>
#endif
#include <spdk/module/bdev/nvme/bdev_nvme.h>
#include <spdk/nvme.h>
}

#include <iomgr/iomgr.hpp>
#include "spdk/reactor_spdk.hpp"
#include "interfaces/spdk_drive_interface.hpp"
#include "watchdog.hpp"

using namespace std::chrono_literals;

namespace iomgr {

#ifndef NDEBUG
std::atomic< uint64_t > drive_iocb::_iocb_id_counter{0};
#endif

SpdkDriveInterface::SpdkDriveInterface(const io_interface_comp_cb_t& cb) : DriveInterface(cb) {}

static void bdev_event_cb(enum spdk_bdev_event_type type, spdk_bdev* bdev, void* event_ctx) {}

struct creat_ctx {
    std::string address;
    drive_type addr_type;
    std::error_condition err;
    std::array< const char*, 128 > names;
    bool is_done{false};
    std::string bdev_name;
    io_fiber_t creator;
    std::mutex mtx;
    std::condition_variable cv;

    void done() {
        {
            std::unique_lock lg{mtx};
            is_done = true;
        }
        cv.notify_all();
    }

    void wait() {
        std::unique_lock lg{mtx};
        cv.wait(lg, [this]() { return is_done; });
    }
};

static void create_fs_bdev(const std::shared_ptr< creat_ctx >& ctx) {
    iomanager.run_on_forget(reactor_regex::least_busy_worker, [ctx]() {
        LOGINFO("Opening {} as an SPDK drive, creating a bdev out of the file, performance will be impacted",
                ctx->address);
        auto const bdev_name = ctx->address + std::string("_bdev");

        DEBUG_ASSERT_EQ((void*)spdk_bdev_get_by_name(bdev_name.c_str()), nullptr);
#ifdef SPDK_DRIVE_USE_URING
        auto bdev = create_uring_bdev(bdev_name.c_str(), ctx->address.c_str(), 512u);
        if (bdev == nullptr) {
            folly::throwSystemError(fmt::format("Unable to open the device={} to create bdev error", bdev_name));
        }
#else
            const int ret{create_aio_bdev(bdev_name.c_str(), ctx->address.c_str(), 512u)};
            if (ret != 0) {
                folly::throwSystemError(
                    fmt::format("Unable to open the device={} to create bdev error={}", bdev_name, ret));
            }
#endif
        ctx->bdev_name = bdev_name;
        ctx->creator = iomanager.iofiber_self();
        ctx->done();
    });
    ctx->wait();
}

static void create_bdev_done(void* cb_ctx, size_t bdev_cnt, int rc) {
    auto* ctx = static_cast< creat_ctx* >(cb_ctx);
    LOGDEBUGMOD(iomgr, "Volume setup for {} received: [rc={}, cnt={}]", ctx->address, rc, bdev_cnt);
    if (0 < bdev_cnt) {
        ctx->bdev_name = std::string{ctx->names[--bdev_cnt]};
        LOGDEBUGMOD(iomgr, "Created BDev: [{}]", ctx->bdev_name);
    }
    ctx->done();
}

static void create_nvme_bdev(const std::shared_ptr< creat_ctx >& ctx) {
    iomanager.run_on_forget(reactor_regex::least_busy_worker, [ctx]() {
        auto address_c = ctx->address.c_str();
        spdk_nvme_transport_id trid;
        std::memset(&trid, 0, sizeof(trid));
        trid.trtype = SPDK_NVME_TRANSPORT_PCIE;

        auto rc = spdk_nvme_transport_id_parse(&trid, address_c);
        if (rc < 0) {
            LOGERROR("Failed to parse given str: {}", address_c);
            ctx->err = std::make_error_condition(std::errc::io_error);
            ctx->done();
            return;
        }

        if (trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
            spdk_pci_addr pci_addr;
            if (spdk_pci_addr_parse(&pci_addr, trid.traddr) < 0) {
                LOGERROR("Invalid traddr={}", address_c);
                ctx->err = std::make_error_condition(std::errc::io_error);
                ctx->done();
                return;
            }
            spdk_pci_addr_fmt(trid.traddr, sizeof(trid.traddr), &pci_addr);
        } else {
            if (trid.subnqn[0] == '\0') { snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN); }
        }

        ctx->creator = iomanager.iofiber_self();

        /* Enumerate all of the controllers */
        spdk_nvme_host_id hostid{};
        if (rc = bdev_nvme_create(&trid, &hostid, "iomgr", ctx->names.data(), ctx->names.size(), nullptr, 0,
                                  create_bdev_done, ctx.get(), nullptr);
            0 != rc) {
            LOGERROR("Failed creating NVMe BDEV from {}, error_code: {}", trid.traddr, rc);
            ctx->err = std::make_error_condition(std::errc::io_error);
            ctx->done();
            return;
        }
    });

    ctx->wait();
}

static void create_dev_internal(const std::shared_ptr< creat_ctx >& ctx) {
    // Check if the device is a file, if so create a bdev out of the file and open that bdev. This is meant only for
    // a testing and docker environment
    try {
        switch (ctx->addr_type) {
        case drive_type::raw_nvme:
            create_nvme_bdev(ctx);
            break;

        case drive_type::file_on_nvme:
        case drive_type::block_nvme:
            create_fs_bdev(ctx);
            break;

        case drive_type::spdk_bdev:
            // Already a bdev, set the ctx as done
            ctx->bdev_name = ctx->address;
            ctx->creator = iomanager.iofiber_self();
            ctx->done();
            break;

        default:
            LOGMSG_ASSERT(0, "Unsupported device={} of type={} being opened", ctx->address, enum_name(ctx->addr_type));
            ctx->err = std::make_error_condition(std::errc::bad_file_descriptor);
        }
    } catch (std::exception& e) {
        if (!ctx->err) {
            LOGMSG_ASSERT(0, "Error in creating bdev for device={} of type={}, Exception={}", ctx->address,
                          enum_name(ctx->addr_type), e.what());
            ctx->err = std::make_error_condition(std::errc::bad_file_descriptor);
        }
    }
}

io_device_ptr SpdkDriveInterface::open_dev(const std::string& devname, drive_type drive_type,
                                           [[maybe_unused]] int oflags) {
    std::unique_lock lg{m_opened_dev_mtx};
    io_device_ptr iodev{nullptr};
    auto it = m_opened_device.find(devname);
    if ((it == m_opened_device.end()) || (it->second == nullptr)) {
        iodev = create_open_dev_internal(devname, drive_type);
        m_opened_device.insert(std::make_pair<>(devname, iodev));
    } else {
        iodev = it->second;
        if (!iodev->ready) { // Closed before, reopen it
            open_dev_internal(iodev);
        }
    }
#ifdef REFCOUNTED_OPEN_DEV
    iodev->opened_count.increment(1);
#endif

    return iodev;
}

io_device_ptr SpdkDriveInterface::create_open_dev_internal(const std::string& devname, drive_type drive_type) {
    io_device_ptr iodev{nullptr};

    // First create the bdev
    const auto ctx{std::make_shared< creat_ctx >()};
    ctx->address = devname;
    ctx->addr_type = drive_type;

    RELEASE_ASSERT((!iomanager.am_i_worker_reactor() || (ctx->addr_type == drive_type::spdk_bdev)),
                   "We cannot open the device from a worker reactor thread unless its a bdev");
    create_dev_internal(ctx);

    if (!ctx->bdev_name.empty() && !ctx->err) {
        iodev = alloc_io_device(null_backing_dev(), 9 /* pri*/, reactor_regex::all_io);
        iodev->devname = devname;
        iodev->alias_name = ctx->bdev_name;
        iodev->creator = ctx->creator;
        open_dev_internal(iodev);
    }
    return iodev;
}

void SpdkDriveInterface::open_dev_internal(const io_device_ptr& iodev) {
    int rc{-1};
    iomanager.run_on_wait(iodev->creator, [this, iodev, &rc]() {
        spdk_bdev_desc* desc{nullptr};
        rc = spdk_bdev_open_ext(iodev->alias_name.c_str(), true, bdev_event_cb, nullptr, &desc);
        if (rc == 0) { iodev->dev = backing_dev_t(desc); }
    });

    if (rc != 0) {
        folly::throwSystemError(fmt::format("Unable to open the device={} error={}", iodev->alias_name, rc));
    }

    // reset counters
    m_outstanding_async_ios = 0;

    // Set the bdev to split on underlying device io boundary.
    auto* bdev = spdk_bdev_get_by_name(iodev->alias_name.c_str());
    if (!bdev) { folly::throwSystemError(fmt::format("Unable to get opened device={}", iodev->alias_name)); }
    bdev->split_on_optimal_io_boundary = true;

    add_io_device(iodev, true /* wait_to_add*/);
    LOGINFOMOD(iomgr, "Device {} bdev_name={} opened successfully", iodev->devname, iodev->alias_name);
}

void SpdkDriveInterface::close_dev(const io_device_ptr& iodev) {
#ifdef REFCOUNTED_OPEN_DEV
    if (!iodev->opened_count.decrement_testz()) { return; }
#endif
    // TODO: In the future might want to add atomic that will block any new read/write access to device that occur
    // after the close is called

    // check if current thread is reactor
    const auto& reactor = iomanager.this_reactor();
    const bool this_thread_reactor = reactor && reactor->is_io_reactor();
    auto* sthread = this_thread_reactor ? spdk_get_thread() : nullptr;

    // wait for outstanding IO's to complete
    LOGINFOMOD(iomgr, "Device {} bdev_name={} close device issued with {} outstanding ios", iodev->devname,
               iodev->alias_name, m_outstanding_async_ios);
    constexpr std::chrono::milliseconds max_wait_ms{5000};
    constexpr std::chrono::milliseconds wait_interval_ms{50};
    const auto start_time = std::chrono::steady_clock::now();
    while (m_outstanding_async_ios != 0) {
        std::this_thread::sleep_for(wait_interval_ms);
        if (sthread) spdk_thread_poll(sthread, 0, 0);
        const auto current_time{std::chrono::steady_clock::now()};
        if (std::chrono::duration_cast< std::chrono::milliseconds >(current_time - start_time).count() >=
            max_wait_ms.count()) {
            LOGERRORMOD(
                iomgr,
                "Device {} bdev_name={} close device timeout waiting for async io's to complete. IO's outstanding: {}",
                iodev->devname, iodev->alias_name, m_outstanding_async_ios);
            break;
        }
    }

    IOInterface::close_dev(iodev);
    DEBUG_ASSERT(iodev->creator != nullptr, "Expect creator of iodev to be non null");

    iomanager.run_on_wait(iodev->creator,
                          [bdev_desc = std::get< spdk_bdev_desc* >(iodev->dev)]() { spdk_bdev_close(bdev_desc); });

    iodev->clear();
}

drive_type SpdkDriveInterface::detect_drive_type(const std::string& devname) {
    /* Lets find out if it is a nvme transport */
    spdk_nvme_transport_id trid;
    std::memset(&trid, 0, sizeof(trid));
    auto* const devname_c{devname.c_str()};

    const auto rc{spdk_nvme_transport_id_parse(&trid, devname_c)};
    if (rc == 0) {
        // assume trid.trtype is PCIE, this if should be reverted after we remove dev_type from caller completely;
        return drive_type::raw_nvme;
    }

    if (spdk_bdev_get_by_name(devname_c)) { return drive_type::spdk_bdev; }

    return drive_type::unknown;
}

void SpdkDriveInterface::init_iface_reactor_context(IOReactor* reactor) {
    if (reactor->is_tight_loop_reactor() && reactor->is_adaptive_loop()) {
        // Allow backoff only if there are no outstanding operations.
        reactor->add_backoff_cb([](IOReactor* reactor) -> bool { return (reactor->m_metrics->outstanding_ops == 0); });
    }
}

void SpdkDriveInterface::init_iodev_reactor_context(const io_device_ptr& iodev, IOReactor* reactor) {
    if (!reactor->is_tight_loop_reactor()) { return; }

    auto orig_thread = spdk_get_thread();
    for (const auto& fiber : reactor->m_io_fibers) {
        spdk_set_thread(fiber->spdk_thr);
        auto dctx = std::make_unique< SpdkDriveDeviceContext >();
        dctx->channel = spdk_bdev_get_io_channel(iodev->bdev_desc());
        if (dctx->channel == NULL) {
            folly::throwSystemError(
                fmt::format("Unable to get io channel for bdev={}", spdk_bdev_get_name(iodev->bdev())));
        }
        {
            // This step ensures that sparse vector of m_iodev_fiber_ctx if need be expands under lock.
            // Once a slot is assigned, there is no contention
            std::unique_lock lk(iodev->m_ctx_init_mtx);
            iodev->m_iodev_fiber_ctx[fiber->ordinal] = std::move(dctx);
        }
    }
    spdk_set_thread(orig_thread);
}

void SpdkDriveInterface::clear_iodev_reactor_context(const io_device_ptr& iodev, IOReactor* reactor) {
    if (!reactor->is_tight_loop_reactor()) { return; }
    auto orig_thread = spdk_get_thread();
    for (const auto& fiber : reactor->m_io_fibers) {
        const auto* dctx = s_cast< SpdkDriveDeviceContext* >(iodev->m_iodev_fiber_ctx[fiber->ordinal].get());
        if (dctx && dctx->channel != NULL) {
            spdk_set_thread(fiber->spdk_thr);
            spdk_put_io_channel(dctx->channel);
        }
        iodev->m_iodev_fiber_ctx[fiber->ordinal].reset();
    }
    spdk_set_thread(orig_thread);
}

static spdk_io_channel* get_io_channel(IODevice* iodev) {
    auto reactor = iomanager.this_reactor();
    auto fiber = reactor->pick_fiber(fiber_regex::random);
    const auto* dctx = s_cast< SpdkDriveDeviceContext* >(iodev->m_iodev_fiber_ctx[fiber->ordinal].get());
    RELEASE_ASSERT_NOTNULL((void*)dctx, "Null SpdkDriveDeviceContext for reactor={} for iodev={}",
                           reactor->reactor_idx(), iodev->devname);
    spdk_set_thread(fiber->spdk_thr);
    return dctx->channel;
}

static void submit_io(void* b);
static bool resubmit_io_on_err(void* b) {
    SpdkIocb* iocb{static_cast< SpdkIocb* >(b)};
    if (iocb->resubmit_cnt > IM_DYNAMIC_CONFIG(drive.max_resubmit_cnt)) { return false; }
    ++iocb->resubmit_cnt;
    COUNTER_INCREMENT(iocb->iface->get_metrics(), resubmit_io_on_err, 1);
    submit_io(b);
    return true;
}

static std::string explain_bdev_io_status(spdk_bdev_io* bdev_io) {
    if (std::string(bdev_io->bdev->module->name) == "nvme") {
        uint32_t cdw0;
        int sct;
        int sc;
        spdk_bdev_io_get_nvme_status(bdev_io, &cdw0, &sct, &sc);
        return fmt::format("cdw0={} sct={}, sc={} internal_status={}", cdw0, sct, sc, bdev_io->internal.status);
    } else {
        return "unknown";
    }
}

static void complete_io(SpdkIocb* iocb, bool is_success) {
#ifdef _PRERELEASE
    if (DriveInterface::inject_delay_if_needed(
            iocb, [is_success](drive_iocb* iocb) { complete_io(r_cast< SpdkIocb* >(iocb), is_success); })) {
        return;
    }
#endif

    iomanager.this_thread_metrics().drive_latency_sum_us += get_elapsed_time_us(iocb->op_submit_time);
    iocb->owns_by_spdk = false;
    DriveInterface::decrement_outstanding_counter(iocb);

    if (is_success) {
        iocb->result = 0;
        std::visit(overloaded{[&](folly::Promise< bool >& p) { p.setValue(true); },
                              [&](FiberManagerLib::Promise< bool >& p) { p.setValue(true); },
                              [&](io_interface_comp_cb_t& cb) { cb(iocb->result); }},
                   iocb->completion);
    } else {
        COUNTER_INCREMENT(iocb->iface->get_metrics(), completion_errors, 1);
        if (resubmit_io_on_err(iocb)) { return; }
        iocb->result = -1;
        std::visit(
            overloaded{[&](folly::Promise< bool >& p) { p.setException(std::ios_base::failure{"spdk io failed"}); },
                       [&](FiberManagerLib::Promise< bool >& p) {
                           p.setException(std::make_exception_ptr(std::ios_base::failure{"spdk io failed"}));
                       },
                       [&](io_interface_comp_cb_t& cb) { cb(iocb->result); }},
            iocb->completion);
    }

    if (iomanager.get_io_wd()->is_on()) { iomanager.get_io_wd()->complete_io(iocb); }
    sisl::ObjectAllocator< SpdkIocb >::deallocate(iocb);
}

static void process_completions(spdk_bdev_io* bdev_io, bool is_success, void* ctx) {
    SpdkIocb* iocb{static_cast< SpdkIocb* >(ctx)};
    DEBUG_ASSERT_NOTNULL((void*)iocb->iodev->bdev_desc());

    // LOGDEBUGMOD(iomgr, "Received completion on bdev = {}", (void*)iocb->iodev->bdev_desc());
    spdk_bdev_free_io(bdev_io);

#ifdef _PRERELEASE
    const auto flip_resubmit_cnt{flip::Flip::instance().get_test_flip< uint32_t >("read_write_resubmit_io")};
    if (flip_resubmit_cnt != boost::none && iocb->resubmit_cnt < flip_resubmit_cnt) { is_success = false; }
#endif
    if (is_success) {
        LOGDEBUGMOD(iomgr, "(bdev_io={}) iocb complete: mode=actual, {}", (void*)bdev_io, iocb->to_string());
    } else {
        LOGERRORMOD(iomgr, "(bdev_io={}) iocb failed with status [{}]: mode=actual, {}", (void*)bdev_io,
                    explain_bdev_io_status(bdev_io), iocb->to_string());
    }
    complete_io(iocb, is_success);
}

static void submit_io(void* b) {
    SpdkIocb* iocb{static_cast< SpdkIocb* >(b)};
    int rc = 0;
    DEBUG_ASSERT_NOTNULL((void*)iocb->iodev->bdev_desc());

    DEBUG_ASSERT((iocb->owns_by_spdk == false), "Duplicate submission of iocb while io pending: {}", iocb->to_string());
    iocb->owns_by_spdk = true;

    iocb->op_submit_time = Clock::now();
    DriveInterface::increment_outstanding_counter(iocb);

    auto orig_thread = spdk_get_thread(); // get_io_channel sets the thread, use this to restore back

    LOGDEBUGMOD(iomgr, "iocb submit: mode=actual, {}", iocb->to_string());
    if (iocb->op_type == DriveOpType::READ) {
        if (iocb->has_iovs()) {
            rc = spdk_bdev_readv(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->get_iovs(), iocb->iovcnt,
                                 iocb->offset, iocb->size, process_completions, (void*)iocb);
        } else {
            rc = spdk_bdev_read(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->get_data(), iocb->offset,
                                iocb->size, process_completions, (void*)iocb);
        }
    } else if (iocb->op_type == DriveOpType::WRITE) {
        if (iocb->has_iovs()) {
            rc = spdk_bdev_writev(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->get_iovs(), iocb->iovcnt,
                                  iocb->offset, iocb->size, process_completions, (void*)iocb);
        } else {
            rc = spdk_bdev_write(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->get_data(), iocb->offset,
                                 iocb->size, process_completions, (void*)iocb);
        }
    } else if (iocb->op_type == DriveOpType::UNMAP) {
        rc = spdk_bdev_unmap(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->offset, iocb->size,
                             process_completions, (void*)iocb);
    } else if (iocb->op_type == DriveOpType::WRITE_ZERO) {
        rc = spdk_bdev_write_zeroes(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->offset, iocb->size,
                                    process_completions, (void*)iocb);
    } else {
        rc = -EOPNOTSUPP;
        LOGDFATAL("Invalid operation type {}", iocb->op_type);
    }
    spdk_set_thread(orig_thread);

    if (rc != 0) {
        // adjust count since unsuccessful command
        if (rc == -ENOMEM) {
            DriveInterface::decrement_outstanding_counter(iocb);
            LOGDEBUGMOD(iomgr, "Bdev is lacking memory to do IO right away, queueing iocb: {}", iocb->to_string());
            COUNTER_INCREMENT(iocb->iface->get_metrics(), queued_ios_for_memory_pressure, 1);
            spdk_bdev_queue_io_wait(iocb->iodev->bdev(), get_io_channel(iocb->iodev), &iocb->io_wait_entry);
            iocb->owns_by_spdk = false;
        } else {
            LOGERRORMOD(iomgr, "iocb {} submission failed with rc={}", iocb->to_string(), rc);
            complete_io(iocb, false /* is_success */);
        }
    }
}

static thread_local SpdkBatchIocb* s_batch_info_ptr = nullptr;

static void update_batch_counter(size_t batch_size) {
    auto& thread_metrics = iomanager.this_thread_metrics();
    thread_metrics.iface_io_actual_count += batch_size;
    ++thread_metrics.iface_io_batch_count;
}

void SpdkDriveInterface::submit_async_io(SpdkIocb* iocb, bool part_of_batch) {
    if (iomanager.am_i_tight_loop_reactor()) {
        LOGDEBUGMOD(iomgr, "iocb submit: mode=tloop, {}", iocb->to_string());
        update_batch_counter(1);
        submit_io(iocb);
    } else if (!part_of_batch) {
        LOGDEBUGMOD(iomgr, "iocb submit: mode=non_tloop, {}", iocb->to_string());
        COUNTER_INCREMENT(m_metrics, num_async_io_non_spdk_thread, 1);
        update_batch_counter(1);
        iomanager.run_on_forget(reactor_regex::least_busy_worker, [iocb, this]() {
            LOGDEBUGMOD(iomgr, "iocb submit: mode=queue_io, {}", iocb->to_string());
            submit_io(iocb);
        });
    } else {
        if (s_batch_info_ptr == nullptr) { s_batch_info_ptr = new SpdkBatchIocb(); }

        iocb->batch_info_ptr = s_batch_info_ptr;
        s_batch_info_ptr->batch_io->push_back(iocb);

        if (s_batch_info_ptr->batch_io->size() == IM_DYNAMIC_CONFIG(drive.num_batch_io_limit)) {
            // this batch is ready to be processed;
            submit_batch();
        }
    }

    if (iomanager.get_io_wd()->is_on()) { IOManager::instance().get_io_wd()->add_io(iocb); }
}

void SpdkDriveInterface::submit_batch() {
    // s_batch_info_ptr could be nullptr when client calls submit_batch
    if (s_batch_info_ptr) {
        update_batch_counter(s_batch_info_ptr->batch_io->size());

        auto const sent_to =
            iomanager.run_on_forget(reactor_regex::least_busy_worker, [batch_info = s_batch_info_ptr]() {
                for (auto& iocb : *(batch_info->batch_io)) {
                    LOGDEBUGMOD(iomgr, "iocb submit: mode=queue_batch_io, {}", iocb->to_string());
                    submit_io(iocb);
                }
            });

        if (sent_to == 0) {
            // if message is not delivered, release memory here;
            delete s_batch_info_ptr;
            LOGMSG_ASSERT(0, "multicast_msg returned failure");
        }

        // reset batch info ptr, memory will be freed by spdk thread after batch io completes;
        s_batch_info_ptr = nullptr;
    }
    // it will be null operation if client calls this function without anything in s_batch_info_ptr
}

void SpdkDriveInterface::submit_sync_io(SpdkIocb* iocb) {
    LOGDEBUGMOD(iomgr, "iocb submit: mode=sync, {}", iocb->to_string());

    iocb->completion = std::move(FiberManagerLib::Promise< bool >{});
    auto f = iocb->fiber_comp_promise().getFuture();
    submit_async_io(iocb, false /* part_of_batch */);
    f.get();
}

folly::Future< bool > SpdkDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset,
                                                      bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE, size, offset);
    iocb->set_data(const_cast< char* >(data));
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->completion = std::move(folly::Promise< bool >{});

    auto ret = iocb->folly_comp_promise().getFuture();
    submit_async_io(iocb, part_of_batch);
    return ret;
}

folly::Future< bool > SpdkDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset,
                                                     bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::READ, size, offset);
    iocb->set_data(data);
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->completion = std::move(folly::Promise< bool >{});

    auto ret = iocb->folly_comp_promise().getFuture();
    submit_async_io(iocb, part_of_batch);
    return ret;
}

folly::Future< bool > SpdkDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                       uint64_t offset, bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE, size, offset);
    iocb->set_iovs(iov, iovcnt);
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->completion = std::move(folly::Promise< bool >{});

    auto ret = iocb->folly_comp_promise().getFuture();
    submit_async_io(iocb, part_of_batch);
    return ret;
}

folly::Future< bool > SpdkDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                                      uint64_t offset, bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::READ, size, offset);
    iocb->set_iovs(iov, iovcnt);
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->completion = std::move(folly::Promise< bool >{});

    auto ret = iocb->folly_comp_promise().getFuture();
    submit_async_io(iocb, part_of_batch);
    return ret;
}

folly::Future< bool > SpdkDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset,
                                                      bool part_of_batch) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::UNMAP, size, offset);
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->completion = std::move(folly::Promise< bool >{});

    auto ret = iocb->folly_comp_promise().getFuture();
    submit_async_io(iocb, part_of_batch);
    return ret;
}

folly::Future< bool > SpdkDriveInterface::async_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) {
    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE_ZERO, size, offset);
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->completion = std::move(folly::Promise< bool >{});

    auto ret = iocb->folly_comp_promise().getFuture();
    submit_async_io(iocb, false);
    return ret;
}

void SpdkDriveInterface::sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) {
    DEBUG_ASSERT_EQ(iomanager.am_i_sync_io_capable(), true, "Sync io attempted not sync io capable thread");

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE, size, offset);
    iocb->set_data(const_cast< char* >(data));
    iocb->io_wait_entry.cb_fn = submit_io;
    submit_sync_io(iocb);
}

void SpdkDriveInterface::sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    DEBUG_ASSERT_EQ(iomanager.am_i_sync_io_capable(), true, "Sync io attempted not sync io capable thread");

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE, size, offset);
    iocb->set_iovs(iov, iovcnt);
    iocb->io_wait_entry.cb_fn = submit_io;
    submit_sync_io(iocb);
}

void SpdkDriveInterface::sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    DEBUG_ASSERT_EQ(iomanager.am_i_sync_io_capable(), true, "Sync io attempted not sync io capable thread");

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::READ, size, offset);
    iocb->set_data(data);
    iocb->io_wait_entry.cb_fn = submit_io;
    submit_sync_io(iocb);
}

void SpdkDriveInterface::sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    DEBUG_ASSERT_EQ(iomanager.am_i_sync_io_capable(), true, "Sync io attempted not sync io capable thread");

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::READ, size, offset);
    iocb->set_iovs(iov, iovcnt);
    iocb->io_wait_entry.cb_fn = submit_io;
    submit_sync_io(iocb);
}

void SpdkDriveInterface::sync_write_zero(IODevice* iodev, uint64_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    DEBUG_ASSERT_EQ(iomanager.am_i_sync_io_capable(), true, "Sync io attempted not sync io capable thread");

    SpdkIocb* iocb = sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE_ZERO, size, offset);
    iocb->io_wait_entry.cb_fn = submit_io;
    submit_sync_io(iocb);
}

size_t SpdkDriveInterface::get_dev_size(IODevice* iodev) {
    return spdk_bdev_get_num_blocks(iodev->bdev()) * spdk_bdev_get_block_size(iodev->bdev());
}

drive_attributes SpdkDriveInterface::get_attributes(const io_device_ptr& dev) const {
    DEBUG_ASSERT_EQ(dev->is_spdk_dev(), true);
    const spdk_bdev* g_bdev{dev->bdev()};
    drive_attributes attr;

    const auto blk_size{spdk_bdev_get_block_size(g_bdev)};

    /* Try to get the atomic physical page size from controller */
    attr.atomic_phys_page_size = 0;
    if (std::strncmp(g_bdev->product_name, "NVMe disk", sizeof("NVMe disk")) == 0) {
        nvme_bdev* n_bdev{static_cast< nvme_bdev* >(g_bdev->ctxt)};

        // Get the namespace data if available
        const spdk_nvme_ns_data* nsdata{spdk_nvme_ns_get_data(n_bdev->nvme_ns->ns)};
        if (nsdata->nsfeat.ns_atomic_write_unit) {
            attr.atomic_phys_page_size = nsdata->nawupf * blk_size;
        } else {
            const spdk_nvme_ctrlr_data* cdata{spdk_nvme_ctrlr_get_data(n_bdev->nvme_ns->ctrlr->ctrlr)};
            attr.atomic_phys_page_size = cdata->awupf * blk_size;
        }
    }

    // TODO: At the moment we would not be able to support drive whose atomic phys page size < 4K. Need a way
    // to ensure that or fail the startup.
    if (attr.atomic_phys_page_size < 4096) {
        attr.atomic_phys_page_size = std::max(blk_size * spdk_bdev_get_acwu(dev->bdev()), 4096u);
    }

    // TODO-1 At the moment we are hard coding this - there is no sure shot way of getting this correctly. Our
    // performance metrics indicate this is the good phys page size. In NVMe 1.4 spec, there is a way to get the page
    // size optimal for NVMe viz. Namespace Preferred Write Granularity (NPWG) and Namespace Preferred Write Alignment
    // (NPWA).
    // TODO-2 Introduce attr.optimal_delete_boundary which can obtained from attribute - Namespace Preferred Deallocate
    // Granularity (NPDG):
    attr.phys_page_size = 4096u;

    // We want alignment to be mininum 512. TODO: Can we let it be 1 byte too??
    attr.align_size = std::max(spdk_bdev_get_buf_align(g_bdev), 512ul);

    return attr;
}

drive_attributes SpdkDriveInterface::get_attributes(const std::string& devname, const drive_type drive_type) {
#ifdef REFCOUNTED_OPEN_DEV
    auto iodev{open_dev(devname, drive_type, 0)};
    const auto ret{get_attributes(iodev)};
    close_dev(iodev);
    return ret;
#else
    return get_attributes(open_dev(devname, drive_type, 0));
#endif
}
} // namespace iomgr

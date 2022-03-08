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
#include <flip/flip.hpp>
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

#include "include/iomgr.hpp"
#include "include/reactor_spdk.hpp"
#include "include/spdk_drive_interface.hpp"

using namespace std::chrono_literals;

namespace iomgr {

namespace {
io_thread_t _non_io_thread{std::make_shared< io_thread >()};
thread_local bool s_temp_thread_created{false};
} // namespace

#ifndef NDEBUG
std::atomic< uint64_t > drive_iocb::_iocb_id_counter{0};
#endif

SpdkDriveInterface::SpdkDriveInterface(const io_interface_comp_cb_t& cb) : DriveInterface(cb) {
    m_my_msg_modid = iomanager.register_msg_module([this](iomgr_msg* msg) { handle_msg(msg); });
}

static void bdev_event_cb(enum spdk_bdev_event_type type, spdk_bdev* bdev, void* event_ctx) {}

struct creat_ctx {
    std::string address;
    drive_type addr_type;
    std::error_condition err;
    std::array< const char*, 128 > names;
    bool is_done{false};
    std::string bdev_name;
    io_thread_t creator;
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

static spdk_thread* create_temp_spdk_thread() {
    auto* sthread = spdk_get_thread();
    if (!s_temp_thread_created) {
        if (sthread != nullptr) { return sthread; } // We are already tight loop reactor

        sthread = IOReactorSPDK::create_spdk_thread();
        if (sthread == nullptr) { throw std::runtime_error("SPDK Thread Create failed"); }
        spdk_set_thread(sthread);
        s_temp_thread_created = true;
    }
    return sthread;
}

static void destroy_temp_spdk_thread() {
    if (s_temp_thread_created) {
        auto* sthread = spdk_get_thread();
        DEBUG_ASSERT_NOTNULL((void*)sthread);
        // NOTE: The set to false must be here or later not to allow overwrite with spdk_set_thread before getting value
        s_temp_thread_created = false;

        spdk_thread_exit(sthread);
        while (!spdk_thread_is_exited(sthread)) {
            spdk_thread_poll(sthread, 0, 0);
        }
        spdk_thread_destroy(sthread);
    }
}

static void create_fs_bdev(const std::shared_ptr< creat_ctx >& ctx) {
    iomanager.run_on(
        thread_regex::least_busy_worker,
        [ctx]([[maybe_unused]] io_thread_addr_t taddr) {
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
            ctx->creator = iomanager.iothread_self();
            ctx->done();
        },
        wait_type_t::no_wait);
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
    iomanager.run_on(
        thread_regex::least_busy_worker,
        [ctx](io_thread_addr_t taddr) {
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
                if (trid.subnqn[0] == '\0') {
                    snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);
                }
            }

            ctx->creator = iomanager.iothread_self();

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
        },
        wait_type_t::no_wait);

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
            ctx->creator = iomanager.iothread_self();
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

IOWatchDog::IOWatchDog() {
    m_wd_on = IM_DYNAMIC_CONFIG(spdk->io_watchdog_timer_on);

    if (m_wd_on) {
        m_timer_hdl = iomanager.schedule_global_timer(
            IM_DYNAMIC_CONFIG(spdk->io_watchdog_timer_sec) * 1000ul * 1000ul * 1000ul, true, nullptr,
            iomgr::thread_regex::all_worker, [this](void* cookie) { io_timer(); });
    }

    LOGINFOMOD(io_wd, "io watchdog turned {}.", m_wd_on ? "ON" : "OFF");
}

IOWatchDog::~IOWatchDog() { m_outstanding_ios.clear(); }

void IOWatchDog::add_io(const io_wd_ptr_t& iocb) {
    {
        std::unique_lock< std::mutex > lk(m_mtx);
        iocb->unique_id = ++m_unique_id;

        const auto result = m_outstanding_ios.insert_or_assign(iocb->unique_id, iocb);
        LOGTRACEMOD(io_wd, "add_io: {}, {}", iocb->unique_id, iocb->to_string());
        RELEASE_ASSERT_EQ(result.second, true, "expecting to insert instead of update");
    }
}

void IOWatchDog::complete_io(const io_wd_ptr_t& iocb) {
    {
        std::unique_lock< std::mutex > lk(m_mtx);
        const auto result = m_outstanding_ios.erase(iocb->unique_id);
        LOGTRACEMOD(io_wd, "complete_io: {}, {}", iocb->unique_id, iocb->to_string());
        RELEASE_ASSERT_EQ(result, 1, "expecting to erase 1 element");
    }
}

bool IOWatchDog::is_on() { return m_wd_on; }

void IOWatchDog::io_timer() {
    {
        std::unique_lock< std::mutex > lk(m_mtx);
        std::vector< io_wd_ptr_t > timeout_reqs;
        // the 1st io iteratred in map will be the oldeset one, because we add io
        // to map when vol_child_req is being created, e.g. op_start_time is from oldeset to latest;
        for (const auto& io : m_outstanding_ios) {
            const auto this_io_dur_us = get_elapsed_time_us(io.second->op_start_time);
            if (this_io_dur_us >= IM_DYNAMIC_CONFIG(spdk->io_timeout_limit_sec) * 1000ul * 1000ul) {
                // coolect all timeout requests
                timeout_reqs.push_back(io.second);
            } else {
                // no need to search for newer requests stored in the map;
                break;
            }
        }

        if (timeout_reqs.size()) {
            LOGCRITICAL_AND_FLUSH(
                "Total num timeout requests: {}, the oldest io req that timeout duration is: {},  iocb: {}",
                timeout_reqs.size(), get_elapsed_time_us(timeout_reqs[0]->op_start_time), timeout_reqs[0]->to_string());

            RELEASE_ASSERT(false, "IO watchdog timeout! timeout_limit: {}, watchdog_timer: {}",
                           IM_DYNAMIC_CONFIG(spdk->io_timeout_limit_sec),
                           IM_DYNAMIC_CONFIG(spdk->io_watchdog_timer_sec));
        } else {
            LOGDEBUGMOD(io_wd, "io_timer passed {}, no timee out IO found. Total outstanding_io_cnt: {}",
                        ++m_wd_pass_cnt, m_outstanding_ios.size());
        }
    }
}

io_device_ptr SpdkDriveInterface::open_dev(const std::string& devname, drive_type drive_type,
                                           [[maybe_unused]] int oflags) {
    io_device_ptr iodev{nullptr};
    m_opened_device.withWLock([this, &devname, &iodev, &drive_type](auto& m) {
        auto it = m.find(devname);
        if ((it == m.end()) || (it->second == nullptr)) {
            iodev = create_open_dev_internal(devname, drive_type);
            m.insert(std::make_pair<>(devname, iodev));
        } else {
            iodev = it->second;
            if (!iodev->ready) { // Closed before, reopen it
                open_dev_internal(iodev);
            }
        }
#ifdef REFCOUNTED_OPEN_DEV
        iodev->opened_count.increment(1);
#endif
    });
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
        iodev = alloc_io_device(null_backing_dev(), 9 /* pri*/, thread_regex::all_io);
        iodev->devname = devname;
        iodev->alias_name = ctx->bdev_name;
        iodev->creator = ctx->creator;
        open_dev_internal(iodev);
    }
    return iodev;
}

void SpdkDriveInterface::open_dev_internal(const io_device_ptr& iodev) {
    int rc{-1};
    iomanager.run_on(
        iodev->creator,
        [this, iodev, &rc](io_thread_addr_t taddr) {
            spdk_bdev_desc* desc{nullptr};
            rc = spdk_bdev_open_ext(iodev->alias_name.c_str(), true, bdev_event_cb, nullptr, &desc);
            if (rc == 0) { iodev->dev = backing_dev_t(desc); }
        },
        wait_type_t::sleep);

    if (rc != 0) {
        folly::throwSystemError(fmt::format("Unable to open the device={} error={}", iodev->alias_name, rc));
    }

    // reset counters
    m_outstanding_async_ios = 0;

    // Set the bdev to split on underlying device io boundary.
    auto* bdev = spdk_bdev_get_by_name(iodev->alias_name.c_str());
    if (!bdev) { folly::throwSystemError(fmt::format("Unable to get opened device={}", iodev->alias_name)); }
    bdev->split_on_optimal_io_boundary = true;

    add_io_device(iodev);
    LOGINFOMOD(iomgr, "Device {} bdev_name={} opened successfully", iodev->devname, iodev->alias_name);
}

void SpdkDriveInterface::close_dev(const io_device_ptr& iodev) {
#ifdef REFCOUNTED_OPEN_DEV
    if (!iodev->opened_count.decrement_testz()) { return; }
#endif
    // TODO: In the future might want to add atomic that will block any new read/write access to device that occur
    // after the close is called

    // check if current thread is reactor
    const auto& reactor{iomanager.this_reactor()};
    const bool this_thread_reactor{reactor && reactor->is_io_reactor()};
    auto* sthread = this_thread_reactor ? spdk_get_thread() : nullptr;

    // wait for outstanding IO's to complete
    LOGINFOMOD(iomgr, "Device {} bdev_name={} close device issued with {} outstanding ios", iodev->devname,
               iodev->alias_name, m_outstanding_async_ios);
    constexpr std::chrono::milliseconds max_wait_ms{5000};
    constexpr std::chrono::milliseconds wait_interval_ms{50};
    const auto start_time{std::chrono::steady_clock::now()};
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

    iomanager.run_on(
        iodev->creator,
        [bdev_desc = std::get< spdk_bdev_desc* >(iodev->dev), this](io_thread_addr_t taddr) {
            spdk_bdev_close(bdev_desc);
        },
        wait_type_t::sleep);

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

void SpdkDriveInterface::init_iface_thread_ctx(const io_thread_t& thr) {
    if (thr->reactor->is_tight_loop_reactor() && thr->reactor->is_adaptive_loop()) {
        // Allow backoff only if there are no outstanding operations.
        thr->reactor->add_backoff_cb(
            [](const io_thread_t& t) -> bool { return (t->reactor->m_metrics->outstanding_ops == 0); });
    }
}

void SpdkDriveInterface::init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) {
    if (!thr->reactor->is_tight_loop_reactor()) {
        // If we are asked to initialize the thread context for non-spdk thread reactor, then create one spdk
        // thread for sync IO which we will use to keep spinning.
        create_temp_spdk_thread();
    }

    auto dctx = std::make_unique< SpdkDriveDeviceContext >();
    dctx->channel = spdk_bdev_get_io_channel(iodev->bdev_desc());
    if (dctx->channel == NULL) {
        folly::throwSystemError(fmt::format("Unable to get io channel for bdev={}", spdk_bdev_get_name(iodev->bdev())));
    }
    iodev->m_iodev_thread_ctx[thr->thread_idx] = std::move(dctx);
}

void SpdkDriveInterface::clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) {
    const auto* dctx = static_cast< SpdkDriveDeviceContext* >(iodev->m_iodev_thread_ctx[thr->thread_idx].get());
    if (dctx->channel != NULL) { spdk_put_io_channel(dctx->channel); }
    iodev->m_iodev_thread_ctx[thr->thread_idx].reset();

    if (!thr->reactor->is_tight_loop_reactor()) { destroy_temp_spdk_thread(); }
}

static spdk_io_channel* get_io_channel(IODevice* iodev) {
    const auto tidx{iomanager.this_reactor()->select_thread()->thread_idx};
    auto* dctx = static_cast< SpdkDriveDeviceContext* >(iodev->m_iodev_thread_ctx[tidx].get());
    RELEASE_ASSERT_NOTNULL((void*)dctx,
                           "Null SpdkDriveDeviceContext for reactor={} selected thread_idx={} for iodev={}",
                           iomanager.this_reactor()->reactor_idx(), tidx, iodev->devname);
    return dctx->channel;
}

static void submit_io(void* b);
static bool resubmit_io_on_err(void* b) {
    SpdkIocb* iocb{static_cast< SpdkIocb* >(b)};
    if (iocb->resubmit_cnt > IM_DYNAMIC_CONFIG(max_resubmit_cnt)) { return false; }
    ++iocb->resubmit_cnt;
    COUNTER_INCREMENT(iocb->iface->get_metrics(), resubmit_io_on_err, 1);
    submit_io(b);
    return true;
}

static void complete_io(SpdkIocb* iocb) {
    SpdkDriveInterface::decrement_outstanding_counter(iocb);

    if (iomanager.get_io_wd()->is_on()) { iomanager.get_io_wd()->complete_io(iocb); }

    // reduce outstanding async io's
    [[maybe_unused]] const auto prev_outstanding_count{
        iocb->iface->m_outstanding_async_ios.fetch_sub(1 + iocb->resubmit_cnt, std::memory_order_relaxed)};
    DEBUG_ASSERT_GT(prev_outstanding_count, iocb->resubmit_cnt,
                    "Expect outstanding count to be greater than resubmit count.");

    sisl::ObjectAllocator< SpdkIocb >::deallocate(iocb);
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

static void process_completions(spdk_bdev_io* bdev_io, bool success, void* ctx) {
    SpdkIocb* iocb{static_cast< SpdkIocb* >(ctx)};
    DEBUG_ASSERT_NOTNULL((void*)iocb->iodev->bdev_desc());

    // LOGDEBUGMOD(iomgr, "Received completion on bdev = {}", (void*)iocb->iodev->bdev_desc());
    spdk_bdev_free_io(bdev_io);

#ifdef _PRERELEASE
    const auto flip_resubmit_cnt{flip::Flip::instance().get_test_flip< uint32_t >("read_write_resubmit_io")};
    if (flip_resubmit_cnt != boost::none && iocb->resubmit_cnt < flip_resubmit_cnt) { success = false; }
#endif

    iocb->owns_by_spdk = false;

    if (success) {
        iocb->result = 0;
        LOGDEBUGMOD(iomgr, "(bdev_io={}) iocb complete: mode=actual, {}", (void*)bdev_io, iocb->to_string());
    } else {
        LOGERRORMOD(iomgr, "(bdev_io={}) iocb failed with status [{}]: mode=actual, {}", (void*)bdev_io,
                    explain_bdev_io_status(bdev_io), iocb->to_string());
        COUNTER_INCREMENT(iocb->iface->get_metrics(), completion_errors, 1);
        if (resubmit_io_on_err(iocb)) { return; }
        iocb->result = -1;
    }

    const bool started_by_this_thread{(iocb->owner_thread == nullptr)};

    const auto& cb{iocb->comp_cb ? iocb->comp_cb : iocb->iface->get_completion_cb()};
    cb(iocb->result, static_cast< uint8_t* >(iocb->user_cookie));

    if (started_by_this_thread) {
        // If the iocb has been issued by this thread, we need to complete io, else that different thread will do so
        LOGDEBUGMOD(iomgr, "iocb complete: mode=tloop, {}", iocb->to_string());
        complete_io(iocb);
    } else {
        // Do not access iocb beyond this point as callback could have freed the iocb
    }
}

static void submit_io(void* b) {
    SpdkIocb* iocb{static_cast< SpdkIocb* >(b)};
    int rc = 0;
    DEBUG_ASSERT_NOTNULL((void*)iocb->iodev->bdev_desc());

    DEBUG_ASSERT((iocb->owns_by_spdk == false), "Duplicate submission of iocb while io pending: {}", iocb->to_string());
    iocb->owns_by_spdk = true;

    // preadd outstanding io's so that if completes quickly count will not become negative
    iocb->iface->m_outstanding_async_ios.fetch_add(1, std::memory_order_relaxed);
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
        // adjust count since unrecognized command
        iocb->iface->m_outstanding_async_ios.fetch_sub(1, std::memory_order_relaxed);
        LOGDFATAL("Invalid operation type {}", iocb->op_type);
        return;
    }

    if (rc != 0) {
        // adjust count since unsuccessful command
        iocb->iface->m_outstanding_async_ios.fetch_sub(1, std::memory_order_relaxed);
        if (rc == -ENOMEM) {
            LOGDEBUGMOD(iomgr, "Bdev is lacking memory to do IO right away, queueing iocb: {}", iocb->to_string());
            COUNTER_INCREMENT(iocb->iface->get_metrics(), queued_ios_for_memory_pressure, 1);
            spdk_bdev_queue_io_wait(iocb->iodev->bdev(), get_io_channel(iocb->iodev), &iocb->io_wait_entry);
        } else {
            LOGERRORMOD(iomgr, "iocb {} submission failed with rc={}", iocb->to_string(), rc);
        }
        iocb->owns_by_spdk = false;
    }
}

void SpdkDriveInterface::increment_outstanding_counter(const SpdkIocb* iocb) {
    /* update outstanding counters */
    switch (iocb->op_type) {
    case DriveOpType::READ:
        COUNTER_INCREMENT(iocb->iface->get_metrics(), outstanding_read_cnt, 1);
        break;
    case DriveOpType::WRITE:
        COUNTER_INCREMENT(iocb->iface->get_metrics(), outstanding_write_cnt, 1);
        break;
    case DriveOpType::UNMAP:
        COUNTER_INCREMENT(iocb->iface->get_metrics(), outstanding_unmap_cnt, 1);
        break;
    case DriveOpType::WRITE_ZERO:
        COUNTER_INCREMENT(iocb->iface->get_metrics(), outstanding_write_zero_cnt, 1);
        break;
    default:
        LOGDFATAL("Invalid operation type {}", iocb->op_type);
    }

    ++(iomanager.this_thread_metrics().outstanding_ops);
}

void SpdkDriveInterface::decrement_outstanding_counter(const SpdkIocb* iocb) {
    /* decrement */
    switch (iocb->op_type) {
    case DriveOpType::READ:
        COUNTER_DECREMENT(iocb->iface->get_metrics(), outstanding_read_cnt, 1);
        break;
    case DriveOpType::WRITE:
        COUNTER_DECREMENT(iocb->iface->get_metrics(), outstanding_write_cnt, 1);
        break;
    case DriveOpType::UNMAP:
        COUNTER_DECREMENT(iocb->iface->get_metrics(), outstanding_unmap_cnt, 1);
        break;
    case DriveOpType::WRITE_ZERO:
        COUNTER_DECREMENT(iocb->iface->get_metrics(), outstanding_write_zero_cnt, 1);
        break;
    default:
        LOGDFATAL("Invalid operation type {}", iocb->op_type);
    }
    --(iomanager.this_thread_metrics().outstanding_ops);
}

inline bool SpdkDriveInterface::try_submit_io(SpdkIocb* iocb, bool part_of_batch) {
    bool ret = true;

    if (iomanager.am_i_tight_loop_reactor()) {
        LOGDEBUGMOD(iomgr, "iocb submit: mode=tloop, {}", iocb->to_string());
        auto& thread_metrics = iomanager.this_thread_metrics();
        ++thread_metrics.io_submissions;
        ++thread_metrics.actual_ios;
        submit_io(iocb);
    } else if (iomanager.am_i_io_reactor()) {
        COUNTER_INCREMENT(m_metrics, num_async_io_non_spdk_thread, 1);
        LOGDEBUGMOD(iomgr, "iocb submit: mode=user_reactor, {}", iocb->to_string());
        submit_async_io_to_tloop_thread(iocb, part_of_batch);
    } else {
        COUNTER_INCREMENT(m_metrics, force_sync_io_non_spdk_thread, 1);
        ret = false;
    }

    // update counter in async path;
    if (ret) { increment_outstanding_counter(iocb); }

    if (iomanager.get_io_wd()->is_on()) { IOManager::instance().get_io_wd()->add_io(iocb); }

    return ret;
}

void SpdkDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE, size, offset, cookie)};
    iocb->set_data(const_cast< char* >(data));
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                    bool part_of_batch) {
    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::READ, size, offset, cookie)};
    iocb->set_data(data);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE, size, offset, cookie)};
    iocb->set_iovs(iov, iovcnt);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::READ, size, offset, cookie)};
    iocb->set_iovs(iov, iovcnt);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(std::move(iocb), part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::UNMAP, size, offset, cookie)};
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) {
    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE_ZERO, size, offset, cookie)};
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, false)) { do_sync_io(iocb, m_comp_cb); }
}

ssize_t SpdkDriveInterface::sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    DEBUG_ASSERT_EQ(iomanager.am_i_tight_loop_reactor(), false, "Sync io on tight loop thread not supported");

    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE, size, offset, nullptr)};
    iocb->set_data(const_cast< char* >(data));
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    DEBUG_ASSERT_EQ(iomanager.am_i_tight_loop_reactor(), false, "Sync io on tight loop thread not supported");

    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::WRITE, size, offset, nullptr)};
    iocb->set_iovs(iov, iovcnt);
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    DEBUG_ASSERT_EQ(iomanager.am_i_tight_loop_reactor(), false, "Sync io on tight loop thread not supported");

    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::READ, size, offset, nullptr)};
    iocb->set_data(data);
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    DEBUG_ASSERT_EQ(iomanager.am_i_tight_loop_reactor(), false, "Sync io on tight loop thread not supported");

    SpdkIocb* iocb{
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, DriveOpType::READ, size, offset, nullptr)};
    iocb->set_iovs(iov, iovcnt);
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::do_sync_io(SpdkIocb* iocb, const io_interface_comp_cb_t& comp_cb) {
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->owner_thread = _non_io_thread;

    // update counter in sync path
    increment_outstanding_counter(iocb);

    if (iomanager.get_io_wd()->is_on()) { IOManager::instance().get_io_wd()->add_io(iocb); }

    const auto& reactor = iomanager.this_reactor();
    if (reactor && reactor->is_io_reactor() && !reactor->is_tight_loop_reactor()) {
        submit_sync_io_in_this_thread(iocb);
    } else {
        submit_sync_io_to_tloop_thread(iocb);
    }

    const auto ret = (iocb->result == 0) ? iocb->size : 0;
    if (comp_cb) comp_cb(iocb->result, static_cast< uint8_t* >(iocb->user_cookie));
    complete_io(iocb);

    return ret;
}

void SpdkDriveInterface::submit_sync_io_to_tloop_thread(SpdkIocb* iocb) {
    iocb->comp_cb = [iocb, this](int64_t res, uint8_t* cookie) {
        {
            std::unique_lock< std::mutex > lk{m_sync_cv_mutex};
            iocb->sync_io_completed = true;
        }
        m_sync_cv.notify_all();
    };

    LOGDEBUGMOD(iomgr, "iocb submit: mode=sync, {}", iocb->to_string());
    auto* msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, reinterpret_cast< uint8_t* >(iocb),
                                  sizeof(SpdkIocb));
    iomanager.multicast_msg(thread_regex::least_busy_worker, msg);

    {
        std::unique_lock< std::mutex > lk{m_sync_cv_mutex};
        m_sync_cv.wait(lk, [&iocb]() { return iocb->sync_io_completed; });
    }

    LOGDEBUGMOD(iomgr, "iocb complete: mode=sync, {}", iocb->to_string());
}

void SpdkDriveInterface::submit_sync_io_in_this_thread(SpdkIocb* iocb) {
    LOGDEBUGMOD(iomgr, "iocb submit: mode=local_sync, {}", iocb->to_string());

    // NOTE: This callback is called in the same thread as the original entry via spdk_thread_poll so not
    // synchronization of sync_io_completed is needed
    iocb->comp_cb = [iocb, this](int64_t res, uint8_t* cookie) { iocb->sync_io_completed = true; };
    submit_io(iocb);

    auto* sthread = spdk_get_thread();
    auto cur_wait_us = max_sync_io_poll_freq_us;
    do {
        std::this_thread::sleep_for(cur_wait_us);
        spdk_thread_poll(sthread, 0, 0);
        if (cur_wait_us > 0us) { cur_wait_us = cur_wait_us - 1us; }
    } while (!iocb->sync_io_completed);

    LOGDEBUGMOD(iomgr, "iocb complete: mode=local_sync, {}", iocb->to_string());
}

static thread_local SpdkBatchIocb* s_batch_info_ptr = nullptr;

void SpdkDriveInterface::submit_batch() {
    // s_batch_info_ptr could be nullptr when client calls submit_batch
    if (s_batch_info_ptr) {
        auto* msg = iomgr_msg::create(spdk_msg_type::QUEUE_BATCH_IO, m_my_msg_modid,
                                      reinterpret_cast< uint8_t* >(s_batch_info_ptr), sizeof(SpdkBatchIocb*));
        const auto sent_to{iomanager.multicast_msg(thread_regex::least_busy_worker, msg)};

        if (sent_to == 0) {
            // if message is not delivered, release memory here;
            delete s_batch_info_ptr;

            // assert in debug and log a message in release;
            LOGMSG_ASSERT(0, "multicast_msg returned failure");
        }

        // reset batch info ptr, memory will be freed by spdk thread after batch io completes;
        s_batch_info_ptr = nullptr;
    }
    // it will be null operation if client calls this function without anything in s_batch_info_ptr
}

void SpdkDriveInterface::submit_async_io_to_tloop_thread(SpdkIocb* iocb, bool part_of_batch) {
    DEBUG_ASSERT_EQ(iomanager.am_i_io_reactor(), true, "Async on non-io reactors not possible");
    auto& thread_metrics = iomanager.this_thread_metrics();

    iocb->owner_thread = iomanager.iothread_self(); // TODO: This makes a shared_ptr copy, see if we can avoid it
    iocb->comp_cb = [this, iocb](int64_t res, uint8_t* cookie) {
        iocb->result = res;
        if (!iocb->batch_info_ptr) {
            auto* reply{iomgr_msg::create(spdk_msg_type::ASYNC_IO_DONE, m_my_msg_modid,
                                          reinterpret_cast< uint8_t* >(iocb), sizeof(SpdkIocb))};
            iomanager.send_msg(iocb->owner_thread, reply);
        } else {
            ++(iocb->batch_info_ptr->num_io_comp);

            // batch io completion count should never exceed number of batch io;
            DEBUG_ASSERT_LE(iocb->batch_info_ptr->num_io_comp, iocb->batch_info_ptr->batch_io->size(),
                            "Batch completion more than submission?");

            // re-use the batch info ptr which contains all the batch iocbs to send/create
            // async_batch_io_done msg;
            if (iocb->batch_info_ptr->num_io_comp == iocb->batch_info_ptr->batch_io->size()) {
                auto* reply{iomgr_msg::create(spdk_msg_type::ASYNC_BATCH_IO_DONE, m_my_msg_modid,
                                              reinterpret_cast< uint8_t* >(iocb->batch_info_ptr),
                                              sizeof(SpdkBatchIocb*))};
                iomanager.send_msg(iocb->owner_thread, reply);
            }

            // if there is any batched io doesn't get all the iocb completion, we will see memory leak in
            // the end reported becuase batch io will not freed by spdk thread due to missing async io done
            // msg;
        }
    };

    if (!part_of_batch) {
        // we don't have a use-case for same user thread to issue part_of_batch to both true and false for now.
        // if we support same user-thread to send both, we should not use iocb->batch_info_ptr to check whether it is
        // batch io in comp_cb (line: 470);
        DEBUG_ASSERT_EQ((void*)iocb->batch_info_ptr, nullptr);
        ++thread_metrics.actual_ios;
        ++thread_metrics.io_submissions;
        auto* msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, reinterpret_cast< uint8_t* >(iocb),
                                      sizeof(SpdkIocb));
        iomanager.multicast_msg(thread_regex::least_busy_worker, msg);
    } else {
        if (s_batch_info_ptr == nullptr) { s_batch_info_ptr = new SpdkBatchIocb(); }

        iocb->batch_info_ptr = s_batch_info_ptr;
        s_batch_info_ptr->batch_io->push_back(iocb);

        if (s_batch_info_ptr->batch_io->size() == IM_DYNAMIC_CONFIG(spdk->num_batch_io_limit)) {
            ++thread_metrics.io_submissions;
            thread_metrics.actual_ios += s_batch_info_ptr->batch_io->size();
            // this batch is ready to be processed;
            submit_batch();
        }
    }
}

void SpdkDriveInterface::handle_msg(iomgr_msg* msg) {
    switch (msg->m_type) {
    case spdk_msg_type::QUEUE_IO: {
        auto* iocb = reinterpret_cast< SpdkIocb* >(msg->data_buf().bytes);
        LOGDEBUGMOD(iomgr, "iocb submit: mode=queue_io, {}", iocb->to_string());
        submit_io(iocb);
        break;
    }

    case spdk_msg_type::QUEUE_BATCH_IO: {
        const auto* batch_info = reinterpret_cast< SpdkBatchIocb* >(msg->data_buf().bytes);
        for (auto& iocb : *(batch_info->batch_io)) {
            LOGDEBUGMOD(iomgr, "iocb submit: mode=queue_batch_io, {}", iocb->to_string());
            submit_io(iocb);
        }
        break;
    }

    case spdk_msg_type::ASYNC_IO_DONE: {
        auto* iocb = reinterpret_cast< SpdkIocb* >(msg->data_buf().bytes);
        LOGDEBUGMOD(iomgr, "iocb complete: mode=user_reactor, {}", iocb->to_string());
        if (m_comp_cb) m_comp_cb(iocb->result, static_cast< uint8_t* >(iocb->user_cookie));
        complete_io(iocb);
        break;
    }

    case spdk_msg_type::ASYNC_BATCH_IO_DONE: {
        const auto* batch_info = reinterpret_cast< SpdkBatchIocb* >(msg->data_buf().bytes);
        for (auto& iocb : *(batch_info->batch_io)) {
            if (m_comp_cb) { m_comp_cb(iocb->result, static_cast< uint8_t* >(iocb->user_cookie)); }
            complete_io(iocb);
        }

        // now return memory to vector pool
        delete batch_info;
        break;
    }
    }
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

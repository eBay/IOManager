#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/spdk_drive_interface.hpp"
#include <folly/Exception.h>
#include <fds/obj_allocator.hpp>
#include <fds/utils.hpp>
#include <filesystem>
#include <thread>
#include <flip/flip.hpp>
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
namespace iomgr {

static io_thread_t _non_io_thread = std::make_shared< io_thread >();
thread_local int SpdkDriveInterface::s_num_user_sync_devs{0};

#ifndef NDEBUG
std::atomic< uint64_t > SpdkIocb::_iocb_id_counter{0};
#endif

SpdkDriveInterface::SpdkDriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb) {
    // m_my_msg_modid = iomanager.register_msg_module(bind_this(handle_msg, 1));
    m_my_msg_modid = iomanager.register_msg_module([this](iomgr_msg* msg) { handle_msg(msg); });
}

static void bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev* bdev, void* event_ctx) {}

struct creat_ctx {
    std::string address;
    iomgr_drive_type addr_type;
    std::error_condition err;
    const char* names[128];
    std::mutex lock;
    std::condition_variable cv;
    bool done{false};
    std::string bdev_name;
};

static void create_fs_bdev(std::shared_ptr< creat_ctx > ctx) {
    {
        auto lg = std::lock_guard< std::mutex >(ctx->lock);
        LOGINFO("Opening {} as an SPDK drive, creating a bdev out of the file, performance will be impacted",
                ctx->address);
        auto const bdev_name = ctx->address + std::string("_bdev");

        assert(spdk_bdev_get_by_name(bdev_name.c_str()) == nullptr);
#ifdef SPDK_DRIVE_USE_URING
        auto bdev = create_uring_bdev(bdev_name.c_str(), ctx->address.c_str(), 512u);
        if (bdev == nullptr) {
            folly::throwSystemError(fmt::format("Unable to open the device={} to create bdev error", bdev_name));
        }
#else
        int ret = create_aio_bdev(bdev_name.c_str(), ctx->address.c_str(), 512u);
        if (ret != 0) {
            folly::throwSystemError(
                fmt::format("Unable to open the device={} to create bdev error={}", bdev_name, ret));
        }
#endif
        ctx->bdev_name = bdev_name;
        ctx->done = true;
    }
    ctx->cv.notify_all();
}

static void create_bdev_done(void* cb_ctx, size_t bdev_cnt, int rc) {
    auto context = static_cast< creat_ctx* >(cb_ctx);
    {
        auto lg = std::lock_guard< std::mutex >(context->lock);
        LOGDEBUGMOD(iomgr, "Volume setup for {} received: [rc={}, cnt={}]", context->address, rc, bdev_cnt);
        if (0 < bdev_cnt) {
            context->bdev_name = std::string(context->names[--bdev_cnt]);
            LOGDEBUGMOD(iomgr, "Created BDev: [{}]", context->bdev_name);
        }
        context->done = true;
    }
    context->cv.notify_all();
}

static void create_nvme_bdev(std::shared_ptr< creat_ctx > ctx) {
    auto address_c = ctx->address.c_str();
    spdk_nvme_transport_id trid;
    memset(&trid, 0, sizeof(trid));
    trid.trtype = SPDK_NVME_TRANSPORT_PCIE;

    do {
        auto rc = spdk_nvme_transport_id_parse(&trid, address_c);
        if (rc < 0) {
            LOGERROR("Failed to parse given str: {}", address_c);
            continue;
        }

        if (trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
            struct spdk_pci_addr pci_addr;
            if (spdk_pci_addr_parse(&pci_addr, trid.traddr) < 0) {
                LOGERROR("Invalid traddr={}", address_c);
                continue;
            }
            spdk_pci_addr_fmt(trid.traddr, sizeof(trid.traddr), &pci_addr);
        } else {
            if (trid.subnqn[0] == '\0') { snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN); }
        }

        /* Enumerate all of the controllers */
        spdk_nvme_host_id hostid{};
        uint32_t count{128};
        if (rc = bdev_nvme_create(&trid, &hostid, "iomgr", ctx->names, count, nullptr, 0, create_bdev_done,
                                  (void*)ctx.get());
            0 != rc) {
            LOGERROR("Failed createing NVMe BDEV from {}", trid.traddr);
        }
        // Good exit path, wait for create_bdev_done!
        return;
    } while (false);
    // Failed!
    {
        auto lg = std::lock_guard< std::mutex >(ctx->lock);
        ctx->err = std::make_error_condition(std::errc::io_error);
        ctx->done = true;
    }
    ctx->cv.notify_all();
}

static void _creat_dev(std::shared_ptr< creat_ctx > ctx) {
    // Check if the device is a file, if so create a bdev out of the file and open that bdev. This is meant only for
    // a testing and docker environment
    try {
        switch (ctx->addr_type) {
        case iomgr_drive_type::raw_nvme:
            create_nvme_bdev(ctx);
            break;

        case iomgr_drive_type::file:
        case iomgr_drive_type::block:
            create_fs_bdev(ctx);
            break;

        case iomgr_drive_type::spdk_bdev:
            // Already a bdev, set the ctx as done
            ctx->bdev_name = ctx->address;
            ctx->done = true;
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

io_device_ptr SpdkDriveInterface::open_dev(const std::string& devname, iomgr_drive_type drive_type,
                                           [[maybe_unused]] int oflags) {
    io_device_ptr iodev{nullptr};
    m_opened_device.withWLock([this, &devname, &iodev, &drive_type](auto& m) {
        auto it = m.find(devname);
        if ((it == m.end()) || (it->second == nullptr)) {
            iodev = _real_create_open_dev(devname, drive_type);
            m.insert(std::make_pair<>(devname, iodev));
        } else {
            iodev = it->second;
            if (!iodev->ready) { // Closed before, reopen it
                iomanager.run_on(
                    thread_regex::least_busy_worker,
                    [this, iodev](io_thread_addr_t taddr) { _open_dev_in_worker(iodev); },
                    true /* wait_for_completion */);
            }
        }
    });
    return iodev;
}

io_device_ptr SpdkDriveInterface::_real_create_open_dev(const std::string& devname, iomgr_drive_type drive_type) {
    io_device_ptr iodev{nullptr};
    RELEASE_ASSERT(!iomanager.am_i_worker_reactor(),
                   "We cannot open the device from a worker reactor thread unless its a bdev");

    // First create the bdev
    auto create_ctx = std::make_shared< creat_ctx >();
    create_ctx->address = devname;
    create_ctx->addr_type = (drive_type == iomgr_drive_type::unknown) ? get_drive_type(devname) : drive_type;
    {
        iomanager.run_on(
            thread_regex::least_busy_worker, [create_ctx](io_thread_addr_t taddr) { _creat_dev(create_ctx); },
            false /* wait_for_completion */);
        auto ul = std::unique_lock< std::mutex >(create_ctx->lock);
        create_ctx->cv.wait(ul, [create_ctx] { return create_ctx->done; });
    }

    if (!create_ctx->bdev_name.empty() && !create_ctx->err) {
        iodev = std::make_shared< IODevice >();
        iodev->thread_scope = thread_regex::all_io;
        iodev->pri = 9;
        iodev->io_interface = this;
        iodev->devname = devname;
        iodev->alias_name = create_ctx->bdev_name;

        // Issue the opendev on any one of the tight loop reactor
        iomanager.run_on(
            thread_regex::least_busy_worker, [this, iodev](io_thread_addr_t taddr) { _open_dev_in_worker(iodev); },
            true /* wait_for_completion */);
    }
    return iodev;
}

void SpdkDriveInterface::_open_dev_in_worker(const io_device_ptr& iodev) {
    struct spdk_bdev_desc* desc = NULL;
    auto rc = spdk_bdev_open_ext(iodev->alias_name.c_str(), true, bdev_event_cb, NULL, &desc);
    if (rc != 0) {
        folly::throwSystemError(fmt::format("Unable to open the device={} error={}", iodev->alias_name, rc));
    }
    iodev->dev = backing_dev_t(desc);
    iodev->creator = iomanager.iothread_self();

    // Set the bdev to split on underlying device io boundary.
    auto bdev = spdk_bdev_get_by_name(iodev->alias_name.c_str());
    if (!bdev) { folly::throwSystemError(fmt::format("Unable to get opened device={}", iodev->alias_name)); }
    bdev->split_on_optimal_io_boundary = true;

    add_io_device(iodev, true /* wait_to_add */);
    LOGINFOMOD(iomgr, "Device {} bdev_name={} opened successfully", iodev->devname, iodev->alias_name);
}

void SpdkDriveInterface::close_dev(const io_device_ptr& iodev) {
    IOInterface::close_dev(iodev);

    assert(iodev->creator != nullptr);
    iomanager.run_on(
        iodev->creator,
        [bdev_desc = std::get< spdk_bdev_desc* >(iodev->dev)](io_thread_addr_t taddr) { spdk_bdev_close(bdev_desc); },
        true /* wait_for_completion */);

    iodev->clear();
}

iomgr_drive_type SpdkDriveInterface::get_drive_type(const std::string& devname) const {
    iomgr_drive_type type = DriveInterface::get_drive_type(devname);
    if (type != iomgr_drive_type::unknown) { return type; }

    /* Lets find out if it is a nvme transport */
    spdk_nvme_transport_id trid;
    memset(&trid, 0, sizeof(trid));
    auto devname_c = devname.c_str();

    auto rc = spdk_nvme_transport_id_parse(&trid, devname_c);
    if ((rc == 0) && (trid.trtype == SPDK_NVME_TRANSPORT_PCIE)) { return iomgr_drive_type::raw_nvme; }

    auto bdev = spdk_bdev_get_by_name(devname_c);
    if (bdev) { return iomgr_drive_type::spdk_bdev; }

    return iomgr_drive_type::unknown;
}

void SpdkDriveInterface::init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) {
    auto dctx = new SpdkDriveDeviceContext();
    iodev->m_thread_local_ctx[thr->thread_idx] = (void*)dctx;
    dctx->channel = spdk_bdev_get_io_channel(iodev->bdev_desc());
    if (dctx->channel == NULL) {
        folly::throwSystemError(fmt::format("Unable to get io channel for bdev={}", spdk_bdev_get_name(iodev->bdev())));
    }
}

void SpdkDriveInterface::clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) {
    auto dctx = (SpdkDriveDeviceContext*)iodev->m_thread_local_ctx[thr->thread_idx];
    if (dctx->channel != NULL) { spdk_put_io_channel(dctx->channel); }
    delete (dctx);
}

void SpdkDriveInterface::_add_to_thread(const io_device_ptr& iodev, const io_thread_t& thr) {
    if (thr->reactor->is_tight_loop_reactor()) {
        IOInterface::_add_to_thread(iodev, thr);
    } else {
        // If we are asked to initialize the thread context for non-spdk thread reactor, then create one spdk
        // thread for sync IO which we will use to keep spinning.
        if (s_num_user_sync_devs == 0) {
            assert(spdk_get_thread() == NULL);
            auto sthread = spdk_thread_create(NULL, NULL);
            if (sthread == NULL) { throw std::runtime_error("SPDK Thread Create failed"); }
            spdk_set_thread(sthread);
        }
        ++s_num_user_sync_devs;
        init_iodev_thread_ctx(iodev, thr);
    }
}

void SpdkDriveInterface::_remove_from_thread(const io_device_ptr& iodev, const io_thread_t& thr) {
    if (thr->reactor->is_tight_loop_reactor()) {
        IOInterface::_remove_from_thread(iodev, thr);
    } else {
        clear_iodev_thread_ctx(iodev, thr);
        if (--s_num_user_sync_devs == 0) {
            auto sthread = spdk_get_thread();
            assert(sthread != NULL);
            spdk_thread_exit(sthread);
            while (!spdk_thread_is_exited(sthread)) {
                spdk_thread_poll(sthread, 0, 0);
            }
            spdk_thread_destroy(sthread);
        }
    }
}

static spdk_io_channel* get_io_channel(IODevice* iodev) {
    auto tidx = iomanager.this_reactor()->select_thread()->thread_idx;
    auto dctx = (SpdkDriveDeviceContext*)iodev->m_thread_local_ctx[tidx];
    return dctx->channel;
}

static void submit_io(void* b);
static bool resubmit_io_on_err(void* b) {
    SpdkIocb* iocb = (SpdkIocb*)b;
    if (iocb->resubmit_cnt > IM_DYNAMIC_CONFIG(max_resubmit_cnt)) { return false; }
    ++iocb->resubmit_cnt;
    COUNTER_INCREMENT(iocb->iface->get_metrics(), resubmit_io_on_err, 1);
    submit_io(b);
    return true;
}

static void complete_io(SpdkIocb* iocb) {
    // As of now we complete the batch as soon as 1 io is completed. We can potentially do batching based on if
    // async thread is completed or not
    auto& ecb = iocb->iface->get_end_of_batch_cb();
    if (ecb) { ecb(1); }

    sisl::ObjectAllocator< SpdkIocb >::deallocate(iocb);
}

static void process_completions(struct spdk_bdev_io* bdev_io, bool success, void* ctx) {
    SpdkIocb* iocb = (SpdkIocb*)ctx;
    assert(iocb->iodev->bdev_desc());
    // LOGDEBUGMOD(iomgr, "Received completion on bdev = {}", (void*)iocb->iodev->bdev_desc());
    spdk_bdev_free_io(bdev_io);

#ifdef _PRERELEASE
    auto flip_resubmit_cnt = flip::Flip::instance().get_test_flip< uint32_t >("read_write_resubmit_io");
    if (flip_resubmit_cnt != boost::none && iocb->resubmit_cnt < flip_resubmit_cnt) { success = false; }
#endif

    if (success) {
        iocb->result = 0;
        LOGDEBUGMOD(iomgr, "(bdev_io={}) iocb complete: mode=actual, {}", (void*)bdev_io, iocb->to_string());
    } else {
        LOGERRORMOD(iomgr, "(bdev_io={}) iocb failed: mode=actual, {}", (void*)bdev_io, iocb->to_string());
        COUNTER_INCREMENT(iocb->iface->get_metrics(), completion_errors, 1);
        if (resubmit_io_on_err(iocb)) { return; }
        iocb->result = -1;
    }

#ifndef NDEBUG
    iocb->owns_by_spdk = false;
#endif

    bool started_by_this_thread = (iocb->owner_thread == nullptr);

    auto& cb = iocb->comp_cb ? iocb->comp_cb : iocb->iface->get_completion_cb();
    cb(*iocb->result, (uint8_t*)iocb->user_cookie);

    if (started_by_this_thread) {
        // If the iocb has been issued by this thread, we need to complete io, else that different thread will do so
        LOGDEBUGMOD(iomgr, "iocb complete: mode=tloop, {}", iocb->to_string());
        complete_io(iocb);
    } else {
        // Do not access iocb beyond this point as callback could have freed the iocb
    }
}

static void submit_io(void* b) {
    SpdkIocb* iocb = (SpdkIocb*)b;
    int rc = 0;
    assert(iocb->iodev->bdev_desc());

#ifndef NDEBUG
    DEBUG_ASSERT((iocb->owns_by_spdk == false), "Duplicate submission of iocb while io pending: {}", iocb->to_string());
    iocb->owns_by_spdk = true;
#endif

    LOGDEBUGMOD(iomgr, "iocb submit: mode=actual, {}", iocb->to_string());
    if (iocb->op_type == SpdkDriveOpType::READ) {
        if (iocb->has_iovs()) {
            rc = spdk_bdev_readv(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->get_iovs(), iocb->iovcnt,
                                 iocb->offset, iocb->size, process_completions, (void*)iocb);
        } else {
            rc = spdk_bdev_read(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->get_data(), iocb->offset,
                                iocb->size, process_completions, (void*)iocb);
        }
    } else if (iocb->op_type == SpdkDriveOpType::WRITE) {
        if (iocb->has_iovs()) {
            rc = spdk_bdev_writev(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->get_iovs(), iocb->iovcnt,
                                  iocb->offset, iocb->size, process_completions, (void*)iocb);
        } else {
            rc = spdk_bdev_write(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->get_data(), iocb->offset,
                                 iocb->size, process_completions, (void*)iocb);
        }
    } else if (iocb->op_type == SpdkDriveOpType::UNMAP) {
        rc = spdk_bdev_unmap(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->offset, iocb->size,
                             process_completions, (void*)iocb);
    } else if (iocb->op_type == SpdkDriveOpType::WRITE_ZERO) {
        rc = spdk_bdev_write_zeroes(iocb->iodev->bdev_desc(), get_io_channel(iocb->iodev), iocb->offset, iocb->size,
                                    process_completions, (void*)iocb);
    } else {
        LOGDFATAL("Invalid operation type {}", iocb->op_type);
        return;
    }

    if (rc != 0) {
        if (rc == -ENOMEM) {
            LOGDEBUGMOD(iomgr, "Bdev is lacking memory to do IO right away, queueing iocb: {}", iocb->to_string());
            COUNTER_INCREMENT(iocb->iface->get_metrics(), queued_ios_for_memory_pressure, 1);
            spdk_bdev_queue_io_wait(iocb->iodev->bdev(), get_io_channel(iocb->iodev), &iocb->io_wait_entry);
        } else {
            LOGERRORMOD(iomgr, "iocb {} submission failed with rc={}", iocb->to_string(), rc);
        }
#ifndef NDEBUG
        iocb->owns_by_spdk = false;
#endif
    }
}

inline bool SpdkDriveInterface::try_submit_io(SpdkIocb* iocb, bool part_of_batch) {
    bool ret = true;
    if (iomanager.am_i_tight_loop_reactor()) {
        LOGDEBUGMOD(iomgr, "iocb submit: mode=tloop, {}", iocb->to_string());
        submit_io(iocb);
    } else if (iomanager.am_i_io_reactor()) {
        COUNTER_INCREMENT(m_metrics, num_async_io_non_spdk_thread, 1);
        LOGDEBUGMOD(iomgr, "iocb submit: mode=user_reactor, {}", iocb->to_string());
        submit_async_io_to_tloop_thread(iocb, part_of_batch);
    } else {
        COUNTER_INCREMENT(m_metrics, force_sync_io_non_spdk_thread, 1);
        ret = false;
    }
    return ret;
}

void SpdkDriveInterface::async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, cookie);
    iocb->set_data(const_cast< char* >(data));
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                                    bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, cookie);
    iocb->set_data(data);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, cookie);
    iocb->set_iovs(iov, iovcnt);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie, bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, cookie);
    iocb->set_iovs(iov, iovcnt);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(std::move(iocb), part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                                     bool part_of_batch) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::UNMAP, size, offset, cookie);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, part_of_batch)) { do_sync_io(iocb, m_comp_cb); }
}

void SpdkDriveInterface::write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) {
    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE_ZERO, size, offset, cookie);
    iocb->io_wait_entry.cb_fn = submit_io;
    if (!try_submit_io(iocb, false)) { do_sync_io(iocb, m_comp_cb); }
}

ssize_t SpdkDriveInterface::sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, nullptr);
    iocb->set_data(const_cast< char* >(data));
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::WRITE, size, offset, nullptr);
    iocb->set_iovs(iov, iovcnt);
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, nullptr);
    iocb->set_data(data);
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    // We should never do sync io on a tight loop thread
    assert(!iomanager.am_i_tight_loop_reactor());

    SpdkIocb* iocb =
        sisl::ObjectAllocator< SpdkIocb >::make_object(this, iodev, SpdkDriveOpType::READ, size, offset, nullptr);
    iocb->set_iovs(iov, iovcnt);
    return do_sync_io(iocb, nullptr);
}

ssize_t SpdkDriveInterface::do_sync_io(SpdkIocb* iocb, const io_interface_comp_cb_t& comp_cb) {
    iocb->io_wait_entry.cb_fn = submit_io;
    iocb->owner_thread = _non_io_thread;

    const auto& reactor = iomanager.this_reactor();
    if (reactor && reactor->is_io_reactor() && !reactor->is_tight_loop_reactor()) {
        submit_sync_io_in_this_thread(iocb);
    } else {
        submit_sync_io_to_tloop_thread(iocb);
    }

    auto ret = (*iocb->result == 0) ? iocb->size : 0;
    if (comp_cb) comp_cb(*iocb->result, (uint8_t*)iocb->user_cookie);
    complete_io(iocb);

    return ret;
}

void SpdkDriveInterface::submit_sync_io_to_tloop_thread(SpdkIocb* iocb) {
    iocb->comp_cb = [&](int64_t res, uint8_t* cookie) {
        std::unique_lock< std::mutex > lk(m_sync_cv_mutex);
        iocb->result = res;
        m_sync_cv.notify_all();
    };

    LOGDEBUGMOD(iomgr, "iocb submit: mode=sync, {}", iocb->to_string());
    auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
    iomanager.multicast_msg(thread_regex::least_busy_worker, msg);

    {
        std::unique_lock< std::mutex > lk(m_sync_cv_mutex);
        m_sync_cv.wait(lk, [&]() { return iocb->result; });
    }

    LOGDEBUGMOD(iomgr, "iocb complete: mode=sync, {}", iocb->to_string());
}

void SpdkDriveInterface::submit_sync_io_in_this_thread(SpdkIocb* iocb) {
    LOGDEBUGMOD(iomgr, "iocb submit: mode=local_sync, {}", iocb->to_string());

    iocb->comp_cb = [iocb, this](int64_t res, uint8_t* cookie) {}; // A Dummy method to differentiate with local submit
    submit_io((void*)iocb);

    auto sthread = spdk_get_thread();
    auto cur_wait_us = max_wait_sync_io_us;
    do {
        std::this_thread::sleep_for(cur_wait_us);
        spdk_thread_poll(sthread, 0, 0);
        if (cur_wait_us > min_wait_sync_io_us) { cur_wait_us = cur_wait_us - 1us; }
    } while (!iocb->result);

    LOGDEBUGMOD(iomgr, "iocb complete: mode=local_sync, {}", iocb->to_string());
}

static thread_local SpdkBatchIocb* s_batch_info_ptr = nullptr;

void SpdkDriveInterface::submit_batch() {
    // s_batch_info_ptr could be nullptr when client calls submit_batch
    if (s_batch_info_ptr) {
        auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_BATCH_IO, m_my_msg_modid, (uint8_t*)s_batch_info_ptr,
                                     sizeof(SpdkBatchIocb*));
        auto sent_to = iomanager.multicast_msg(thread_regex::least_busy_worker, msg);

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
    assert(iomanager.am_i_io_reactor()); // We have to run reactor otherwise async response will not be handled.

    iocb->owner_thread = iomanager.iothread_self(); // TODO: This makes a shared_ptr copy, see if we can avoid it
    iocb->comp_cb = [this, iocb](int64_t res, uint8_t* cookie) {
        iocb->result = res;
        if (!iocb->batch_info_ptr) {
            auto reply =
                iomgr_msg::create(spdk_msg_type::ASYNC_IO_DONE, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
            iomanager.send_msg(iocb->owner_thread, reply);
        } else {
            ++(iocb->batch_info_ptr->num_io_comp);

            // batch io completion count should never exceed number of batch io;
            assert(iocb->batch_info_ptr->num_io_comp <= iocb->batch_info_ptr->batch_io->size());

            // re-use the batch info ptr which contains all the batch iocbs to send/create
            // async_batch_io_done msg;
            if (iocb->batch_info_ptr->num_io_comp == iocb->batch_info_ptr->batch_io->size()) {
                auto reply = iomgr_msg::create(spdk_msg_type::ASYNC_BATCH_IO_DONE, m_my_msg_modid,
                                               (uint8_t*)iocb->batch_info_ptr, sizeof(SpdkBatchIocb*));
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
        assert(iocb->batch_info_ptr == nullptr);
        auto msg = iomgr_msg::create(spdk_msg_type::QUEUE_IO, m_my_msg_modid, (uint8_t*)iocb, sizeof(SpdkIocb));
        iomanager.multicast_msg(thread_regex::least_busy_worker, msg);
    } else {
        if (s_batch_info_ptr == nullptr) { s_batch_info_ptr = new SpdkBatchIocb(); }

        iocb->batch_info_ptr = s_batch_info_ptr;
        s_batch_info_ptr->batch_io->push_back(iocb);

        if (s_batch_info_ptr->batch_io->size() == IM_DYNAMIC_CONFIG(spdk->num_batch_io_limit)) {
            // this batch is ready to be processed;
            submit_batch();
        }
    }
}

void SpdkDriveInterface::handle_msg(iomgr_msg* msg) {
    switch (msg->m_type) {
    case spdk_msg_type::QUEUE_IO: {
        auto iocb = (SpdkIocb*)msg->data_buf().bytes;
        LOGDEBUGMOD(iomgr, "iocb submit: mode=queue_io, {}", iocb->to_string());
        submit_io((void*)iocb);
        break;
    }

    case spdk_msg_type::QUEUE_BATCH_IO: {
        auto batch_info = (SpdkBatchIocb*)msg->data_buf().bytes;
        for (auto& iocb : *(batch_info->batch_io)) {
            LOGDEBUGMOD(iomgr, "iocb submit: mode=queue_batch_io, {}", iocb->to_string());
            submit_io((void*)iocb);
        }
        break;
    }

    case spdk_msg_type::ASYNC_IO_DONE: {
        auto iocb = (SpdkIocb*)msg->data_buf().bytes;
        LOGDEBUGMOD(iomgr, "iocb complete: mode=user_reactor, {}", iocb->to_string());
        if (m_comp_cb) m_comp_cb(*iocb->result, (uint8_t*)iocb->user_cookie);
        complete_io(iocb);
        break;
    }

    case spdk_msg_type::ASYNC_BATCH_IO_DONE: {
        auto batch_info = (SpdkBatchIocb*)(msg->data_buf().bytes);
        for (auto& iocb : *(batch_info->batch_io)) {
            if (m_comp_cb) { m_comp_cb(*iocb->result, (uint8_t*)iocb->user_cookie); }
            complete_io(iocb);
        }

        // now return memory to vector pool
        delete batch_info;
        break;
    }
    }
}

size_t SpdkDriveInterface::get_size(IODevice* iodev) {
    return spdk_bdev_get_num_blocks(iodev->bdev()) * spdk_bdev_get_block_size(iodev->bdev());
}

drive_attributes SpdkDriveInterface::get_attributes(const io_device_ptr& dev) const {
    assert(dev->is_spdk_dev());
    struct spdk_bdev* g_bdev = dev->bdev();
    drive_attributes attr;

    auto blk_size = spdk_bdev_get_block_size(g_bdev);

    /* Try to get the atomic physical page size from controller */
    attr.atomic_phys_page_size = 0;
    if (strncmp(g_bdev->product_name, "NVMe disk", sizeof("NVMe disk")) == 0) {
        struct nvme_bdev* n_bdev = (struct nvme_bdev*)g_bdev->ctxt;

        // Get the namespace data if available
        const struct spdk_nvme_ns_data* nsdata = spdk_nvme_ns_get_data(n_bdev->nvme_ns->ns);
        if (nsdata->nsfeat.ns_atomic_write_unit) {
            attr.atomic_phys_page_size = nsdata->nawupf * blk_size;
        } else {
            const struct spdk_nvme_ctrlr_data* cdata = spdk_nvme_ctrlr_get_data(n_bdev->nvme_bdev_ctrlr->ctrlr);
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

drive_attributes SpdkDriveInterface::get_attributes(const std::string& devname, const iomgr_drive_type drive_type) {
    return get_attributes(open_dev(devname, drive_type, 0));
}

} // namespace iomgr

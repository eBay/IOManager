//
// Created by Gupta, Sounak on 01/15/20.
//
#include <folly/Exception.h>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>
#include <sys/uio.h>
#include <fstream>
#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/spdk_drive_interface.hpp"

namespace iomgr {
using namespace std;

thread_local spdk_thread_context* SpdkDriveInterface::_spdk_ctx;

SpdkDriveInterface::SpdkDriveInterface(const io_interface_comp_cb_t& cb) : m_comp_cb(cb) {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "hs_code";
    opts.shm_id = -1;
    if (!spdk_env_init(&opts)) {
        std::stringstream ss;
        ss << "Unable to initialize spdk environment";
        folly::throwSystemError(ss.str());
    }
    spdk_unaffinitize_thread();
    if (!spdk_thread_lib_init(NULL, 0)) {
        std::stringstream ss;
        ss << "Unable to initialize spdk_thread_library";
        folly::throwSystemError(ss.str());
    }
};

device_t SpdkDriveInterface::open_dev(std::string devname, int oflags) {
    struct spdk_bdev_desc* desc = NULL;
    if (!spdk_bdev_open_ext(devname.c_str(), true, NULL, NULL, &desc)) {
        std::stringstream ss;
        ss << "Unable to open the device " << devname;
        folly::throwSystemError(ss.str());
    }
    LOGINFOMOD(iomgr, "Device {} opened successfully, desc={}", devname, desc);
    device_t dev = desc;
    return dev;
}

void SpdkDriveInterface::on_io_thread_start(IOThreadEpoll* iomgr_ctx) {
    struct spdk_thread* thread = spdk_thread_create("io_manager_thread", NULL);
    if (!thread) {
        std::stringstream ss;
        ss << "Unable to allocate the init thread";
        folly::throwSystemError(ss.str());
    }
    spdk_set_thread(thread);
    _spdk_ctx = new spdk_thread_context();
    for (int i = 0; i < MAX_OUTSTANDING_IO; i++) {
        auto info = (iocb_info*)malloc(sizeof(iocb_info));
        _spdk_ctx->iocb_list.push(info);
    }
}

void SpdkDriveInterface::add_fd(device_t dev, int priority) {
    [[maybe_unused]] struct spdk_bdev_desc* desc = std::get< 1 >(dev);
}

void SpdkDriveInterface::on_io_thread_stopped(IOThreadEpoll* iomgr_ctx) {
    struct spdk_thread* thread = spdk_get_thread();
    spdk_thread_destroy(thread);
    delete _spdk_ctx;
}

void process_completions(struct spdk_bdev_io* bdev_io, bool success, void* cb_arg) {
    struct iocb_info* iocb = cb_arg;
    LOGTRACEMOD(iomgr, "Received completion on bdev = {} channel = {}", iocb->bdev_desc, iocb->bdev_io_channel);
    spdk_bdev_free_io(bdev_io);
    if (success) {
        LOGINFOMOD(iomgr, "bdev io completed successfully. io info: {}", ctx->to_string());
    } else {
        COUNTER_INCREMENT(m_metrics, completion_errors, 1);
        spdk_put_io_channel(iocb->bdev_io_channel);
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "bdev io error. context = " << iocb;
        folly::throwSystemError(ss.str());
    }

    if (m_comp_cb) m_comp_cb(0, iocb->cookie);
    *ctx->sync_io_arg = true;
    _spdk_ctx->iocb_list.push(iocb);
}

void submit(void* arg) {
    auto ctx = (iocb_info*)arg;
    int rc = 0;

    /* If it is a scatter gather IO */
    if (ctx->is_scatter_gather) {
        auto iov = std::get< 1 >(ctx->data);
        if (ctx->is_write) {
            rc = spdk_bdev_writev(ctx->bdev_desc, ctx->bdev_io_channel, iov.first, iov.second, ctx->offset, ctx->size,
                                  process_completions, ctx);
        } else {
            rc = spdk_bdev_readv(ctx->bdev_desc, ctx->bdev_io_channel, iov.first, iov.second, ctx->offset, ctx->size,
                                 process_completions, ctx);
        }
    } else {
        auto buff = std::get< 0 >(ctx->data);
        if (ctx->is_write) {
            rc = spdk_bdev_write(ctx->bdev_desc, ctx->bdev_io_channel, buff, ctx->offset, ctx->size,
                                 process_completions, ctx);
        } else {
            rc = spdk_bdev_read(ctx->bdev_desc, ctx->bdev_io_channel, buff, ctx->offset, ctx->size, process_completions,
                                ctx);
        }
    }
    if (rc == -ENOMEM) {
        COUNTER_INCREMENT(m_metrics, io_eagain_iocb, 1);
        LOGTRACEMOD(iomgr, "Queueing io\n");
        /* In case we cannot perform I/O now, queue I/O */
        spdk_bdev_queue_io_wait(ctx->bdev_io_wait.bdev, ctx->bdev_io_channel, &ctx->bdev_io_wait);
    } else if (rc) {
        if (ctx->is_write) {
            COUNTER_INCREMENT(m_metrics, write_io_submission_errors, 1);
        } else {
            COUNTER_INCREMENT(m_metrics, read_io_submission_errors, 1);
        }
        LOGERRORMOD(iomgr, "io submit fail: io info: {}, errno: {}", ctx->to_string(), spdk_strerror(-rc));
        spdk_put_io_channel(ctx->bdev_io_channel);
        spdk_bdev_close(ctx->bdev_desc);
        if (m_comp_cb) m_comp_cb(rc, ctx->cookie);
        _spdk_ctx->iocb_list.push(ctx);
    }
}

void SpdkDriveInterface::async_write(device_t dev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie) {
    if (_spdk_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_write_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, io_empty_iocb, 1);
        LOGWARNMOD(iomgr, "Not enough available iocbs to schedule an IO: \
                                            size {}, offset {}",
                   size, offset);
        std::stringstream ss;
        ss << "Not enough available iocbs to schedule an io: size " << size << ", offset " << offset;
        folly::throwSystemError(ss.str());
    }
    struct iocb_info* iocb = _spdk_ctx->iocb_list.top();
    _spdk_ctx->iocb_list.pop();

    /* Populate the io callback structure */
    if ((iocb->bdev_desc = std::get< 1 >(dev)) == NULL) {
        std::stringstream ss;
        ss << "Invalid bdev " << iocb->bdev_desc;
        folly::throwSystemError(ss.str());
    }

    if ((iocb->bdev_io_channel = spdk_bdev_get_io_channel(iocb->bdev_desc)) == NULL) {
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Bdev " << iocb->bdev_desc << " could not create a valid io channel";
        folly::throwSystemError(ss.str());
    }

    if (data == NULL) {
        spdk_put_io_channel(iocb->bdev_io_channel);
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Failed to allocate buffer";
        folly::throwSystemError(ss.str());
    }
    iocb->data = data;
    iocb->is_scatter_gather = false;
    iocb->size = size;
    iocb->offset = offset;
    iocb->cookie = cookie;
    iocb->is_write = true;
    iocb->sync_arg = NULL;
    iocb->bdev_io_wait.bdev = spdk_bdev_desc_get_bdev(iocb->bdev_desc);
    iocb->bdev_io_wait.cb_fn = submit;
    iocb->bdev_io_wait.cb_arg = iocb;
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_write_error_flip")) {
        m_comp_cb(homestore::homestore_error::write_failed, cookie);
        return;
    }
#endif
    submit(iocb);
    COUNTER_INCREMENT(m_metrics, async_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));
}

void SpdkDriveInterface::async_read(device_t dev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie) {
    if (_spdk_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_read_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, io_empty_iocb, 1);
        LOGWARNMOD(iomgr, "Not enough available iocbs to schedule an IO: \
                                            size {}, offset {}",
                   size, offset);
        std::stringstream ss;
        ss << "Not enough available iocbs to schedule an io: size " << size << ", offset " << offset;
        folly::throwSystemError(ss.str());
    }
    struct iocb_info* iocb = _spdk_ctx->iocb_list.top();
    _spdk_ctx->iocb_list.pop();

    /* Populate the io callback structure */
    if ((iocb->bdev_desc = std::get< 1 >(dev)) == NULL) {
        std::stringstream ss;
        ss << "Invalid bdev " << iocb->bdev_desc;
        folly::throwSystemError(ss.str());
    }

    if ((iocb->bdev_io_channel = spdk_bdev_get_io_channel(iocb->bdev_desc)) == NULL) {
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Bdev " << iocb->bdev_desc << " could not create a valid io channel";
        folly::throwSystemError(ss.str());
    }

    if (data == NULL) {
        spdk_put_io_channel(iocb->bdev_io_channel);
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Failed to allocate buffer";
        folly::throwSystemError(ss.str());
    }
    iocb->data = data;
    iocb->is_scatter_gather = false;
    iocb->size = size;
    iocb->offset = offset;
    iocb->cookie = cookie;
    iocb->is_write = false;
    iocb->sync_arg = NULL;
    iocb->bdev_io_wait.bdev = spdk_bdev_desc_get_bdev(iocb->bdev_desc);
    iocb->bdev_io_wait.cb_fn = submit;
    iocb->bdev_io_wait.cb_arg = iocb;
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_read_error_flip")) {
        m_comp_cb(homestore::homestore_error::read_failed, cookie);
        return;
    }
#endif
    submit(iocb);
    COUNTER_INCREMENT(m_metrics, async_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));
}

void SpdkDriveInterface::async_writev(device_t dev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                      uint8_t* cookie) {
    if (_spdk_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_write_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, io_empty_iocb, 1);
        LOGWARNMOD(iomgr, "Not enough available iocbs to schedule an IO: \
                                            size {}, offset {}",
                   size, offset);
        std::stringstream ss;
        ss << "Not enough available iocbs to schedule an io: size " << size << ", offset " << offset;
        folly::throwSystemError(ss.str());
    }
    struct iocb_info* iocb = _spdk_ctx->iocb_list.top();
    _spdk_ctx->iocb_list.pop();

    /* Populate the io callback structure */
    if ((iocb->bdev_desc = std::get< 1 >(dev)) == NULL) {
        std::stringstream ss;
        ss << "Invalid bdev " << iocb->bdev_desc;
        folly::throwSystemError(ss.str());
    }

    if ((iocb->bdev_io_channel = spdk_bdev_get_io_channel(iocb->bdev_desc)) == NULL) {
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Bdev " << iocb->bdev_desc << " could not create a valid io channel";
        folly::throwSystemError(ss.str());
    }
    iocb->data = std::make_pair(iov, iovcnt);
    iocb->is_scatter_gather = true;
    iocb->size = size;
    iocb->offset = offset;
    iocb->cookie = cookie;
    iocb->is_write = true;
    iocb->sync_arg = NULL;
    iocb->bdev_io_wait.bdev = spdk_bdev_desc_get_bdev(iocb->bdev_desc);
    iocb->bdev_io_wait.cb_fn = submit;
    iocb->bdev_io_wait.cb_arg = iocb;
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_write_error_flip")) {
        m_comp_cb(homestore::homestore_error::write_failed, cookie);
        return;
    }
#endif
    submit(iocb);
    COUNTER_INCREMENT(m_metrics, async_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));
}

void SpdkDriveInterface::async_readv(device_t dev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                                     uint8_t* cookie) {
    if (_spdk_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_read_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, io_empty_iocb, 1);
        LOGWARNMOD(iomgr, "Not enough available iocbs to schedule an IO: \
                                            size {}, offset {}",
                   size, offset);
        std::stringstream ss;
        ss << "Not enough available iocbs to schedule an io: size " << size << ", offset " << offset;
        folly::throwSystemError(ss.str());
    }
    struct iocb_info* iocb = _spdk_ctx->iocb_list.top();
    _spdk_ctx->iocb_list.pop();

    /* Populate the io callback structure */
    if ((iocb->bdev_desc = std::get< 1 >(dev)) == NULL) {
        std::stringstream ss;
        ss << "Invalid bdev " << iocb->bdev_desc;
        folly::throwSystemError(ss.str());
    }

    if ((iocb->bdev_io_channel = spdk_bdev_get_io_channel(iocb->bdev_desc)) == NULL) {
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Bdev " << iocb->bdev_desc << " could not create a valid io channel";
        folly::throwSystemError(ss.str());
    }
    iocb->data = std::make_pair(iov, iovcnt);
    iocb->is_scatter_gather = true;
    iocb->size = size;
    iocb->offset = offset;
    iocb->cookie = cookie;
    iocb->is_write = false;
    iocb->sync_arg = NULL;
    iocb->bdev_io_wait.bdev = spdk_bdev_desc_get_bdev(iocb->bdev_desc);
    iocb->bdev_io_wait.cb_fn = submit;
    iocb->bdev_io_wait.cb_arg = iocb;
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_read_error_flip")) {
        m_comp_cb(homestore::homestore_error::read_failed, cookie);
        return;
    }
#endif
    submit(iocb);
    COUNTER_INCREMENT(m_metrics, async_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));
}

ssize_t SpdkDriveInterface::sync_write(device_t dev, const char* data, uint32_t size, uint64_t offset) {
    if (_spdk_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_sync_write_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, io_empty_iocb, 1);
        LOGWARNMOD(iomgr, "Not enough available iocbs to schedule an IO: \
                                            size {}, offset {}",
                   size, offset);
        std::stringstream ss;
        ss << "Not enough available iocbs to schedule an io: size " << size << ", offset " << offset;
        folly::throwSystemError(ss.str());
    }
    struct iocb_info* iocb = _spdk_ctx->iocb_list.top();
    _spdk_ctx->iocb_list.pop();

    /* Populate the io callback structure */
    if ((iocb->bdev_desc = std::get< 1 >(dev)) == NULL) {
        std::stringstream ss;
        ss << "Invalid bdev " << iocb->bdev_desc;
        folly::throwSystemError(ss.str());
    }

    if ((iocb->bdev_io_channel = spdk_bdev_get_io_channel(iocb->bdev_desc)) == NULL) {
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Bdev " << iocb->bdev_desc << " could not create a valid io channel";
        folly::throwSystemError(ss.str());
    }

    if (data == NULL) {
        spdk_put_io_channel(iocb->bdev_io_channel);
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Failed to allocate buffer";
        folly::throwSystemError(ss.str());
    }
    iocb->data = data;
    iocb->is_scatter_gather = false;
    iocb->cookie = NULL;
    iocb->is_write = true;
    bool is_sync_io_done = false;
    iocb->sync_io_arg = &is_sync_io_done;
    iocb->bdev_io_wait.bdev = spdk_bdev_desc_get_bdev(iocb->bdev_desc);
    iocb->bdev_io_wait.cb_fn = submit;
    iocb->bdev_io_wait.cb_arg = iocb;
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_write_error_flip")) {
        m_comp_cb(homestore::homestore_error::write_failed, cookie);
        return;
    }
#endif
    submit(iocb);
    while (!is_sync_io_done)
        ;
    COUNTER_INCREMENT(m_metrics, sync_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));
    return size;
}

ssize_t SpdkDriveInterface::sync_writev(device_t dev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    if (_spdk_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_sync_write_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, io_empty_iocb, 1);
        LOGWARNMOD(iomgr, "Not enough available iocbs to schedule an IO: \
                                            size {}, offset {}",
                   size, offset);
        std::stringstream ss;
        ss << "Not enough available iocbs to schedule an io: size " << size << ", offset " << offset;
        folly::throwSystemError(ss.str());
    }
    struct iocb_info* iocb = _spdk_ctx->iocb_list.top();
    _spdk_ctx->iocb_list.pop();

    /* Populate the io callback structure */
    if ((iocb->bdev_desc = std::get< 1 >(dev)) == NULL) {
        std::stringstream ss;
        ss << "Invalid bdev " << iocb->bdev_desc;
        folly::throwSystemError(ss.str());
    }

    if ((iocb->bdev_io_channel = spdk_bdev_get_io_channel(iocb->bdev_desc)) == NULL) {
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Bdev " << iocb->bdev_desc << " could not create a valid io channel";
        folly::throwSystemError(ss.str());
    }

    iocb->data = std::make_pair(iov, iovcnt);
    iocb->is_scatter_gather = true;
    iocb->cookie = NULL;
    iocb->is_write = true;
    bool is_sync_io_done = false;
    iocb->sync_io_arg = &is_sync_io_done;
    iocb->bdev_io_wait.bdev = spdk_bdev_desc_get_bdev(iocb->bdev_desc);
    iocb->bdev_io_wait.cb_fn = submit;
    iocb->bdev_io_wait.cb_arg = iocb;
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_write_error_flip")) {
        m_comp_cb(homestore::homestore_error::write_failed, cookie);
        return;
    }
#endif
    submit(iocb);
    while (!is_sync_io_done)
        ;
    COUNTER_INCREMENT(m_metrics, sync_write_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, write_io_sizes, (((size - 1) / 1024) + 1));
    return size;
}

ssize_t SpdkDriveInterface::sync_read(device_t dev, char* data, uint32_t size, uint64_t offset) {
    if (_spdk_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_sync_read_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, io_empty_iocb, 1);
        LOGWARNMOD(iomgr, "Not enough available iocbs to schedule an IO: \
                                            size {}, offset {}",
                   size, offset);
        std::stringstream ss;
        ss << "Not enough available iocbs to schedule an io: size " << size << ", offset " << offset;
        folly::throwSystemError(ss.str());
    }
    struct iocb_info* iocb = _spdk_ctx->iocb_list.top();
    _spdk_ctx->iocb_list.pop();

    /* Populate the io callback structure */
    if ((iocb->bdev_desc = std::get< 1 >(dev)) == NULL) {
        std::stringstream ss;
        ss << "Invalid bdev " << iocb->bdev_desc;
        folly::throwSystemError(ss.str());
    }

    if ((iocb->bdev_io_channel = spdk_bdev_get_io_channel(iocb->bdev_desc)) == NULL) {
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Bdev " << iocb->bdev_desc << " could not create a valid io channel";
        folly::throwSystemError(ss.str());
    }

    if (data == NULL) {
        spdk_put_io_channel(iocb->bdev_io_channel);
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Failed to allocate buffer";
        folly::throwSystemError(ss.str());
    }
    iocb->data = data;
    iocb->is_scatter_gather = false;
    iocb->size = size;
    iocb->offset = offset;
    iocb->cookie = NULL;
    iocb->is_write = false;
    bool is_sync_io_done = false;
    iocb->sync_io_arg = &is_sync_io_done;
    iocb->bdev_io_wait.bdev = spdk_bdev_desc_get_bdev(iocb->bdev_desc);
    iocb->bdev_io_wait.cb_fn = submit;
    iocb->bdev_io_wait.cb_arg = iocb;
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_read_error_flip")) {
        m_comp_cb(homestore::homestore_error::read_failed, cookie);
        return;
    }
#endif
    submit(iocb);
    while (!is_sync_io_done)
        ;
    COUNTER_INCREMENT(m_metrics, sync_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));
    return size;
}

ssize_t SpdkDriveInterface::sync_readv(device_t dev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) {
    if (_spdk_ctx->iocb_list.empty()
#ifdef _PRERELEASE
        || Flip::instance().test_flip("io_sync_read_iocb_empty_flip")
#endif
    ) {
        COUNTER_INCREMENT(m_metrics, io_empty_iocb, 1);
        LOGWARNMOD(iomgr, "Not enough available iocbs to schedule an IO: \
                                            size {}, offset {}",
                   size, offset);
        std::stringstream ss;
        ss << "Not enough available iocbs to schedule an io: size " << size << ", offset " << offset;
        folly::throwSystemError(ss.str());
    }
    struct iocb_info* iocb = _spdk_ctx->iocb_list.top();
    _spdk_ctx->iocb_list.pop();

    /* Populate the io callback structure */
    if ((iocb->bdev_desc = std::get< 1 >(dev)) == NULL) {
        std::stringstream ss;
        ss << "Invalid bdev " << iocb->bdev_desc;
        folly::throwSystemError(ss.str());
    }

    if ((iocb->bdev_io_channel = spdk_bdev_get_io_channel(iocb->bdev_desc)) == NULL) {
        spdk_bdev_close(iocb->bdev_desc);
        std::stringstream ss;
        ss << "Bdev " << iocb->bdev_desc << " could not create a valid io channel";
        folly::throwSystemError(ss.str());
    }
    iocb->data = std::make_pair(iov, iovcnt);
    iocb->is_scatter_gather = true;
    iocb->size = size;
    iocb->offset = offset;
    iocb->cookie = NULL;
    iocb->is_write = false;
    bool is_sync_io_done = false;
    iocb->sync_io_arg = &is_sync_io_done;
    iocb->bdev_io_wait.bdev = spdk_bdev_desc_get_bdev(iocb->bdev_desc);
    iocb->bdev_io_wait.cb_fn = submit;
    iocb->bdev_io_wait.cb_arg = iocb;
#ifdef _PRERELEASE
    if (Flip::instance().test_flip("io_sync_read_error_flip")) {
        m_comp_cb(homestore::homestore_error::read_failed, cookie);
        return;
    }
#endif
    submit(iocb);
    while (!is_sync_io_done)
        ;
    COUNTER_INCREMENT(m_metrics, sync_read_count, 1);
    HISTOGRAM_OBSERVE(m_metrics, read_io_sizes, (((size - 1) / 1024) + 1));
    return size;
}
} // namespace iomgr

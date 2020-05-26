#pragma once

#include "drive_interface.hpp"
#include <metrics/metrics.hpp>
#include <fds/utils.hpp>
#include <optional>
#include <spdk/bdev.h>

struct spdk_io_channel;

namespace iomgr {
struct SpdkDriveDeviceContext {
    struct spdk_io_channel* channel;
};

struct spdk_msg_type {
    static constexpr int QUEUE_IO = 100;
    static constexpr int ASYNC_IO_DONE = 101;
};

struct SpdkIocb;
class SpdkDriveInterface : public DriveInterface {
    friend struct SpdkIocb;

public:
    SpdkDriveInterface(const io_interface_comp_cb_t& cb);
    io_device_ptr open_dev(const std::string& devname, int oflags) override;
    ssize_t sync_write(io_device_t* iodev, const char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_writev(io_device_t* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    ssize_t sync_read(io_device_t* iodev, char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_readv(io_device_t* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    void async_write(io_device_t* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void async_writev(io_device_t* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                      bool part_of_batch = false) override;
    void async_read(io_device_t* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                    bool part_of_batch = false) override;
    void async_readv(io_device_t* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void on_io_thread_start(IOReactor* iomgr_ctx) override;
    void on_io_thread_stopped(IOReactor* iomgr_ctx) override;

    virtual void on_add_iodev_to_reactor(IOReactor* ctx, const io_device_ptr& iodev) override;
    virtual void on_remove_iodev_from_reactor(IOReactor* ctx, const io_device_ptr& iodev) override;

private:
    void do_async_in_iomgr_thread(SpdkIocb* iocb);
    void handle_msg(iomgr_msg* msg);
    ssize_t do_sync_io(SpdkIocb* iocb);

private:
    io_interface_comp_cb_t m_comp_cb;
    msg_module_id_t m_my_msg_modid;
    std::mutex m_sync_cv_mutex;
    std::condition_variable m_sync_cv;
};

struct SpdkIocb {
    SpdkIocb(io_device_t* iodev, bool is_read, uint32_t size, uint64_t offset, void* cookie) :
            iodev(iodev),
            is_read(is_read),
            size(size),
            offset(offset),
            user_cookie(cookie) {
        io_wait_entry.bdev = iodev->bdev();
        io_wait_entry.cb_arg = (void*)this;
        comp_cb = ((SpdkDriveInterface*)iodev->io_interface)->m_comp_cb;
    }

    ~SpdkIocb() {
        if (queued && iovs) { delete iovs; }
    }

    void copy_iovs() {
        auto _iovs = new iovec[iovcnt];
        memcpy((void*)_iovs, (void*)iovs, iovcnt * sizeof(iovec));
        iovs = iovs;
        queued = true;
    }

    std::string to_string() const {
        return fmt::format("is_read={}, size={}, offset={}, iovcnt={}", is_read, size, offset, iovcnt);
    }

    io_device_t* iodev;
    bool is_read;
    uint32_t size;
    uint64_t offset;
    void* user_cookie = nullptr;
    char* user_data = nullptr;
    iovec* iovs = nullptr;
    int iovcnt = 0;
    std::optional< int > result;
    bool queued = false;
    io_interface_comp_cb_t comp_cb = nullptr;
    spdk_bdev_io_wait_entry io_wait_entry;
};
} // namespace iomgr
//
// Created by Kadayam, Hari on Sept 21 2019.
//
#pragma once

#include <unistd.h>
#include <string>
#include <stack>
#include <atomic>
#include <mutex>
#include "endpoint.hpp"

namespace iomgr {

struct spdk_bdev;
struct spdk_bdev_desc;
struct spdk_io_channel;
struct spdk_bdev_io_wait_entry;

struct bdev_context {
    spdk_bdev*      bdev;
    spdk_bdev_desc* bdev_desc;
    std::string     bdev_name;
};

struct bdev_thread_context {
    bdev_context*            bdev_ctx;
    spdk_io_channel*         bdev_io_channel;
    spdk_bdev_io_wait_entry* bdev_io_wait;
};

class SPDKDriveEndPoint : public EndPoint {
public:
    SPDKDriveEndPoint(const endpoint_comp_cb_t& cb);

    bdev_context* open_dev(std::string devname);

    void sync_write(int data_fd, const char* data, uint32_t size, uint64_t offset);
    void sync_writev(int data_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset);
    void sync_read(int data_fd, char* data, uint32_t size, uint64_t offset);
    void sync_readv(int data_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset);
    void async_write(int data_fd, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie);
    void async_writev(int data_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                      uint8_t* cookie);
    void async_read(int data_fd, char* data, uint32_t size, uint64_t offset, uint8_t* cookie);
    void async_readv(int data_fd, const struct iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie);
    void process_completions(int fd, void* cookie, int event);
    void on_io_thread_start(ioMgrThreadContext* iomgr_ctx) override;
    void on_io_thread_stopped(ioMgrThreadContext* iomgr_ctx) override;

private:
};
} // namespace iomgr

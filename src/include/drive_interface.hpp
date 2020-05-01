//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_DRIVE_INTERFACE_HPP
#define IOMGR_DRIVE_INTERFACE_HPP

#include <fcntl.h>
#include "io_interface.hpp"

namespace iomgr {
enum class drive_interface_type { aio, spdk, uioring };

class DriveInterface : public IOInterface {
public:
    void on_io_thread_start(ioMgrThreadContext* iomgr_ctx) override { (void)iomgr_ctx; };
    void on_io_thread_stopped(ioMgrThreadContext* iomgr_ctx) override { (void)iomgr_ctx; };

    virtual drive_interface_type interface_type() const = 0;

    virtual void attach_completion_cb(const io_interface_comp_cb_t& cb) = 0;
    virtual void attach_end_of_batch_cb(const io_interface_end_of_batch_cb_t& cb) = 0;
    virtual void detach_end_of_batch_cb() = 0;
    virtual int open_dev(std::string devname, int oflags) = 0;
    virtual void add_fd(int fd, int priority = 9) = 0;
    virtual ssize_t sync_write(int data_fd, const char* data, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_writev(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_read(int data_fd, char* data, uint32_t size, uint64_t offset) = 0;
    virtual ssize_t sync_readv(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) = 0;
    virtual void async_write(int data_fd, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                             bool part_of_batch = false) = 0;
    virtual void async_writev(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset,
                              uint8_t* cookie, bool part_of_batch = false) = 0;
    virtual void async_read(int data_fd, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                            bool part_of_batch = false) = 0;
    virtual void async_readv(int data_fd, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                             bool part_of_batch = false) = 0;
    virtual void submit_batch() = 0;
};
} // namespace iomgr
#endif // IOMGR_DEFAULT_INTERFACE_HPP

#pragma once

#include "drive_interface.hpp"
#include <metrics/metrics.hpp>
#include <fds/utils.hpp>
#include <optional>
#include <spdk/bdev.h>
#include "iomgr_msg.hpp"
#include <utility/enum.hpp>

struct spdk_io_channel;

namespace iomgr {
struct SpdkDriveDeviceContext {
    struct spdk_io_channel* channel;
};

struct spdk_msg_type {
    static constexpr int QUEUE_IO = 100;
    static constexpr int ASYNC_IO_DONE = 101;
    static constexpr int QUEUE_BATCH_IO = 102;
};

struct SpdkIocb;

static constexpr uint32_t SPDK_BATCH_IO_NUM = 2;

static_assert(SPDK_BATCH_IO_NUM == 2);

class SpdkDriveInterface : public DriveInterface {
    friend struct SpdkIocb;

public:
    SpdkDriveInterface(const io_interface_comp_cb_t& cb = nullptr);
    drive_interface_type interface_type() const override { return drive_interface_type::spdk; }

    void attach_completion_cb(const io_interface_comp_cb_t& cb) override { m_comp_cb = cb; }
    void attach_end_of_batch_cb(const io_interface_end_of_batch_cb_t& cb) override { m_io_end_of_batch_cb = cb; }
    void detach_end_of_batch_cb() override { m_io_end_of_batch_cb = nullptr; }

    io_device_ptr open_dev(const std::string& devname, int oflags) override;
    void close_dev(const io_device_ptr& iodev) override;

    size_t get_size(IODevice* iodev) override;
    virtual void submit_batch(){};

    ssize_t sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    ssize_t sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) override;
    ssize_t sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset) override;
    void async_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void async_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                      bool part_of_batch = false) override;
    void async_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset, uint8_t* cookie,
                    bool part_of_batch = false) override;
    void async_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;
    void async_unmap(IODevice* iodev, uint32_t size, uint64_t offset, uint8_t* cookie,
                     bool part_of_batch = false) override;

    io_interface_comp_cb_t& get_completion_cb() { return m_comp_cb; }
    io_interface_end_of_batch_cb_t& get_end_of_batch_cb() { return m_io_end_of_batch_cb; }

private:
    io_device_ptr _open_dev(const std::string& devname);
    void init_iface_thread_ctx(const io_thread_t& thr) override {}
    void clear_iface_thread_ctx(const io_thread_t& thr) override {}

    void init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override;
    void clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override;

    bool try_submit_io(SpdkIocb* iocb);
    void do_async_in_tloop_thread(SpdkIocb* iocb);
    void handle_msg(iomgr_msg* msg);
    ssize_t do_sync_io(SpdkIocb* iocb);

private:
    io_interface_comp_cb_t m_comp_cb;
    msg_module_id_t m_my_msg_modid;
    std::mutex m_sync_cv_mutex;
    std::condition_variable m_sync_cv;
    io_interface_end_of_batch_cb_t m_io_end_of_batch_cb;
    std::mutex m_batch_mtx;
    std::vector<SpdkIocb*> m_batch_io;
};

ENUM(SpdkDriveOpType, uint8_t, WRITE, READ, UNMAP)

struct SpdkIocb {
    SpdkIocb(SpdkDriveInterface* iface, IODevice* iodev, SpdkDriveOpType op_type, uint32_t size, uint64_t offset,
             void* cookie) :
            iodev(iodev), iface(iface), op_type(op_type), size(size), offset(offset), user_cookie(cookie) {
        io_wait_entry.bdev = iodev->bdev();
        io_wait_entry.cb_arg = (void*)this;
        comp_cb = ((SpdkDriveInterface*)iodev->io_interface)->m_comp_cb;
    }

    ~SpdkIocb() {}

    void copy_iovs() {
        addln_iovs = std::unique_ptr< iovec[] >(new iovec[iovcnt]);
        memcpy((void*)addln_iovs.get(), (void*)iovs, iovcnt * sizeof(iovec));
        iovs = addln_iovs.get();
    }

    std::string to_string() const {
        auto str = fmt::format("op_type={}, size={}, offset={}, iovcnt={} data={}", enum_name(op_type), size, offset,
                               iovcnt, (void*)user_data);
        for (auto i = 0; i < iovcnt; ++i) {
            str += fmt::format("iov[{}]=<base={},len={}>", i, iovs[i].iov_base, iovs[i].iov_len);
        }
        return str;
    }

    IODevice* iodev;
    SpdkDriveInterface* iface;
    SpdkDriveOpType op_type;
    uint32_t size;
    uint64_t offset;
    void* user_cookie = nullptr;
    char* user_data = nullptr;
    iovec* iovs = nullptr;
    int iovcnt = 0;
    std::unique_ptr< iovec[] > addln_iovs; // In case we are wait queued, need to copy iovs here
    std::optional< int > result;
    io_thread_t owner_thread = nullptr; // Owner thread (nullptr if same owner as processor)
    io_interface_comp_cb_t comp_cb = nullptr;
    spdk_bdev_io_wait_entry io_wait_entry;
};
} // namespace iomgr

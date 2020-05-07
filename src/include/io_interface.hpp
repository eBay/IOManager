//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_INTERFACE_HPP
#define IOMGR_INTERFACE_HPP

#include <functional>
#include <variant>
#include <memory>

namespace iomgr {
typedef std::function< void(int64_t res, uint8_t* cookie) > io_interface_comp_cb_t;
typedef std::function< void(int nevents) > io_interface_end_of_batch_cb_t;

struct spdk_bdev;
struct io_device_t;
using ev_callback = std::function< void(io_device_t* iodev, void* cookie, int events) >;
using backing_dev_t = std::variant< int, spdk_bdev* >;
using io_device_ptr = std::shared_ptr< io_device_t >;

class IOThreadContext;
class IOInterface {
protected:
public:
    explicit IOInterface() {}
    virtual ~IOInterface() = default;

    virtual void on_io_thread_start(IOThreadContext* ctx) = 0;
    virtual void on_io_thread_stopped(IOThreadContext* ctx) = 0;
};

class GenericIOInterface : public IOInterface {
public:
    virtual void on_io_thread_start(__attribute__((unused)) IOThreadContext* ctx);
    virtual void on_io_thread_stopped(__attribute__((unused)) IOThreadContext* ctx);

    io_device_ptr make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                 bool is_per_thread_dev, const ev_callback& cb);
    void remove_io_device(const io_device_ptr& iodev);
};
} // namespace iomgr
#endif // IOMGR_INTERFACE_HPP

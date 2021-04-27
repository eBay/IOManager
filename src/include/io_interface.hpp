//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_INTERFACE_HPP
#define IOMGR_INTERFACE_HPP

#include <functional>
#include <variant>
#include <memory>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif
#include <folly/Synchronized.h>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif
#include "reactor.hpp"

namespace iomgr {
typedef std::function< void(int64_t res, uint8_t* cookie) > io_interface_comp_cb_t;
typedef std::function< void(int nevents) > io_interface_end_of_batch_cb_t;

class IOReactor;

using io_interface_id_t = uint32_t;

class IOInterface {
protected:
public:
    explicit IOInterface();
    virtual ~IOInterface();

    virtual void add_io_device(const io_device_ptr& iodev, bool wait_to_add = true,
                               const std::function< void(io_device_ptr) >& add_comp_cb = nullptr);
    virtual void remove_io_device(const io_device_ptr& iodev, bool wait_to_remove = true,
                                  const std::function< void(io_device_ptr) >& remove_comp_cb = nullptr);
    virtual void close_dev(const io_device_ptr& iodev);

    void on_io_thread_start(const io_thread_t& thr);
    void on_io_thread_stopped(const io_thread_t& thr);

    thread_regex scope() const { return m_thread_scope; }
    void set_scope(thread_regex t) { m_thread_scope = t; }
    [[nodiscard]] virtual bool is_spdk_interface() const { return false; }

protected:
    virtual void init_iface_thread_ctx(const io_thread_t& thr) = 0;
    virtual void clear_iface_thread_ctx(const io_thread_t& thr) = 0;
    virtual void init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) = 0;
    virtual void clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) = 0;

    virtual void _add_to_thread(const io_device_ptr& iodev, const io_thread_t& thr);
    virtual void _remove_from_thread(const io_device_ptr& iodev, const io_thread_t& thr);

protected:
    // std::shared_mutex m_mtx;
    std::unordered_map< backing_dev_t, io_device_ptr > m_iodev_map;
    sisl::sparse_vector< void* > m_thread_local_ctx;
    thread_regex m_thread_scope = thread_regex::all_io;
};

class GenericIOInterface : public IOInterface {
public:
    io_device_ptr make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                 thread_specifier scope, const ev_callback& cb);
    io_device_ptr make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                 bool is_per_thread_dev, const ev_callback& cb);

private:
    void init_iface_thread_ctx(const io_thread_t& thr) override {}
    void clear_iface_thread_ctx(const io_thread_t& thr) override {}
    void init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override {}
    void clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override {}
};
} // namespace iomgr
#endif // IOMGR_INTERFACE_HPP

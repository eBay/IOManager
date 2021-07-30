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
#include "iomgr_types.hpp"

namespace iomgr {
class IOReactor;

struct IOInterfaceThreadContext {
    virtual ~IOInterfaceThreadContext() = default;
};

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

    io_device_ptr alloc_io_device(const backing_dev_t dev, const int events_interested, const int pri, void* cookie,
                                  const thread_specifier& scope, const ev_callback& cb);
    inline io_device_ptr alloc_io_device(const backing_dev_t dev, const int pri, const thread_specifier& scope) {
        return alloc_io_device(dev, 0, pri, nullptr, scope, nullptr);
    }

    thread_regex scope() const { return m_thread_scope; }
    void set_scope(thread_regex t) { m_thread_scope = t; }
    [[nodiscard]] virtual bool is_spdk_interface() const { return false; }

protected:
    virtual void init_iface_thread_ctx(const io_thread_t& thr) = 0;
    virtual void clear_iface_thread_ctx(const io_thread_t& thr) = 0;
    virtual void init_iodev_thread_ctx(const io_device_const_ptr& iodev, const io_thread_t& thr) = 0;
    virtual void clear_iodev_thread_ctx(const io_device_const_ptr& iodev, const io_thread_t& thr) = 0;

    virtual bool add_to_my_reactor(const io_device_const_ptr& iodev, const io_thread_t& thr);
    virtual bool remove_from_my_reactor(const io_device_const_ptr& iodev, const io_thread_t& thr);

protected:
    // std::shared_mutex m_mtx;
    std::unordered_map< backing_dev_t, io_device_ptr > m_iodev_map;
    sisl::sparse_vector< std::unique_ptr< IOInterfaceThreadContext > > m_thread_local_ctx;
    thread_regex m_thread_scope{thread_regex::all_io};
};

struct GenericInterfaceThreadContext : public IOInterfaceThreadContext {
    virtual ~GenericInterfaceThreadContext() = default;
    listen_sentinel_cb_t listen_sentinel_cb;
};

class GenericIOInterface : public IOInterface {
public:
    io_device_ptr make_io_device(const backing_dev_t dev, const int events_interested, const int pri, void* cookie,
                                 const thread_specifier& scope, const ev_callback& cb);
    io_device_ptr make_io_device(const backing_dev_t dev, const int events_interested, const int pri, void* cookie,
                                 const bool is_per_thread_dev, const ev_callback& cb);

    void attach_listen_sentinel_cb(const listen_sentinel_cb_t& cb, const run_method_t& on_attach_closure);
    void detach_listen_sentinel_cb(const run_method_t& on_detach_closure);
    listen_sentinel_cb_t& get_listen_sentinel_cb();

private:
    void init_iface_thread_ctx(const io_thread_t& thr) override;
    void clear_iface_thread_ctx(const io_thread_t& thr) override;
    void init_iodev_thread_ctx(const io_device_const_ptr& iodev, const io_thread_t& thr) override {}
    void clear_iodev_thread_ctx(const io_device_const_ptr& iodev, const io_thread_t& thr) override {}
    GenericInterfaceThreadContext* thread_ctx();

private:
    listen_sentinel_cb_t m_listen_sentinel_cb;
};
} // namespace iomgr
#endif // IOMGR_INTERFACE_HPP

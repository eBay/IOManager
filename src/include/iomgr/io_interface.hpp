/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Harihara Kadayam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **************************************************************************/
#ifndef IOMGR_INTERFACE_HPP
#define IOMGR_INTERFACE_HPP

#include <functional>
#include <variant>
#include <memory>

#include <iomgr/iomgr_types.hpp>

namespace iomgr {
class IOReactor;

class IOInterface {
protected:
public:
    explicit IOInterface();
    virtual ~IOInterface();

    virtual void add_io_device(const io_device_ptr& iodev, bool wait_to_add = false);
    virtual void remove_io_device(const io_device_ptr& iodev, bool wait_to_remove = false);
    virtual void close_dev(const io_device_ptr& iodev);
    virtual std::string name() const = 0;

    void on_reactor_start(IOReactor* reactor);
    void on_reactor_stop(IOReactor* reactor);

    io_device_ptr alloc_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                  const thread_specifier& scope, const ev_callback& cb);
    inline io_device_ptr alloc_io_device(backing_dev_t dev, int pri, const thread_specifier& scope) {
        return alloc_io_device(dev, 0, pri, nullptr, scope, nullptr);
    }

    reactor_regex scope() const { return m_thread_scope; }
    void set_scope(reactor_regex t) { m_thread_scope = t; }
    virtual bool is_spdk_interface() const { return false; }

    virtual void init_iodev_reactor_context(const io_device_ptr& iodev, IOReactor* reactor){};
    virtual void clear_iodev_reactor_context(const io_device_ptr& iodev, IOReactor* reactor){};

protected:
    virtual void init_iface_reactor_context(IOReactor* reactor) = 0;
    virtual void clear_iface_reactor_context(IOReactor* reactor) = 0;

protected:
    std::shared_mutex m_mtx;
    std::unordered_map< backing_dev_t, io_device_ptr > m_iodev_map;
    reactor_regex m_thread_scope{reactor_regex::all_io};
};

class GenericIOInterface : public IOInterface {
public:
    std::string name() const override { return "generic_interface"; }
    io_device_ptr make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                 const thread_specifier& scope, const ev_callback& cb);
    io_device_ptr make_io_device(backing_dev_t dev, int events_interested, int pri, void* cookie,
                                 bool is_per_thread_dev, const ev_callback& cb);

    void attach_listen_sentinel_cb(const listen_sentinel_cb_t& cb);
    void detach_listen_sentinel_cb();
    listen_sentinel_cb_t& get_listen_sentinel_cb();

private:
    void init_iface_reactor_context(IOReactor* reactor) override;
    void clear_iface_reactor_context(IOReactor* reactor) override;

private:
    static thread_local listen_sentinel_cb_t t_listen_sentinel_cb; // Copy this in every reactor thread
    listen_sentinel_cb_t m_listen_sentinel_cb;
};
} // namespace iomgr
#endif // IOMGR_INTERFACE_HPP

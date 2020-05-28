//
// Created by Kadayam, Hari on 4/10/19.
//

#ifndef IOMGR_IOMGR_MSG_HPP
#define IOMGR_IOMGR_MSG_HPP

#include <iostream>
#include <folly/Traits.h>
#include <fds/utils.hpp>
#include <fds/obj_allocator.hpp>

namespace iomgr {

struct io_device_t;
typedef std::shared_ptr< io_device_t > io_device_ptr;
using msg_module_id_t = uint32_t;

struct iomgr_msg_type {
    static constexpr int UNKNOWN = 0;
    static constexpr int RESCHEDULE = 1;
    static constexpr int DESIGNATE_IO_THREAD = 2;  // Designate this thread as io thread
    static constexpr int RELINQUISH_IO_THREAD = 3; // Unmark yourself from io thread and exit io loop
    static constexpr int ADD_DEVICE = 4;           // Add an iodevice to the io thread
    static constexpr int REMOVE_DEVICE = 5;        // Remove an iodevice to the io thread
    static constexpr int RUN_METHOD = 6;           // Run the method in your thread
};

struct reschedule_data_t {
    std::shared_ptr< io_device_t > iodev;
    int event;
};

typedef std::function< void(void) > run_method_t;
using msg_data_t = std::variant< sisl::blob, reschedule_data_t, run_method_t >;

struct iomgr_msg {
    friend class sisl::ObjectAllocator< iomgr_msg >;

    int m_type = 0;                    // Type of the message (different meaning for different modules)
    msg_module_id_t m_dest_module = 0; // Default to internal module
    bool m_is_sync_msg = false;
    msg_data_t m_data;

    template < class... Args >
    static iomgr_msg* create(Args&&... args) {
        return sisl::ObjectAllocator< iomgr_msg >::make_object(std::forward< Args >(args)...);
    }

    virtual void free_yourself() { sisl::ObjectAllocator< iomgr_msg >::deallocate(this); }

    virtual iomgr_msg* clone() {
        auto new_msg = sisl::ObjectAllocator< iomgr_msg >::make_object(m_type, m_dest_module, m_data);
        new_msg->m_is_sync_msg = m_is_sync_msg;
        return new_msg;
    }

    bool is_sync_msg() const { return m_is_sync_msg; }
    // iomgr_msg &operator=(const iomgr_msg &msg) = default;
    sisl::blob data_buf() const { return std::get< sisl::blob >(m_data); }
    const run_method_t& method_data() const { return std::get< run_method_t >(m_data); }
    const reschedule_data_t& schedule_data() const { return std::get< reschedule_data_t >(m_data); }
    const std::shared_ptr< io_device_t >& iodevice_data() const { return schedule_data().iodev; }
    int event() const { return schedule_data().event; }

protected:
    iomgr_msg() = default;
    iomgr_msg(const iomgr_msg& msg) = default;
    iomgr_msg(int type, msg_module_id_t module, const msg_data_t& d) : m_type(type), m_dest_module(module), m_data(d) {}
    iomgr_msg(int type, msg_module_id_t module, void* buf = nullptr, uint32_t size = 0u) :
            iomgr_msg(type, module, msg_data_t(sisl::blob{(uint8_t*)buf, size})) {}
    iomgr_msg(int type, msg_module_id_t module, const std::shared_ptr< io_device_t >& iodev, int event) :
            iomgr_msg(type, module, msg_data_t(reschedule_data_t{iodev, event})) {}
    iomgr_msg(int type, msg_module_id_t module, const run_method_t& fn) : iomgr_msg(type, module, msg_data_t(fn)) {}

    virtual ~iomgr_msg() = default;
};

struct sync_iomgr_msg : public iomgr_msg {
    friend class sisl::ObjectAllocator< sync_iomgr_msg >;

    std::mutex m_mtx;
    std::condition_variable m_cv;
    std::atomic< int32_t > m_pending = 0;

    iomgr_msg* clone() override {
        auto new_msg = sisl::ObjectAllocator< sync_iomgr_msg >::make_object(m_type, m_dest_module, m_data);
        new_msg->m_is_sync_msg = m_is_sync_msg;
        return new_msg;
    }

    static sync_iomgr_msg* cast(iomgr_msg* msg) { return static_cast< sync_iomgr_msg* >(msg); }

    template < class... Args >
    static sync_iomgr_msg* create(Args&&... args) {
        return sisl::ObjectAllocator< sync_iomgr_msg >::make_object(std::forward< Args >(args)...);
    }

    void free_yourself() override { sisl::ObjectAllocator< sync_iomgr_msg >::deallocate(this); }

    void pending() {
        std::lock_guard< std::mutex > lck(m_mtx);
        ++m_pending;
    }

    void done() {
        std::lock_guard< std::mutex > lck(m_mtx);
        --m_pending;
        m_cv.notify_one();
    }

    void wait() {
        std::unique_lock< std::mutex > lck(m_mtx);
        m_cv.wait(lck, [this] { return (m_pending == 0); });
    }

protected:
    sync_iomgr_msg(const sync_iomgr_msg& msg) = default;

    template < class... Args >
    sync_iomgr_msg(Args&&... args) : iomgr_msg(std::forward< Args >(args)...) {
        m_is_sync_msg = true;
    }

    virtual ~sync_iomgr_msg() = default;
};
} // namespace iomgr

#if 0
namespace folly {
template <>
FOLLY_ASSUME_RELOCATABLE(iomgr::iomgr_msg);
} // namespace folly
#endif

#endif // IOMGR_IOMGR_MSG_HPP

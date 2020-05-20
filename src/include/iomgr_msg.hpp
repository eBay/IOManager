//
// Created by Kadayam, Hari on 4/10/19.
//

#ifndef IOMGR_IOMGR_MSG_HPP
#define IOMGR_IOMGR_MSG_HPP

#include <iostream>
#include <folly/Traits.h>

namespace iomgr {

struct io_device_t;
typedef std::shared_ptr< io_device_t > io_device_ptr;
using msg_module_id_t = uint32_t;

enum class iomgr_msg_type {
    UNKNOWN = 0,
    RESCHEDULE,           // Reschedule the IO polling to your thread
    WAKEUP,               // Wakeup and start doing some io
    SHUTDOWN,             // Shutdown this thread
    DESIGNATE_IO_THREAD,  // Designate this thread as io thread
    RELINQUISH_IO_THREAD, // Unmark yourself from io thread and exit io loop
    ADD_DEVICE,           // Add an iodevice to the io thread
    REMOVE_DEVICE,        // Remove an iodevice to the io thread
    RUN_METHOD,           // Run the method in your thread
    CUSTOM_MSG,           // Any custom message
};

struct iomgr_msg {
    int m_type = 0;                    // Type of the message (different meaning for different modules)
    msg_module_id_t m_dest_module = 0; // Default to internal module
    bool is_sync_msg = false;
    std::shared_ptr< io_device_t > m_iodev = nullptr; // device which this message pertaining to. Can be null
    int m_event = -1;                                 // EPOLL event if any to be notified to the callback
    void* m_data_buf = nullptr;
    uint32_t m_data_size = 0;

    iomgr_msg() = default;
    iomgr_msg(const iomgr_msg& msg) = default;
    // iomgr_msg &operator=(const iomgr_msg &msg) = default;
    iomgr_msg(int type, msg_module_id_t module = 0, const io_device_ptr& iodev = nullptr, int event = -1,
              void* buf = nullptr, uint32_t size = 0) :
            m_type(type),
            m_dest_module(module),
            m_iodev(iodev),
            m_event(event),
            m_data_buf(buf),
            m_data_size(size) {}

    template < typename T >
    void set_type(T t) {
        m_type = (int)t;
    }

    template < typename T >
    T get_type() const {
        return (T)m_type;
    }
};

struct sync_iomgr_msg {
    std::mutex m_mtx;
    std::condition_variable m_cv;
    iomgr_msg m_msg;
    std::atomic< int32_t > m_pending = 0;
    void* m_user_data_buf = nullptr;

    sync_iomgr_msg(int type, msg_module_id_t module = 0, const io_device_ptr& iodev = nullptr, int event = -1,
                   void* buf = nullptr, uint32_t size = 0) :
            m_msg(type, module, iodev, event, (void*)this, size + sizeof(sync_iomgr_msg)),
            m_user_data_buf(buf) {
        m_msg.is_sync_msg = true;
    }

    static sync_iomgr_msg& to_sync_msg(const iomgr_msg& msg) { return *((sync_iomgr_msg*)msg.m_data_buf); }

    iomgr_msg& msg() { return m_msg; }
    void* data_buf() { return m_user_data_buf; }
    uint32_t data_size() const { return m_msg.m_data_size - sizeof(sync_iomgr_msg); }

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
};
} // namespace iomgr

namespace folly {
template <>
FOLLY_ASSUME_RELOCATABLE(iomgr::iomgr_msg);
} // namespace folly

#endif // IOMGR_IOMGR_MSG_HPP

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

enum iomgr_msg_type {
    UNKNOWN = 0,
    RESCHEDULE = 1,       // Reschedule the IO polling to your thread
    WAKEUP,               // Wakeup and start doing some io
    SHUTDOWN,             // Shutdown this thread
    DESIGNATE_IO_THREAD,  // Designate this thread as io thread
    RELINQUISH_IO_THREAD, // Unmark yourself from io thread and exit io loop
    RUN_METHOD,           // Run the method in your thread
    CUSTOM_MSG,           // Any custom message
};

struct iomgr_msg {
    iomgr_msg_type m_type = iomgr_msg_type::UNKNOWN;
    std::shared_ptr< io_device_t > m_iodev = nullptr; // device which this message pertaining to. Can be null
    int m_event = -1;                                 // EPOLL event if any to be notified to the callback
    void* m_data_buf = nullptr;
    uint32_t m_data_size = 0;

    iomgr_msg() = default;
    iomgr_msg(const iomgr_msg& msg) = default;
    // iomgr_msg &operator=(const iomgr_msg &msg) = default;
    iomgr_msg(iomgr_msg_type type, const io_device_ptr& iodev = nullptr, int event = -1, void* buf = nullptr,
              uint32_t size = 0) :
            m_type(type),
            m_iodev(iodev),
            m_event(event),
            m_data_buf(buf),
            m_data_size(size) {}
};
} // namespace iomgr

namespace folly {
template <>
FOLLY_ASSUME_RELOCATABLE(iomgr::iomgr_msg);
} // namespace folly

#endif // IOMGR_IOMGR_MSG_HPP

//
// Created by Kadayam, Hari on 4/10/19.
//

#ifndef IOMGR_IOMGR_MSG_HPP
#define IOMGR_IOMGR_MSG_HPP

#include <iostream>
#include <folly/Traits.h>

namespace iomgr {

struct fd_info;
enum iomgr_msg_type {
    UNKNOWN = 0,
    RESCHEDULE = 1,       // Reschedule the IO polling to your thread
    WAKEUP,               // Wakeup and start doing some io
    SHUTDOWN,             // Shutdown this thread
    DESIGNATE_IO_THREAD,  // Designate this thread as io thread
    RELINQUISH_IO_THREAD, // Unmark yourself from io thread and exit io loop
};

struct iomgr_msg {
    iomgr_msg_type m_type = iomgr_msg_type::UNKNOWN;
    fd_info*       m_fd_info = nullptr; // fd which this message pertaining to. Can be null
    int            m_event = -1;        // EPOLL event if any to be notified to the callback
    void*          m_data_buf = nullptr;
    uint32_t       m_data_size = 0;

    iomgr_msg() = default;
    iomgr_msg(const iomgr_msg& msg) = default;
    // iomgr_msg &operator=(const iomgr_msg &msg) = default;
    iomgr_msg(iomgr_msg_type type, fd_info* info = nullptr, int event = -1, void* buf = nullptr, uint32_t size = 0) :
            m_type(type),
            m_fd_info(info),
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

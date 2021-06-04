//
// Created by Kadayam, Hari on 4/10/19.
//

#ifndef IOMGR_IOMGR_MSG_HPP
#define IOMGR_IOMGR_MSG_HPP

#include <iostream>
#include <folly/Traits.h>
#include <fds/buffer.hpp>
#include <fds/obj_allocator.hpp>
#include "reactor.hpp"
#include "iomgr_types.hpp"

namespace iomgr {

struct iomgr_msg_type {
    static constexpr int UNKNOWN = 0;
    static constexpr int RESCHEDULE = 1;
    static constexpr int DESIGNATE_IO_THREAD = 2;  // Designate this thread as io thread
    static constexpr int RELINQUISH_IO_THREAD = 3; // Unmark yourself from io thread and exit io loop
#if 0
    static constexpr int ADD_DEVICE = 4;           // Add an iodevice to the io thread
    static constexpr int REMOVE_DEVICE = 5;        // Remove an iodevice to the io thread
#endif
    static constexpr int RUN_METHOD = 6; // Run the method in your thread
};

struct _sync_sem_block {
    std::mutex m_mtx;
    std::condition_variable m_cv;
    int32_t m_pending = 0;

    void pending() {
        std::lock_guard< std::mutex > lck(m_mtx);
        ++m_pending;
    }

    int32_t num_pending() {
        std::lock_guard< std::mutex > lck(m_mtx);
        return m_pending;
    }

    void done() {
        std::lock_guard< std::mutex > lck(m_mtx);
        --m_pending;
        m_cv.notify_one();
    }
};

struct iomgr_msg {
    friend class sisl::ObjectAllocator< iomgr_msg >;

    int m_type = 0;                               // Type of the message (different meaning for different modules)
    msg_module_id_t m_dest_module = 0;            // Default to internal module
    io_thread_addr_t m_dest_thread;               // Where this message is headed to
    std::shared_ptr< _sync_sem_block > m_msg_sem; // Semaphore for sync messages
    msg_data_t m_data;

    template < class... Args >
    static iomgr_msg* create(Args&&... args) {
        return sisl::ObjectAllocator< iomgr_msg >::make_object(std::forward< Args >(args)...);
    }

    static void free(iomgr_msg* msg) { sisl::ObjectAllocator< iomgr_msg >::deallocate(msg); }

    virtual iomgr_msg* clone() {
        auto new_msg = sisl::ObjectAllocator< iomgr_msg >::make_object(m_type, m_dest_module, m_data);
        new_msg->m_msg_sem = m_msg_sem;
        return new_msg;
    }

    bool has_sem_block() const { return (m_msg_sem != nullptr); }
    // iomgr_msg &operator=(const iomgr_msg &msg) = default;
    sisl::blob data_buf() const { return std::get< sisl::blob >(m_data); }
    const run_method_t& method_data() const { return std::get< run_method_t >(m_data); }
    const reschedule_data_t& schedule_data() const { return std::get< reschedule_data_t >(m_data); }
    const std::shared_ptr< IODevice >& iodevice_data() const { return schedule_data().iodev; }
    int event() const { return schedule_data().event; }

protected:
    iomgr_msg() = default;
    iomgr_msg(const iomgr_msg& msg) = default;
    iomgr_msg(int type, msg_module_id_t module, const msg_data_t& d) : m_type(type), m_dest_module(module), m_data(d) {}
    iomgr_msg(int type, msg_module_id_t module, void* buf = nullptr, uint32_t size = 0u) :
            iomgr_msg(type, module, msg_data_t(sisl::blob{(uint8_t*)buf, size})) {}
    iomgr_msg(int type, msg_module_id_t module, const std::shared_ptr< IODevice >& iodev, int event) :
            iomgr_msg(type, module, msg_data_t(reschedule_data_t{iodev, event})) {}
    iomgr_msg(int type, msg_module_id_t module, const auto& fn) : iomgr_msg(type, module, msg_data_t(fn)) {}

    virtual ~iomgr_msg() = default;
};

struct sync_iomgr_msg {
    std::shared_ptr< _sync_sem_block > blk;
    iomgr_msg* base_msg;

    template < class... Args >
    sync_iomgr_msg(Args&&... args) {
        blk = std::make_shared< _sync_sem_block >();
        base_msg = sisl::ObjectAllocator< iomgr_msg >::make_object(std::forward< Args >(args)...);
        base_msg->m_msg_sem = blk;
    }

    void wait() {
        std::unique_lock< std::mutex > lck(blk->m_mtx);
        blk->m_cv.wait(lck, [this] { return (blk->m_pending == 0); });
    }
};
} // namespace iomgr

#if 0
namespace folly {
template <>
FOLLY_ASSUME_RELOCATABLE(iomgr::iomgr_msg);
} // namespace folly
#endif

#endif // IOMGR_IOMGR_MSG_HPP

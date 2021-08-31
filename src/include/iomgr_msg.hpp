//
// Created by Kadayam, Hari on 4/10/19.
//

#ifndef IOMGR_IOMGR_MSG_HPP
#define IOMGR_IOMGR_MSG_HPP

#include <iostream>
#include <folly/Traits.h>
#include <fds/buffer.hpp>
#include <fds/obj_allocator.hpp>
#include <utility/enum.hpp>
#include <utility/atomic_counter.hpp>
#include <utility/obj_life_counter.hpp>
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

ENUM(msg_direction_t, uint8_t,
     send,            // Plain request which doesn't need any response.
     send_need_reply, // Request with a need a response after message delivered
     reply            // Response for the request
);

struct iomgr_msg;
struct sync_msg_base {
    iomgr_msg* m_base_msg;

    virtual void set_pending() = 0;
    virtual int32_t num_pending() const = 0;
    virtual void one_completion() = 0;
    virtual void received_reply() = 0;
    virtual void wait() = 0;
    virtual bool sender_needed_iothread() const = 0;
    virtual void free_base_msg();
    iomgr_msg* base_msg() { return m_base_msg; }
};

struct iomgr_msg : public sisl::ObjLifeCounter< iomgr_msg > {
    friend class sisl::ObjectAllocator< iomgr_msg >;

    int m_type = 0;                    // Type of the message (different meaning for different modules)
    msg_module_id_t m_dest_module = 0; // Default to internal module
    io_thread_addr_t m_dest_thread;    // Where this message is headed to
    bool m_is_reply{false};
    std::shared_ptr< sync_msg_base > m_sync_msg; // Backpointer to sync messages
    msg_data_t m_data;

    template < class... Args >
    static iomgr_msg* create(Args&&... args) {
        return sisl::ObjectAllocator< iomgr_msg >::make_object(std::forward< Args >(args)...);
    }

    static void free(iomgr_msg* msg) { sisl::ObjectAllocator< iomgr_msg >::deallocate(msg); }

    static void completed(iomgr_msg* msg) {
        if (msg->m_sync_msg) {
            if (msg->m_is_reply) {
                msg->m_sync_msg->received_reply();
            } else {
                bool cloned_msg = (msg != msg->m_sync_msg->base_msg());
                msg->m_sync_msg->one_completion();

                // Base msgs are freed by the sync_msg class. We need to free only cloned messages
                if (cloned_msg) { iomgr_msg::free(msg); }
            }
        } else {
            iomgr_msg::free(msg);
        }
    }

    virtual iomgr_msg* clone() {
        auto new_msg = sisl::ObjectAllocator< iomgr_msg >::make_object(m_type, m_dest_module, m_data);
        new_msg->m_sync_msg = m_sync_msg;
        return new_msg;
    }

    bool is_sync_msg() const { return (m_sync_msg != nullptr); }

    // iomgr_msg &operator=(const iomgr_msg &msg) = default;
    sisl::blob data_buf() const { return std::get< sisl::blob >(m_data); }
    const run_method_t& method_data() const { return std::get< run_method_t >(m_data); }
    const reschedule_data_t& schedule_data() const { return std::get< reschedule_data_t >(m_data); }
    const std::shared_ptr< IODevice >& iodevice_data() const { return schedule_data().iodev; }
    int event() const { return schedule_data().event; }

    void set_pending() {
        if (is_sync_msg()) { m_sync_msg->set_pending(); }
    }

protected:
    iomgr_msg() = default;
    iomgr_msg(const iomgr_msg& msg) = default;
    iomgr_msg(int type, msg_module_id_t module, const msg_data_t& d) : m_type{type}, m_dest_module{module}, m_data{d} {}
    iomgr_msg(int type, msg_module_id_t module, void* buf = nullptr, uint32_t size = 0u) :
            m_type{type}, m_dest_module{module}, m_data{sisl::blob{(uint8_t*)buf, size}} {}
    iomgr_msg(int type, msg_module_id_t module, const std::shared_ptr< IODevice >& iodev, int event) :
            m_type{type}, m_dest_module{module}, m_data{reschedule_data_t{iodev, event}} {}
    iomgr_msg(int type, msg_module_id_t module, const auto& fn) :
            m_type{type}, m_dest_module{module}, m_data{run_method_t{fn}} {}

    // iomgr_msg(type, module, msg_data_t(sisl::blob{(uint8_t*)buf, size})) {}
    // iomgr_msg(int type, msg_module_id_t module, const std::shared_ptr< IODevice >& iodev, int event) :
    //        iomgr_msg(type, module, msg_data_t(reschedule_data_t{iodev, event})) {}
    // iomgr_msg(int type, msg_module_id_t module, const auto& fn) :
    //        iomgr_msg(type, module, msg_data_t(run_method_t{fn})) {}

    virtual ~iomgr_msg() = default;
};

struct sync_iomgr_msg : public sync_msg_base {
    mutable std::mutex m_mtx;
    std::condition_variable m_cv;
    int32_t m_pending{0};

public:
    template < class... Args >
    static std::shared_ptr< sync_iomgr_msg > create(Args&&... args) {
        auto smsg = std::make_shared< sync_iomgr_msg >();
        smsg->m_base_msg = iomgr_msg::create(std::forward< Args >(args)...);
        smsg->m_base_msg->m_sync_msg = smsg;
        return smsg;
    }

    virtual ~sync_iomgr_msg() = default;

    void set_pending() override {
        std::lock_guard< std::mutex > lck(m_mtx);
        ++m_pending;
    }

    int32_t num_pending() const override {
        std::lock_guard< std::mutex > lck(m_mtx);
        return m_pending;
    }

    void one_completion() override {
        {
            std::lock_guard< std::mutex > lck(m_mtx);
            --m_pending;
        }
        m_cv.notify_one();
    }

    void wait() override {
        std::unique_lock< std::mutex > lck(m_mtx);
        m_cv.wait(lck, [this] { return (m_pending == 0); });
    }

    void received_reply() override { assert(0); }

    bool sender_needed_iothread() const override { return false; }
};

struct spin_iomgr_msg : public sync_msg_base {
    sisl::atomic_counter< int32_t > m_pending{1};
    bool m_reply_rcvd{false};
    io_thread_t m_sender_thread;

public:
    template < class... Args >
    static std::shared_ptr< spin_iomgr_msg > create(Args&&... args) {
        auto smsg = std::make_shared< spin_iomgr_msg >();
        smsg->m_base_msg = iomgr_msg::create(std::forward< Args >(args)...);
        smsg->set_sender_thread();
        smsg->m_base_msg->m_sync_msg = smsg;
        return smsg;
    }

    virtual ~spin_iomgr_msg() = default;
    void set_sender_thread();
    void set_pending() override { m_pending.increment(1); }
    int32_t num_pending() const override { return m_pending.get(); }

    void one_completion() override;
    void received_reply() override { m_reply_rcvd = true; }
    void wait() override;
    bool sender_needed_iothread() const override { return true; }
};

} // namespace iomgr

#if 0
namespace folly {
template <>
FOLLY_ASSUME_RELOCATABLE(iomgr::iomgr_msg);
} // namespace folly
#endif

#endif // IOMGR_IOMGR_MSG_HPP

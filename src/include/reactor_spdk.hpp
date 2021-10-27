#pragma once
#include "reactor.hpp"
#include "io_interface.hpp"
#include <spdk/thread.h>

struct spdk_thread;

namespace iomgr {

class IOReactorSPDK : public IOReactor {
    friend class IOManager;

public:
    IOReactorSPDK() = default;

    bool is_iodev_addable(const io_device_const_ptr& iodev, const io_thread_t& thread) const override;
    static void deliver_msg_direct(spdk_thread* to_thread, iomgr_msg* msg);
    static int event_about_spdk_thread(struct spdk_thread* thread, enum spdk_thread_op op);
    static bool reactor_thread_op_supported(enum spdk_thread_op op);
    static std::string gen_spdk_thread_name();
    static bool is_iomgr_created_spdk_thread(const spdk_thread* thread);

private:
    const char* loop_type() const override { return "SPDK"; }
    bool reactor_specific_init_thread(const io_thread_t& thr) override;
    void reactor_specific_exit_thread(const io_thread_t& thr) override;
    void add_external_spdk_thread(struct spdk_thread* sthread);
    void listen() override;
    int add_iodev_internal(const io_device_const_ptr& iodev, const io_thread_t& thr) override;
    int remove_iodev_internal(const io_device_const_ptr& iodev, const io_thread_t& thr) override;
    bool put_msg(iomgr_msg* msg) override;
    bool is_tight_loop_reactor() const override { return true; };

private:
    std::vector< spdk_thread* > m_external_spdk_threads;
};
} // namespace iomgr

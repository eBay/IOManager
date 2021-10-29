#pragma once
#include "reactor.hpp"
#include "io_interface.hpp"

struct spdk_thread;

namespace iomgr {

class IOReactorSPDK : public IOReactor {
    friend class IOManager;

public:
    IOReactorSPDK() = default;

    bool is_iodev_addable(const io_device_const_ptr& iodev, const io_thread_t& thread) const override;
    static void deliver_msg_direct(spdk_thread* to_thread, iomgr_msg* msg);
    static spdk_thread* create_spdk_thread();

private:
    const char* loop_type() const override { return "SPDK"; }
    bool reactor_specific_init_thread(const io_thread_t& thr) override;
    void reactor_specific_exit_thread(const io_thread_t& thr) override;
    void listen() override;
    int add_iodev_internal(const io_device_const_ptr& iodev, const io_thread_t& thr) override;
    int remove_iodev_internal(const io_device_const_ptr& iodev, const io_thread_t& thr) override;
    bool put_msg(iomgr_msg* msg) override;
    bool is_tight_loop_reactor() const override { return true; };
};
} // namespace iomgr

#pragma once
#include "reactor.hpp"
#include "io_interface.hpp"

struct spdk_thread;

namespace iomgr {

class IOReactorSPDK : public IOReactor {
    friend class IOManager;

public:
    IOReactorSPDK() = default;

    bool is_iodev_addable(const io_device_ptr& iodev, const io_thread_t& thread) const override;
    static void deliver_msg_direct(spdk_thread* to_thread, iomgr_msg* msg);

private:
    const char* loop_type() const override { return "SPDK"; }
    bool reactor_specific_init_thread(const io_thread_t& thr) override;
    void reactor_specific_exit_thread(const io_thread_t& thr) override;
    void listen() override;
    int _add_iodev_to_thread(const io_device_ptr& iodev, const io_thread_t& thr) override;
    int _remove_iodev_from_thread(const io_device_ptr& iodev, const io_thread_t& thr) override;
    bool put_msg(iomgr_msg* msg) override;
    bool is_tight_loop_reactor() const override { return true; };
};
} // namespace iomgr

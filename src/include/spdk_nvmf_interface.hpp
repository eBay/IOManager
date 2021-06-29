//
// Created by Kadayam, Hari on 05/26/20.
//

#include "io_interface.hpp"
#include "reactor.hpp"
#include "iomgr_msg.hpp"

struct spdk_nvmf_poll_group;
struct spdk_nvmf_tgt;

namespace iomgr {
struct SpdkNvmfContext : public IOInterfaceThreadContext {
    struct spdk_nvmf_poll_group* poll_group;
};

class IOReactor;
class SpdkNvmfInterface : public IOInterface {
public:
    SpdkNvmfInterface(struct spdk_nvmf_tgt* tgt);
    [[nodiscard]] bool is_spdk_interface() const override { return true; }

private:
    void init_iface_thread_ctx(const io_thread_t& thr) override;
    void clear_iface_thread_ctx(const io_thread_t& thr) override;

    void init_iodev_thread_ctx(const io_device_const_ptr& iodev, const io_thread_t& thr) override;
    void clear_iodev_thread_ctx(const io_device_const_ptr& iodev, const io_thread_t& thr) override;

private:
    spdk_nvmf_tgt* m_nvmf_tgt; // TODO: Make this support a vector of targets which can be added dynamically.
};
} // namespace iomgr
#pragma once

#include <iomgr/iomgr_types.hpp>
#include "iomgr_impl.hpp"
#include "mempool_spdk.hpp"

namespace iomgr {
class IOManagerSpdkImpl : public IOManagerImpl {
public:
    IOManagerSpdkImpl(uint64_t total_hugepage_size);
    void pre_interface_init() override;
    void post_interface_init() override;
    sys_thread_id_t create_reactor(const std::string& name, loop_type_t loop_type, int slot_num,
                                   thread_state_notifier_t&& notifier) override;
    void pre_interface_stop() override;
    void post_interface_stop() override;

private:
    bool is_spdk_inited() const;
    void start_spdk();
    void stop_spdk();

private:
    shared< SpdkMemPool > m_mempool;
    uint64_t m_total_hugepage_size;
    bool m_spdk_reinit_needed{false};
    bool m_is_cpu_pinning_enabled{false};
    bool m_do_init_bdev{false};
};

} // namespace iomgr
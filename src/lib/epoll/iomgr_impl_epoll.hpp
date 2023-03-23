#pragma once

#include <iomgr/iomgr_types.hpp>
#include "iomgr_impl.hpp"

namespace iomgr {
class IOManagerEpollImpl : public IOManagerImpl {
public:
    IOManagerEpollImpl() = default;
    ~IOManagerEpollImpl() override = default;
    void pre_interface_init() override;
    void post_interface_init() override;
    sys_thread_id_t create_reactor_impl(const std::string& name, loop_type_t loop_type, uint32_t num_fibers,
                                        int slot_num, thread_state_notifier_t&& notifier) override;
    void pre_interface_stop() override;
    void post_interface_stop() override;
};

} // namespace iomgr

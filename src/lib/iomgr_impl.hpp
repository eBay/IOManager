#pragma once
#include <iomgr/iomgr_types.hpp>

namespace iomgr {
class IOManagerImpl {
public:
    virtual ~IOManagerImpl() = default;

    virtual void pre_interface_init() = 0;
    virtual void post_interface_init() = 0;
    virtual sys_thread_id_t create_reactor_impl(const std::string& name, loop_type_t loop_type, uint32_t num_fibers,
                                                int slot_num, thread_state_notifier_t&& notifier) = 0;
    virtual void pre_interface_stop() = 0;
    virtual void post_interface_stop() = 0;
};
} // namespace iomgr

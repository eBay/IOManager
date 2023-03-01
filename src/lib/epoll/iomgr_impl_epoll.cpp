#include <iomgr/iomgr.hpp>
#include <sisl/utility/thread_factory.hpp>
#include "epoll/iomgr_impl_epoll.hpp"
#include "epoll_mem.hpp"

namespace iomgr {
void IOManagerEpollImpl::pre_interface_init() {
    sisl::AlignedAllocator::instance().set_allocator(std::move(new IOMgrAlignedAllocImpl()));
}

void IOManagerEpollImpl::post_interface_init() {}

sys_thread_id_t IOManagerEpollImpl::create_reactor(const std::string& name, loop_type_t loop_type, int slot_num,
                                                   thread_state_notifier_t&& notifier) {
    auto sthread = sisl::named_thread(name, [slot_num, loop_type, name, n = std::move(notifier)]() mutable {
        iomanager._run_io_loop(slot_num, loop_type, name, nullptr, std::move(n));
    });
    sthread.detach();
    return sys_thread_id_t{std::move(sthread)};
}

void IOManagerEpollImpl::pre_interface_stop() {}

void IOManagerEpollImpl::post_interface_stop() {}
} // namespace iomgr
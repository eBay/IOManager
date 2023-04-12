#ifdef USE_BOOST_FIBER
#include "reactor/reactor.hpp"
#include <iomgr/fiber_lib.hpp>

namespace iomgr {
///////////////////////// FiberManagerLib Section //////////////////////////////
FiberManagerLib::FiberManagerLib() : m_this_fiber{[](IOFiber* f) {}} {}

std::unique_ptr< IOFiber > FiberManagerLib::create_iofiber(IOReactor* reactor, uint32_t fiber_ordinal) {
    return std::make_unique< IOFiberBoostImpl >(reactor, fiber_ordinal);
}

void FiberManagerLib::start_iofiber(IOFiber* f, const std::function< void(IOFiber*) >& channel_loop) {
    boost::fibers::fiber([f, this, channel_loop]() {
        set_this_iofiber(f);
        channel_loop(f);
    }).detach();
}

void FiberManagerLib::set_this_iofiber(IOFiber* f) {
    auto* fiber = r_cast< IOFiberBoostImpl* >(f);
    fiber->fiber_id = boost::this_fiber::get_id();
    m_this_fiber.reset(fiber);
}

IOFiber* FiberManagerLib::iofiber_self() const { return &(*m_this_fiber); };

void FiberManagerLib::yield() { boost::this_fiber::yield(); }

///////////////////////// IOFiberBoostImpl Section //////////////////////////////
IOFiberBoostImpl::IOFiberBoostImpl(IOReactor* r, uint32_t ordinal) : IOFiber{r, ordinal}, channel{max_channel_cap} {}

bool IOFiberBoostImpl::push_msg(iomgr_msg* msg) {
    auto status = channel.try_push(msg);
    if (status == boost::fibers::channel_op_status::full) { m_overflow_msgs.push(msg); }
    return (status == boost::fibers::channel_op_status::success);
}

iomgr_msg* IOFiberBoostImpl::pop_msg() {
    iomgr_msg* msg{nullptr};
    channel.pop(msg);

    // We poped a msg, so overflow msgs, it can be pushed at the tail of the fiber channel queue
    if (!m_overflow_msgs.empty()) {
        auto status = channel.try_push(m_overflow_msgs.front());
        if (status != boost::fibers::channel_op_status::success) {
            LOGMSG_ASSERT_EQ((int)status, (int)boost::fibers::channel_op_status::success,
                             "Moving msg from overflow to fiber loop channel has failed, unexpected");
        } else {
            m_overflow_msgs.pop();
        }
    }
    return msg;
}

void IOFiberBoostImpl::close_channel() { channel.close(); }
} // namespace iomgr
#endif

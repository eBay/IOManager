#ifdef USE_FOLLY_FIBER
#include "reactor/reactor.hpp"
#include <iomgr/fiber_lib.hpp>
#include <folly/io/async/AsyncTimeout.h>

namespace iomgr {
/////////////////////////////////////// FiberManagerLib Section ///////////////////////////////////////
FiberManagerLib::FiberManagerLib() :
        m_fiber_mgr{folly::fibers::LocalType< IOFiberFollyImpl* >(), std::make_unique< ReactorLoopController >()} {}

std::unique_ptr< IOFiber > FiberManagerLib::create_iofiber(IOReactor* reactor, uint32_t fiber_ordinal) {
    return std::make_unique< IOFiberFollyImpl >(reactor, fiber_ordinal);
}

void FiberManagerLib::start_iofiber(IOFiber* f, const std::function< void(IOFiber*) >& channel_loop) {
    m_fiber_mgr.addTask([this, f, channel_loop]() {
        set_this_iofiber(f);
        channel_loop(f);
    });
}

void FiberManagerLib::set_this_iofiber(IOFiber* f) {
    auto* fiber = r_cast< IOFiberFollyImpl* >(f);
    folly::fibers::local< IOFiberFollyImpl* >() = fiber;
}

IOFiber* FiberManagerLib::iofiber_self() const { return folly::fibers::local< IOFiberFollyImpl* >(); };

void FiberManagerLib::yield() { m_fiber_mgr.yield(); }

void FiberManagerLib::yield_main() { m_fiber_mgr.loopUntilNoReadyImpl(); }

/////////////////////////////////////// ReactorLoopController Section ///////////////////////////////////////
ReactorLoopController::ReactorLoopController() :
        m_tm_mgr(std::make_unique< SimpleTimeoutManager >(*this)),
        m_wheel_timer(folly::HHWheelTimer::newTimer(m_tm_mgr.get())) {}

void ReactorLoopController::runLoop() {
    do {
        if (m_remote_run_count < m_remote_schedule_count) {
            for (; m_remote_schedule_count < m_remote_schedule_count; ++m_remote_run_count) {
                if (m_fm->shouldRunLoopRemote()) { m_fm->loopUntilNoReadyImpl(); }
            }
        } else {
            m_fm->loopUntilNoReadyImpl();
        }
    } while (m_remote_run_count < m_remote_schedule_count);
}

void ReactorLoopController::schedule() {}

void ReactorLoopController::scheduleThreadSafe() { ++m_remote_schedule_count; }

void ReactorLoopController::runEagerFiber(folly::fibers::Fiber* f) { m_fm->runEagerFiberImpl(f); }

folly::HHWheelTimer* ReactorLoopController::timer() { return m_wheel_timer.get(); }

////////////////////////// Used the code from folly to implement a simple version of TimeoutManager
/**
 * A simple version of TimeoutManager that maintains only a single AsyncTimeout
 * object that is used by HHWheelTimer in SimpleLoopController.
 */
class ReactorLoopController::SimpleTimeoutManager : public folly::TimeoutManager {
public:
    explicit SimpleTimeoutManager(ReactorLoopController& lc) : m_rlc(lc) {}

    void attachTimeoutManager(folly::AsyncTimeout*, folly::TimeoutManager::InternalEnum) final {}
    void detachTimeoutManager(folly::AsyncTimeout*) final {}

    bool scheduleTimeout(folly::AsyncTimeout* obj, timeout_type timeout) final {
        // Make sure that we don't try to use this manager with two timeouts.
        // CHECK(!timeout_ || timeout_->first == obj);
        m_timeout.emplace(obj, std::chrono::steady_clock::now() + timeout);
        return true;
    }

    void cancelTimeout(folly::AsyncTimeout* obj) final {
        // CHECK(timeout_ && timeout_->first == obj);
        m_timeout.reset();
    }

    void bumpHandlingTime() final {}

    // bool isInTimeoutManagerThread() final { return m_rlc.isInLoopThread(); }
    bool isInTimeoutManagerThread() final { return false; }

    void runTimeouts() {
        std::chrono::steady_clock::time_point tp = std::chrono::steady_clock::now();
        if (!m_timeout || tp < m_timeout->second) { return; }

        auto* timeout = m_timeout->first;
        m_timeout.reset();
        timeout->timeoutExpired();
    }

private:
    ReactorLoopController& m_rlc;
    folly::Optional< std::pair< folly::AsyncTimeout*, std::chrono::steady_clock::time_point > > m_timeout;
};

/////////////////////////////////////// IOFiberFollyImpl Section ///////////////////////////////////////
IOFiberFollyImpl::IOFiberFollyImpl(IOReactor* r, uint32_t ordinal) : IOFiber{r, ordinal} {}

bool IOFiberFollyImpl::push_msg(iomgr_msg* msg) {
    channel.push(msg);
    channel_baton.post();
    return true;
}

iomgr_msg* IOFiberFollyImpl::pop_msg() {
    iomgr_msg* msg{nullptr};
    channel_baton.wait();
    // LOGMSG_ASSERT_NE(channel.empty(), true, "Fiber channel baton wokenup but no msg in queue");
    if (!channel.empty()) {
        msg = channel.front();
        channel.pop();
    }
    return msg;
}

void IOFiberFollyImpl::close_channel() {
    std::queue< iomgr_msg* > empty;
    std::swap(channel, empty);
}
} // namespace iomgr
#endif

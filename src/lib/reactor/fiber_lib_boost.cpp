#ifndef USE_FOLLY_FIBER
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

uint32_t FiberManagerLib::iofiber_self_ordinal() const {
    auto f = iofiber_self();
    return f ? f->ordinal : 0;
}

void FiberManagerLib::yield() { boost::this_fiber::yield(); }

void FiberManagerLib::yield_main() { boost::this_fiber::yield(); }

////////////////////////// shared_mutex implementation ///////////////////////////
void FiberManagerLib::shared_mutex::lock_shared() {
    do {
        boost::fibers::context* active_ctx = boost::fibers::context::active();

        boost::fibers::detail::spinlock_lock lk{m_wait_q_splk};
        if ((m_write_owner != nullptr) || (m_write_waiters != 0)) {
            // Either Owned by the writer or some writer is waiting, add to the waiter q
            LOGTRACEMOD(iomgr, "[Reader Lock for ctx={}]: Queued. Owned by a writer={} and/or writers are waiting={}",
                        (void*)active_ctx, (void*)m_write_owner, m_write_waiters);
            m_wait_q.suspend_and_wait(lk, active_ctx);
        } else {
            ++m_readers; // No fiber is holding write access or needing write access
            LOGTRACEMOD(iomgr, "[Reader Lock for ctx={}]: Won, num_readers={}", (void*)active_ctx, m_readers);
            break;
        }
    } while (true);
}

void FiberManagerLib::shared_mutex::lock() {
    bool am_i_write_waiter{false};
    do {
        boost::fibers::context* active_ctx = boost::fibers::context::active();
        boost::fibers::detail::spinlock_lock lk{m_wait_q_splk};
        if ((m_write_owner == nullptr) && (m_readers == 0)) {
            m_write_owner = active_ctx; // No exclusive and shared access yet
            if (am_i_write_waiter) {
                --m_write_waiters;
                LOGTRACEMOD(iomgr, "[Writer Lock for ctx={}]: Was waiter, now won, num_write_waiters={}",
                            (void*)active_ctx, m_write_waiters);
            } else {
                LOGTRACEMOD(iomgr, "[Writer Lock for ctx={}]: Won", (void*)active_ctx);
            }
            break;
        } else {
            // Owned by the writer or reader, add ourselves to writer queue
            if (s_writer_priority) {
                if (!am_i_write_waiter) {
                    ++m_write_waiters;
                    am_i_write_waiter = true;
                }
                LOGTRACEMOD(iomgr, "[Writer Lock for ctx={}]: Queued, owned by owner={} num_readers={}",
                            (void*)active_ctx, (void*)m_write_owner, m_readers);
            }
            m_wait_q.suspend_and_wait(lk, active_ctx);
            LOGTRACEMOD(iomgr, "[Writer Lock for ctx={}]: Queued earlier now awakened, num_readers={}",
                        (void*)active_ctx, m_readers);
        }
    } while (true);
}

void FiberManagerLib::shared_mutex::unlock_shared() {
    boost::fibers::detail::spinlock_lock lk{m_wait_q_splk};
    if (m_readers == 0) {
        throw boost::fibers::lock_error{std::make_error_code(std::errc::operation_not_permitted),
                                        "boost fiber: duplicate unlock shared, can cause deadlocks"};
    }
    --m_readers;
    LOGTRACEMOD(iomgr, "Reader unlock, num_readers={}", m_readers);
    m_wait_q.notify_all();
}

void FiberManagerLib::shared_mutex::unlock() {
    boost::fibers::context* active_ctx = boost::fibers::context::active();

    boost::fibers::detail::spinlock_lock lk{m_wait_q_splk};
    if (m_write_owner != active_ctx) {
        throw boost::fibers::lock_error{std::make_error_code(std::errc::operation_not_permitted),
                                        "boost fiber: unlock called by different fiber to the one locked it"};
    }
    m_write_owner = nullptr;
    LOGTRACEMOD(iomgr, "[Writer unLock for ctx={}]: unlocked", (void*)active_ctx);
    m_wait_q.notify_all();
}

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

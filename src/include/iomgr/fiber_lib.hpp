#pragma once

#include <boost/fiber/all.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/detail/spinlock.hpp>
#include <boost/fiber/waker.hpp>

struct spdk_thread;

namespace iomgr {
class IOReactor;
struct iomgr_msg;

struct IOFiber {
public:
    IOReactor* reactor;             // Reactor this fiber is currently attached to
    spdk_thread* spdk_thr{nullptr}; // In case of spdk, each fiber becomes spdk thread
    uint32_t ordinal;               // Global ordinal of this fiber (unique id across iomgr)

public:
    IOFiber(IOReactor* r, uint32_t o) : reactor{r}, ordinal{o} {}
    virtual ~IOFiber() = default;

    virtual bool push_msg(iomgr_msg* msg) = 0;
    virtual iomgr_msg* pop_msg() = 0;
    virtual void close_channel() = 0;
};

class FiberManagerLib {
private:
    boost::fibers::fiber_specific_ptr< IOFiber > m_this_fiber;

public:
    template < typename T >
    class Future : public boost::fibers::future< T > {};

    template < typename T >
    class Promise : public boost::fibers::promise< T > {
    public:
        /*
        const decltype(&Promise< T >::set_value) setValue = &Promise< T >::set_value;
        const auto setException = set_exception;
        const auto getFuture = get_future;
        */

        void setValue(T t) { this->set_value(std::move(t)); }

        template < typename... Args >
        void setException(Args&&... args) {
            this->set_exception(args...);
        }

        Future< T > getFuture() {
            auto f{this->get_future()};
            return std::move(*((Future< T >*)&f));
        }
    };

    using mutex = boost::fibers::mutex;
    class shared_mutex {
    public:
        void lock_shared();
        void lock();
        void unlock_shared();
        void unlock();

    private:
        static constexpr bool s_writer_priority{true};
        boost::fibers::detail::spinlock m_wait_q_splk{};
        boost::fibers::wait_queue m_wait_q;
        boost::fibers::context* m_write_owner{nullptr};
        uint32_t m_readers{0};
        uint32_t m_write_waiters{0};
    };

    FiberManagerLib();
    std::unique_ptr< IOFiber > create_iofiber(IOReactor* reactor, uint32_t fiber_ordinal);
    void start_iofiber(IOFiber* f, const std::function< void(IOFiber*) >& channel_loop);
    IOFiber* iofiber_self() const;
    uint32_t iofiber_self_ordinal() const;
    void set_this_iofiber(IOFiber* f);
    void start_io_fiber();
    void yield();
    void yield_main();
};

struct IOFiberBoostImpl : public IOFiber {
    static constexpr size_t max_channel_cap{1024};

public:
    boost::fibers::fiber::id fiber_id;                     // Boost specific fiber id
    boost::fibers::buffered_channel< iomgr_msg* > channel; // Channel to exchange between main and this fiber
    std::queue< iomgr_msg* > m_overflow_msgs;              // Overflow queue if msgs can't be put in channel

public:
    IOFiberBoostImpl(IOReactor* r, uint32_t ordinal);
    bool push_msg(iomgr_msg* msg) override;
    iomgr_msg* pop_msg() override;
    void close_channel() override;
};

}; // namespace iomgr

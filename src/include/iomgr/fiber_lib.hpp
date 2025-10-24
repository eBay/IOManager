#pragma once

#ifdef USE_FOLLY_FIBER
#include <folly/fibers/FiberManager.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <folly/futures/Future.h>
#include <folly/fibers/Promise.h>
#pragma GCC diagnostic pop
#else
#include <boost/fiber/all.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/detail/spinlock.hpp>
#include <boost/fiber/waker.hpp>
#endif

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

#ifndef USE_FOLLY_FIBER
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
#else
class FiberManagerLib {
private:
    folly::fibers::FiberManager m_fiber_mgr;

public:
    template < typename T, typename BatonT = folly::fibers::Baton >
    struct SharedState {
        folly::Try< T > val;
        BatonT baton;
    };

    template < typename T, typename BatonT = folly::fibers::Baton >
    class Future {
    private:
        std::shared_ptr< SharedState< T, BatonT > > m_shared_state;

    public:
        Future(cshared< SharedState< T, BatonT > >& s) : m_shared_state{s} {}

        Future(const Future&) = delete;
        Future& operator=(const Future&) = delete;

        Future(Future&& other) noexcept : m_shared_state{std::move(other.m_shared_state)} {}
        Future& operator=(Future&& other) {
            std::swap(m_shared_state, other.m_shared_state);
            return *this;
        }

        T get() {
            m_shared_state->baton.wait();
            return *m_shared_state->val;
        }
    };

    // Simpler implementation of Promise to ensure what we need for iomanager use case
    template < typename T, typename BatonT = folly::fibers::Baton >
    class Promise {
    private:
        std::shared_ptr< SharedState< T, BatonT > > m_shared_state;

    public:
        Promise() { m_shared_state = std::make_shared< SharedState< T, BatonT > >(); }

        ~Promise() {
            // if (m_shared_state->val) {
            //     setException(folly::make_exception_wrapper< std::logic_error >("promise not fulfilled"));
            // }
        }

        // not copyable
        Promise(const Promise&) = delete;
        Promise& operator=(const Promise&) = delete;

        // movable
        Promise(Promise&& other) noexcept : m_shared_state{std::move(other.m_shared_state)} {}
        Promise& operator=(Promise&& other) {
            std::swap(m_shared_state, other.m_shared_state);
            return *this;
        }

        void setValue() {
            static_assert(std::is_same< T, void >::value, "Use setValue(value) instead");
            setTry(folly::Try< void >());
        }

        template < class M >
        void setValue(M&& value) {
            static_assert(!std::is_same< T, void >::value, "Use setValue() instead");
            setTry(folly::Try< T >(std::forward< M >(value)));
        }

        void setTry(folly::Try< T >&& t) {
            // if (m_shared_state->val) { throw std::logic_error("promise already fulfilled"); }

            m_shared_state->val = std::move(t);

            // Baton::post has to be the last step here, since if Promise is not owned by the posting thread, it may be
            // destroyed right after Baton::post is called.
            m_shared_state->baton.post();
        }

        template < class F >
        void setWith(F&& func) {
            setTry(folly::makeTryWith(std::forward< F >(func)));
        }

        void setException(folly::exception_wrapper e) { setTry(folly::Try< T >(e)); }

        void setException(std::exception_ptr e) { setException(folly::exception_wrapper{e}); }

        // Wrapper method to return baton mimicing as future
        Future< T > getFuture() { return Future< T >{m_shared_state}; }
    };

    FiberManagerLib();
    std::unique_ptr< IOFiber > create_iofiber(IOReactor* reactor, uint32_t fiber_ordinal);
    void start_iofiber(IOFiber* f, const std::function< void(IOFiber*) >& channel_loop);
    IOFiber* iofiber_self() const;
    uint32_t iofiber_self_ordinal() const;
    void set_this_iofiber(IOFiber* f);
    void start_io_fiber();
    void yield();
    void yield_main(); // Yield main fiber

    using mutex = folly::fibers::TimedMutex;
    using shared_mutex = folly::fibers::TimedRWMutex;
};

class ReactorLoopController : public folly::fibers::LoopController {
private:
    class SimpleTimeoutManager;

    folly::fibers::FiberManager* m_fm{nullptr};
    std::unique_ptr< SimpleTimeoutManager > m_tm_mgr;
    std::shared_ptr< folly::HHWheelTimer > m_wheel_timer;
    std::atomic< int > m_remote_schedule_count{0};
    int m_remote_run_count{0};

public:
    ReactorLoopController();
    void setFiberManager(folly::fibers::FiberManager* mgr) override { m_fm = mgr; }
    void runLoop() override;
    void schedule() override;
    void scheduleThreadSafe() override;
    void runEagerFiber(folly::fibers::Fiber* f) override;
    folly::HHWheelTimer* timer() override;
};

struct IOFiberFollyImpl : public IOFiber {
public:
    std::queue< iomgr_msg* > channel;
    folly::fibers::Baton channel_baton;

public:
    IOFiberFollyImpl(IOReactor* r, uint32_t ordinal);
    bool push_msg(iomgr_msg* msg) override;
    iomgr_msg* pop_msg() override;
    void close_channel() override;
};
#endif

}; // namespace iomgr

//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
// Taken from boost code of round robin algorithm and modified for iomgr reactor pattern
#include <condition_variable>
#include <chrono>
#include <mutex>

#include <boost/fiber/algo/algorithm.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/detail/config.hpp>
#include <boost/fiber/scheduler.hpp>

namespace iomgr {

class io_fiber_picker : public boost::fibers::algo::algorithm {
private:
    typedef boost::fibers::scheduler::ready_queue_type rqueue_type;

    rqueue_type m_rqueue{};
    std::mutex m_mtx{};
    std::condition_variable m_cnd{};
    bool m_flag{false};
    boost::fibers::context* m_main_ctx{nullptr};

public:
    io_fiber_picker() = default;

    io_fiber_picker(io_fiber_picker const&) = delete;
    io_fiber_picker& operator=(io_fiber_picker const&) = delete;

    void awakened(boost::fibers::context*) noexcept override;

    boost::fibers::context* pick_next() noexcept override;

    bool has_ready_fibers() const noexcept override;

    void suspend_until(std::chrono::steady_clock::time_point const&) noexcept override;

    void notify() noexcept override;
};

} // namespace iomgr
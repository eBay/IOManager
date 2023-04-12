//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
// Taken from boost code of round robin algorithm and modified for iomgr reactor pattern

#include <boost/assert.hpp>
#include <boost/context/detail/prefetch.hpp>
#include "fiber_picker.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#include BOOST_ABI_PREFIX
#endif

namespace iomgr {

void io_fiber_picker::awakened(boost::fibers::context* ctx) noexcept {
    BOOST_ASSERT(nullptr != ctx);
    BOOST_ASSERT(!ctx->ready_is_linked());
    BOOST_ASSERT(ctx->is_resumable());
    if (ctx->is_context(boost::fibers::type::main_context)) { m_main_ctx = ctx; }
    ctx->ready_link(m_rqueue);
}

boost::fibers::context* io_fiber_picker::pick_next() noexcept {
    boost::fibers::context* victim = nullptr;
    if (!m_rqueue.empty()) {
        victim = &m_rqueue.front();
        m_rqueue.pop_front();
        boost::context::detail::prefetch_range(victim, sizeof(boost::fibers::context));
        BOOST_ASSERT(nullptr != victim);
        BOOST_ASSERT(!victim->ready_is_linked());
        BOOST_ASSERT(victim->is_resumable());
    }
    return victim;
}

bool io_fiber_picker::has_ready_fibers() const noexcept { return !m_rqueue.empty(); }

#if 0
void io_fiber_picker::suspend_until(std::chrono::steady_clock::time_point const& time_point) noexcept {
    if ((std::chrono::steady_clock::time_point::max)() == time_point) {
        std::unique_lock< std::mutex > lk{m_mtx};
        m_cnd.wait(lk, [&]() { return m_flag; });
        m_flag = false;
    } else {
        std::unique_lock< std::mutex > lk{m_mtx};
        m_cnd.wait_until(lk, time_point, [&]() { return m_flag; });
        m_flag = false;
    }
}

void io_fiber_picker::notify() noexcept {
    std::unique_lock< std::mutex > lk{m_mtx};
    m_flag = true;
    lk.unlock();
    m_cnd.notify_all();
}
#endif

void io_fiber_picker::suspend_until(std::chrono::steady_clock::time_point const& time_point) noexcept {
    m_main_ctx->ready_link(m_rqueue);
}

void io_fiber_picker::notify() noexcept {}

} // namespace iomgr

#ifdef BOOST_HAS_ABI_HEADERS
#include BOOST_ABI_SUFFIX
#endif

//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_DEFAULT_ENDPOINT_HPP
#define IOMGR_DEFAULT_ENDPOINT_HPP

#include "endpoint.hpp"
#include <functional>

namespace iomgr {
class TestEndPoint : public EndPoint {
public:
    TestEndPoint(const std::function< void(bool) >& thread_status_notify) : m_thread_notifier(thread_status_notify) {}
    void on_thread_start() override { m_thread_notifier(true); };
    void on_thread_exit() override { m_thread_notifier(false); };

private:
    std::function< void(bool thread_started) > m_thread_notifier;
};
} // namespace iomgr
#endif // IOMGR_DEFAULT_ENDPOINT_HPP

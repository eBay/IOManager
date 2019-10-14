#pragma once
#include "../include/iomgr.hpp"
#include <thread>
#include <chrono>
#include <gtest/gtest.h>
#include <atomic>
#include <unordered_map>
#include <sds_logging/logging.h>

namespace iomgr {

enum class EvtTriggerType { TYPE_1, TYPE_2 };

class test_ep : public iomgr::IOInterface {
public:
    test_ep(std::shared_ptr< iomgr::ioMgr > iomgr_ptr);
    ~test_ep() = default;
    void shutdown_local() override;
    void init_local() override;
    void print_perf() override;
};

class IOMgrTester : public ::testing::Test {
    // typedef std::function<void(const int fd, const void* cookie)> trigger_evt_func_t;
    typedef std::function< void() > callback_t;

public:
    // Create a bounded lock protected queue.
    // IOMgrTester(int num_threads, int num_priorities, EvtTriggerType t, callback_t cb);
    IOMgrTester();
    ~IOMgrTester();

    void start();
    void stop();

    void     count_cb(int fd);
    void     set_ev_tri_type(EvtTriggerType t);
    bool     wait_for_result(const uint64_t timeout_secs);
    uint64_t get_cb_cnt() const;

private:
    void        process_ev_callback(const int fd, const void* cookie __attribute__((unused)), const int event);
    int         rand_fd();
    std::size_t get_thread_id() noexcept;
    std::size_t get_thread_id_linear() noexcept;

private:
    std::shared_ptr< iomgr::ioMgr > m_iomgr;
    test_ep*                        m_ep;
    std::vector< int >              m_ev_fd;
    EvtTriggerType                  m_ev_tri_type;
    callback_t                      m_cb;
    uint64_t                        m_num_attemps;
    uint64_t                        m_cb_cnt;
    std::mutex                      m_lk;
};
} // namespace iomgr

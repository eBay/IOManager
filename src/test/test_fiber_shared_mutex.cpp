/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Author/Developer(s): Harihara Kadayam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#include <iostream>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <iomgr/iomgr.hpp>
#include <iomgr/io_environment.hpp>
#include <iomgr/fiber_lib.hpp>

SISL_OPTION_GROUP(test_fiber_shared_mutex,
                  (num_threads, "", "num_threads", "number of threads",
                   ::cxxopts::value< uint32_t >()->default_value("10"), "number"),
                  (num_fibers, "", "num_fibers", "number of fibers per thread",
                   ::cxxopts::value< uint32_t >()->default_value("20"), "number"),
                  (num_iters, "", "num_iters", "number of iterations",
                   ::cxxopts::value< uint64_t >()->default_value("10000"), "number"),
                  (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"));

#define ENABLED_OPTIONS logging, iomgr, test_fiber_shared_mutex, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

static uint64_t g_prev_val{0};
static uint64_t g_cur_val{1};

class SharedMutexTest : public testing::Test {
protected:
    iomgr::FiberManagerLib::shared_mutex m_cb_mtx;
    std::vector< iomgr::io_fiber_t > m_fibers;
    uint64_t m_count_per_fiber{0};
    std::mutex m_test_done_mtx;
    std::condition_variable m_test_done_cv;
    uint32_t m_test_count{0};

protected:
    void SetUp() override {
        auto nthreads = SISL_OPTIONS["num_threads"].as< uint32_t >();
        auto nfibers = SISL_OPTIONS["num_fibers"].as< uint32_t >();
        auto is_spdk = SISL_OPTIONS["spdk"].as< bool >();
        auto num_iters = sisl::round_up(SISL_OPTIONS["num_iters"].as< uint64_t >(), nthreads);

        if (is_spdk && nthreads > 2) {
            LOGINFO("Spdk with more than 2 threads will cause overburden test systems, changing nthreads to 2");
            nthreads = 2;
        }

        LOGINFO("Starting iomgr with {} threads, spdk: {}", nthreads, is_spdk);
        ioenvironment.with_iomgr(
            iomgr::iomgr_params{.num_threads = nthreads, .is_spdk = is_spdk, .num_fibers = nfibers + 1});

        // Get all the sync io'able fibers into a single list
        std::mutex mtx;
        iomanager.run_on_wait(iomgr::reactor_regex::all_worker, [this, &mtx]() {
            auto fv = iomanager.sync_io_capable_fibers();
            std::unique_lock lg(mtx);
            m_fibers.insert(m_fibers.end(), fv.begin(), fv.end());
        });

        m_test_count = m_fibers.size();
        // m_count_per_fiber = (num_iters - 1) / (nthreads * nfibers) + 1;
        m_count_per_fiber = num_iters;
    }

    void TearDown() override { iomanager.stop(); }

    void all_writer() {
        for (uint64_t i{0}; i < m_count_per_fiber; ++i) {
            write_once();
        }

        LOGINFO("Fiber completed {} of exclusive locks", m_count_per_fiber);
        {
            std::unique_lock lg(m_test_done_mtx);
            if (--m_test_count == 0) { m_test_done_cv.notify_one(); }
        }
    }

    void all_reader() {
        for (uint64_t i{0}; i < m_count_per_fiber; ++i) {
            read_once();
        }

        LOGINFO("Fiber completed {} of shared locks", m_count_per_fiber);
        {
            std::unique_lock lg(m_test_done_mtx);
            if (--m_test_count == 0) { m_test_done_cv.notify_one(); }
        }
    }

    void random_reader_writer() {
        static thread_local std::random_device rd{};
        static thread_local std::default_random_engine re{rd()};
        std::uniform_int_distribution< int > read_or_write{0, 1};

        uint64_t read_count{0};
        uint64_t write_count{0};
        for (uint64_t i{0}; i < m_count_per_fiber; ++i) {
            if (read_or_write(re) == 0) {
                write_once();
                ++write_count;
            } else {
                read_once();
                ++read_count;
            }
        }

        LOGINFO("Fiber completed shared_locks={} exclusive_locks={}", read_count, write_count);
        {
            std::unique_lock lg(m_test_done_mtx);
            if (--m_test_count == 0) { m_test_done_cv.notify_one(); }
        }
    }

    void write_once() {
        m_cb_mtx.lock();
        ASSERT_EQ((g_prev_val + 1), g_cur_val);
        g_prev_val = g_cur_val++;
        m_cb_mtx.unlock();
    }

    void read_once() {
        m_cb_mtx.lock_shared();
        ASSERT_EQ((g_prev_val + 1), g_cur_val);
        m_cb_mtx.unlock_shared();
    }
};

TEST_F(SharedMutexTest, single_writer_multiple_readers) {
    iomanager.run_on_forget(m_fibers[0], [this]() { all_writer(); });
    for (auto it = m_fibers.begin() + 1; it < m_fibers.end(); ++it) {
        iomanager.run_on_forget(*it, [this]() { all_reader(); });
    }

    {
        std::unique_lock< std::mutex > lk(m_test_done_mtx);
        m_test_done_cv.wait(lk, [&]() { return m_test_count == 0; });
    }
}

TEST_F(SharedMutexTest, random_reader_writers) {
    for (const auto& f : m_fibers) {
        iomanager.run_on_forget(f, [this]() { random_reader_writer(); });
    }

    {
        std::unique_lock< std::mutex > lk(m_test_done_mtx);
        m_test_done_cv.wait(lk, [&]() { return m_test_count == 0; });
    }
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS)
    sisl::logging::SetLogger("test_fiber_shared_mutex");
    spdlog::set_pattern("[%D %T%z] [%^%L%$] [%t] %v");

    auto ret = RUN_ALL_TESTS();
    return ret;
}

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
#include <shared_mutex>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <iomgr/iomgr.hpp>
#include <iomgr/io_environment.hpp>

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SISL_OPTION_GROUP(test_fiber_shared_mutex,
                  (num_threads, "", "num_threads", "number of threads",
                   ::cxxopts::value< uint32_t >()->default_value("10"), "number"),
                  (num_iters, "", "num_iters", "number of iterations",
                   ::cxxopts::value< uint64_t >()->default_value("10000"), "number"),);

#define ENABLED_OPTIONS logging, iomgr, test_fiber_shared_mutex, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

static uint64_t g_prev_val{0};
static uint64_t g_cur_val{1};

class SharedMutexTest : public testing::Test {
protected:
    std::shared_mutex m_cb_mtx;
    uint64_t m_count_per_thread{0};
    std::mutex m_test_done_mtx;
    std::condition_variable m_test_done_cv;
    uint32_t m_test_count{0};

protected:
    void SetUp() override {
        auto nthreads = SISL_OPTIONS["num_threads"].as< uint32_t >();        auto num_iters = sisl::round_up(SISL_OPTIONS["num_iters"].as< uint64_t >(), nthreads);        ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = nthreads});

        m_test_count = nthreads;
        m_count_per_thread = num_iters;
    }

    void TearDown() override { iomanager.stop(); }

    void all_writer() {
        for (uint64_t i{0}; i < m_count_per_thread; ++i) {
            write_once();
        }
        LOGINFO("Thread completed {} exclusive locks", m_count_per_thread);
        {
            std::unique_lock lg(m_test_done_mtx);
            if (--m_test_count == 0) { m_test_done_cv.notify_one(); }
        }
    }

    void all_reader() {
        for (uint64_t i{0}; i < m_count_per_thread; ++i) {
            read_once();
        }
        LOGINFO("Thread completed {} shared locks", m_count_per_thread);
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
        for (uint64_t i{0}; i < m_count_per_thread; ++i) {
            if (read_or_write(re) == 0) {
                write_once();
                ++write_count;
            } else {
                read_once();
                ++read_count;
            }
        }

        LOGINFO("Thread completed shared_locks={} exclusive_locks={}", read_count, write_count);
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
    auto nthreads = SISL_OPTIONS["num_threads"].as< uint32_t >();

    // First reactor does all writes; the rest do reads.
    // We use run_on_forget to reactor_regex::all_worker, but the first invocation
    // sends a writer and the rest send readers. Since we can't easily target "first
    // reactor only", simplify: send one writer per run. Use round_robin to spread.
    iomanager.run_on_forget(iomgr::reactor_regex::all_worker, [this, nthreads]() {
        static std::atomic< int > counter{0};
        if (counter.fetch_add(1) == 0) {
            all_writer();
        } else {
            all_reader();
        }
    });

    {
        std::unique_lock< std::mutex > lk(m_test_done_mtx);
        m_test_done_cv.wait(lk, [&]() { return m_test_count == 0; });
    }
}

TEST_F(SharedMutexTest, random_reader_writers) {
    iomanager.run_on_forget(iomgr::reactor_regex::all_worker, [this]() { random_reader_writer(); });

    {
        std::unique_lock< std::mutex > lk(m_test_done_mtx);
        m_test_done_cv.wait(lk, [&]() { return m_test_count == 0; });
    }
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS)
    sisl::logging::SetLogger("test_shared_mutex");
    spdlog::set_pattern("[%D %T%z] [%^%L%$] [%t] %v");

    auto ret = RUN_ALL_TESTS();
    return ret;
}

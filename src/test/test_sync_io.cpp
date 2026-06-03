/************************************************************************
 * Copyright 2017-2019 eBay Inc.
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
 **************************************************************************/

// Exercises iomgr::sync_wait -- the blocking drive-I/O bridge that runs the op on iomgr's dedicated sync
// reactor. The defining property under the stackless-coroutine (v13) model is that sync_wait is safe to call
// from ANY thread, INCLUDING an io-reactor: a reactor that issued the op cannot reap its own completion while
// blocked, so the op is run on a separate dedicated reactor instead. The on_reactor / concurrent_from_reactors
// cases below would deadlock (or trip the old off-reactor assert) if that routing regressed.

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#endif

#include <sisl/fds/utils.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <gtest/gtest.h>

#include <iomgr/io_environment.hpp>
#include <iomgr/iomgr.hpp>
#include <iomgr/drive.hpp>
#include <iomgr/io_op.hpp>

using namespace iomgr;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SISL_OPTION_GROUP(test_sync_io,
                  (dev, "", "dev", "dev", ::cxxopts::value< std::string >()->default_value("/tmp/test_sync_io"),
                   "path"))

#define ENABLED_OPTIONS logging, iomgr, test_sync_io, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

class SyncIoTest : public ::testing::Test {
public:
    static constexpr uint64_t k_dev_size{32 * 1024 * 1024};

    void SetUp() override {
        m_dev = SISL_OPTIONS["dev"].as< std::string >();
        if (!std::filesystem::exists(m_dev)) {
            auto fd = ::open(m_dev.c_str(), O_RDWR | O_CREAT, 0666);
            ASSERT_NE(fd, -1) << "open of device " << m_dev << " failed";
            ASSERT_EQ(::fallocate(fd, 0, 0, k_dev_size), 0) << "fallocate of " << m_dev << " failed";
            ::close(fd);
        }
        // Two worker reactors plus the dedicated sync reactor that iomanager::start() creates internally.
        ioenvironment.with_iomgr(iomgr_params{.num_threads = 2});
        m_drive = iomgr::open_drive(m_dev, O_CREAT | O_RDWR).value();
        m_align = iomgr::attributes_of(m_dev).align_size;
        ASSERT_GT(m_align, 0u);
    }

    void TearDown() override {
        m_drive.reset(); // RAII close; must precede iomanager::stop()
        iomanager.stop();
    }

    // Write a known pattern at `offset`, read it back -- both via blocking iomgr::sync_wait -- and assert the
    // data round-trips. Callable from any thread; the assertions record on whatever thread invokes it.
    void sync_write_read_roundtrip(uint64_t offset, uint8_t pattern) {
        const uint32_t io_sz = static_cast< uint32_t >(m_align * 4);
        uint8_t* wbuf = iomanager.iobuf_alloc(m_align, io_sz);
        uint8_t* rbuf = iomanager.iobuf_alloc(m_align, io_sz);
        std::memset(wbuf, pattern, io_sz);
        std::memset(rbuf, static_cast< uint8_t >(~pattern), io_sz);

        auto const wr =
            iomgr::sync_wait(iomgr::async_write(m_drive, reinterpret_cast< const char* >(wbuf), io_sz, offset));
        ASSERT_TRUE(wr.has_value()) << "sync_wait(async_write) failed: " << wr.error().message();
        EXPECT_EQ(wr.value(), io_sz);

        auto const rr = iomgr::sync_wait(iomgr::async_read(m_drive, reinterpret_cast< char* >(rbuf), io_sz, offset));
        ASSERT_TRUE(rr.has_value()) << "sync_wait(async_read) failed: " << rr.error().message();
        EXPECT_EQ(rr.value(), io_sz);

        EXPECT_EQ(std::memcmp(wbuf, rbuf, io_sz), 0) << "data mismatch at offset " << offset;

        iomanager.iobuf_free(wbuf);
        iomanager.iobuf_free(rbuf);
    }

protected:
    std::string m_dev;
    iomgr::drive_handle m_drive;
    uint64_t m_align{0};
};

// Baseline: sync_wait from a plain (non-reactor) thread -- the gtest main thread.
TEST_F(SyncIoTest, off_reactor) {
    ASSERT_FALSE(iomanager.am_i_io_reactor());
    sync_write_read_roundtrip(0, 0xA5);
}

// The defining case: sync_wait issued FROM an io-reactor must complete (not deadlock). run_on_wait blocks the
// main thread until the worker reactor has finished, and the worker reactor itself blocks inside sync_wait
// until the dedicated sync reactor reaps the op -- a three-thread hand-off that only works because the op is
// never run on the (blocked) caller's reactor.
TEST_F(SyncIoTest, on_reactor) {
    iomanager.run_on_wait(reactor_regex::random_worker, [this]() {
        ASSERT_TRUE(iomanager.am_i_io_reactor());
        sync_write_read_roundtrip(m_align * 16, 0x5A);
    });
}

// Every worker reactor issues blocking sync I/O concurrently, each to its own region; all must complete. This
// stresses the single sync reactor servicing multiple in-flight blocking callers at once.
TEST_F(SyncIoTest, concurrent_from_reactors) {
    std::atomic< uint32_t > slot{0};
    iomanager.run_on_wait(reactor_regex::all_worker, [this, &slot]() {
        ASSERT_TRUE(iomanager.am_i_io_reactor());
        auto const i = slot.fetch_add(1);
        sync_write_read_roundtrip(m_align * 64 * (i + 1), static_cast< uint8_t >(0x11 * (i + 1)));
    });
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger("test_sync_io");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
    return RUN_ALL_TESTS();
}

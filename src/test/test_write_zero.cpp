#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <random>

#ifdef __linux__
#include <fcntl.h>
#endif

#include <fds/utils.hpp>
#include <iomgr.hpp>
#include <sds_logging/logging.h>
#include <sds_options/options.h>

#include <gtest/gtest.h>

using namespace iomgr;
using namespace std::chrono_literals;

THREAD_BUFFER_INIT;
SDS_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SDS_OPTION_GROUP(test_write_zeros,
                 (dev, "", "dev", "dev", ::cxxopts::value< std::string >()->default_value("/tmp/wz1"), "path"),
                 (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"),
                 (size, "", "size", "size", ::cxxopts::value< uint64_t >()->default_value("2147483648"), "number"),
                 (offset, "", "offset", "offset", ::cxxopts::value< uint64_t >()->default_value("0"), "number"))

#define ENABLED_OPTIONS logging, iomgr, test_write_zeros, config
SDS_OPTIONS_ENABLE(ENABLED_OPTIONS)

static struct Runner {
    std::mutex cv_mutex;
    std::condition_variable comp_cv;
    size_t n_running_threads{0};

    void start() {
        std::unique_lock< std::mutex > lk{cv_mutex};
        ++n_running_threads;
    }

    void wait() {
        std::unique_lock< std::mutex > lk{cv_mutex};
        comp_cv.wait(lk, [&] { return (n_running_threads == 0); });
    }

    void job_done() {
        {
            std::unique_lock< std::mutex > lk{cv_mutex};
            --n_running_threads;
        }
        comp_cv.notify_one();
    }
} s_runner;

static constexpr uint64_t max_io_size{8 * 1024 * 1024}; // 8MB
#define g_drive_iface iomanager.default_drive_interface()

using random_bytes_engine = std::independent_bits_engine< std::default_random_engine, CHAR_BIT, unsigned char >;

class WriteZeroTest : public ::testing::Test {
public:
    void SetUp() override {
        m_size = SDS_OPTIONS["size"].as< uint64_t >();
        m_offset = SDS_OPTIONS["offset"].as< uint64_t >();

        const auto dev{SDS_OPTIONS["dev"].as< std::string >()};
        const auto dev_size{SDS_OPTIONS["size"].as< uint64_t >() + SDS_OPTIONS["offset"].as< uint64_t >()};

        if (!std::filesystem::exists(std::filesystem::path{dev})) {
            LOGINFO("Device {} doesn't exists, creating a file for size {}", dev, dev_size);
            auto fd = ::open(dev.c_str(), O_RDWR | O_CREAT, 0666);
            ASSERT_NE(fd, -1) << "Open of device " << dev << " failed";
            const auto ret{fallocate(fd, 0, 0, dev_size)};
            ASSERT_EQ(ret, 0) << "fallocate of device " << dev << " for size " << dev_size << " failed";
        }

        iomanager.start(1, SDS_OPTIONS["spdk"].as< bool >());
        g_drive_iface->attach_completion_cb(bind_this(WriteZeroTest::on_io_completion, 2));
        m_iodev = g_drive_iface->open_dev(dev, iomgr_drive_type::unknown, O_CREAT | O_RDWR | O_DIRECT);
        m_driveattr = g_drive_iface->get_attributes(dev, iomgr_drive_type::unknown);

        s_runner.start();
    }

    void TearDown() override { iomanager.stop(); }

    void write_zero_test() {
        auto remain_size{m_size};
        auto cur_offset{m_offset};

        // First fill in the entire set with some value in specific pattern
        random_bytes_engine rbe;
        LOGINFO("Filling the device with random bytes from offset={} size={}", cur_offset, remain_size);
        auto buf{iomanager.iobuf_alloc(m_driveattr.align_size, max_io_size)};
        for (uint64_t i{0}; i < max_io_size; ++i) {
            buf[i] = rbe();
        }

        m_start_time = Clock::now();
        while (remain_size > 0) {
            const auto this_sz{std::min(max_io_size, remain_size)};
            const auto ret{g_drive_iface->sync_write(m_iodev.get(), (const char*)buf, (uint32_t)this_sz, cur_offset)};
            ASSERT_EQ((size_t)ret, this_sz) << "Expected sync_write to be successful";
            cur_offset += this_sz;
            remain_size -= this_sz;
        }
        LOGINFO("Filling with rand bytes for size={} offset={} completed in {} usecs, now filling with 0s", m_size,
                m_offset, get_elapsed_time_us(m_start_time));
        iomanager.iobuf_free(buf);

        // Now issue write zeros
        m_start_time = Clock::now();
        g_drive_iface->write_zero(m_iodev.get(), m_size, m_offset, nullptr);
    }

    void on_io_completion(int64_t res, [[maybe_unused]] void* cookie) {
        auto remain_size{m_size};
        auto cur_offset{m_offset};

        ASSERT_EQ(res, 0) << "Expected write_zeros to be successful";
        LOGINFO("Write zeros of size={} completed in {} microseconds, reading it back to validate 0s", remain_size,
                get_elapsed_time_us(m_start_time));
        auto buf{iomanager.iobuf_alloc(m_driveattr.align_size, max_io_size)};

        // Read back and ensure all bytes are 0s
        m_start_time = Clock::now();
        while (remain_size > 0) {
            const auto this_sz{std::min(max_io_size, remain_size)};
            const auto ret{g_drive_iface->sync_read(m_iodev.get(), (char*)buf, (uint32_t)this_sz, cur_offset)};
            ASSERT_EQ((size_t)ret, this_sz) << "Expected sync_read to be successful";
            bool all_zero{true};
            size_t remain_ret{static_cast<size_t>(ret)};
            const int* pInt{reinterpret_cast< int* >(buf)};
            for (; remain_ret >= sizeof(int); remain_ret -= sizeof(int), ++pInt) {
                if (*pInt != 0) {
                    all_zero = false;
                    break;
                }
            }
            if (all_zero && (remain_ret > 0)) {
                const uint8_t* pByte{reinterpret_cast< const uint8_t* >(pInt)};
                for (; remain_ret > 0; --remain_ret, ++pByte) {
                    if (*pByte != 0x00) {
                        all_zero = false;
                        break;
                    }
                }
            }
            ASSERT_TRUE(all_zero) << "Expected all bytes to be zero, but not";
            cur_offset += this_sz;
            remain_size -= this_sz;
        }
        LOGINFO("Write zeros of size={} validated in {} microseconds", remain_size, get_elapsed_time_us(m_start_time));
        iomanager.iobuf_free(buf);
        s_runner.job_done();
    }

protected:
    uint64_t m_size;
    uint64_t m_offset;
    io_device_ptr m_iodev;
    iomgr::drive_attributes m_driveattr;
    Clock::time_point m_start_time;
};

TEST_F(WriteZeroTest, fill_zero_validate) {
    iomanager.run_on(
        thread_regex::least_busy_worker, [this]([[maybe_unused]] auto taddr) { this->write_zero_test(); },
        wait_type_t::no_wait);
    s_runner.wait();
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SDS_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sds_logging::SetLogger("test_write_zero");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    auto ret{RUN_ALL_TESTS()};
    return ret;
}

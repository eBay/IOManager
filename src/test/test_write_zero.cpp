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

#include <sisl/fds/utils.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <gtest/gtest.h>

#include <iomgr/io_environment.hpp>
#include <iomgr/iomgr.hpp>

using namespace iomgr;
using namespace std::chrono_literals;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SISL_OPTION_GROUP(test_write_zeros,
                  (dev, "", "dev", "dev", ::cxxopts::value< std::string >()->default_value("/tmp/wz1"), "path"),
                  (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"),
                  //(size, "", "size", "size", ::cxxopts::value< uint64_t >()->default_value("2147483648"), "number"),
                  (size, "", "size", "size", ::cxxopts::value< uint64_t >()->default_value("2097152"), "number"),
                  (offset, "", "offset", "offset", ::cxxopts::value< uint64_t >()->default_value("0"), "number"))

#define ENABLED_OPTIONS logging, iomgr, test_write_zeros, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

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

using random_bytes_engine = std::independent_bits_engine< std::default_random_engine, CHAR_BIT, unsigned char >;

class WriteZeroTest : public ::testing::Test {
public:
    void SetUp() override {
        m_size = SISL_OPTIONS["size"].as< uint64_t >();
        m_offset = SISL_OPTIONS["offset"].as< uint64_t >();

        const auto dev{SISL_OPTIONS["dev"].as< std::string >()};
        const auto dev_size{SISL_OPTIONS["size"].as< uint64_t >() + SISL_OPTIONS["offset"].as< uint64_t >()};

        if (!std::filesystem::exists(std::filesystem::path{dev})) {
            LOGINFO("Device {} doesn't exists, creating a file for size {}", dev, dev_size);
            auto fd = ::open(dev.c_str(), O_RDWR | O_CREAT, 0666);
            ASSERT_NE(fd, -1) << "Open of device " << dev << " failed";
            const auto ret{fallocate(fd, 0, 0, dev_size)};
            ASSERT_EQ(ret, 0) << "fallocate of device " << dev << " for size " << dev_size << " failed";
        }

        const auto is_spdk = SISL_OPTIONS["spdk"].as< bool >();
        ioenvironment.with_iomgr(iomgr_params{.num_threads = 1, .is_spdk = is_spdk});

        int oflags{O_CREAT | O_RDWR};
        if (is_spdk) { oflags |= O_DIRECT; }
        m_iodev = iomgr::DriveInterface::open_dev(dev, oflags);
        m_driveattr = iomgr::DriveInterface::get_attributes(dev);

        s_runner.start();
    }

    void TearDown() override {
        m_iodev->drive_interface()->close_dev(m_iodev);
        iomanager.stop();
    }

    struct io_req {
        uint8_t* buf{nullptr};
        uint64_t size{0};
    };

    void write_zero_test() {
        auto remain_size{m_size};
        auto cur_offset{m_offset};

        m_iodev->drive_interface()->attach_completion_cb(bind_this(WriteZeroTest::on_write_completion, 2));

        // First fill in the entire set with some value in specific pattern
        random_bytes_engine rbe;
        LOGINFO("Filling the device with random bytes from offset={} size={}", cur_offset, remain_size);
        uint8_t* buf = iomanager.iobuf_alloc(m_driveattr.align_size, max_io_size);
        for (uint64_t i{0}; i < max_io_size; ++i) {
            buf[i] = rbe();
        }

        m_start_time = Clock::now();
        while (remain_size > 0) {
            const auto this_sz = std::min(max_io_size, remain_size);
            io_req* req = new io_req();
            req->buf = buf;
            req->size = this_sz;
            m_iodev->drive_interface()->async_write(m_iodev.get(), (const char*)buf, (uint32_t)this_sz, cur_offset,
                                                    (uint8_t*)req);
            cur_offset += this_sz;
            remain_size -= this_sz;
        }
    }

    void on_write_completion(int64_t res, uint8_t* cookie) {
        ASSERT_EQ(res, 0) << "Expected write_zeros to be successful";
        io_req* req = (io_req*)cookie;
        m_filled_size += req->size;

        if (m_filled_size == m_size) {
            LOGINFO("Filling with rand bytes for size={} offset={} completed in {} usecs, now filling with 0s", m_size,
                    m_offset, get_elapsed_time_us(m_start_time));
            m_iodev->drive_interface()->attach_completion_cb(bind_this(WriteZeroTest::on_zero_completion, 2));
            iomanager.iobuf_free(req->buf);

            // Now issue write zeros
            m_start_time = Clock::now();
            m_iodev->drive_interface()->write_zero(m_iodev.get(), m_size, m_offset, nullptr);
        }
        delete req;
    }

    void on_zero_completion(int64_t res, [[maybe_unused]] void* cookie) {
        ASSERT_EQ(res, 0) << "Expected write_zeros to be successful";
        LOGINFO("Write zeros of size={} completed in {} microseconds, reading it back to validate 0s", m_size,
                get_elapsed_time_us(m_start_time));

        m_iodev->drive_interface()->attach_completion_cb(bind_this(WriteZeroTest::on_read_completion, 2));

        auto remain_size{m_size};
        auto cur_offset{m_offset};

        // Read back and ensure all bytes are 0s
        m_start_time = Clock::now();
        while (remain_size > 0) {
            const auto this_sz{std::min(max_io_size, remain_size)};

            io_req* req = new io_req();
            req->buf = iomanager.iobuf_alloc(m_driveattr.align_size, max_io_size);
            req->size = this_sz;
            m_iodev->drive_interface()->async_read(m_iodev.get(), (char*)req->buf, (uint32_t)this_sz, cur_offset,
                                                   (uint8_t*)req);
            cur_offset += this_sz;
            remain_size -= this_sz;
        }
    }

    void on_read_completion(int64_t res, uint8_t* cookie) {
        ASSERT_EQ(res, 0) << "Expected read to be successful";
        bool all_zero{true};
        io_req* req = (io_req*)cookie;
        size_t remain_size = req->size;

        const int* pInt = reinterpret_cast< int* >(req->buf);
        for (; remain_size >= sizeof(int); remain_size -= sizeof(int), ++pInt) {
            if (*pInt != 0) {
                all_zero = false;
                break;
            }
        }

        if (all_zero && (remain_size > 0)) {
            const uint8_t* pByte{reinterpret_cast< const uint8_t* >(pInt)};
            for (; remain_size > 0; --remain_size, ++pByte) {
                if (*pByte != 0x00) {
                    all_zero = false;
                    break;
                }
            }
        }
        ASSERT_TRUE(all_zero) << "Expected all bytes to be zero, but not";

        m_validated_size += req->size;
        iomanager.iobuf_free(req->buf);
        delete req;

        if (m_validated_size == m_size) {
            LOGINFO("Write zeros of size={} validated in {} microseconds", remain_size,
                    get_elapsed_time_us(m_start_time));
            s_runner.job_done();
        }
    }

protected:
    uint64_t m_size;
    uint64_t m_offset;
    uint64_t m_filled_size{0};
    uint64_t m_validated_size{0};
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
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger("test_write_zero");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    auto ret{RUN_ALL_TESTS()};
    return ret;
}

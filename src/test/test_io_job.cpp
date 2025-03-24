#include <cassert>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#ifdef __linux__
#include <fcntl.h>
#endif

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <gtest/gtest.h>

#include <iomgr/iomgr.hpp>
#include "io_examiner/io_job.hpp"

using namespace iomgr;
SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SISL_OPTION_GROUP(test_io,
                  (run_time, "", "run_time", "run time for io", ::cxxopts::value< uint32_t >()->default_value("60"),
                   "seconds"),
                  (num_threads, "", "num_threads", "num_threads - default 2 for spdk and 8 for non-spdk",
                   ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
                  (blk_size, "", "blk_size", "blk size in KB for IO",
                   ::cxxopts::value< uint32_t >()->default_value("0"), "number"),
                  (qdepth, "", "qdepth", "qdepth for IO",
                   ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
                  (load_type, "", "load_type", "io_type for IO, 0 - random, 1 - same, 2 - sequential",
                   ::cxxopts::value< uint32_t >()->default_value("0"), "number"),
                  (device_list, "", "device_list", "List of device paths",
                   ::cxxopts::value< std::vector< std::string > >(), "path [...]"),
                  (device_size, "", "device_size", "size of devices to do IO on",
                   ::cxxopts::value< uint64_t >()->default_value("1073741824"), "size"),
                  (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"))

#define ENABLED_OPTIONS logging, iomgr, test_io, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

TEST(IOMgrTest, basic_io_test) {
    const auto nthreads = SISL_OPTIONS["num_threads"].as< uint32_t >();
    const auto is_spdk = SISL_OPTIONS["spdk"].as< bool >();
    auto examiner = std::make_shared< iomgr::IOExaminer >(nthreads, false /* integrated mode */, is_spdk);

    // Create an add the device
    std::vector< std::string > devs{fmt::format("/tmp/io_test_{}", is_spdk ? "spdk" : "epoll")};
    if (SISL_OPTIONS.count("device_list")) { devs = SISL_OPTIONS["device_list"].as< std::vector< std::string > >(); }
    const auto dev_size = SISL_OPTIONS["device_size"].as< uint64_t >();
    for (const auto& dev : devs) {
        const std::filesystem::path file_path{dev};
        if (!std::filesystem::exists(file_path)) {
            LOGINFO("Device {} doesn't exists, creating a file for size {}", dev, dev_size);
            const auto fd{::open(dev.c_str(), O_RDWR | O_CREAT, 0666)};
            assert(fd > 0);
            std::filesystem::resize_file(file_path, dev_size);
            ::close(fd);
        }
        examiner->add_device(dev, O_RDWR);
    }

    IOJobCfg cfg;
    cfg.max_disk_capacity = dev_size;
    cfg.run_time = SISL_OPTIONS["run_time"].as< uint32_t >();
    cfg.io_dist = {{io_type_t::write, 100}, {io_type_t::read, 0}};
    cfg.load_type = (load_type_t)SISL_OPTIONS["load_type"].as< uint32_t >();
    cfg.io_blk_size = SISL_OPTIONS["blk_size"].as< uint32_t >()*1024;
    cfg.qdepth = SISL_OPTIONS["qdepth"].as< uint32_t >();

    IOJob job(examiner, cfg);
    job.start_job(wait_till_t::completion);
    examiner->close_devices();
    LOGINFO("Result: {}", job.job_result());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger("io_tests");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    return RUN_ALL_TESTS();
}

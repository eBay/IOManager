#include <gtest/gtest.h>
#include <iomgr.hpp>
#include <sds_logging/logging.h>
#include <sds_options/options.h>
#include <sisl/fds/atomic_status_counter.hpp>
#include "io_examiner/io_job.hpp"

using namespace iomgr;
SDS_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SDS_OPTION_GROUP(test_io,
                 (run_time, "", "run_time", "run time for io", ::cxxopts::value< uint32_t >()->default_value("60"),
                  "seconds"),
                 (num_threads, "", "num_threads", "num_threads - default 2 for spdk and 8 for non-spdk",
                  ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
                 (device_list, "", "device_list", "List of device paths",
                  ::cxxopts::value< std::vector< std::string > >(), "path [...]"),
                 (device_size, "", "device_size", "size of devices to do IO on",
                  ::cxxopts::value< uint64_t >()->default_value("1073741824"), "size"))

#define ENABLED_OPTIONS logging, iomgr, test_io, config
SDS_OPTIONS_ENABLE(ENABLED_OPTIONS)

TEST(IOMgrTest, basic_io_test) {
    auto nthreads = SDS_OPTIONS["num_threads"].as< uint32_t >();
    auto examiner = std::make_shared< iomgr::IOExaminer >(nthreads, false /* integrated mode */);

    // Create an add the device
    std::vector< std::string > devs{"/tmp/io_test"}; // Default if user has not provided
    if (SDS_OPTIONS.count("device_list")) { devs = SDS_OPTIONS["device_list"].as< std::vector< std::string > >(); }
    auto dev_size = SDS_OPTIONS["device_size"].as< uint64_t >();
    for (const auto& dev : devs) {
        auto fd = open(dev.c_str(), O_RDWR | O_CREAT, 0666);
        fallocate(fd, 0, 0, dev_size);
        close(fd);
        examiner->add_device(dev, O_RDWR);
    }

    IOJobCfg cfg;
    cfg.max_disk_capacity = dev_size;
    cfg.run_time = SDS_OPTIONS["run_time"].as< uint32_t >();
    cfg.io_dist = {{io_type_t::write, 50}, {io_type_t::read, 50}};

    IOJob job(examiner, cfg);
    job.start_job(wait_till_t::completion);

    LOGINFO("Result: {}", job.job_result());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SDS_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sds_logging::SetLogger("io_tests");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    return RUN_ALL_TESTS();
}

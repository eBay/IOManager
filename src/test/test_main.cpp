/*
 * Copyright 2018 by eBay Corporation
 */
#include <sds_logging/logging.h>
#include <sds_options/options.h>
#include "iomgr_test.hpp"

using log_level = spdlog::level::level_enum;

SDS_LOGGING_INIT(iomgr)

namespace iomgr {
const size_t num_secs_timeout = 10;
uint32_t num_ep = 1;
uint32_t num_threads = 8;
uint32_t num_pri = 10; 
uint64_t num_attemps = 200;
uint64_t wait_delta_ms = 10;
uint64_t num_ev_fd = 1;


TEST_F(IOMgrTester, single_ev_fd_trigger_type1) {
    num_ev_fd = 1;
    this->set_ev_tri_type(EvtTriggerType::TYPE_1);
    this->start();

    wait_for_result(num_secs_timeout);

    EXPECT_EQ(this->get_cb_cnt(), num_attemps);
    this->stop();
}

TEST_F(IOMgrTester, single_ev_fd_trigger_type2) {
    num_ev_fd = 1;
    this->set_ev_tri_type(EvtTriggerType::TYPE_2);
    this->start();

    wait_for_result(num_secs_timeout);

    EXPECT_EQ(this->get_cb_cnt(), num_attemps);
    this->stop();
}

TEST_F(IOMgrTester, multiple_ev_fd_trigger_type1) {
    num_ev_fd = 8;
    this->set_ev_tri_type(EvtTriggerType::TYPE_1);
    this->start();

    wait_for_result(num_secs_timeout);

    EXPECT_EQ(this->get_cb_cnt(), num_attemps);
    this->stop();
}


TEST_F(IOMgrTester, multiple_ev_fd_trigger_type2) {
    num_ev_fd = 8;
    this->set_ev_tri_type(EvtTriggerType::TYPE_2);
    this->start();

    wait_for_result(num_secs_timeout);

    EXPECT_EQ(this->get_cb_cnt(), num_attemps);
    this->stop();
}
}

SDS_OPTION_GROUP(test_iomgr,
(num_threads, "", "num_threads", "num threads for io", ::cxxopts::value<uint32_t>()->default_value("8"), "number"),
(num_attemps, "", "num_attemps", "num attemps to make", ::cxxopts::value<uint32_t>()->default_value("200"), "number"))
//(num_ev_fd, "", "num_ev_fd", "num fds for event", ::cxxopts::value<uint32_t>()->default_value("8"), "number"))

#define ENABLED_OPTIONS logging, test_iomgr 
SDS_OPTIONS_ENABLE(ENABLED_OPTIONS)

int main(int argc, char* argv[]) {
   testing::InitGoogleTest(&argc, argv);
   SDS_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS)
   sds_logging::SetLogger("example");
   spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

   iomgr::num_threads = SDS_OPTIONS["num_threads"].as<uint32_t>();
   iomgr::num_attemps = SDS_OPTIONS["num_attemps"].as<uint32_t>();
   // iomgr::num_ev_fd = SDS_OPTIONS["num_ev_fd"].as<uint32_t>();

   LOGINFO("{}, {}", iomgr::num_threads, iomgr::num_attemps);
   return RUN_ALL_TESTS();
}

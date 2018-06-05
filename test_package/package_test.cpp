/*
 * Copyright 2018 by eBay Corporation
 */
#include <sds_logging/logging.h>
#include <iomgr/iomgr.hpp>

using log_level = spdlog::level::level_enum;

static std::shared_ptr<spdlog::logger> logger_;

namespace sds_logging {
std::shared_ptr<spdlog::logger> GetLogger() {
   return logger_;
}
}

int main(int argc, char* argv[]) {
   spdlog::set_level(log_level::trace);
   spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
   logger_ = spdlog::stdout_color_mt("example");

   iomgr::ioMgr io_mgr(0, 2);
   LOGINFO("Created ioMgr");
   io_mgr.start();
   io_mgr.print_perf_cntrs();
   io_mgr.stop();
   LOGINFO("success...");

   return 0;
}

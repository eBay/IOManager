/*
 * Copyright 2018 by eBay Corporation
 */
#include <sds_logging/logging.h>
#include <iomgr/iomgr.hpp>

using log_level = spdlog::level::level_enum;

SDS_LOGGING_INIT

int main(int argc, char* argv[]) {
   spdlog::set_level(log_level::trace);
   spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
   sds_logging::SetLogger(spdlog::stdout_color_mt("example"));

   {
      iomgr::ioMgr io_mgr(0, 8);
      LOGINFO("Created ioMgr");
      io_mgr.start();
      io_mgr.print_perf_cntrs();
   }
   LOGINFO("success...");

   return 0;
}

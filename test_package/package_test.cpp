/*
 * Copyright 2018 by eBay Corporation
 */
#include <sds_logging/logging.h>
#include <iomgr/iomgr.hpp>

using log_level = spdlog::level::level_enum;

SDS_LOGGING_INIT(iomgr)
SDS_OPTIONS_ENABLE(logging)

int main(int argc, char* argv[]) {
   SDS_OPTIONS_LOAD(argc, argv, logging)
   sds_logging::SetLogger(spdlog::stdout_color_mt("example"));
   spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
   SDS_LOG_LEVEL(iomgr, log_level::trace);

   {
      iomgr::ioMgr io_mgr(0, 8);
      LOGINFO("Created ioMgr");
      io_mgr.start();
      io_mgr.print_perf_cntrs();
   }
   LOGINFO("success...");

   return 0;
}

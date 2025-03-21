#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <condition_variable>
#include <mutex>

/* Facility headers */
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

/* IOPath */
#include <iomgr/io_environment.hpp>

SISL_OPTION_GROUP(test_hs_vol,
                  (io_threads, "", "io_threads", "Number of IO threads",
                   cxxopts::value<uint32_t>()->default_value("1"), "count"))

#define ENABLED_OPTIONS logging, iomgr, test_hs_vol, config
#define SPDK_LOG_MODS IOMGR_LOG_MODS, flip

SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

constexpr size_t Ki = 1024;
constexpr size_t Mi = Ki * Ki;
constexpr size_t Gi = Ki * Mi;

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS)
    sisl::logging::SetLogger("spdk_volume");
    sisl::logging::install_crash_handler();
    spdlog::set_pattern("[%D %T.%e] [%^%l%$] [%t] %v");

    auto params = iomgr::iomgr_params { SISL_OPTIONS["io_threads"].as<uint32_t>(), false };
    // Start the IOManager
    ioenvironment.with_iomgr(params);

    iomanager.stop();
    LOGINFO("Done.");
    return 0;
}

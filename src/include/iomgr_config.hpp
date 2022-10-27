#pragma once
#include <sisl/settings/settings.hpp>
#include <sisl/options/options.h>
#include "iomgr_config_generated.h"

SETTINGS_INIT(iomgrcfg::IomgrSettings, iomgr_config);

#define IM_DYNAMIC_CONFIG_WITH(...) SETTINGS(iomgr_config, __VA_ARGS__)
#define IM_DYNAMIC_CONFIG_THIS(...) SETTINGS_THIS(iomgr_config, __VA_ARGS__)
#define IM_DYNAMIC_CONFIG(...) SETTINGS_VALUE(iomgr_config, __VA_ARGS__)

#define IM_SETTINGS_FACTORY() SETTINGS_FACTORY(iomgr_config)

class IOMgrDynamicConfig {
public:
    static constexpr std::string_view default_cpuset_path = "/sys/fs/cgroup/cpuset/cpuset.cpus";

    // This method sets up the default for settings factory when there is no override specified in the json
    // file and .fbs cannot specify default because they are not scalar.
    static void init_settings_default() {
        bool is_modified = false;
        IM_SETTINGS_FACTORY().modifiable_settings([&is_modified](auto& s) {
            /* Read in path for cpu corelist, if it is not set already - first time */
            auto& cpuset_path = s.cpuset_path;
            if (cpuset_path.empty()) {
                cpuset_path = default_cpuset_path;
                is_modified = true;
            }
            auto& encryption = s.io_env->encryption;
            if (SISL_OPTIONS.count("encryption")) {
                encryption = SISL_OPTIONS["encryption"].as< bool >();
                is_modified = true;
            }
            auto& authorization = s.io_env->authorization;
            if (SISL_OPTIONS.count("authorization")) {
                authorization = SISL_OPTIONS["authorization"].as< bool >();
                is_modified = true;
            }
            // Any more default overrides or set non-scalar entries come here
        });

        if (is_modified) { IM_SETTINGS_FACTORY().save(); }
    }
};

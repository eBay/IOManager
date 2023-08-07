/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Harihara Kadayam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **************************************************************************/
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
            // Any more default overrides or set non-scalar entries come here
        });

        if (is_modified) { IM_SETTINGS_FACTORY().save(); }
    }
};

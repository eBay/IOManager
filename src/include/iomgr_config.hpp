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
    static constexpr std::string_view default_megacli_path = "/bin/megacli";
    static constexpr std::string_view default_auth_allowed_apps = "all";

    static std::string get_env(const std::string& env_str) {
        auto env_var = getenv(env_str.c_str());
        return (env_var != nullptr) ? std::string(env_var) : "";
    }

    inline static const std::string default_app_name{get_env("APP_NAME")};
    inline static const std::string default_app_inst_name{get_env("APP_INST_NAME")};
    inline static const std::string default_pod_name{get_env("POD_NAME")};
    inline static const std::string default_app_env{get_env("APP_ENV")};
    inline static const std::string default_ssl_cert_file{get_env("SSL_CERT")};
    inline static const std::string default_ssl_key_file{get_env("SSL_KEY")};
    inline static const std::string default_tf_token_url{get_env("TOKEN_URL")};
    inline static const std::string default_issuer{get_env("TOKEN_ISSUER")};
    inline static const std::string default_server{get_env("TOKEN_SERVER")};
    inline static const std::string default_grant_path{get_env("TOKEN_GRANT")};

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
            auto& secure_zone = s.io_env->secure_zone;
            if (SISL_OPTIONS.count("secure_zone")) {
                secure_zone = SISL_OPTIONS["secure_zone"].as< bool >();
                is_modified = true;
            }
            auto& ssl_cert_file = s.security->ssl_cert_file;
            if (ssl_cert_file.empty()) {
                ssl_cert_file = default_ssl_cert_file;
                is_modified = true;
            }
            auto& ssl_key_file = s.security->ssl_key_file;
            if (ssl_key_file.empty()) {
                ssl_key_file = default_ssl_key_file;
                is_modified = true;
            }
            auto& server = s.security->trf_client->server;
            if (server.empty()) {
                server = std::string_view(default_server);
                is_modified = true;
            }
            auto& grant_path = s.security->trf_client->grant_path;
            if (grant_path.empty()) {
                grant_path = std::string_view(default_grant_path);
                is_modified = true;
            }
            auto& auth_allowed_apps = s.security->auth_manager->auth_allowed_apps;
            if (auth_allowed_apps.empty()) {
                auth_allowed_apps = default_auth_allowed_apps;
                is_modified = true;
            }
            auto& issuer = s.security->auth_manager->issuer;
            if (issuer.empty()) {
                issuer = default_issuer;
                is_modified = true;
            }
            auto& tf_token_url = s.security->auth_manager->tf_token_url;
            if (tf_token_url.empty()) {
                tf_token_url = default_tf_token_url;
                is_modified = true;
            }
            auto& app_name = s.security->trf_client->app_name;
            if (app_name.empty()) {
                app_name = std::string_view(default_app_name);
                is_modified = true;
            }
            auto& app_inst_name = s.security->trf_client->app_inst_name;
            if (app_inst_name.empty()) {
                app_inst_name = std::string_view(default_app_inst_name);
                is_modified = true;
            }
            auto& app_env = s.security->trf_client->app_env;
            if (app_name.empty()) {
                app_env = std::string_view(default_app_env);
                is_modified = true;
            }
            auto& pod_name = s.security->trf_client->pod_name;
            if (pod_name.empty()) {
                pod_name = std::string_view(default_pod_name);
                is_modified = true;
            }

            // Any more default overrides or set non-scalar entries come here
        });

        if (is_modified) { IM_SETTINGS_FACTORY().save(); }
    }
};

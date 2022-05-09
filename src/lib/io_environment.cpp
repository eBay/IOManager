#include "io_environment.hpp"

namespace iomgr {

IOEnvironment& IOEnvironment::with_http_server() {
    if (!m_http_server) {
        sisl::HttpServerConfig cfg;
        if (IM_DYNAMIC_CONFIG(io_env.secure_zone)) {
            cfg.is_tls_enabled = true;
            cfg.tls_cert_path = IM_DYNAMIC_CONFIG(security.ssl_cert_file);
            cfg.tls_key_path = IM_DYNAMIC_CONFIG(security.ssl_key_file);
            cfg.is_auth_enabled = true;
        } else {
            cfg.is_tls_enabled = false;
            cfg.is_auth_enabled = false;
        }
        cfg.bind_address = "0.0.0.0";
        cfg.server_port = IM_DYNAMIC_CONFIG(io_env.http_port);
        cfg.read_write_timeout_secs = 10;
        m_http_server = std::make_shared< sisl::HttpServer >(cfg);
        m_http_server->start();
    }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_file_watcher() {
    if (!m_file_watcher) {
        m_file_watcher = std::make_shared< sisl::FileWatcher >();
        m_file_watcher->start();
    }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_auth_security() {
    if (IM_DYNAMIC_CONFIG(io_env->secure_zone)) {
        // setup Auth Manager
        if (!m_auth_manager) {
            sisl::AuthMgrConfig auth_cfg{IM_DYNAMIC_CONFIG(security->auth_manager->tf_token_url),
                                         IM_DYNAMIC_CONFIG(security->ssl_cert_file),
                                         IM_DYNAMIC_CONFIG(security->ssl_key_file),
                                         IM_DYNAMIC_CONFIG(security->ssl_ca_file),
                                         IM_DYNAMIC_CONFIG(security->auth_manager->leeway),
                                         IM_DYNAMIC_CONFIG(security->auth_manager->auth_allowed_apps),
                                         IM_DYNAMIC_CONFIG(security->auth_manager->issuer),
                                         IM_DYNAMIC_CONFIG(security->auth_manager->verify)};

            m_auth_manager = std::make_shared< sisl::AuthManager >(auth_cfg);
        }

        if (!m_trf_client) {
            // setup trustfabric token client
            sisl::TrfClientConfig trf_cfg{IM_DYNAMIC_CONFIG(security->trf_client->app_name),
                                          IM_DYNAMIC_CONFIG(security->trf_client->app_inst_name),
                                          IM_DYNAMIC_CONFIG(security->trf_client->app_env),
                                          IM_DYNAMIC_CONFIG(security->trf_client->pod_name),
                                          IM_DYNAMIC_CONFIG(security->trf_client->server),
                                          IM_DYNAMIC_CONFIG(security->auth_manager->leeway),
                                          fmt::format("{}_{}.cg",
                                                      IM_DYNAMIC_CONFIG(security->trf_client->grant_path_prefix),
                                                      IM_DYNAMIC_CONFIG(security->trf_client->pod_name)),
                                          IM_DYNAMIC_CONFIG(security->auth_manager->verify),
                                          IM_DYNAMIC_CONFIG(security->ssl_cert_file),
                                          IM_DYNAMIC_CONFIG(security->ssl_key_file),
                                          IM_DYNAMIC_CONFIG(security->ssl_ca_file)};

            m_trf_client = std::make_shared< sisl::TrfClient >(trf_cfg);
        }
        // to watch cert file changes
        with_file_watcher();
    }

    return get_instance();
}

bool IOEnvironment::register_cert_reload_cb(const std::string& listener_id, const sisl::file_event_cb_t& handler) {
    const bool register_cert =
        m_file_watcher->register_listener(IM_DYNAMIC_CONFIG(security->ssl_cert_file), listener_id, handler);
    const bool register_key =
        m_file_watcher->register_listener(IM_DYNAMIC_CONFIG(security->ssl_key_file), listener_id, handler);
    const bool register_ca =
        m_file_watcher->register_listener(IM_DYNAMIC_CONFIG(security->ssl_ca_file), listener_id, handler);
    return register_cert && register_key && register_ca;
}

bool IOEnvironment::unregister_cert_reload_cb(const std::string& listener_id) {
    const bool unregister_cert =
        m_file_watcher->unregister_listener(IM_DYNAMIC_CONFIG(security->ssl_cert_file), listener_id);
    const bool unregister_key =
        m_file_watcher->unregister_listener(IM_DYNAMIC_CONFIG(security->ssl_key_file), listener_id);
    const bool unregister_ca =
        m_file_watcher->unregister_listener(IM_DYNAMIC_CONFIG(security->ssl_ca_file), listener_id);
    return unregister_cert && unregister_key && unregister_ca;
}

} // namespace iomgr
#include "io_environment.hpp"
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-function-type"
#endif
#include "http_server.hpp"
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif

namespace iomgr {

IOEnvironment::IOEnvironment() {
    // init default settings
    IOMgrDynamicConfig::init_settings_default();
    SecurityDynamicConfig::init_settings_default();
}

IOEnvironment::~IOEnvironment() {
    if (m_http_server) { m_http_server->stop(); }
    if (m_file_watcher) { m_file_watcher->stop(); }
}

IOEnvironment& IOEnvironment::with_http_server() {
    if (!m_http_server) {
        iomgr::HttpServerConfig cfg;
        if (IM_DYNAMIC_CONFIG(io_env.encryption)) {
            cfg.is_tls_enabled = true;
            cfg.tls_cert_path = SECURITY_DYNAMIC_CONFIG(ssl_cert_file);
            cfg.tls_key_path = SECURITY_DYNAMIC_CONFIG(ssl_key_file);
            cfg.is_auth_enabled = true;
        } else {
            cfg.is_tls_enabled = false;
            cfg.is_auth_enabled = false;
        }
        cfg.bind_address = "0.0.0.0";
        cfg.server_port = IM_DYNAMIC_CONFIG(io_env.http_port);
        cfg.read_write_timeout_secs = 10;
        m_http_server = std::make_shared< iomgr::HttpServer >(cfg);
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
    if (IM_DYNAMIC_CONFIG(io_env->authorization)) {
        if (!m_auth_manager) { m_auth_manager = std::make_shared< sisl::AuthManager >(); }
        if (!m_trf_client) { m_trf_client = std::make_shared< sisl::TrfClient >(); }
    }

    return get_instance();
}

} // namespace iomgr

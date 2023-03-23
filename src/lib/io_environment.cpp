/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Ravi Nagarjuna Akella
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
#include <iomgr/io_environment.hpp>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-function-type"
#endif
#ifdef WITH_EVHTP
#include <iomgr/http_server.hpp>
#endif
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif
#include "iomgr_config.hpp"

namespace iomgr {

IOEnvironment::IOEnvironment() {
    // init default settings
    IOMgrDynamicConfig::init_settings_default();
    SecurityDynamicConfig::init_settings_default();
}

IOEnvironment::~IOEnvironment() {
#ifdef WITH_EVHTP
    if (m_http_server) { m_http_server->stop(); }
#endif
    if (m_file_watcher) { m_file_watcher->stop(); }
}

#ifdef WITH_EVHTP

void IOEnvironment::restart_http_server() {
    m_http_server.reset();
    with_http_server();
}

IOEnvironment& IOEnvironment::with_http_server() {
    if (!m_http_server) {
        iomgr::HttpServerConfig cfg;
        cfg.bind_address = "0.0.0.0";
        cfg.server_port = IM_DYNAMIC_CONFIG(io_env.http_port);
        cfg.read_write_timeout_secs = 10;
        if (IM_DYNAMIC_CONFIG(io_env.encryption)) {
            cfg.is_tls_enabled = true;
            cfg.tls_cert_path = SECURITY_DYNAMIC_CONFIG(ssl_cert_file);
            cfg.tls_key_path = SECURITY_DYNAMIC_CONFIG(ssl_key_file);
            cfg.is_auth_enabled = true;
        } else {
            cfg.is_tls_enabled = false;
            cfg.is_auth_enabled = false;
        }

        // get auth_manager if auth is enabled
        with_auth_manager();
        m_http_server = std::make_shared< iomgr::HttpServer >(cfg, get_auth_manager());
        m_http_server->start();
    }

    return get_instance();
}

#endif // #ifdef WITH_EVHTP

IOEnvironment& IOEnvironment::with_file_watcher() {
    if (!m_file_watcher) {
        m_file_watcher = std::make_shared< sisl::FileWatcher >();
        m_file_watcher->start();
    }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_auth_security() { return with_auth_manager().with_trf_client(); }

IOEnvironment& IOEnvironment::with_auth_manager() {
    if (IM_DYNAMIC_CONFIG(io_env->authorization)) {
        if (!m_auth_manager) { m_auth_manager = std::make_shared< sisl::AuthManager >(); }
    }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_trf_client() {
    if (IM_DYNAMIC_CONFIG(io_env->authorization)) {
        if (!m_trf_client) { m_trf_client = std::make_shared< sisl::TrfClient >(); }
    }

    return get_instance();
}

std::string IOEnvironment::get_ssl_cert() const {
    return (IM_DYNAMIC_CONFIG(io_env->encryption)) ? SECURITY_DYNAMIC_CONFIG(ssl_cert_file) : "";
}
std::string IOEnvironment::get_ssl_key() const {
    return (IM_DYNAMIC_CONFIG(io_env->encryption)) ? SECURITY_DYNAMIC_CONFIG(ssl_key_file) : "";
}

} // namespace iomgr

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
#include "io_environment.hpp"
#include "http_server.hpp"
#include <sisl/sobject/sobject.hpp>

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

void IOEnvironment::restart_http_server() {
    m_http_server.reset();
    with_http_server();
}

IOEnvironment& IOEnvironment::with_http_server() {
    if (!m_http_server) { m_http_server = std::make_shared< iomgr::HttpServer >(); }

    return get_instance();
}

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

IOEnvironment& IOEnvironment::with_object_manager() {
    if (!m_object_mgr) { m_object_mgr = std::make_shared< sisl::sobject_manager >(); }

    return get_instance();
}

} // namespace iomgr

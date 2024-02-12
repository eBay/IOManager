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
#include <iomgr/http_server.hpp>
#include "iomgr_config.hpp"

#include <sisl/sobject/sobject.hpp>
#include <sisl/grpc/rpc_client.hpp>

namespace iomgr {

IOEnvironment::IOEnvironment() {
    // init default settings
    IOMgrDynamicConfig::init_settings_default();
}

IOEnvironment::~IOEnvironment() {
    if (m_http_server) { m_http_server->stop(); }
    if (m_file_watcher) { m_file_watcher->stop(); }
    sisl::GrpcAsyncClientWorker::shutdown_all();
}

void IOEnvironment::restart_http_server(std::string const& ssl_cert, std::string const& ssl_key) {
    m_http_server.reset();
    with_http_server(ssl_cert, ssl_key);
}

void IOEnvironment::restart_http_server() {
    m_http_server.reset();
    with_http_server();
}

IOEnvironment& IOEnvironment::with_http_server() { return with_http_server("", ""); }

IOEnvironment& IOEnvironment::with_http_server(std::string const& ssl_cert, std::string const& ssl_key) {
    if (!m_http_server) { m_http_server = std::make_shared< iomgr::HttpServer >(ssl_cert, ssl_key); }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_file_watcher() {
    if (!m_file_watcher) {
        m_file_watcher = std::make_shared< sisl::FileWatcher >();
        m_file_watcher->start();
    }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_token_verifier(std::shared_ptr< sisl::TokenVerifier >&& token_verifier) {
    if (!m_token_verifier) { m_token_verifier = token_verifier; }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_token_client(std::shared_ptr< sisl::TokenClient >&& token_client) {
    if (!m_token_client) { m_token_client = token_client; }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_object_manager() {
    if (!m_object_mgr) { m_object_mgr = std::make_shared< sisl::sobject_manager >(); }

    return get_instance();
}

IOEnvironment& IOEnvironment::with_grpc_client_workers(std::string const& name, uint32_t const num_workers) {
    sisl::GrpcAsyncClientWorker::create_worker(name, num_workers);
    return get_instance();
}

std::string IOEnvironment::get_ssl_cert() const { return m_ssl_cert; }
std::string IOEnvironment::get_ssl_key() const { return m_ssl_key; }
void IOEnvironment::set_ssl_certs(std::string const& ssl_cert, std::string const& ssl_key) {
    m_ssl_cert = ssl_cert;
    m_ssl_key = ssl_key;
}

} // namespace iomgr

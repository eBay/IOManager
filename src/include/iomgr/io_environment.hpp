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
#pragma once

#include <sisl/file_watcher/file_watcher.hpp>
#include <sisl/auth_manager/auth_manager.hpp>
#include <sisl/auth_manager/trf_client.hpp>
#include <sisl/auth_manager/security_config.hpp>

#include <iomgr/iomgr.hpp>
namespace iomgr {

class HttpServer;
class IOEnvironment {
public:
    static IOEnvironment& get_instance() {
        static IOEnvironment instance;
        return instance;
    }

    template < typename... Args >
    IOEnvironment& with_iomgr(Args&&... args) {
        iomanager.start(std::forward< Args >(args)...);
        return get_instance();
    }
    IOEnvironment& with_http_server();
    IOEnvironment& with_file_watcher();
    IOEnvironment& with_auth_security();
    IOEnvironment& with_auth_manager();
    IOEnvironment& with_trf_client();

    std::shared_ptr< iomgr::HttpServer > get_http_server() { return m_http_server; }
    std::shared_ptr< sisl::AuthManager > get_auth_manager() { return m_auth_manager; }
    std::shared_ptr< sisl::TrfClient > get_trf_client() { return m_trf_client; }
    std::shared_ptr< sisl::FileWatcher > get_file_watcher() { return m_file_watcher; }
    std::string get_ssl_cert() const;
    std::string get_ssl_key() const;
    void restart_http_server();

private:
    IOEnvironment();
    ~IOEnvironment();

    std::shared_ptr< iomgr::HttpServer > m_http_server;
    std::shared_ptr< sisl::AuthManager > m_auth_manager;
    std::shared_ptr< sisl::TrfClient > m_trf_client;
    std::shared_ptr< sisl::FileWatcher > m_file_watcher;

    uint32_t app_mem_size_mb{0}; // Overriding parameters if any
    uint32_t hugepage_size_mb{0};
};
#define ioenvironment iomgr::IOEnvironment::get_instance()

} // namespace iomgr

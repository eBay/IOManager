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

#include "iomgr.hpp"

#include <sisl/file_watcher/file_watcher.hpp>
#include <sisl/auth_manager/token_verifier.hpp>
#include <sisl/auth_manager/token_client.hpp>

namespace sisl {
class sobject_manager;
}

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
    IOEnvironment& with_http_server(std::string const& ssl_cert, std::string const& ssl_key);
    IOEnvironment& with_file_watcher();
    IOEnvironment& with_token_verifier(std::shared_ptr< sisl::TokenVerifier >&& token_verifier);
    IOEnvironment& with_token_client(std::shared_ptr< sisl::TokenClient >&& token_client);
    IOEnvironment& with_object_manager();

    std::shared_ptr< iomgr::HttpServer > get_http_server() { return m_http_server; }
    std::shared_ptr< sisl::TokenVerifier > get_token_verifier() { return m_token_verifier; }
    std::shared_ptr< sisl::TokenClient > get_token_client() { return m_token_client; }
    std::shared_ptr< sisl::FileWatcher > get_file_watcher() { return m_file_watcher; }
    std::shared_ptr< sisl::sobject_manager > get_object_mgr() { return m_object_mgr; }

    void restart_http_server();
    void restart_http_server(std::string const& ssl_cert, std::string const& ssl_key);

private:
    IOEnvironment();
    ~IOEnvironment();

    std::shared_ptr< iomgr::HttpServer > m_http_server;
    std::shared_ptr< sisl::TokenVerifier > m_token_verifier;
    std::shared_ptr< sisl::TokenClient > m_token_client;
    std::shared_ptr< sisl::FileWatcher > m_file_watcher;
    std::shared_ptr< sisl::sobject_manager > m_object_mgr;

    bool m_secure_zone;
};
#define ioenvironment iomgr::IOEnvironment::get_instance()

} // namespace iomgr

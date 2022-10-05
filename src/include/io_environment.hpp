/**
 * @brief Interface to initialize and access all the global modules like iomanager, http_server, file_watcher etc
 */

#pragma once

#include "iomgr.hpp"
#include "iomgr_config.hpp"

#include <sisl/file_watcher/file_watcher.hpp>
#include <sisl/auth_manager/auth_manager.hpp>
#include <sisl/auth_manager/trf_client.hpp>
#include <sisl/auth_manager/security_config.hpp>

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

    std::shared_ptr< iomgr::HttpServer > get_http_server() { return m_http_server; }
    std::shared_ptr< sisl::AuthManager > get_auth_manager() { return m_auth_manager; }
    std::shared_ptr< sisl::TrfClient > get_trf_client() { return m_trf_client; }
    std::shared_ptr< sisl::FileWatcher > get_file_watcher() { return m_file_watcher; }
    std::string get_ssl_cert() const {
        return (IM_DYNAMIC_CONFIG(io_env->secure_zone)) ? SECURITY_DYNAMIC_CONFIG(ssl_cert_file) : "";
    }
    std::string get_ssl_key() const {
        return (IM_DYNAMIC_CONFIG(io_env->secure_zone)) ? SECURITY_DYNAMIC_CONFIG(ssl_key_file) : "";
    }

private:
    IOEnvironment();
    ~IOEnvironment();

    std::shared_ptr< iomgr::HttpServer > m_http_server;
    std::shared_ptr< sisl::AuthManager > m_auth_manager;
    std::shared_ptr< sisl::TrfClient > m_trf_client;
    std::shared_ptr< sisl::FileWatcher > m_file_watcher;
};
#define ioenvironment iomgr::IOEnvironment::get_instance()

} // namespace iomgr

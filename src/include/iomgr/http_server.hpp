#pragma once

#include <pistache/endpoint.h>
#include <pistache/http.h>
#include <pistache/router.h>
#include <pistache/http_headers.h>
#include <nlohmann/json.hpp>

#include <sisl/utility/enum.hpp>

namespace sisl {
class GrpcTokenVerifier;
}

namespace iomgr {

ENUM(url_type, uint8_t,
     localhost, // url can only be called from localhost
     safe,      // Can be called from any host
     regular);

class HttpServer {
public:
    HttpServer();
    HttpServer(std::string const& ssl_cert, std::string const& ssl_key);

    // All the routes should be setup before calling start()
    void start();

    void setup_route(Pistache::Http::Method method, std::string resource, Pistache::Rest::Route::Handler handler,
                     url_type const& type = url_type::regular);

    void stop();

    void setup_ssl(std::string const& ssl_cert, std::string const& ssl_key);

    // auth related apis
    bool do_auth(Pistache::Http::Request& request, Pistache::Http::ResponseWriter& response);
    bool is_localaddr_url(std::string const& url) const;
    bool is_safe_url(std::string const& url) const;
    bool is_secure_zone() const;
    bool auth_verify(Pistache::Http::Request& request, Pistache::Http::ResponseWriter& response) const;

private:
    void get_local_ips();
    bool is_local_addr(std::string const& addr) const;

private:
    std::unique_ptr< Pistache::Http::Endpoint > m_http_endpoint;
    Pistache::Rest::Router m_router;
    bool m_secure_zone;
    std::atomic< bool > m_server_running{false};
    std::unordered_set< std::string > m_safelist;
    std::unordered_set< std::string > m_localhost_list;
    std::unordered_set< std::string > m_local_ips;
};

using url_t = iomgr::url_type;

} // namespace iomgr
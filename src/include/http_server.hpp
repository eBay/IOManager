#pragma once

#include <pistache/endpoint.h>
#include <pistache/http.h>
#include <pistache/router.h>
#include <pistache/http_headers.h>
#include <nlohmann/json.hpp>

#include <sisl/utility/enum.hpp>

namespace iomgr {

class HttpServer {
public:
    enum class url_type {
        localhost, // url can only be called from localhost
        safe,      // Can be called from any host
        regular
    };

public:
    HttpServer();

    // All the routes should be setup before calling start()
    void start();

    void setup_route(Pistache::Http::Method method, std::string resource, Pistache::Rest::Route::Handler handler,
                     url_type const& type = url_type::regular);

    void stop();

    void setup_ssl();

    // auth related apis
    bool do_auth(Pistache::Http::Request& request, Pistache::Http::ResponseWriter& response);
    bool is_localaddr_url(std::string const& url) const;
    bool is_safe_url(std::string const& url) const;
    bool is_secure_zone() const;

private:
    void get_local_ips();
    bool is_local_addr(std::string const& addr) const;

private:
    std::unique_ptr< Pistache::Http::Endpoint > m_http_endpoint;
    Pistache::Rest::Router m_router;
    std::unordered_set< std::string > m_safelist;
    std::unordered_set< std::string > m_localhost_list;
    std::unordered_set< std::string > m_local_ips;
};

using url_t = HttpServer::url_type;

} // namespace iomgr
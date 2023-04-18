#pragma once

#include <pistache/endpoint.h>
#include <pistache/http.h>
#include <pistache/router.h>
#include <pistache/http_headers.h>
#include <nlohmann/json.hpp>

namespace iomgr {

class HttpServer {
public:
    HttpServer();

    void start();

    void setup_route(Pistache::Http::Method method, std::string const& resource,
                     Pistache::Rest::Route::Handler handler);

    void stop();

    void setup_ssl();

private:
    std::unique_ptr< Pistache::Http::Endpoint > m_http_endpoint;
    Pistache::Rest::Router m_router;
};

} // namespace iomgr
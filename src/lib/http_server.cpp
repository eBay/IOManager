#include "http_server.hpp"
#include "iomgr_config.hpp"
#include <sisl/auth_manager/security_config.hpp>

namespace iomgr {

HttpServer::HttpServer() {
    Pistache::Address addr(Pistache::Ipv4::any(), Pistache::Port(IM_DYNAMIC_CONFIG(io_env.http_port)));
    m_http_endpoint = std::make_unique< Pistache::Http::Endpoint >(addr);
    auto flags = Pistache::Tcp::Options::ReuseAddr;
    auto opts = Pistache::Http::Endpoint::options()
                    .threadsName("http_server")
                    .maxRequestSize(IM_DYNAMIC_CONFIG(io_env.http_max_request_size))
                    .threads(IM_DYNAMIC_CONFIG(io_env.http_num_threads))
                    .flags(flags);
    m_http_endpoint->init(opts);
    setup_ssl();
}

void HttpServer::start() {
    m_http_endpoint->setHandler(m_router.handler());
    m_http_endpoint->serveThreaded();
}

void HttpServer::setup_route(Pistache::Http::Method method, std::string const& resource,
                             Pistache::Rest::Route::Handler handler) {
    m_router.addRoute(std::move(method), resource, std::move(handler));
    m_http_endpoint->setHandler(m_router.handler());
}

void HttpServer::stop() { m_http_endpoint->shutdown(); }

void HttpServer::setup_ssl() {
    if (IM_DYNAMIC_CONFIG(io_env.encryption)) {
        m_http_endpoint->useSSL(SECURITY_DYNAMIC_CONFIG(ssl_cert_file), SECURITY_DYNAMIC_CONFIG(ssl_key_file));
    }
}

} // namespace iomgr
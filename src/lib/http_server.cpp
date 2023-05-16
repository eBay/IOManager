#include "http_server.hpp"
#include "iomgr_config.hpp"
#include <sisl/auth_manager/security_config.hpp>
#include <ifaddrs.h>
#include <arpa/inet.h>

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
    get_local_ips();
}

void HttpServer::start() {
    // setup auth middleware
    m_router.addMiddleware(Pistache::Rest::Routes::middleware(&HttpServer::do_auth, this));

    // setup all routes and start the server
    m_http_endpoint->setHandler(m_router.handler());
    m_http_endpoint->serveThreaded();
    m_server_running = true;
}

void HttpServer::setup_route(Pistache::Http::Method method, std::string resource,
                             Pistache::Rest::Route::Handler handler, url_type const& type) {
    DEBUG_ASSERT(!m_server_running, "Initiated route setup after server started");
    if (m_server_running) {
        LOGWARN("Could not setup route {}, server is in running state.", resource)
        return;
    }

    m_router.addRoute(std::move(method), resource, std::move(handler));

    if (type == url_type::localhost) {
        m_localhost_list.emplace(std::move(resource));
    } else if (type == url_type::safe) {
        m_safelist.emplace(std::move(resource));
    }
}

bool HttpServer::do_auth(Pistache::Http::Request& request, Pistache::Http::ResponseWriter& response) {
    if (is_safe_url(request.resource())) { return true; }
    if (is_localaddr_url(request.resource()) || is_secure_zone()) { return is_local_addr(request.address().host()); }

    // add additional auth rules here
    return true;
}

void HttpServer::stop() {
    m_http_endpoint->shutdown();
    m_server_running = false;
}

void HttpServer::setup_ssl() {
    if (IM_DYNAMIC_CONFIG(io_env.encryption)) {
        m_http_endpoint->useSSL(SECURITY_DYNAMIC_CONFIG(ssl_cert_file), SECURITY_DYNAMIC_CONFIG(ssl_key_file));
    }
}

void HttpServer::get_local_ips() {
    struct ifaddrs* interfaces = nullptr;
    struct ifaddrs* temp_addr = nullptr;
    auto error = getifaddrs(&interfaces);
    if (error != 0) { LOGWARN("getifaddrs returned non zero code: {}", error); }
    temp_addr = interfaces;
    while (temp_addr != nullptr) {
        if (temp_addr->ifa_addr->sa_family == AF_INET) {
            m_local_ips.emplace(inet_ntoa(((struct sockaddr_in*)temp_addr->ifa_addr)->sin_addr));
        }
        temp_addr = temp_addr->ifa_next;
    }
    freeifaddrs(interfaces);
}

bool HttpServer::is_localaddr_url(std::string const& url) const { return m_localhost_list.count(url) > 0; }

bool HttpServer::is_safe_url(std::string const& url) const { return m_safelist.count(url) > 0; }

bool HttpServer::is_local_addr(std::string const& addr) const { return m_local_ips.count(addr) > 0; }

bool HttpServer::is_secure_zone() const {
    return IM_DYNAMIC_CONFIG(io_env->encryption) || IM_DYNAMIC_CONFIG(io_env->authorization);
}

} // namespace iomgr
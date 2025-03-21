//
// Created by Kadayam, Hari on 12/14/18.
//

#include <cpr/cpr.h>
#include <gtest/gtest.h>

#include "iomgr/io_environment.hpp"
#include "iomgr/http_server.hpp"

SISL_OPTIONS_ENABLE(logging)

using namespace Pistache;
using namespace Pistache::Rest;

class HTTPServerTest : public ::testing::Test {
public:
    HTTPServerTest() = default;
    HTTPServerTest(const HTTPServerTest&) = delete;
    HTTPServerTest& operator=(const HTTPServerTest&) = delete;
    HTTPServerTest(HTTPServerTest&&) noexcept = delete;
    HTTPServerTest& operator=(HTTPServerTest&&) noexcept = delete;
    virtual ~HTTPServerTest() override = default;

    virtual void SetUp() override {
        m_server = std::make_unique< iomgr::HttpServer >();
        std::vector< iomgr::http_route > routes = {
            {Http::Method::Get, "/api/v1/sayHello", Routes::bind(&HTTPServerTest::say_hello, this)},
            {Http::Method::Get, "/api/v1/yourNamePlease", Routes::bind(&HTTPServerTest::say_name, this)},
            {Http::Method::Post, "/api/v1/postResource/", Routes::bind(&HTTPServerTest::post_resource, this)},
            {Http::Method::Get, "/api/v1/getResource", Routes::bind(&HTTPServerTest::get_resource, this)},
            {Http::Method::Put, "/api/v1/putResource", Routes::bind(&HTTPServerTest::put_resource, this)},
            {Http::Method::Delete, "/api/v1/deleteResource", Routes::bind(&HTTPServerTest::delete_resource, this)},
        };
        m_server->setup_routes(routes);
        m_server->start();
    }

    virtual void TearDown() override { m_server->stop(); }

    void say_hello(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok, "Hello client from async_http server\n");
    }

    void say_name(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok, "I am the iomgr (sizzling) http server \n");
    }

    void post_resource(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok, "post");
    }

    void get_resource(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok, "get");
    }

    void put_resource(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok, "put");
    }

    void delete_resource(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok, "delete");
    }

protected:
    std::unique_ptr< iomgr::HttpServer > m_server;
};

TEST_F(HTTPServerTest, BasicTest) {
    const cpr::Url url{"http://127.0.0.1:5000/api/v1/sayHello"};
    auto resp{cpr::Get(url)};
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "Hello client from async_http server\n");

    static const cpr::Url url1{"http://127.0.0.1:5000/api/v1/getResource"};
    resp = cpr::Get(url1);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "get");

    const cpr::Url url2{"http://127.0.0.1:5000/api/v1/postResource"};
    resp = cpr::Post(url2);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "post");

    const cpr::Url url3{"http://127.0.0.1:5000/api/v1/putResource"};
    resp = cpr::Put(url3);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "put");

    const cpr::Url url4{"http://127.0.0.1:5000/api/v1/deleteResource"};
    resp = cpr::Delete(url4);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "delete");
}

TEST_F(HTTPServerTest, ParallelTestWithWait) {
    const auto thread_func{[](const size_t iterations) {
        const cpr::Url url{"http://127.0.0.1:5000/api/v1/yourNamePlease"};
        for (size_t iteration{0}; iteration < iterations; ++iteration) {
            const auto resp{cpr::Get(url)};
            ASSERT_EQ(resp.status_code, cpr::status::HTTP_OK);
            ASSERT_EQ(resp.text, "I am the iomgr (sizzling) http server \n");
        }
    }};

    constexpr size_t num_iterations{100};
    const size_t num_threads{std::max< size_t >(std::thread::hardware_concurrency(), 2)};
    std::vector< std::thread > workers;
    for (size_t thread_num{0}; thread_num < num_threads; ++thread_num) {
        workers.emplace_back(thread_func, num_iterations);
    }

    for (auto& worker : workers) {
        if (worker.joinable()) worker.join();
    }
}

TEST_F(HTTPServerTest, ParallelTestWithoutWait) {
    const auto thread_func{[](const size_t iterations) {
        const cpr::Url url{"http://127.0.0.1:5051/api/v1/yourNamePlease"};
        for (size_t iteration{0}; iteration < iterations; ++iteration) {
            [[maybe_unused]] auto response{cpr::PostAsync(url)};
        }
    }};

    constexpr size_t num_iterations{100};
    const size_t num_threads{std::max< size_t >(std::thread::hardware_concurrency(), 2)};
    std::vector< std::thread > workers;
    for (size_t thread_num{0}; thread_num < num_threads; ++thread_num) {
        workers.emplace_back(thread_func, num_iterations);
    }

    for (auto& worker : workers) {
        if (worker.joinable()) worker.join();
    }

    // exit while server processing
}

TEST_F(HTTPServerTest, RestartTest) {
    m_server->restart("", "");
    const cpr::Url url{"http://127.0.0.1:5000/api/v1/sayHello"};
    auto resp{cpr::Get(url)};
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "Hello client from async_http server\n");

    static const cpr::Url url1{"http://127.0.0.1:5000/api/v1/getResource"};
    resp = cpr::Get(url1);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "get");

    const cpr::Url url2{"http://127.0.0.1:5000/api/v1/postResource"};
    resp = cpr::Post(url2);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "post");

    const cpr::Url url3{"http://127.0.0.1:5000/api/v1/putResource"};
    resp = cpr::Put(url3);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "put");

    const cpr::Url url4{"http://127.0.0.1:5000/api/v1/deleteResource"};
    resp = cpr::Delete(url4);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "delete");
}

class HTTPServerParamsTest : public HTTPServerTest {
protected:
    void SetUp() override {
        m_server = std::make_unique< iomgr::HttpServer >();
        m_server->setup_route(Http::Method::Post, "/api/v1/level1",
                              Routes::bind(&HTTPServerParamsTest::create_level1, this));
        m_server->setup_route(Http::Method::Post, "/api/v1/level1/:level/level2",
                              Routes::bind(&HTTPServerParamsTest::create_level2, this));
        m_server->setup_route(Http::Method::Get, "/api/v1/level1",
                              Routes::bind(&HTTPServerParamsTest::get_level1, this));
        m_server->setup_route(Http::Method::Get, "/api/v1/level1/:level",
                              Routes::bind(&HTTPServerParamsTest::get_level1, this));
        m_server->start();
    }

    void create_level1(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok, "Level1");
    }

    void create_level2(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        auto level{request.param(":level").as< std::string >()};
        EXPECT_EQ(level, "Level1");
        response.send(Pistache::Http::Code::Ok, "Level2");
    }

    void get_level1(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        auto level{request.hasParam(":level") ? request.param(":level").as< std::string >() : ""};
        std::string resp = (level.empty()) ? "Level1" : "Level2";
        if (!level.empty()) { EXPECT_EQ(level, "Level1"); }
        auto q{request.query().get("query")};
        if (q) { resp += q.value(); }
        response.send(Pistache::Http::Code::Ok, resp);
    }
};

TEST_F(HTTPServerParamsTest, BasicTest) {
    cpr::Url url{"http://127.0.0.1:5000/api/v1/level1"};
    cpr::Response resp = cpr::Post(url);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "Level1");

    cpr::Url url1{"http://127.0.0.1:5000/api/v1/level1/Level1/level2"};
    resp = cpr::Post(url1);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "Level2");

    cpr::Url url2 = {"http://127.0.0.1:5000/api/v1/level1/"};
    resp = cpr::Get(url2);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "Level1");

    cpr::Url url3 = {"http://127.0.0.1:5000/api/v1/level1/Level1"};
    resp = cpr::Get(url3);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "Level2");

    cpr::Url url4 = {"http://127.0.0.1:5000/api/v1/level1/Level1?query=dummy"};
    resp = cpr::Get(url4);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "Level2dummy");
}

class HTTPServerAuthTest : public HTTPServerTest {
protected:
    void SetUp() override {
        m_server = std::make_unique< iomgr::HttpServer >();
        m_server->setup_route(Http::Method::Post, "/api/v1/localapi",
                              Routes::bind(&HTTPServerAuthTest::local_api, this), iomgr::url_t::localhost);
        m_server->setup_route(Http::Method::Get, "/api/v1/safeapi", Routes::bind(&HTTPServerAuthTest::safe_api, this),
                              iomgr::url_t::safe);
        m_server->setup_route(Http::Method::Get, "/api/v1/level1/regularapi",
                              Routes::bind(&HTTPServerAuthTest::local_api, this));

        EXPECT_TRUE(m_server->is_localaddr_url("/api/v1/localapi"));
        EXPECT_TRUE(m_server->is_safe_url("/api/v1/safeapi"));
        EXPECT_FALSE(m_server->is_safe_url("/api/v1/level1/regularapi"));
        EXPECT_FALSE(m_server->is_localaddr_url("/api/v1/level1/regularapi"));
        m_server->start();
    }

    void local_api(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok);
    }

    void safe_api(const Pistache::Rest::Request& request, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok);
    }
};

TEST_F(HTTPServerAuthTest, BasicTest) {
    cpr::Url url{"http://127.0.0.1:5000/api/v1/localapi"};
    cpr::Response resp = cpr::Post(url);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);

    cpr::Url url2 = {"http://127.0.0.1:5000/api/v1/safeapi"};
    resp = cpr::Get(url2);
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging)

    sisl::logging::SetLogger("test_http_server");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    return RUN_ALL_TESTS();
}

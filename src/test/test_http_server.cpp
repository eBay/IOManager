//
// Tests for ioenvironment.with_http_server()
//
#include <httplib/httplib.h>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <sisl/http/http_server.hpp>

#include "iomgr/io_environment.hpp"

SISL_LOGGING_INIT()
SISL_OPTIONS_ENABLE(logging)

class WithHttpServerTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = 1}).with_http_server();
        auto server = ioenvironment.get_http_server();
        server->setup_routes(
            {{sisl::http_method::Get, "/api/v1/ping",
              [](const httplib::Request&, httplib::Response& res) { res.set_content("pong", "text/plain"); }}});
        server->start();
    }

    static void TearDownTestSuite() { iomanager.stop(); }
};

TEST_F(WithHttpServerTest, BasicRoute) {
    auto resp = cpr::Get(cpr::Url{"http://127.0.0.1:5000/api/v1/ping"});
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "pong");
}

TEST_F(WithHttpServerTest, RestartRoute) {
    ioenvironment.restart_http_server();
    auto resp = cpr::Get(cpr::Url{"http://127.0.0.1:5000/api/v1/ping"});
    EXPECT_EQ(resp.status_code, cpr::status::HTTP_OK);
    EXPECT_EQ(resp.text, "pong");
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging)
    sisl::logging::SetLogger("test_http_server");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
    return RUN_ALL_TESTS();
}

/************************************************************************
 * Copyright 2017-2019 eBay Inc.
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

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

extern "C" {
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
}

#include <sisl/async/light_task.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <gtest/gtest.h>

#include <iomgr/io_environment.hpp>
#include <iomgr/iomgr.hpp>
#include <iomgr/net_op.hpp>

using namespace iomgr;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)
SISL_OPTIONS_ENABLE(logging, iomgr, config)

namespace {

std::pair< int, int > make_connected_pair() {
    int fds[2];
    RELEASE_ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0, "socketpair() failed: {}", strerror(errno));
    return {fds[0], fds[1]};
}

// A bound, listening AF_UNIX socket -- distinct from make_connected_pair(): listen() requires a socket
// that was bound and never connected, which a socketpair() endpoint already is not.
std::pair< int, std::string > make_listening_socket() {
    int const fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    RELEASE_ASSERT_GE(fd, 0, "socket() failed: {}", strerror(errno));
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::string const path = "/tmp/iomgr_net_op_test_" + std::to_string(::getpid()) + "_" + std::to_string(fd);
    RELEASE_ASSERT_LT(path.size(), sizeof(addr.sun_path), "unix socket path too long: {}", path);
    ::unlink(path.c_str());
    std::copy(path.begin(), path.end(), addr.sun_path);
    RELEASE_ASSERT_EQ(::bind(fd, reinterpret_cast< sockaddr* >(&addr), sizeof(addr)), 0, "bind() failed: {}",
                      strerror(errno));
    RELEASE_ASSERT_EQ(::listen(fd, 1), 0, "listen() failed: {}", strerror(errno));
    return {fd, path};
}

IOReactor* pick_a_worker_reactor() {
    IOReactor* reactor{nullptr};
    iomanager.run_on_wait(reactor_regex::random_worker, [&reactor]() { reactor = iomanager.this_reactor(); });
    return reactor;
}

std::vector< IOReactor* > two_distinct_worker_reactors() {
    std::mutex m;
    std::vector< IOReactor* > reactors;
    iomanager.run_on_wait(reactor_regex::all_worker, [&]() {
        std::lock_guard< std::mutex > lg(m);
        reactors.push_back(iomanager.this_reactor());
    });
    return reactors;
}

// Dispatches fn() (an awaitable-returning callable) onto `reactor` via run_on_forget -- so a genuine
// suspension inside it doesn't block that reactor from later resuming it -- and returns a future for the
// result. The caller decides whether to poll it with wait_for or block on get().
template < typename T, typename Fn >
std::future< T > start(IOReactor* reactor, Fn&& fn) {
    auto done = std::make_shared< std::promise< T > >();
    auto fut = done->get_future();
    iomanager.run_on_forget(reactor, [fn = std::forward< Fn >(fn), done]() mutable {
        [](auto fn, std::shared_ptr< std::promise< T > > done) -> sisl::async::light_task< void > {
            done->set_value(co_await fn());
        }(std::move(fn), std::move(done)).detach();
    });
    return fut;
}

} // namespace

class NetOpTest : public ::testing::Test {
public:
    void SetUp() override { ioenvironment.with_iomgr(iomgr_params{.num_threads = 2}); }
    void TearDown() override { iomanager.stop(); }
};

TEST_F(NetOpTest, RecvSendRoundTrip) {
    auto const [fd0, fd1] = make_connected_pair();
    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    std::array< uint8_t, 5 > read_buf{};
    io_result send_result{0};
    io_result recv_result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle a(fd0, thread_specifier{iomanager.this_reactor()});
        socket_handle b(fd1, thread_specifier{iomanager.this_reactor()});
        send_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t const > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_send(s, buf);
            }(a, std::span< uint8_t const >(payload)));
        recv_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_recv(s, buf);
            }(b, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(send_result.has_value());
    EXPECT_EQ(*send_result, payload.size());
    ASSERT_TRUE(recv_result.has_value());
    EXPECT_EQ(*recv_result, payload.size());
    EXPECT_EQ(read_buf, payload);
}

TEST_F(NetOpTest, RecvZeroIsEof) {
    auto const [fd0, fd1] = make_connected_pair();
    ::close(fd1); // peer gone before the recv is even attempted
    std::array< uint8_t, 5 > read_buf{};
    io_result result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle sock(fd0, thread_specifier{iomanager.this_reactor()});
        result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_recv(s, buf);
            }(sock, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0u);
}

TEST_F(NetOpTest, RecvSuspendsUntilDataArrives) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 5 > read_buf{};

    auto fut = start< io_result >(reactor, [&]() { return async_recv(sock, std::span< uint8_t >(read_buf)); });
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    ASSERT_EQ(::write(fd1, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()));

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, payload.size());
    EXPECT_EQ(read_buf, payload);
    ::close(fd1);
}

TEST_F(NetOpTest, SocketTeardownFailsParkedRecv) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    std::optional< socket_handle > sock;
    sock.emplace(fd0, thread_specifier{reactor});

    io_result result{0};
    bool completed{false};
    std::array< uint8_t, 5 > read_buf{};

    iomanager.run_on_wait(reactor, [&]() {
        [](socket_handle& s, std::span< uint8_t > buf, io_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_recv(s, buf);
            done = true;
        }(*sock, std::span< uint8_t >(read_buf), result, completed).detach();
    });
    ASSERT_FALSE(completed); // nothing written yet; the recv is parked

    sock.reset(); // destroyed from this (non-owning) thread -- exercises the forced-close path

    ASSERT_TRUE(completed);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::connection_aborted));
    ::close(fd1);
}

TEST_F(NetOpTest, ListenerTeardownFailsParkedAccept) {
    auto const [fd0, path] = make_listening_socket();
    auto* const reactor = pick_a_worker_reactor();
    std::optional< listener_handle > listener;
    listener.emplace(fd0, thread_specifier{reactor});

    accept_result result{-1};
    bool completed{false};

    iomanager.run_on_wait(reactor, [&]() {
        [](listener_handle& l, accept_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_accept(l);
            done = true;
        }(*listener, result, completed).detach();
    });
    ASSERT_FALSE(completed);

    listener.reset();

    ASSERT_TRUE(completed);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::connection_aborted));
    ::unlink(path.c_str());
}

TEST_F(NetOpTest, RecvExactAcrossTwoWrites) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 10 > read_buf{};

    auto fut = start< io_result >(reactor, [&]() { return recv_exact(sock, std::span< uint8_t >(read_buf)); });

    std::array< uint8_t, 4 > const chunk1{'a', 'b', 'c', 'd'};
    ASSERT_EQ(::write(fd1, chunk1.data(), chunk1.size()), static_cast< ssize_t >(chunk1.size()));
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    std::array< uint8_t, 6 > const chunk2{'e', 'f', 'g', 'h', 'i', 'j'};
    ASSERT_EQ(::write(fd1, chunk2.data(), chunk2.size()), static_cast< ssize_t >(chunk2.size()));

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, read_buf.size());
    std::array< uint8_t, 10 > const expected{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
    EXPECT_EQ(read_buf, expected);
    ::close(fd1);
}

TEST_F(NetOpTest, RecvExactShortEofIsError) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 10 > read_buf{};

    auto fut = start< io_result >(reactor, [&]() { return recv_exact(sock, std::span< uint8_t >(read_buf)); });

    std::array< uint8_t, 4 > const chunk{'a', 'b', 'c', 'd'};
    ASSERT_EQ(::write(fd1, chunk.data(), chunk.size()), static_cast< ssize_t >(chunk.size()));
    ::close(fd1); // peer gone before the remaining 6 bytes ever arrive

    auto const result = fut.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::connection_aborted));
}

TEST_F(NetOpTest, SendAllThenRecvExactRoundTrip) {
    auto const [fd0, fd1] = make_connected_pair();
    std::vector< uint8_t > payload(4096);
    for (size_t i = 0; i < payload.size(); ++i) { payload[i] = static_cast< uint8_t >(i); }
    std::vector< uint8_t > read_buf(payload.size());
    io_result result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle a(fd0, thread_specifier{iomanager.this_reactor()});
        socket_handle b(fd1, thread_specifier{iomanager.this_reactor()});
        result = sisl::async::sync_get(
            [](socket_handle& snd, socket_handle& rcv, std::span< uint8_t const > out,
               std::span< uint8_t > in) -> sisl::async::light_task< io_result > {
                auto sr = co_await send_all(snd, out);
                if (!sr) { co_return sr; }
                co_return co_await recv_exact(rcv, in);
            }(a, b, std::span< uint8_t const >(payload), std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, payload.size());
    EXPECT_EQ(read_buf, payload);
}

TEST_F(NetOpTest, AcceptThenPlaceOnDifferentReactor) {
    auto reactors = two_distinct_worker_reactors();
    ASSERT_GE(reactors.size(), 2u);
    IOReactor* const accepting_reactor = reactors[0];
    IOReactor* const placement_reactor = reactors[1];

    auto const [listen_sock, path] = make_listening_socket();
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::copy(path.begin(), path.end(), addr.sun_path);

    listener_handle listener(listen_sock, thread_specifier{accepting_reactor});
    auto accepted_fut = start< accept_result >(accepting_reactor, [&]() { return async_accept(listener); });

    int const client_sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT_GE(client_sock, 0);
    ASSERT_EQ(::connect(client_sock, reinterpret_cast< sockaddr* >(&addr), sizeof(addr)), 0);

    auto const accept_result_v = accepted_fut.get();
    ASSERT_TRUE(accept_result_v.has_value());
    int const server_side_fd = *accept_result_v;

    // Place the accepted connection on a reactor other than the one that accepted it: construction may
    // run from any thread, including the accepting reactor.
    socket_handle placed(server_side_fd, thread_specifier{placement_reactor});

    std::array< uint8_t, 4 > const payload{'p', 'i', 'n', 'g'};
    ASSERT_EQ(::write(client_sock, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()));

    std::array< uint8_t, 4 > read_buf{};
    io_result recv_result{0};
    iomanager.run_on_wait(placement_reactor, [&]() {
        recv_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await recv_exact(s, buf);
            }(placed, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(recv_result.has_value());
    EXPECT_EQ(read_buf, payload);

    ::close(client_sock);
    ::unlink(path.c_str());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging, iomgr, config);
    sisl::logging::SetLogger("test_net_op");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
    return RUN_ALL_TESTS();
}

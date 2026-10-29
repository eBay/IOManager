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

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
}

#include <sisl/async/light_task.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <gtest/gtest.h>

#if defined(__SANITIZE_ADDRESS__)
#include <sanitizer/lsan_interface.h>
#endif

#include <iomgr/accept_op.hpp>
#include <iomgr/connect_op.hpp>
#include <iomgr/drive.hpp>
#include <iomgr/io_environment.hpp>
#include <iomgr/iomgr.hpp>
#include <iomgr/net_op.hpp>
#include "reactor/reactor.hpp" // internal: whitebox check of IOReactor::get_poll_interval() below

using namespace iomgr;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)
SISL_OPTIONS_ENABLE(logging, iomgr, config)

namespace {

// Marks a deliberately-never-freed allocation (see NetOpReactorStopTest below) as a known leak
// instead of one LeakSanitizer should report. No-op in non-sanitized builds.
void ignore_leak(void* p) {
#if defined(__SANITIZE_ADDRESS__)
    __lsan_ignore_object(p);
#else
    (void)p;
#endif
}

std::pair< int, int > make_connected_pair() {
    int fds[2];
    RELEASE_ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0, "socketpair() failed: {}", strerror(errno));
    return {fds[0], fds[1]};
}

// A bound, listening AF_UNIX socket -- distinct from make_connected_pair(): listen() requires a socket
// that was bound and never connected, which a socketpair() endpoint already is not.
std::pair< int, std::string > make_listening_socket(int backlog = 1) {
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
    RELEASE_ASSERT_EQ(::listen(fd, backlog), 0, "listen() failed: {}", strerror(errno));
    return {fd, path};
}

sockaddr_un make_addr(std::string const& path) {
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::copy(path.begin(), path.end(), addr.sun_path);
    return addr;
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
        }(std::move(fn), std::move(done))
                                                                      .detach();
    });
    return fut;
}

// Unlike every socket_handle&/listener_handle& above, these take the handle as a coroutine PARAMETER:
// it lives in the coroutine's own frame and is destroyed by co_return, not from the test thread.
sisl::async::light_task< void > recv_until_eof_then_destroy(socket_handle sock, std::promise< io_result > done) {
    std::array< uint8_t, 16 > buf{};
    io_result last{0};
    for (;;) {
        last = co_await async_recv(sock, std::span< uint8_t >(buf));
        if (!last || *last == 0) break;
    }
    done.set_value(last);
    co_return; // `sock` destructs here, nested inside the EOF recv's own completion
}

// Same shape, but a send on the same socket is still parked when the recv side hits EOF.
sisl::async::light_task< void > recv_until_eof_with_concurrent_parked_send(socket_handle sock,
                                                                           std::vector< uint8_t > send_payload,
                                                                           std::promise< io_result > done) {
    detach(async_send(sock, std::span< uint8_t const >(send_payload)), [](io_result) {});

    std::array< uint8_t, 16 > buf{};
    io_result last{0};
    for (;;) {
        last = co_await async_recv(sock, std::span< uint8_t >(buf));
        if (!last || *last == 0) break;
    }
    done.set_value(last);
    co_return; // `sock` destructs here -- parked_send may still be set
}

sisl::async::light_task< void > accept_once_then_destroy(listener_handle listener, std::promise< accept_result > done) {
    auto result = co_await async_accept(listener);
    done.set_value(std::move(result));
    co_return; // `listener` destructs here, nested inside the accept's own completion
}

// Puts a disk write and a net recv outstanding on `reactor` together, alternating which resolves first
// across iterations, and checks poll_interval is restored after every cycle. Safe to call from any
// thread; every local here is thread-local to this call.
void hammer_disk_and_net(IOReactor* reactor, drive_handle const& drive, int iterations, uint64_t offset_base,
                         int original_poll_interval) {
    for (int iter = 0; iter < iterations; ++iter) {
        auto const [fd0, fd1] = make_connected_pair();
        socket_handle sock(fd0, thread_specifier{reactor});
        std::array< uint8_t, 5 > read_buf{};
        std::array< char, 4096 > write_buf{};

        std::promise< io_result > drive_promise;
        std::promise< io_result > net_promise;
        auto drive_fut = drive_promise.get_future();
        auto net_fut = net_promise.get_future();

        iomanager.run_on_wait(reactor, [&]() {
            detach(async_write(drive, write_buf.data(), write_buf.size(),
                               offset_base + static_cast< uint64_t >(iter) * write_buf.size()),
                   [&drive_promise](io_result r) { drive_promise.set_value(std::move(r)); });
            detach(async_recv(sock, std::span< uint8_t >(read_buf)),
                   [&net_promise](io_result r) { net_promise.set_value(std::move(r)); });
        });

        std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
        if (iter % 2 == 0) {
            // Nudge net to resolve first: write to unblock it, then wait on it, before waiting on drive.
            ASSERT_EQ(::write(fd1, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()))
                << "iter " << iter;
            ASSERT_TRUE(net_fut.get().has_value()) << "iter " << iter;
            ASSERT_TRUE(drive_fut.get().has_value()) << "iter " << iter;
        } else {
            // Nudge drive to resolve first: wait on it before even writing to unblock net.
            ASSERT_TRUE(drive_fut.get().has_value()) << "iter " << iter;
            ASSERT_EQ(::write(fd1, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()))
                << "iter " << iter;
            ASSERT_TRUE(net_fut.get().has_value()) << "iter " << iter;
        }

        int current_poll_interval{};
        iomanager.run_on_wait(reactor, [&]() { current_poll_interval = reactor->get_poll_interval(); });
        ASSERT_EQ(current_poll_interval, original_poll_interval) << "iter " << iter;

        ::close(fd1);
    }
}

} // namespace

class NetOpTest : public ::testing::Test {
public:
    void SetUp() override { ioenvironment.with_iomgr(iomgr_params{.num_threads = 2}); }
    void TearDown() override { iomanager.stop(); }
};

// Confirms iomgr_params::net_uring_qdepth is wired through to the net channel's ring and that a small
// override doesn't break ordinary operation. Does not exercise genuine ring exhaustion -- an ordinary
// burst self-heals via acquire_sqe's own flush-and-retry; see net_op.cpp's exception_to_error_condition
// for the exhaustion path itself, which is verified by reading the code.
TEST(NetOpCustomQdepthTest, ExplicitOverrideStillWorks) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 2, .net_uring_qdepth = 8});
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
    ASSERT_TRUE(recv_result.has_value());
    EXPECT_EQ(read_buf, payload);
    iomanager.stop();
}

// Regression check that an extreme depth/concurrency mismatch (100 sockets on a ring sized for 2) never
// crashes or hangs. Like ExplicitOverrideStillWorks above, this doesn't reach genuine ring exhaustion --
// acquire_sqe's flush-and-retry self-heals an ordinary burst regardless of configured depth.
TEST(NetOpRingPressureTest, ManyConcurrentOpsOnATinyRingAllResolve) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 1, .net_uring_qdepth = 2});
    auto* const reactor = pick_a_worker_reactor();

    constexpr int kNumSockets = 100;
    std::vector< int > peer_fds(kNumSockets);
    std::vector< std::optional< socket_handle > > socks(kNumSockets);
    std::vector< std::vector< uint8_t > > read_bufs(kNumSockets, std::vector< uint8_t >(16));

    for (int i = 0; i < kNumSockets; ++i) {
        auto const [fd0, fd1] = make_connected_pair();
        peer_fds[i] = fd1;
        socks[i].emplace(fd0, thread_specifier{reactor});
    }

    std::vector< std::future< io_result > > futs;
    futs.reserve(kNumSockets);
    for (int i = 0; i < kNumSockets; ++i) {
        futs.push_back(start< io_result >(
            reactor, [&, i]() { return async_recv(*socks[i], std::span< uint8_t >(read_bufs[i])); }));
    }
    for (int i = 0; i < kNumSockets; ++i) {
        uint8_t const b = static_cast< uint8_t >(i);
        ASSERT_EQ(::write(peer_fds[i], &b, 1), 1);
    }

    for (int i = 0; i < kNumSockets; ++i) {
        auto const r = futs[i].get();
        ASSERT_TRUE(r.has_value()) << "socket " << i << ": " << r.error().message();
    }

    // socket_handle destruction must happen before the reactor stops (see socket_handle's class
    // comment) -- explicit here rather than relying on scope exit, since that would run after the
    // iomanager.stop() below.
    socks.clear();
    for (int fd : peer_fds) {
        ::close(fd);
    }
    iomanager.stop();
}

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

// Complements RecvZeroIsEof: covers the write side failing once the peer is gone (MSG_NOSIGNAL turns what
// would be a process-killing SIGPIPE into an ordinary error result). Also the only test exercising
// detach()'s callback-based path rather than a coroutine.
TEST_F(NetOpTest, DetachInvokesCallbackOnCompletion) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 5 > read_buf{};

    std::promise< io_result > result_promise;
    auto result_fut = result_promise.get_future();
    iomanager.run_on_wait(reactor, [&]() {
        detach(async_recv(sock, std::span< uint8_t >(read_buf)),
               [&result_promise](io_result r) { result_promise.set_value(std::move(r)); });
    });
    ASSERT_EQ(result_fut.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout); // parked

    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    ASSERT_EQ(::write(fd1, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()));

    auto const result = result_fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(read_buf, payload);
    ::close(fd1);
}

TEST_F(NetOpTest, SendFailsWhenPeerClosed) {
    auto const [fd0, fd1] = make_connected_pair();
    ::close(fd1); // peer gone before the send is even attempted
    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    io_result result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle sock(fd0, thread_specifier{iomanager.this_reactor()});
        result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t const > buf) -> sisl::async::light_task< io_result > {
                co_return co_await async_send(s, buf);
            }(sock, std::span< uint8_t const >(payload)));
    });

    ASSERT_FALSE(result.has_value());
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

TEST_F(NetOpTest, SendSuspendsUntilPeerDrains) {
    auto const [fd0, fd1] = make_connected_pair();
    // Shrink both ends of the pipe's effective capacity so a send_all of a comfortably larger payload is
    // guaranteed to exhaust it and genuinely park, regardless of this host's default socket buffer sizes.
    int const small_buf_size = 4096;
    ::setsockopt(fd0, SOL_SOCKET, SO_SNDBUF, &small_buf_size, sizeof(small_buf_size));
    ::setsockopt(fd1, SOL_SOCKET, SO_RCVBUF, &small_buf_size, sizeof(small_buf_size));

    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});

    std::vector< uint8_t > payload(1024 * 1024);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast< uint8_t >(i);
    }

    auto fut = start< io_result >(reactor, [&]() { return send_all(sock, std::span< uint8_t const >(payload)); });
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);

    std::vector< uint8_t > received(payload.size());
    size_t total_read{0};
    while (total_read < received.size()) {
        ssize_t const n = ::read(fd1, received.data() + total_read, received.size() - total_read);
        ASSERT_GT(n, 0);
        total_read += static_cast< size_t >(n);
    }

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, payload.size());
    EXPECT_EQ(received, payload);
    ::close(fd1);
}

TEST_F(NetOpTest, SocketTeardownFailsParkedRecv) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    std::optional< socket_handle > sock;
    sock.emplace(fd0, thread_specifier{reactor});

    std::array< uint8_t, 5 > read_buf{};
    auto done = std::make_shared< std::promise< io_result > >();
    auto fut = done->get_future();

    iomanager.run_on_wait(reactor, [&]() {
        [](socket_handle& s, std::span< uint8_t > buf,
           std::shared_ptr< std::promise< io_result > > done) -> sisl::async::light_task< void > {
            done->set_value(co_await async_recv(s, buf));
        }(*sock, std::span< uint8_t >(read_buf), done)
                                                                     .detach();
    });
    ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::timeout); // nothing written yet; parked

    // Destroyed from this (non-owning) thread -- exercises the forced-close path. Teardown resolves the
    // parked waiter asynchronously, so wait for it rather than asserting immediately.
    sock.reset();

    ASSERT_EQ(fut.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    auto const result = fut.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::connection_aborted));
    ::close(fd1);
}

TEST_F(NetOpTest, MoveAssignTearsDownPreviousConnection) {
    auto const [fd0, fd1] = make_connected_pair();
    auto const [fd2, fd3] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();

    socket_handle old_sock(fd0, thread_specifier{reactor});
    socket_handle new_sock(fd2, thread_specifier{reactor});

    std::array< uint8_t, 5 > old_read_buf{};
    auto old_done = std::make_shared< std::promise< io_result > >();
    auto old_fut = old_done->get_future();

    iomanager.run_on_wait(reactor, [&]() {
        [](socket_handle& s, std::span< uint8_t > buf,
           std::shared_ptr< std::promise< io_result > > done) -> sisl::async::light_task< void > {
            done->set_value(co_await async_recv(s, buf));
        }(old_sock, std::span< uint8_t >(old_read_buf), old_done)
                                                                     .detach();
    });
    ASSERT_EQ(old_fut.wait_for(std::chrono::seconds(0)), std::future_status::timeout); // parked

    // Move-assigning over old_sock while its recv is still parked must tear down its previous connection
    // (fail the parked waiter, close fd0) before taking ownership of new_sock's, resolved asynchronously.
    old_sock = std::move(new_sock);

    ASSERT_EQ(old_fut.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    auto const old_result = old_fut.get();
    ASSERT_FALSE(old_result.has_value());
    EXPECT_EQ(old_result.error(), std::make_error_condition(std::errc::connection_aborted));

    // old_sock now owns what was new_sock's connection and should work normally.
    std::array< uint8_t, 4 > const payload{'p', 'i', 'n', 'g'};
    ASSERT_EQ(::write(fd3, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()));

    std::array< uint8_t, 4 > read_buf{};
    io_result recv_result{0};
    iomanager.run_on_wait(reactor, [&]() {
        recv_result = sisl::async::sync_get(
            [](socket_handle& s, std::span< uint8_t > buf) -> sisl::async::light_task< io_result > {
                co_return co_await recv_exact(s, buf);
            }(old_sock, std::span< uint8_t >(read_buf)));
    });

    ASSERT_TRUE(recv_result.has_value());
    EXPECT_EQ(read_buf, payload);
    ::close(fd1);
    ::close(fd3);
}

TEST_F(NetOpTest, ListenerTeardownFailsParkedAccept) {
    auto const [fd0, path] = make_listening_socket();
    auto* const reactor = pick_a_worker_reactor();
    std::optional< listener_handle > listener;
    listener.emplace(fd0, thread_specifier{reactor});

    auto done = std::make_shared< std::promise< accept_result > >();
    auto fut = done->get_future();

    iomanager.run_on_wait(reactor, [&]() {
        [](listener_handle& l, std::shared_ptr< std::promise< accept_result > > done)
            -> sisl::async::light_task< void > { done->set_value(co_await async_accept(l)); }(*listener, done).detach();
    });
    ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

    // Resolution is asynchronous (see socket_handle::~socket_handle), so wait for it.
    listener.reset();

    ASSERT_EQ(fut.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    auto const result = fut.get();
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
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast< uint8_t >(i);
    }
    std::vector< uint8_t > read_buf(payload.size());
    io_result result{0};

    iomanager.run_on_wait(reactor_regex::random_worker, [&]() {
        socket_handle a(fd0, thread_specifier{iomanager.this_reactor()});
        socket_handle b(fd1, thread_specifier{iomanager.this_reactor()});
        result = sisl::async::sync_get([](socket_handle& snd, socket_handle& rcv, std::span< uint8_t const > out,
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
    sockaddr_un addr = make_addr(path);

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

TEST_F(NetOpTest, ConnectSucceedsAndRoundTrips) {
    auto const [listen_fd, path] = make_listening_socket();
    auto* const reactor = pick_a_worker_reactor();
    listener_handle listener(listen_fd, thread_specifier{reactor});
    sockaddr_un addr = make_addr(path);

    int const client_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);

    std::expected< void, std::error_condition > connect_result;
    accept_result accept_res{-1};

    // connect() can complete as soon as the kernel enqueues the pending connection into the listener's
    // backlog -- it doesn't need to race an in-flight accept, so issuing them sequentially is deterministic.
    iomanager.run_on_wait(reactor, [&]() {
        [](listener_handle& l, int cfd, sockaddr_un a, IOReactor* r, std::expected< void, std::error_condition >& cres,
           accept_result& ares) -> sisl::async::light_task< void > {
            cres = co_await async_connect(cfd, reinterpret_cast< sockaddr* >(&a), sizeof(a), thread_specifier{r});
            ares = co_await async_accept(l);
        }(listener, client_fd, addr, reactor, connect_result, accept_res)
                                       .detach();
    });

    ASSERT_TRUE(connect_result.has_value());
    ASSERT_TRUE(accept_res.has_value());

    socket_handle client(client_fd, thread_specifier{reactor});
    socket_handle server(*accept_res, thread_specifier{reactor});

    std::array< uint8_t, 4 > const payload{'p', 'i', 'n', 'g'};
    io_result rt_result{0};
    iomanager.run_on_wait(reactor, [&]() {
        rt_result = sisl::async::sync_get([](socket_handle& snd, socket_handle& rcv,
                                             std::span< uint8_t const > out) -> sisl::async::light_task< io_result > {
            auto sr = co_await send_all(snd, out);
            if (!sr) { co_return sr; }
            std::array< uint8_t, 4 > in{};
            auto rr = co_await recv_exact(rcv, std::span< uint8_t >(in));
            if (!rr) { co_return rr; }
            co_return io_result{std::equal(in.begin(), in.end(), out.begin()) ? *rr : 0};
        }(client, server, std::span< uint8_t const >(payload)));
    });

    ASSERT_TRUE(rt_result.has_value());
    EXPECT_EQ(*rt_result, payload.size());
    ::unlink(path.c_str());
}

TEST_F(NetOpTest, ConnectToNonexistentPathFails) {
    auto* const reactor = pick_a_worker_reactor();
    sockaddr_un addr = make_addr("/tmp/iomgr_net_op_test_no_such_socket_" + std::to_string(::getpid()));

    int const client_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);

    auto fut = start< std::expected< void, std::error_condition > >(reactor, [&]() {
        return async_connect(client_fd, reinterpret_cast< sockaddr* >(&addr), sizeof(addr), thread_specifier{reactor});
    });

    auto const result = fut.get();
    ASSERT_FALSE(result.has_value());
    ::close(client_fd);
}

TEST_F(NetOpTest, ConnectTimesOut) {
    // 192.0.2.0/24 is RFC 5737 TEST-NET-1: reserved, never assigned or routable. A connect() to it starts
    // a real handshake that never gets a SYN-ACK or RST, so it stays pending deterministically.
    auto* const reactor = pick_a_worker_reactor();
    int const client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(80);
    ASSERT_EQ(::inet_pton(AF_INET, "192.0.2.1", &addr.sin_addr), 1);

    auto fut = start< std::expected< void, std::error_condition > >(reactor, [&]() {
        return async_connect(client_fd, reinterpret_cast< sockaddr* >(&addr), sizeof(addr), thread_specifier{reactor},
                             std::chrono::milliseconds(50));
    });

    auto const result = fut.get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::make_error_condition(std::errc::timed_out));

    ::close(client_fd);
}

// Many socket_handles pinned to the same reactor, recv'ing concurrently: checks the shared per-reactor
// ring disambiguates completions correctly by user_data under real multiplexing.
TEST_F(NetOpTest, ManyConcurrentRecvsOnOneReactorDontCrossTalk) {
    constexpr int kNumSockets = 32;
    auto* const reactor = pick_a_worker_reactor();

    std::vector< int > peer_fds(kNumSockets);
    std::vector< std::optional< socket_handle > > socks(kNumSockets);
    std::vector< std::vector< uint8_t > > payloads(kNumSockets);
    std::vector< std::vector< uint8_t > > read_bufs(kNumSockets);

    for (int i = 0; i < kNumSockets; ++i) {
        auto const [fd0, fd1] = make_connected_pair();
        peer_fds[i] = fd1;
        socks[i].emplace(fd0, thread_specifier{reactor});
        payloads[i] = std::vector< uint8_t >(256, static_cast< uint8_t >(i));
        read_bufs[i] = std::vector< uint8_t >(256);
    }

    std::vector< std::future< io_result > > futs;
    futs.reserve(kNumSockets);
    for (int i = 0; i < kNumSockets; ++i) {
        futs.push_back(start< io_result >(
            reactor, [&, i]() { return recv_exact(*socks[i], std::span< uint8_t >(read_bufs[i])); }));
    }
    // All kNumSockets recvs are now parked concurrently on the same reactor's ring before any data
    // arrives. Writing from the peer side afterward (order doesn't matter -- the kernel buffers each
    // independently) is what actually forces all of them to complete via uring, not the fast path.
    for (int i = 0; i < kNumSockets; ++i) {
        ASSERT_EQ(::write(peer_fds[i], payloads[i].data(), payloads[i].size()),
                  static_cast< ssize_t >(payloads[i].size()));
    }

    for (int i = 0; i < kNumSockets; ++i) {
        auto const r = futs[i].get();
        ASSERT_TRUE(r.has_value()) << "socket " << i;
        EXPECT_EQ(read_bufs[i], payloads[i]) << "socket " << i << " got cross-talked data";
    }

    for (int fd : peer_fds) {
        ::close(fd);
    }
}

// Same as ManyConcurrentRecvsOnOneReactorDontCrossTalk, but across multiple rings on different reactor
// threads active at the same time, to rule out accidentally-shared state between net channels.
TEST_F(NetOpTest, ConcurrentActivityAcrossMultipleReactorsDoesntCrossTalk) {
    auto reactors = two_distinct_worker_reactors();
    ASSERT_GE(reactors.size(), 2u);

    struct conn {
        int peer_fd;
        std::optional< socket_handle > sock;
        std::vector< uint8_t > payload;
        std::vector< uint8_t > read_buf;
    };
    constexpr int kPerReactor = 16;
    std::vector< conn > conns(reactors.size() * kPerReactor);

    for (size_t r = 0; r < reactors.size(); ++r) {
        for (int i = 0; i < kPerReactor; ++i) {
            auto& c = conns[r * kPerReactor + static_cast< size_t >(i)];
            auto const [fd0, fd1] = make_connected_pair();
            c.peer_fd = fd1;
            c.sock.emplace(fd0, thread_specifier{reactors[r]});
            c.payload = std::vector< uint8_t >(128, static_cast< uint8_t >(r * kPerReactor + static_cast< size_t >(i)));
            c.read_buf = std::vector< uint8_t >(128);
        }
    }

    std::vector< std::future< io_result > > futs;
    futs.reserve(conns.size());
    for (size_t r = 0; r < reactors.size(); ++r) {
        for (int i = 0; i < kPerReactor; ++i) {
            auto& c = conns[r * kPerReactor + static_cast< size_t >(i)];
            futs.push_back(start< io_result >(
                reactors[r], [&c]() { return recv_exact(*c.sock, std::span< uint8_t >(c.read_buf)); }));
        }
    }
    for (auto& c : conns) {
        ASSERT_EQ(::write(c.peer_fd, c.payload.data(), c.payload.size()), static_cast< ssize_t >(c.payload.size()));
    }
    for (size_t i = 0; i < conns.size(); ++i) {
        auto const r = futs[i].get();
        ASSERT_TRUE(r.has_value()) << "conn " << i;
        EXPECT_EQ(conns[i].read_buf, conns[i].payload) << "conn " << i << " got cross-talked data";
    }
    for (auto& c : conns) {
        ::close(c.peer_fd);
    }
}

// Regression test for a reactor running disk and net I/O outstanding at the same time (no other test in
// this file does). Net no longer tight-polls (see net_io_started), so drive is enter_tight_poll's only
// caller; this now hardens drive's own save/restore under concurrent unrelated net traffic, rather than
// reproducing the multi-caller corruption IOReactor::enter_tight_poll's refcount design guards against.
TEST_F(NetOpTest, DriveTightPollRestoresCorrectlyWithConcurrentNetIO) {
    std::string const dev_path = "/tmp/iomgr_net_op_test_drive_" + std::to_string(::getpid());
    {
        int const fd = ::open(dev_path.c_str(), O_RDWR | O_CREAT, 0666);
        ASSERT_GE(fd, 0);
        ::close(fd);
    }
    std::filesystem::resize_file(dev_path, 1024 * 1024);
    auto drive_result = open_drive(dev_path, O_CREAT | O_RDWR);
    ASSERT_TRUE(drive_result.has_value());
    auto drive = std::move(drive_result.value());

    auto* const reactor = pick_a_worker_reactor();
    int original_poll_interval{};
    iomanager.run_on_wait(reactor, [&]() { original_poll_interval = reactor->get_poll_interval(); });

    auto const [fd0, fd1] = make_connected_pair();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 5 > read_buf{};
    std::array< char, 4096 > write_buf{};

    std::promise< io_result > drive_promise;
    std::promise< io_result > net_promise;
    auto drive_fut = drive_promise.get_future();
    auto net_fut = net_promise.get_future();

    iomanager.run_on_wait(reactor, [&]() {
        // Drive goes outstanding first (guaranteed to suspend); net goes outstanding second while it's
        // still in flight -- the interleaving the bug needs.
        detach(async_write(drive, write_buf.data(), write_buf.size(), 0),
               [&drive_promise](io_result r) { drive_promise.set_value(std::move(r)); });
        detach(async_recv(sock, std::span< uint8_t >(read_buf)),
               [&net_promise](io_result r) { net_promise.set_value(std::move(r)); });
    });

    // Drive resolves first, net second, so net is the side that transitions outstanding back to zero last.
    auto const drive_r = drive_fut.get();
    ASSERT_TRUE(drive_r.has_value());

    std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
    ASSERT_EQ(::write(fd1, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()));
    auto const net_r = net_fut.get();
    ASSERT_TRUE(net_r.has_value());

    int final_poll_interval{};
    iomanager.run_on_wait(reactor, [&]() { final_poll_interval = reactor->get_poll_interval(); });
    EXPECT_EQ(final_poll_interval, original_poll_interval)
        << "reactor poll_interval was left at " << final_poll_interval << " instead of restored to "
        << original_poll_interval << " -- an idle reactor stuck tight-polling forever";

    ::close(fd1);
    drive.reset();
    std::filesystem::remove(dev_path);
}

// Hammers both completion orderings across many iterations on one reactor, rather than trusting the
// single ordering DriveTightPollRestoresCorrectlyWithConcurrentNetIO happens to exercise.
TEST_F(NetOpTest, HammeredDriveTightPollRestoresCorrectlyWithConcurrentNetIO) {
    std::string const dev_path = "/tmp/iomgr_net_op_test_drive_hammer_" + std::to_string(::getpid());
    {
        int const fd = ::open(dev_path.c_str(), O_RDWR | O_CREAT, 0666);
        ASSERT_GE(fd, 0);
        ::close(fd);
    }
    std::filesystem::resize_file(dev_path, 8 * 1024 * 1024);
    auto drive_result = open_drive(dev_path, O_CREAT | O_RDWR);
    ASSERT_TRUE(drive_result.has_value());
    auto drive = std::move(drive_result.value());

    auto* const reactor = pick_a_worker_reactor();
    int original_poll_interval{};
    iomanager.run_on_wait(reactor, [&]() { original_poll_interval = reactor->get_poll_interval(); });

    hammer_disk_and_net(reactor, drive, 100, 0, original_poll_interval);

    drive.reset();
    std::filesystem::remove(dev_path);
}

// Same hammering, but on two reactors genuinely in parallel, with separate drive files to keep the two
// loops' disk I/O fully independent.
TEST_F(NetOpTest, HammeredConcurrentDiskAndNetIOAcrossMultipleReactorsInParallel) {
    auto reactors = two_distinct_worker_reactors();
    ASSERT_GE(reactors.size(), 2u);

    std::string const dev_path_a = "/tmp/iomgr_net_op_test_drive_hammer_a_" + std::to_string(::getpid());
    std::string const dev_path_b = "/tmp/iomgr_net_op_test_drive_hammer_b_" + std::to_string(::getpid());
    for (auto const& path : {dev_path_a, dev_path_b}) {
        int const fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0666);
        ASSERT_GE(fd, 0);
        ::close(fd);
        std::filesystem::resize_file(path, 8 * 1024 * 1024);
    }
    auto drive_a_result = open_drive(dev_path_a, O_CREAT | O_RDWR);
    auto drive_b_result = open_drive(dev_path_b, O_CREAT | O_RDWR);
    ASSERT_TRUE(drive_a_result.has_value());
    ASSERT_TRUE(drive_b_result.has_value());
    auto drive_a = std::move(drive_a_result.value());
    auto drive_b = std::move(drive_b_result.value());

    int poll_interval_a{};
    int poll_interval_b{};
    iomanager.run_on_wait(reactors[0], [&]() { poll_interval_a = reactors[0]->get_poll_interval(); });
    iomanager.run_on_wait(reactors[1], [&]() { poll_interval_b = reactors[1]->get_poll_interval(); });

    std::thread t1([&]() { hammer_disk_and_net(reactors[0], drive_a, 50, 0, poll_interval_a); });
    std::thread t2([&]() { hammer_disk_and_net(reactors[1], drive_b, 50, 0, poll_interval_b); });
    t1.join();
    t2.join();

    drive_a.reset();
    drive_b.reset();
    std::filesystem::remove(dev_path_a);
    std::filesystem::remove(dev_path_b);
}

// Many cycles of parking-then-tearing-down and parking-then-completing in quick succession, to give the
// cancel-vs-complete race in run_uring_op/cancel_guard repeated chances to misbehave.
TEST_F(NetOpTest, RepeatedTeardownDuringFlightDoesNotRaceOrLeak) {
    auto* const reactor = pick_a_worker_reactor();
    constexpr int kIterations = 200;
    for (int iter = 0; iter < kIterations; ++iter) {
        auto const [fd0, fd1] = make_connected_pair();
        auto sock = std::make_unique< socket_handle >(fd0, thread_specifier{reactor});
        std::array< uint8_t, 5 > read_buf{};

        if (iter % 2 == 0) {
            // Cancellation path: tear down while still genuinely parked (cancel_guard's destructor path).
            // Resolution is asynchronous, so wait for it rather than asserting immediately after reset().
            auto done = std::make_shared< std::promise< io_result > >();
            auto fut = done->get_future();
            iomanager.run_on_wait(reactor, [&]() {
                [](socket_handle& s, std::span< uint8_t > buf,
                   std::shared_ptr< std::promise< io_result > > done) -> sisl::async::light_task< void > {
                    done->set_value(co_await async_recv(s, buf));
                }(*sock, std::span< uint8_t >(read_buf), done)
                                                                             .detach();
            });
            ASSERT_EQ(fut.wait_for(std::chrono::seconds(0)), std::future_status::timeout) << "iter " << iter;
            sock.reset(); // destroyed from this thread, still parked
            ASSERT_EQ(fut.wait_for(std::chrono::seconds(1)), std::future_status::ready) << "iter " << iter;
            ASSERT_FALSE(fut.get().has_value()) << "iter " << iter;
        } else {
            // Normal-completion path: let it resolve naturally before tearing down (the post-co_await
            // completion path in run_uring_op, with cancel_guard's destructor a no-op).
            auto fut = start< io_result >(reactor, [&]() { return async_recv(*sock, std::span< uint8_t >(read_buf)); });
            std::array< uint8_t, 5 > const payload{'h', 'e', 'l', 'l', 'o'};
            ASSERT_EQ(::write(fd1, payload.data(), payload.size()), static_cast< ssize_t >(payload.size()))
                << "iter " << iter;
            auto const r = fut.get();
            ASSERT_TRUE(r.has_value()) << "iter " << iter;
            sock.reset(); // already resolved -- teardown here is the "nothing parked" fast path
        }
        ::close(fd1);
    }
}

// Unlike every other test above, the socket is destroyed by its own handler coroutine ending (reads
// until EOF, falls out of scope) rather than from the test thread after the op resolved.
TEST_F(NetOpTest, HandlerOwnedSocketDestroyedFromWithinOwnRecvCompletion) {
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});

    // run_on_forget takes its callable by const auto& and copy-constructs it internally, so the
    // dispatched lambda itself must be copyable -- shared_ptr (copyable regardless of pointee) is how
    // move-only state (socket_handle, std::promise) rides along, same idiom start<T>() uses above.
    auto sock_ptr = std::make_shared< socket_handle >(std::move(sock));
    auto done_ptr = std::make_shared< std::promise< io_result > >();
    auto fut = done_ptr->get_future();
    iomanager.run_on_forget(reactor, [sock_ptr, done_ptr]() mutable {
        recv_until_eof_then_destroy(std::move(*sock_ptr), std::move(*done_ptr)).detach();
    });
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    ::close(fd1); // peer closes -> recv resolves with 0 (EOF), triggering the handler's co_return

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0u);
}

// Same as above, but a send is still parked on the socket too, so teardown() takes its cancel-and-wait
// path instead of the "nothing parked" fast path.
TEST_F(NetOpTest, HandlerOwnedSocketWithConcurrentParkedSendDestroyedFromWithinOwnRecvCompletion) {
    auto const [fd0, fd1] = make_connected_pair();
    // Shrink both ends' effective buffering so the send below is guaranteed to still be parked (nobody
    // ever reads fd1) when EOF arrives on the recv side; same trick as SendSuspendsUntilPeerDrains.
    int const small_buf_size = 4096;
    ::setsockopt(fd0, SOL_SOCKET, SO_SNDBUF, &small_buf_size, sizeof(small_buf_size));
    ::setsockopt(fd1, SOL_SOCKET, SO_RCVBUF, &small_buf_size, sizeof(small_buf_size));

    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});

    auto sock_ptr = std::make_shared< socket_handle >(std::move(sock));
    auto payload_ptr = std::make_shared< std::vector< uint8_t > >(1024 * 1024);
    auto done_ptr = std::make_shared< std::promise< io_result > >();
    auto fut = done_ptr->get_future();
    iomanager.run_on_forget(reactor, [sock_ptr, payload_ptr, done_ptr]() mutable {
        recv_until_eof_with_concurrent_parked_send(std::move(*sock_ptr), std::move(*payload_ptr), std::move(*done_ptr))
            .detach();
    });
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    // Close only fd1's read side (shutdown WR) so fd0's send stays accepted into fd1's (tiny) receive
    // buffer without erroring, while fd0's recv still sees EOF once that FIN arrives.
    ::shutdown(fd1, SHUT_WR);

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0u);
    ::close(fd1);
}

// Listener analog: a handler that owns its listener and destroys it right after accept() resolves,
// from inside that same completion.
TEST_F(NetOpTest, HandlerOwnedListenerDestroyedFromWithinOwnAcceptCompletion) {
    auto const [fd0, path] = make_listening_socket();
    auto* const reactor = pick_a_worker_reactor();
    listener_handle listener(fd0, thread_specifier{reactor});

    auto listener_ptr = std::make_shared< listener_handle >(std::move(listener));
    auto done_ptr = std::make_shared< std::promise< accept_result > >();
    auto fut = done_ptr->get_future();
    iomanager.run_on_forget(reactor, [listener_ptr, done_ptr]() mutable {
        accept_once_then_destroy(std::move(*listener_ptr), std::move(*done_ptr)).detach();
    });
    ASSERT_EQ(fut.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    int const client_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    RELEASE_ASSERT_GE(client_fd, 0, "socket() failed: {}", strerror(errno));
    auto const addr = make_addr(path);
    ASSERT_EQ(::connect(client_fd, reinterpret_cast< sockaddr const* >(&addr), sizeof(addr)), 0);

    auto const result = fut.get();
    ASSERT_TRUE(result.has_value());
    ::close(client_fd);
    ::unlink(path.c_str());
}

// Not a NetOpTest fixture: exercises a caller that does NOT follow socket_handle's "destroy before the
// reactor stops" contract, so it needs its own start/stop rather than NetOpTest::TearDown's.
TEST(NetOpReactorStopTest, CancelsOutstandingRecvInstead) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 2});
    auto const [fd0, fd1] = make_connected_pair();
    IOReactor* reactor{nullptr};
    iomanager.run_on_wait(reactor_regex::random_worker, [&reactor]() { reactor = iomanager.this_reactor(); });

    // Heap-allocated and deliberately leaked: destroying it would dispatch onto `reactor`, which is about
    // to stop.
    auto* const sock = new socket_handle(fd0, thread_specifier{reactor});
    ignore_leak(sock);

    io_result result{0};
    bool completed{false};
    std::array< uint8_t, 5 > read_buf{};

    iomanager.run_on_wait(reactor, [&]() {
        [](socket_handle& s, std::span< uint8_t > buf, io_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_recv(s, buf);
            done = true;
        }(*sock, std::span< uint8_t >(read_buf), result, completed)
                                                                                          .detach();
    });
    ASSERT_FALSE(completed); // parked; nothing written

    iomanager.stop(); // must actively cancel the still-outstanding recv, not hang waiting for it

    EXPECT_TRUE(completed);
    EXPECT_FALSE(result.has_value());
    ::close(fd1);
    ::close(fd0);
}

// Same drain path as CancelsOutstandingRecvInstead, for a parked accept and a parked (no-timeout) connect,
// since drain_outstanding_ops() cancels the whole ring in one shot and must cover every op kind.
TEST(NetOpReactorStopTest, CancelsOutstandingAcceptAndConnectInstead) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 2});
    IOReactor* reactor{nullptr};
    iomanager.run_on_wait(reactor_regex::random_worker, [&reactor]() { reactor = iomanager.this_reactor(); });

    auto const [listen_fd, path] = make_listening_socket();
    auto* const listener = new listener_handle(listen_fd, thread_specifier{reactor}); // leaked; see above
    ignore_leak(listener);

    accept_result accept_res{-1};
    bool accept_completed{false};
    iomanager.run_on_wait(reactor, [&]() {
        [](listener_handle& l, accept_result& out, bool& done) -> sisl::async::light_task< void > {
            out = co_await async_accept(l);
            done = true;
        }(*listener, accept_res, accept_completed)
                                                                      .detach();
    });
    ASSERT_FALSE(accept_completed);

    // Same TEST-NET-1 target as ConnectTimesOut, with no timeout: genuinely still pending (not resolved
    // by its own timeout) by the time stop() runs below.
    int const client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(80);
    ASSERT_EQ(::inet_pton(AF_INET, "192.0.2.1", &addr.sin_addr), 1);

    std::expected< void, std::error_condition > connect_res;
    bool connect_completed{false};
    iomanager.run_on_wait(reactor, [&]() {
        [](int fd, sockaddr_in a, IOReactor* r, std::expected< void, std::error_condition >& out,
           bool& done) -> sisl::async::light_task< void > {
            out = co_await async_connect(fd, reinterpret_cast< sockaddr* >(&a), sizeof(a), thread_specifier{r});
            done = true;
        }(client_fd, addr, reactor, connect_res, connect_completed)
                              .detach();
    });
    ASSERT_FALSE(connect_completed);

    iomanager.stop(); // must cancel both, not hang

    EXPECT_TRUE(accept_completed);
    EXPECT_FALSE(accept_res.has_value());
    EXPECT_TRUE(connect_completed);
    ASSERT_FALSE(connect_res.has_value());
    // No timeout was armed on this connect -- cancelled only by the bulk cancel at reactor stop, not by its
    // own (nonexistent) deadline, so this must be connection_aborted, not timed_out.
    EXPECT_EQ(connect_res.error(), std::make_error_condition(std::errc::connection_aborted));

    ::close(client_fd);
    ::unlink(path.c_str());
}

// A disk write and a net recv both outstanding on the same reactor when it stops. Checks only that
// neither hangs: the net recv must resolve via cancellation (nothing ever unblocks it), while the disk
// write may resolve via normal completion or cancellation depending on timing -- either is acceptable.
TEST(NetOpReactorStopTest, CancelsOutstandingDiskAndNetIOInstead) {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 1});
    std::string const dev_path = "/tmp/iomgr_net_op_test_drive_stop_" + std::to_string(::getpid());
    {
        int const fd = ::open(dev_path.c_str(), O_RDWR | O_CREAT, 0666);
        ASSERT_GE(fd, 0);
        ::close(fd);
    }
    std::filesystem::resize_file(dev_path, 1024 * 1024);
    auto drive_result = open_drive(dev_path, O_CREAT | O_RDWR);
    ASSERT_TRUE(drive_result.has_value());
    auto drive = std::move(drive_result.value());

    IOReactor* reactor{nullptr};
    iomanager.run_on_wait(reactor_regex::random_worker, [&reactor]() { reactor = iomanager.this_reactor(); });

    auto const [fd0, fd1] = make_connected_pair();
    auto* const sock = new socket_handle(fd0, thread_specifier{reactor}); // leaked; see above
    ignore_leak(sock);
    std::array< uint8_t, 5 > read_buf{};
    std::array< char, 4096 > write_buf{};

    io_result drive_result_out{0};
    io_result net_result_out{0};
    bool drive_completed{false};
    bool net_completed{false};

    iomanager.run_on_wait(reactor, [&]() {
        detach(async_write(drive, write_buf.data(), write_buf.size(), 0), [&](io_result r) {
            drive_result_out = std::move(r);
            drive_completed = true;
        });
        detach(async_recv(*sock, std::span< uint8_t >(read_buf)), [&](io_result r) {
            net_result_out = std::move(r);
            net_completed = true;
        });
    });

    // No wait: stop immediately, to maximize the chance both are still genuinely in flight.
    iomanager.stop(); // must cancel/drain both, not hang

    EXPECT_TRUE(drive_completed);
    EXPECT_TRUE(net_completed);
    EXPECT_FALSE(net_result_out.has_value()); // nothing ever wrote to fd1 to unblock it

    ::close(fd1);
    drive.reset();
    std::filesystem::remove(dev_path);
}

namespace {
void trigger_second_concurrent_recv() {
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 1});
    auto const [fd0, fd1] = make_connected_pair();
    auto* const reactor = pick_a_worker_reactor();
    socket_handle sock(fd0, thread_specifier{reactor});
    std::array< uint8_t, 5 > buf1{};
    std::array< uint8_t, 5 > buf2{};
    iomanager.run_on_wait(reactor, [&]() {
        // Both park on the same socket before either can complete (nothing is ever written) -- the
        // second async_recv's await_suspend finds parked_recv already occupied.
        [](socket_handle& s, std::span< uint8_t > b) -> sisl::async::light_task< void > {
            co_await async_recv(s, b);
        }(sock, std::span< uint8_t >(buf1))
                                                            .detach();
        [](socket_handle& s, std::span< uint8_t > b) -> sisl::async::light_task< void > {
            co_await async_recv(s, b);
        }(sock, std::span< uint8_t >(buf2))
                                                            .detach();
    });
    ::close(fd1);
}
} // namespace

// Two recv loops racing the same socket_handle concurrently must abort, not corrupt the parked-pointer
// slot. "threadsafe" death-test style is required since this process is multi-threaded (worker reactors).
// Matched on SIGABRT rather than the log message: sisl's logging sink isn't captured by gtest's
// death-test runner, so text matching isn't reliable here.
TEST(NetOpDeathTest, SecondConcurrentRecvOnSameSocketAborts) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(trigger_second_concurrent_recv(), ::testing::KilledBySignal(SIGABRT), "");
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging, iomgr, config);
    sisl::logging::SetLogger("test_net_op");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
    return RUN_ALL_TESTS();
}

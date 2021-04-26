#include <gtest/gtest.h>
#include <vector>
#include <chrono>
#include <mutex>

#include <iomgr.hpp>
#include <sds_logging/logging.h>
#include <sds_options/options.h>
#include <utility/thread_factory.hpp>
#include <fds/utils.hpp>

using namespace iomgr;
using namespace std::chrono_literals;

THREAD_BUFFER_INIT;
SDS_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SDS_OPTION_GROUP(test_msg,
                 (io_threads, "", "io_threads", "io_threads - default 2 for spdk and 8 for non-spdk",
                  ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
                 (client_threads, "", "client_threads", "client_threads",
                  ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                 (iters, "", "iters", "iters", ::cxxopts::value< uint64_t >()->default_value("10000"), "number"),
                 (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"))

#define ENABLED_OPTIONS logging, iomgr, test_msg
SDS_OPTIONS_ENABLE(ENABLED_OPTIONS)

struct timer_test_info {
    std::mutex mtx;
    timer_handle_t hdl;
    uint64_t nanos_after;
    Clock::time_point start_timer_time;
    int64_t pending_count;
    int64_t timer_call_count{0};

    timer_test_info(const uint64_t t, const uint32_t num_iters) : nanos_after{t}, pending_count{num_iters} {}
};

static uint32_t g_io_threads{0};
static uint32_t g_client_threads{0};
static bool g_is_spdk{false};
static uint64_t g_iters{0};
static std::vector< std::unique_ptr< timer_test_info > > g_timer_infos;

void glob_setup() {
    g_is_spdk = SDS_OPTIONS["spdk"].as< bool >();
    g_io_threads = SDS_OPTIONS["io_threads"].as< uint32_t >();
    if ((SDS_OPTIONS.count("io_threads") == 0) && g_is_spdk) { g_io_threads = 2; }
    g_client_threads = SDS_OPTIONS["client_threads"].as< uint32_t >();
    g_iters = SDS_OPTIONS["iters"].as< uint64_t >();

    iomanager.start(g_io_threads, g_is_spdk);
}

void glob_teardown() { iomanager.stop(); }

class MsgTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    void msg_sender_thread(const bool is_sync, const thread_specifier& to_threads, const run_method_t& receiver) {
        for (uint64_t i{0}; i < g_iters; ++i) {
            int count{0};
            if (std::holds_alternative< io_thread_t >(to_threads)) {
                count = iomanager.run_on(std::get< io_thread_t >(to_threads), receiver, is_sync);
            } else if (std::holds_alternative< thread_regex >(to_threads)) {
                count = iomanager.run_on(std::get< thread_regex >(to_threads), receiver, is_sync);
            }
            ASSERT_GT(count, 0) << "Expect messages to be sent to atleast 1 thread";
            m_sent_count.fetch_add(count);
        }
    }

    void sync_msg_test(const thread_specifier& to_threads, const run_method_t& receiver) {
        std::vector< std::thread > ts;
        for (uint32_t i{0}; i < g_client_threads; ++i) {
            ts.push_back(std::move(sisl::thread_factory("test_thread", &MsgTest::msg_sender_thread, this,
                                                        true /* is_sync */, to_threads, receiver)));
        }
        for (auto& t : ts) {
            t.join();
        }
        ASSERT_EQ(m_sent_count, m_rcvd_count) << "Missing messages";
    }

    void async_msg_test(const thread_specifier& to_threads, const run_method_t& receiver) {
        std::vector< std::thread > ts;
        for (uint32_t i{0}; i < g_client_threads; ++i) {
            ts.push_back(std::move(sisl::thread_factory("test_thread", &MsgTest::msg_sender_thread, this,
                                                        false /* is_sync */, to_threads, receiver)));
        }
        for (auto& t : ts) {
            t.join();
        }

        const auto max_wait_time{10000ms};
        const auto check_freq{100ms};
        auto waited_time{0ms};
        while (m_sent_count != m_rcvd_count) {
            ASSERT_LT(waited_time, max_wait_time)
                << max_wait_time.count()
                << " seconds have passed and messages are not delivered yet, sent_count=" << m_sent_count
                << " rcvd_count=" << m_rcvd_count;
            std::this_thread::sleep_for(check_freq);
            waited_time += check_freq;
        }
    }

    static const uint64_t early_tolerance_ns = 500 * 1000;
    static const uint64_t late_tolerance_ns = 3 * 1000 * 1000;

    static void validate_timeout(void* arg) {
        timer_test_info* ti = reinterpret_cast< timer_test_info* >(arg);
        std::lock_guard< std::mutex > lg(ti->mtx);
        auto elapsed_time_ns = get_elapsed_time_ns(ti->start_timer_time) / ++ti->timer_call_count;
        ASSERT_GE(elapsed_time_ns, ti->nanos_after - early_tolerance_ns) << "Received timeout earlier than expected";
        ASSERT_LT(elapsed_time_ns, ti->nanos_after + late_tolerance_ns) << "Received timeout much later than expected";
        if (--ti->pending_count == 0) { iomanager.cancel_timer(ti->hdl); }
    }

    void msg_with_timer_test(const bool is_sync, const thread_specifier& to_threads) {
        auto ti = std::make_unique< timer_test_info >(1 * 1000ul * 1000ul, g_iters);
        ti->start_timer_time = Clock::now();
        ti->hdl = iomanager.schedule_global_timer(ti->nanos_after, true, ti.get(), thread_regex::all_worker,
                                                  validate_timeout);

        auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
        is_sync ? sync_msg_test(to_threads, sink) : async_msg_test(to_threads, sink);

        {
            std::lock_guard< std::mutex > lg(ti->mtx);
            if (ti->pending_count > 0) { iomanager.cancel_timer(ti->hdl); }
        }

        g_timer_infos.push_back(std::move(ti));
    }

protected:
    std::atomic< int64_t > m_sent_count{0};
    std::atomic< int64_t > m_rcvd_count{0};
};

TEST_F(MsgTest, sync_broadcast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    sync_msg_test(thread_regex::all_io, sink);
}

TEST_F(MsgTest, async_broadcast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    async_msg_test(thread_regex::all_io, sink);
}

TEST_F(MsgTest, sync_randomcast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    sync_msg_test(thread_regex::random_worker, sink);
}

TEST_F(MsgTest, async_randomcast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    async_msg_test(thread_regex::random_worker, sink);
}

TEST_F(MsgTest, sync_multicast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    sync_msg_test(thread_regex::least_busy_io, sink);
}

TEST_F(MsgTest, async_multicast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    async_msg_test(thread_regex::least_busy_io, sink);
}

TEST_F(MsgTest, async_relay_broadcast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    auto relay = [this, sink]([[maybe_unused]] auto taddr) {
        ++this->m_rcvd_count;
        const auto count{iomanager.run_on(thread_regex::all_io, sink, false)};
        ASSERT_GT(count, 0) << "Expect messages to be sent to atleast 1 thread";
        m_sent_count.fetch_add(count);
    };
    async_msg_test(thread_regex::least_busy_io, relay); // Send it to one thread which broadcast to all io threads
}

TEST_F(MsgTest, async_relay_randomcast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    auto relay = [this, sink]([[maybe_unused]] auto taddr) {
        ++this->m_rcvd_count;
        const auto count{iomanager.run_on(thread_regex::random_worker, sink, false)};
        ASSERT_GT(count, 0) << "Expect messages to be sent to atleast 1 thread";
        m_sent_count.fetch_add(count);
    };
    async_msg_test(thread_regex::least_busy_io, relay); // Send it to one thread which broadcast to all io threads
}

TEST_F(MsgTest, async_relay_multicast_msg) {
    auto sink = [this]([[maybe_unused]] auto taddr) { ++this->m_rcvd_count; };
    auto relay = [this, sink]([[maybe_unused]] auto taddr) {
        ++this->m_rcvd_count;
        const auto count{iomanager.run_on(thread_regex::least_busy_io, sink, false)};
        ASSERT_GT(count, 0) << "Expect messages to be sent to atleast 1 thread";
        m_sent_count.fetch_add(count);
    };
    async_msg_test(thread_regex::least_busy_io, relay); // Send it to one thread which broadcast to all io threads
}

TEST_F(MsgTest, sync_broadcast_msg_with_timer) { msg_with_timer_test(true /* is_sync */, thread_regex::all_io); }

TEST_F(MsgTest, async_broadcast_msg_with_timer) { msg_with_timer_test(false /* is_sync */, thread_regex::all_io); }

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SDS_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sds_logging::SetLogger("msg_test");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    glob_setup();
    auto ret{RUN_ALL_TESTS()};
    glob_teardown();
    return ret;
}
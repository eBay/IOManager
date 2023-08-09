#include <gtest/gtest.h>
#include <vector>
#include <chrono>
#include <mutex>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/utility/thread_factory.hpp>
#include <sisl/fds/buffer.hpp>

#include <iomgr/io_environment.hpp>
#include <iomgr/iomgr.hpp>

using namespace iomgr;
using namespace std::chrono_literals;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SISL_OPTION_GROUP(test_msg,
                  (io_threads, "", "io_threads", "io_threads - default 2 for spdk and 8 for non-spdk",
                   ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
                  (client_threads, "", "client_threads", "client_threads",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                  (iters, "", "iters", "iters", ::cxxopts::value< uint64_t >()->default_value("10000"), "number"),
                  (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"))

#define ENABLED_OPTIONS logging, iomgr, test_msg, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

using run_method_t = std::function< void(void) >;

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
// static std::vector< std::unique_ptr< timer_test_info > > g_timer_infos;

void glob_setup() {
    g_is_spdk = SISL_OPTIONS["spdk"].as< bool >();
    g_io_threads = SISL_OPTIONS["io_threads"].as< uint32_t >();
    if ((SISL_OPTIONS.count("io_threads") == 0) && g_is_spdk) { g_io_threads = 2; }
    g_client_threads = SISL_OPTIONS["client_threads"].as< uint32_t >();
    g_iters = SISL_OPTIONS["iters"].as< uint64_t >();

    ioenvironment.with_iomgr(iomgr_params{.num_threads = g_client_threads, .is_spdk = g_is_spdk});
}

void glob_teardown() { iomanager.stop(); }

class MsgTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    void msg_sender_thread(bool is_wait, const thread_specifier& dest, const run_method_t& receiver) {
        static thread_local uint64_t this_thread_sent_count{0};
        for (uint64_t i{0}; i < g_iters; ++i) {
            int count{0};
            if (std::holds_alternative< io_fiber_t >(dest)) {
                count = iomanager.run_on(is_wait, std::get< io_fiber_t >(dest), receiver);
            } else if (std::holds_alternative< reactor_regex >(dest)) {
                count = iomanager.run_on(is_wait, std::get< reactor_regex >(dest), receiver);
            }
            // LOGINFO("Message sent iter={} total={}", i, m_sent_count.load() + count);
            this_thread_sent_count += count;
            ASSERT_GT(count, 0) << "Expect messages to be sent to atleast 1 thread";
            m_sent_count.fetch_add(count);
        }
        // LOGINFO("Sent {} messages from this thread", this_thread_sent_count);
    }

    void sync_msg_test(const thread_specifier& dest, const run_method_t& receiver) {
        std::vector< std::thread > ts;
        for (uint32_t i{0}; i < g_client_threads; ++i) {
            ts.push_back(std::move(
                sisl::thread_factory("test_thread", &MsgTest::msg_sender_thread, this, true, dest, receiver)));
        }
        for (auto& t : ts) {
            if (t.joinable()) t.join();
        }
        ASSERT_EQ(m_sent_count, m_rcvd_count) << "Missing messages";
    }

    void async_msg_test(const thread_specifier& dest, const run_method_t& receiver) {
        std::vector< std::thread > ts;
        for (uint32_t i{0}; i < g_client_threads; ++i) {
            ts.push_back(std::move(
                sisl::thread_factory("test_thread", &MsgTest::msg_sender_thread, this, false, dest, receiver)));
        }
        for (auto& t : ts) {
            if (t.joinable()) t.join();
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
    static const uint64_t late_tolerance_ns = 4 * 1000 * 1000;

    static void validate_timeout(void* arg) {
        timer_test_info* ti = reinterpret_cast< timer_test_info* >(arg);
        // std::lock_guard< std::mutex > lg(ti->mtx);
        auto elapsed_time_ns = get_elapsed_time_ns(ti->start_timer_time) / ++ti->timer_call_count;
        ASSERT_GE(elapsed_time_ns, ti->nanos_after - early_tolerance_ns) << "Received timeout earlier than expected";
        ASSERT_LT(elapsed_time_ns, ti->nanos_after + late_tolerance_ns) << "Received timeout much later than expected";
        // if (--ti->pending_count == 0) { iomanager.cancel_timer(ti->hdl); }
    }

    void msg_with_timer_test(bool is_wait, const thread_specifier& dest) {
        auto ti = std::make_unique< timer_test_info >(1 * 1000ul * 1000ul, g_iters);
        ti->start_timer_time = Clock::now();
        ti->hdl = iomanager.schedule_global_timer(ti->nanos_after, true, ti.get(), reactor_regex::all_worker,
                                                  validate_timeout, true /* wait */);

        auto sink = [this]() { ++this->m_rcvd_count; };
        if (is_wait) {
            sync_msg_test(dest, sink);
        } else {
            async_msg_test(dest, sink);
        }

        {
            // std::lock_guard< std::mutex > lg(ti->mtx);
            // if (ti->pending_count > 0) { iomanager.cancel_timer(ti->hdl); }
        }
        iomanager.cancel_timer(ti->hdl, true);
        // g_timer_infos.push_back(std::move(ti));
    }

protected:
    std::atomic< int64_t > m_sent_count{0};
    std::atomic< int64_t > m_rcvd_count{0};
};

/**************************Broadcast Msg ************************************/
TEST_F(MsgTest, sync_broadcast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    sync_msg_test(reactor_regex::all_io, sink);
}

TEST_F(MsgTest, async_broadcast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    async_msg_test(reactor_regex::all_io, sink);
}

/**************************Randomcast Msg ************************************/
TEST_F(MsgTest, sync_randomcast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    sync_msg_test(reactor_regex::random_worker, sink);
}

TEST_F(MsgTest, async_randomcast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    async_msg_test(reactor_regex::random_worker, sink);
}

/**************************Multicast Msg ************************************/
TEST_F(MsgTest, sync_multicast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    sync_msg_test(reactor_regex::least_busy_io, sink);
}

TEST_F(MsgTest, async_multicast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    async_msg_test(reactor_regex::least_busy_io, sink);
}

/**************************Relay Broadcast/Randomcast/Multicast Msg ************************/
TEST_F(MsgTest, async_relay_broadcast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    auto relay = [this, sink]() {
        ++this->m_rcvd_count;
        const auto count{iomanager.run_on_forget(reactor_regex::all_io, sink)};
        ASSERT_GT(count, 0) << "Expect messages to be sent to atleast 1 thread";
        m_sent_count.fetch_add(count);
    };
    async_msg_test(reactor_regex::least_busy_io, relay); // Send it to one thread which broadcast to all io threads
}

TEST_F(MsgTest, async_relay_randomcast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    auto relay = [this, sink]() {
        ++this->m_rcvd_count;
        const auto count{iomanager.run_on_forget(reactor_regex::random_worker, sink)};
        ASSERT_GT(count, 0) << "Expect messages to be sent to atleast 1 thread";
        m_sent_count.fetch_add(count);
    };
    async_msg_test(reactor_regex::least_busy_io, relay); // Send it to one thread which broadcast to all io threads
}

TEST_F(MsgTest, async_relay_multicast_msg) {
    auto sink = [this]() { ++this->m_rcvd_count; };
    auto relay = [this, sink]() {
        ++this->m_rcvd_count;
        const auto count{iomanager.run_on_forget(reactor_regex::least_busy_io, sink)};
        ASSERT_GT(count, 0) << "Expect messages to be sent to atleast 1 thread";
        m_sent_count.fetch_add(count);
    };
    async_msg_test(reactor_regex::least_busy_io, relay); // Send it to one thread which broadcast to all io threads
}

/**************************Messages with timer ************************/
TEST_F(MsgTest, sync_broadcast_msg_with_timer) { msg_with_timer_test(true, reactor_regex::all_io); }
TEST_F(MsgTest, async_broadcast_msg_with_timer) { msg_with_timer_test(false, reactor_regex::all_io); }

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger("msg_test");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    glob_setup();
    auto ret = RUN_ALL_TESTS();
    glob_teardown();
    return ret;
}

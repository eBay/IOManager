#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <sys/resource.h>
#include <thread>
#include <vector>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/utility/thread_factory.hpp>
#include <gtest/gtest.h>

#include <iomgr/iomgr.hpp>
#include <iomgr/io_environment.hpp>
#include <iomgr/timer.hpp>

using namespace iomgr;
using namespace std::chrono_literals;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SISL_OPTION_GROUP(
    test_timer,
    (io_threads, "", "io_threads", "io_threads", ::cxxopts::value< uint32_t >()->default_value("4"), "number"),
    (user_threads, "", "user_threads", "user_threads", ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
    (num_timers, "", "num_timers", "num_timers", ::cxxopts::value< uint64_t >()->default_value("1000"), "number"),
    (time_check, "Need timeout time check?", "time_check", "time_check",
     ::cxxopts::value< bool >()->default_value("false"), "true or false"),
    (iters, "", "iters", "iters", ::cxxopts::value< uint64_t >()->default_value("100"), "number"))
#define ENABLED_OPTIONS logging, iomgr, test_timer, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

struct timer_test_info {
    static std::atomic< uint64_t > s_unique_id_gen;

    std::mutex timer_mtx;
    uint64_t nanos_after;
    Clock::time_point start_timer_time;
    int64_t pending_count;
    int64_t timer_call_count{0};
    thread_specifier scope;
    bool is_active{true};
    bool is_auto_recurring;
    timer_handle_t hdl;
    uint64_t unique_id;

    timer_test_info(const uint64_t t, const uint32_t num_iters, const bool auto_recurring) :
            nanos_after{t}, pending_count{num_iters}, is_auto_recurring{auto_recurring} {
        unique_id = ++s_unique_id_gen;
    }

    bool is_thread_local_timer() const { return std::holds_alternative< IOReactor* >(scope); }
};

std::atomic< uint64_t > timer_test_info::s_unique_id_gen{0};

static uint32_t g_io_threads{0};
static uint32_t g_user_threads{0};
static uint64_t g_num_timers{0};
static uint64_t g_iters{0};
static bool g_need_time_check{false};
static std::vector< timer_handle_t > g_thdls;

void glob_setup() {
    // Each recurring timer holds its own timerfd; raise the soft limit so 1000 concurrent
    // timers don't exhaust the default ulimit -n (1024) on Linux CI runners.
    struct rlimit rl;
    getrlimit(RLIMIT_NOFILE, &rl);
    rl.rlim_cur = std::min(rl.rlim_max, static_cast< rlim_t >(65536));
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
        LOGWARN("Failed to raise RLIMIT_NOFILE to {}: {}", rl.rlim_cur, strerror(errno));
    }

    g_io_threads = SISL_OPTIONS["io_threads"].as< uint32_t >();
    g_user_threads = SISL_OPTIONS["user_threads"].as< uint32_t >();
    g_num_timers = SISL_OPTIONS["num_timers"].as< uint64_t >();
    g_iters = SISL_OPTIONS["num_timers"].as< uint64_t >();
    g_need_time_check = SISL_OPTIONS["time_check"].as< bool >();

    ioenvironment.with_iomgr(iomgr_params{.num_threads = g_io_threads});
}

void glob_teardown() { iomanager.stop(); }

class TimerTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    static constexpr uint64_t early_tolerance_ns{500 * 1000};
    static constexpr uint64_t late_tolerance_ns{20 * 1000 * 1000};

    void validate_timeout(void* arg) {
        timer_test_info* ti = reinterpret_cast< timer_test_info* >(arg);
        // ASSERT_EQ(ti->is_active, true) << "Timer armed after it is cancelled";

        if (g_need_time_check) {
            // Enabling time check if a little tricky to run on all types of environments. Hence making it
            // as an option. Enable it only on a targetted system and not by default.
            const auto elapsed_time_ns{get_elapsed_time_ns(ti->start_timer_time) / ++ti->timer_call_count};
            ASSERT_GE(elapsed_time_ns, ti->nanos_after - early_tolerance_ns)
                << "Received timeout earlier than expected";
            ASSERT_LT(elapsed_time_ns, ti->nanos_after + late_tolerance_ns)
                << "Received timeout much later than expected";
        }

        if (--ti->pending_count == 0) {
            finish_timer(ti);
        } else if (!ti->is_auto_recurring) {
            resume_timer(ti);
        } else {
            LOGDEBUG("recurring timer_id={} completed an iteration, still {} to go", ti->unique_id, ti->pending_count);
        }
    }

    void resume_timer(timer_test_info* ti) {
        LOGDEBUG("Resuming timer_id={} for next iteration, still {} to go", ti->unique_id, ti->pending_count);
        if (ti->is_thread_local_timer()) {
            ti->hdl = iomanager.schedule_thread_timer(ti->nanos_after, false /* auto recurring */, ti,
                                                      bind_this(TimerTest::validate_timeout, 1));
        } else {
            ti->hdl = iomanager.schedule_global_timer(ti->nanos_after, false /* auto recurring */, ti,
                                                      std::get< reactor_regex >(ti->scope),
                                                      bind_this(TimerTest::validate_timeout, 1), true /* wait */);
        }
    }

    static std::string timer_scope_string(const thread_specifier scope) {
        if (std::holds_alternative< IOReactor* >(scope)) { return "local"; }
        if (std::get< reactor_regex >(scope) == reactor_regex::all_worker) { return "all_worker"; }
        return "all_user";
    }

    void create_timer(const uint64_t nanos_after, const thread_specifier scope, const bool recurring) {
        auto ti = std::make_unique< timer_test_info >(nanos_after, g_iters, recurring);
        LOGDEBUG("Creating timer_id={} {} {} timer for {} ns for {} iterations", ti->unique_id,
                 timer_scope_string(scope), (recurring ? "recurring" : "one_time"), nanos_after, g_iters);
        ti->start_timer_time = Clock::now();
        if (std::holds_alternative< IOReactor* >(scope)) {
            ti->scope = iomanager.this_reactor();
            ti->hdl = iomanager.schedule_thread_timer(nanos_after, recurring, ti.get(),
                                                      bind_this(TimerTest::validate_timeout, 1));
        } else {
            ti->scope = scope;
            ti->hdl =
                iomanager.schedule_global_timer(nanos_after, recurring, ti.get(), std::get< reactor_regex >(scope),
                                                bind_this(TimerTest::validate_timeout, 1), true /* wait */);
        }

        {
            std::unique_lock< std::mutex > lk{m_list_mtx};
            m_timer_infos.emplace_back(std::move(ti));
            ++m_pending_timers;
        }
    }

    void create_random_timers(const thread_specifier scope, const bool recurring) {
        std::random_device rd{};
        std::default_random_engine engine{rd()};
        std::uniform_int_distribution< uint64_t > rand_freq_ns{500 * 1000, 5 * 1000 * 1000};

        for (uint64_t i{0}; i < g_num_timers; ++i) {
            create_timer(rand_freq_ns(engine), scope, recurring);
        }
    }

    void finish_timer(timer_test_info* ti) {
        iomanager.cancel_timer(ti->hdl, false /* wait_to_cancel */);
        ti->is_active = false;
        bool notify{false};
        {
            std::unique_lock< std::mutex > lk{m_list_mtx};
            notify = (--m_pending_timers == 0);
            LOGDEBUG("Finishing timer_id={} still {} more timers to finish", ti->unique_id, m_pending_timers);
        }

        if (notify) { m_cv.notify_one(); }
    }

    void wait_for_all_timers() {
        std::unique_lock< std::mutex > lk{m_list_mtx};
        m_cv.wait(lk, [this] { return (m_pending_timers == 0); });
    }

protected:
    std::mutex m_list_mtx;
    std::condition_variable m_cv;
    int64_t m_pending_timers{0};
    std::vector< std::unique_ptr< timer_test_info > > m_timer_infos;
};

/**************************Broadcast Msg ************************************/
TEST_F(TimerTest, global_recurring_timer) {
    create_random_timers(reactor_regex::all_worker, true /* recurring */);
    wait_for_all_timers();
}

// Exercises the RAII timer API: a one-shot fires once and self-removes, a recurring timer stops
// when its token is dropped, and moving a token transfers ownership without double-cancelling.
TEST_F(TimerTest, raii_token_lifecycle) {
    std::atomic< int > oneshot_count{0};
    std::atomic< int > recurring_count{0};

    // (1) Fire-and-forget one-shot fires exactly once, then self-removes (nothing to cancel).
    iomgr::schedule_oneshot(
        2ms, reactor_regex::all_worker, [&oneshot_count]() { ++oneshot_count; }, true /* wait_to_schedule */);
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(oneshot_count.load(), 1);

    // (2) A recurring timer stops firing once its token is destroyed.
    {
        auto tok = iomgr::schedule_recurring(
            1ms, reactor_regex::all_worker, [&recurring_count]() { ++recurring_count; }, true /* wait_to_schedule */);
        ASSERT_TRUE(tok.active());
        while (recurring_count.load() < 3) {
            std::this_thread::sleep_for(1ms);
        }
    } // <-- token destructor cancels the recurring timer
    std::this_thread::sleep_for(50ms); // let any in-flight callbacks drain
    const int settled = recurring_count.load();
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(recurring_count.load(), settled) << "recurring timer kept firing after its token was dropped";

    // (3) Move transfers ownership; the moved-from token is inactive and won't double-cancel.
    auto a = iomgr::schedule_recurring(50ms, reactor_regex::all_worker, []() {}, true /* wait_to_schedule */);
    ASSERT_TRUE(a.active());
    auto b = std::move(a);
    EXPECT_FALSE(a.active());
    EXPECT_TRUE(b.active());
    b.cancel(true /* wait */);
    EXPECT_FALSE(b.active());
    b.cancel(); // idempotent
    EXPECT_FALSE(b.active());
}

// Tests for 2-param void(void*, uint64_t) callbacks that receive exp_count directly.
TEST_F(TimerTest, thread_timer_2param_cb) {
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic< uint64_t > total_exp_count{0};
    std::atomic< int > call_count{0};
    constexpr int target_calls{5};

    // Schedule, run, and cancel all on the same reactor to avoid cross-thread cancel issues.
    timer_handle_t hdl;
    IOReactor* timer_reactor{nullptr};
    iomanager.run_on(true /* wait */, reactor_regex::random_worker, [&]() {
        timer_reactor = iomanager.this_reactor();
        hdl = iomanager.schedule_thread_timer(5 * 1000 * 1000 /* 5ms */, true /* recurring */, nullptr,
                                              [&](void*, uint64_t exp_count) {
                                                  EXPECT_GE(exp_count, 1u);
                                                  total_exp_count.fetch_add(exp_count, std::memory_order_relaxed);
                                                  if (++call_count >= target_calls) { cv.notify_one(); }
                                              });
    });

    {
        std::unique_lock< std::mutex > lk{mtx};
        cv.wait_for(lk, 2s, [&] { return call_count.load() >= target_calls; });
    }
    EXPECT_GE(call_count.load(), target_calls);
    EXPECT_GE(total_exp_count.load(), static_cast< uint64_t >(target_calls));

    // Cancel on the owning reactor.
    iomanager.run_on(true /* wait */, timer_reactor, [&]() { iomanager.cancel_timer(hdl, false); });
}

TEST_F(TimerTest, global_timer_2param_cb) {
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic< uint64_t > total_exp_count{0};
    std::atomic< int > call_count{0};
    constexpr int target_calls{5};

    auto hdl = iomanager.schedule_global_timer(
        5 * 1000 * 1000 /* 5ms */, true /* recurring */, nullptr, reactor_regex::all_worker,
        [&](void*, uint64_t exp_count) {
            EXPECT_GE(exp_count, 1u);
            total_exp_count.fetch_add(exp_count, std::memory_order_relaxed);
            if (++call_count >= target_calls) { cv.notify_one(); }
        },
        true /* wait */);

    {
        std::unique_lock< std::mutex > lk{mtx};
        cv.wait_for(lk, 2s, [&] { return call_count.load() >= target_calls; });
    }
    EXPECT_GE(call_count.load(), target_calls);
    EXPECT_GE(total_exp_count.load(), static_cast< uint64_t >(target_calls));
    iomanager.cancel_timer(hdl, true /* wait */);
}

/* NOTE: Make sure this is the last test case, so that iomanager stop is running in parallel to timer test */
TEST_F(TimerTest, timer_parallel_to_shutdown) {
    std::random_device rd{};
    std::default_random_engine engine{rd()};
    std::uniform_int_distribution< uint64_t > rand_freq_ns{500 * 1000, 5 * 1000 * 1000};

    // NOTE: The timer handles must be global(file scope) otherwise when this function exits they will be destroyed
    // before the cancel timers can complete causing a segmentation fault
    for (uint64_t i{0}; i < g_num_timers; ++i) {
        g_thdls.emplace_back(iomanager.schedule_global_timer(
            rand_freq_ns(engine), true /* recurring */, nullptr, reactor_regex::all_worker, [](void*) {},
            true /* wait */));
    }
    for (auto& thdl : g_thdls) {
        iomanager.cancel_timer(thdl, false /* wait_to_cancel */);
    }
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger("timer_test");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    glob_setup();
    auto ret{RUN_ALL_TESTS()};
    glob_teardown();
    return ret;
}

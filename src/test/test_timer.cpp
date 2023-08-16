#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/utility/thread_factory.hpp>

#include <iomgr/iomgr.hpp>
#include <iomgr/io_environment.hpp>

using namespace iomgr;
using namespace std::chrono_literals;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SISL_OPTION_GROUP(test_timer,
                  (io_threads, "", "io_threads", "io_threads - default 2 for spdk and 8 for non-spdk",
                   ::cxxopts::value< uint32_t >()->default_value("4"), "number"),
                  (user_threads, "", "user_threads", "user_threads", ::cxxopts::value< uint32_t >()->default_value("2"),
                   "number"),
                  (num_timers, "", "num_timers", "num_timers", ::cxxopts::value< uint64_t >()->default_value("100"),
                   "number"),
                  (time_check, "Need timeout time check?", "time_check", "time_check",
                   ::cxxopts::value< bool >()->default_value("false"), "true or false"),
                  (iters, "", "iters", "iters", ::cxxopts::value< uint64_t >()->default_value("100"), "number"),
                  (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"))

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

    bool is_global() const { return std::holds_alternative< io_fiber_t >(scope); }
};

std::atomic< uint64_t > timer_test_info::s_unique_id_gen{0};

static uint32_t g_io_threads{0};
static uint32_t g_user_threads{0};
static bool g_is_spdk{false};
static uint64_t g_num_timers{0};
static uint64_t g_iters{0};
static bool g_need_time_check{false};
static std::vector< timer_handle_t > g_thdls;

void glob_setup() {
    g_is_spdk = SISL_OPTIONS["spdk"].as< bool >();
    g_io_threads = SISL_OPTIONS["io_threads"].as< uint32_t >();
    if ((SISL_OPTIONS.count("io_threads") == 0) && g_is_spdk) { g_io_threads = 2; }
    g_user_threads = SISL_OPTIONS["user_threads"].as< uint32_t >();
    g_num_timers = SISL_OPTIONS["num_timers"].as< uint64_t >();
    g_iters = SISL_OPTIONS["num_timers"].as< uint64_t >();
    g_need_time_check = SISL_OPTIONS["time_check"].as< bool >();

    ioenvironment.with_iomgr(iomgr_params{.num_threads = g_io_threads, .is_spdk = g_is_spdk});
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
        if (std::holds_alternative< io_fiber_t >(ti->scope)) {
            ti->hdl = iomanager.schedule_thread_timer(ti->nanos_after, false /* auto recurring */, ti,
                                                      bind_this(TimerTest::validate_timeout, 1));
        } else {
            ti->hdl = iomanager.schedule_global_timer(ti->nanos_after, false /* auto recurring */, ti,
                                                      std::get< reactor_regex >(ti->scope),
                                                      bind_this(TimerTest::validate_timeout, 1), true /* wait */);
        }
    }

    static std::string timer_scope_string(const thread_specifier scope) {
        if (std::holds_alternative< io_fiber_t >(scope)) { return "local"; }
        if (std::get< reactor_regex >(scope) == reactor_regex::all_worker) { return "all_worker"; }
        return "all_user";
    }

    void create_timer(const uint64_t nanos_after, const thread_specifier scope, const bool recurring) {
        auto ti = std::make_unique< timer_test_info >(nanos_after, g_iters, recurring);
        LOGDEBUG("Creating timer_id={} {} {} timer for {} ns for {} iterations", ti->unique_id,
                 timer_scope_string(scope), (recurring ? "recurring" : "one_time"), nanos_after, g_iters);
        ti->start_timer_time = Clock::now();
        if (std::holds_alternative< io_fiber_t >(scope)) {
            ti->scope = iomanager.iofiber_self();
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

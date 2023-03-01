#pragma once

#include <filesystem>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <sisl/logging/logging.h>

#include <sisl/fds/atomic_status_counter.hpp>
#include <atomic>
#include <sisl/utility/enum.hpp>
#include <iomgr/iomgr.hpp>
#include "io_examiner.hpp"

namespace iomgr {

/**************** Common class created for all tests ***************/
struct JobCfg {
    JobCfg() = default;
    JobCfg(const JobCfg&) = default;
    JobCfg(JobCfg&&) noexcept = delete;
    JobCfg& operator=(const JobCfg&) = default;
    JobCfg& operator=(JobCfg&&) noexcept = delete;

    uint64_t run_time{60};
    uint64_t interrupt_ops_sec{0};
};

ENUM(job_status_t, uint8_t, not_started, running, stopped, completed);
ENUM(wait_till_t, uint8_t, none, execution, completion);

class Job {
public:
    Job(const std::shared_ptr< IOExaminer >& examiner, const JobCfg& cfg) :
            m_examiner{examiner}, m_cfg{cfg}, m_start_time(Clock::now()) {
        if (cfg.interrupt_ops_sec > 0) {
            iomanager.schedule_global_timer(cfg.interrupt_ops_sec * 1000ul * 1000ul * 1000ul, true, nullptr,
                                            iomgr::thread_regex::all_worker,
                                            [this](void* cookie) { try_run_one_iteration(); });
        }
    }
    virtual ~Job() = default;
    Job(const Job&) = delete;
    Job(Job&&) noexcept = delete;
    Job& operator=(const Job&) = delete;
    Job& operator=(Job&&) noexcept = delete;

    virtual void run_one_iteration() = 0;
    virtual bool time_to_stop() const = 0;
    virtual bool is_job_done() const = 0;
    virtual bool is_async_job() const = 0;
    virtual std::string job_name() const = 0;

    void start_job(wait_till_t wait_till = wait_till_t::completion) {
        iomanager.run_on(iomgr::thread_regex::all_worker,
                         [this](iomgr::io_thread_addr_t a) { start_in_this_thread(); });
        if (wait_till == wait_till_t::execution) {
            wait_for_execution();
        } else if (wait_till == wait_till_t::completion) {
            wait_for_completion();
        }
    }

    virtual void start_in_this_thread() {
        m_status_threads_executing.set_status(job_status_t::running);

        try_run_one_iteration();
        if (time_to_stop()) { notify_completions(); }
        // TODO: Replace with iomanager submit batch
        // VolInterface::get_instance()->submit_io_batch();
    }

    virtual void try_run_one_iteration() {
        if (!time_to_stop() && m_status_threads_executing.increment_if_status(job_status_t::running)) {
            run_one_iteration();
            m_status_threads_executing.decrement_testz_and_test_status(job_status_t::stopped);
        }
        if (time_to_stop()) { notify_completions(); }
    }

    void notify_completions() {
        std::unique_lock< std::mutex > lk(m_mutex);
        LOGDEBUG("notifying completions");
        if (is_job_done()) {
            m_status_threads_executing.set_status(job_status_t::completed);
            m_notify_job_done = true;
        } else {
            m_status_threads_executing.set_status(job_status_t::stopped);
        }

        m_execution_cv.notify_all();
        if (m_notify_job_done) { m_completion_cv.notify_all(); }
    }

    virtual void wait_for_execution() {
        std::unique_lock< std::mutex > lk(m_mutex);
        if (!m_notify_job_done) {
            m_execution_cv.wait(lk, [this] {
                auto status = m_status_threads_executing.get_status();
                LOGINFO("status {}", status);
                return (((status == job_status_t::stopped) || (status == job_status_t::completed)) &&
                        (m_status_threads_executing.count() == 0));
            });
        }
        LOGINFO("Job {} is done executing", job_name());
    }

    virtual void wait_for_completion() {
        std::unique_lock< std::mutex > lk(m_mutex);
        if (!m_notify_job_done) {
            m_completion_cv.wait(lk, [this] {
                return ((m_status_threads_executing.get_status() == job_status_t::completed) &&
                        (m_status_threads_executing.count() == 0));
            });
        }
        LOGINFO("Job {} is completed", job_name());
    }

protected:
    std::shared_ptr< IOExaminer > m_examiner;
    JobCfg m_cfg;
    mutable std::mutex m_mutex;
    std::condition_variable m_execution_cv;
    std::condition_variable m_completion_cv;

    bool m_notify_job_done{false};
    sisl::atomic_status_counter< job_status_t, job_status_t::not_started > m_status_threads_executing;

    Clock::time_point m_start_time;
};
} // namespace iomgr

#include <iomgr/iomgr.hpp>

#include "watchdog.hpp"
#include "iomgr_config.hpp"

namespace iomgr {
IOWatchDog::IOWatchDog() {
    m_wd_on = IM_DYNAMIC_CONFIG(drive.io_watchdog_timer_on);
    if (m_wd_on) {
        m_timer_hdl = iomanager.schedule_global_timer(
            IM_DYNAMIC_CONFIG(drive.io_watchdog_timer_sec) * 1000ul * 1000ul * 1000ul, true, nullptr,
            iomgr::reactor_regex::all_worker, [this](void* cookie) { io_timer(); });
        LOGINFOMOD(io_wd, "IO watchdown turned ON. WARNING: IO Performance could be impacted");
    } else {
        LOGINFOMOD(io_wd, "IO watchdog turned OFF");
    }
}

IOWatchDog::~IOWatchDog() { m_outstanding_ios.clear(); }

void IOWatchDog::add_io(drive_iocb* iocb) {
    {
        std::unique_lock< std::mutex > lk(m_mtx);
        iocb->unique_id = ++m_unique_id;

        const auto result = m_outstanding_ios.insert_or_assign(iocb->unique_id, iocb);
        LOGTRACEMOD(io_wd, "add_io: {}, {}", iocb->unique_id, iocb->to_string());
        RELEASE_ASSERT_EQ(result.second, true, "expecting to insert instead of update");
    }
}

void IOWatchDog::complete_io(drive_iocb* iocb) {
    {
        std::unique_lock< std::mutex > lk(m_mtx);
        const auto result = m_outstanding_ios.erase(iocb->unique_id);
        LOGTRACEMOD(io_wd, "complete_io: {}, {}", iocb->unique_id, iocb->to_string());
        RELEASE_ASSERT_EQ(result, 1, "expecting to erase 1 element");
    }
}

bool IOWatchDog::is_on() const { return m_wd_on; }

void IOWatchDog::io_timer() {
    {
        std::unique_lock< std::mutex > lk(m_mtx);
        std::vector< drive_iocb* > timeout_reqs;
        // the 1st io iteratred in map will be the oldeset one, because we add io
        // to map when vol_child_req is being created, e.g. op_start_time is from oldeset to latest;
        for (const auto& io : m_outstanding_ios) {
            const auto this_io_dur_us = get_elapsed_time_us(io.second->op_start_time);
            if (this_io_dur_us >= IM_DYNAMIC_CONFIG(drive.io_timeout_limit_sec) * 1000ul * 1000ul) {
                // coolect all timeout requests
                timeout_reqs.push_back(io.second);
            } else {
                // no need to search for newer requests stored in the map;
                break;
            }
        }

        if (timeout_reqs.size()) {
            LOGCRITICAL_AND_FLUSH(
                "Total num timeout requests: {}, the oldest io req that timeout duration is: {},  iocb: {}",
                timeout_reqs.size(), get_elapsed_time_us(timeout_reqs[0]->op_start_time), timeout_reqs[0]->to_string());

            RELEASE_ASSERT(false, "IO watchdog timeout! timeout_limit: {}, watchdog_timer: {}",
                           IM_DYNAMIC_CONFIG(drive.io_timeout_limit_sec),
                           IM_DYNAMIC_CONFIG(drive.io_watchdog_timer_sec));
        } else {
            LOGDEBUGMOD(io_wd, "io_timer passed {}, no timee out IO found. Total outstanding_io_cnt: {}",
                        ++m_wd_pass_cnt, m_outstanding_ios.size());
        }
    }
}
} // namespace iomgr

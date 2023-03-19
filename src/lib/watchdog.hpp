#pragma once

#include <map>
#include <mutex>

#include <iomgr/drive_interface.hpp>
#include <iomgr/iomgr_timer.hpp>

namespace iomgr {
class IOWatchDog {
public:
    IOWatchDog();
    ~IOWatchDog();

    void add_io(drive_iocb* iocb);
    void complete_io(drive_iocb* iocb);

    void io_timer();
    bool is_on() const;

    IOWatchDog(const IOWatchDog&) = delete;
    IOWatchDog(IOWatchDog&&) noexcept = delete;
    IOWatchDog& operator=(const IOWatchDog&) = delete;
    IOWatchDog& operator=(IOWatchDog&&) noexcept = delete;

private:
    bool m_wd_on{false};
    iomgr::timer_handle_t m_timer_hdl;
    std::mutex m_mtx;
    std::map< uint64_t, drive_iocb* > m_outstanding_ios;
    uint64_t m_wd_pass_cnt{0}; // total watchdog check passed count
    uint64_t m_unique_id{0};
};
} // namespace iomgr
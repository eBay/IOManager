/**
 * Copyright eBay Corporation 2018
 */

#pragma once

#include <sds_logging/logging.h>
#include "io_interface.hpp"
#include "iomgr_msg.hpp"
#include "iomgr_timer.hpp"
#include <metrics/metrics.hpp>
#include <chrono>

SDS_LOGGING_DECL(iomgr);

struct spdk_thread;
namespace iomgr {

typedef std::function< void(bool) > thread_state_notifier_t;
typedef std::variant< int, spdk_thread* > io_thread_id_t;

struct io_device_t;

class IOInterface;
class ioMgrThreadMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit ioMgrThreadMetrics(uint32_t thread_num) :
            sisl::MetricsGroupWrapper("ioMgrThreadMetrics", std::to_string(thread_num)) {
        LOGINFO("Registring metrics group name = ioMgrThreadMetrics, thread_num = {}, inst name = {}", thread_num,
                std::to_string(thread_num));

        REGISTER_GAUGE(iomgr_thread_io_count, "IO Manager per thread IO count");
        REGISTER_GAUGE(iomgr_thread_total_msg_recvd, "Total message received for this thread");
        REGISTER_GAUGE(iomgr_thread_rescheduled_in, "Count of times IOs rescheduled into this thread");
        REGISTER_GAUGE(iomgr_thread_rescheduled_out, "Count of times IOs rescheduled out of this thread");

        register_me_to_farm();

        attach_gather_cb(std::bind(&ioMgrThreadMetrics::on_gather, this));
    }

    ~ioMgrThreadMetrics() {
        detach_gather_cb();
        deregister_me_from_farm();
    }

    void on_gather() {
        GAUGE_UPDATE(*this, iomgr_thread_io_count, io_count);
        GAUGE_UPDATE(*this, iomgr_thread_total_msg_recvd, msg_recvd_count);
        GAUGE_UPDATE(*this, iomgr_thread_rescheduled_in, rescheduled_in);
        GAUGE_UPDATE(*this, iomgr_thread_rescheduled_out, rescheduled_out);
    }

    uint64_t io_count = 0;
    uint64_t msg_recvd_count = 0;
    uint64_t rescheduled_in = 0;
    uint64_t rescheduled_out = 0;
};

// typedef std::function< bool(std::shared_ptr< fd_info >) > fd_selector_t;
typedef std::function< bool(const io_device_ptr&) > iodev_selector_t;

class IOReactor {
    friend class IOManager;

public:
    virtual ~IOReactor();
    virtual void run(bool is_iomgr_created = false, const iodev_selector_t& iodev_selector = nullptr,
                     const thread_state_notifier_t& thread_state_notifier = nullptr);
    bool is_io_thread() const { return m_is_io_thread; };
    bool deliver_msg(iomgr_msg* msg);

    virtual bool is_tight_loop_thread() const = 0;
    virtual io_thread_id_t my_io_thread_id() const = 0;
    virtual void listen() = 0;
    int add_iodev_to_reactor(const io_device_ptr& iodev);
    int remove_iodev_from_reactor(const io_device_ptr& iodev);

    virtual bool put_msg(iomgr_msg* msg) = 0;
    virtual void init(bool wait_till_ready);
    virtual void stop();
    virtual bool is_iodev_addable(const io_device_ptr& iodev) const;
    virtual uint32_t get_num_iodevs() const { return m_n_iodevices; }
    virtual void handle_msg(iomgr_msg* msg);

protected:
    virtual bool iocontext_init() = 0;
    virtual void iocontext_exit() = 0;
    virtual int _add_iodev_to_reactor(const io_device_ptr& iodev) = 0;
    virtual int _remove_iodev_from_reactor(const io_device_ptr& iodev) = 0;

    void on_user_iodev_notification(io_device_t* iodev, int event);
    void notify_thread_state(bool is_started);

protected:
    int m_thread_num; // Thread num

protected:
    std::atomic< bool > m_is_io_thread = false;
    bool m_is_iomgr_thread = false; // Is this thread created by iomanager itself
    int64_t m_count = 0;            // Count of operations this thread is handling.
    bool m_keep_running = true;
    std::unique_ptr< ioMgrThreadMetrics > m_metrics;

    std::unique_ptr< timer > m_thread_timer;
    thread_state_notifier_t m_this_thread_notifier;

    iodev_selector_t m_iodev_selector = nullptr;
    uint32_t m_n_iodevices = 0;
};
} // namespace iomgr

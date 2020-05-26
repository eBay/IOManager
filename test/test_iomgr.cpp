/*
 * Copyright 2018 by eBay Corporation
 */
#include <aio_drive_interface.hpp>
#include <iomgr.hpp>
#include <sds_logging/logging.h>
#include <sds_options/options.h>

using log_level = spdlog::level::level_enum;

THREAD_BUFFER_INIT;

SDS_LOGGING_INIT(iomgr)
SDS_OPTIONS_ENABLE(logging)

using namespace iomgr;

// Constants
static constexpr size_t nthreads = 1;
static constexpr size_t total_dev_size = 512 * 1024 * 1024;
static constexpr size_t io_size = 4096;
static constexpr int read_pct = 50;
static constexpr size_t each_thread_size = total_dev_size / nthreads;
// static constexpr size_t max_ios_per_thread = 10000000;
static constexpr size_t max_ios_per_thread = 10000;

std::shared_ptr< AioDriveInterface > g_aio_iface;
io_device_ptr g_iodev = nullptr;

std::atomic< size_t > next_available_range = 0;

struct Runner {
    std::mutex cv_mutex;
    std::condition_variable comp_cv;
    int n_running_threads = nthreads;

    void wait() {
        std::unique_lock< std::mutex > lk(cv_mutex);
        comp_cv.wait(lk, [&] { return (n_running_threads == 0); });
    }

    void job_done() {
        {
            std::unique_lock< std::mutex > lk(cv_mutex);
            --n_running_threads;
        }
        comp_cv.notify_one();
    }
};

struct Workload {
    size_t nios_issued = 0;
    size_t nios_completed = 0;
    bool is_preload_phase = true;
    size_t offset_start = 0;
    size_t offset_end = 0;
    size_t next_io_offset = 0;
    int available_qs = 8;
};

static thread_local Workload work;
static Runner runner;

static void do_write_io(size_t offset) {
    std::array< size_t, io_size / sizeof(size_t) > wbuf;
    wbuf.fill(offset);

    // memset(wbuf, offset, io_size);
    g_aio_iface->async_write(g_iodev.get(), (const char*)wbuf.data(), io_size, offset, (uint8_t*)&work);
    // LOGINFO("Write on Offset {}", offset);
}

static void do_read_io(size_t offset) {
    std::array< size_t, io_size / sizeof(size_t) > rbuf;
    g_aio_iface->async_read(g_iodev.get(), (char*)rbuf.data(), io_size, offset, (uint8_t*)&work);
    // LOGINFO("Read on Offset {}", offset);
}

static void issue_preload() {
    static std::once_flag flag1;
    std::call_once(flag1, [&]() { g_iodev = g_aio_iface->open_dev("/tmp/f1", O_CREAT | O_RDWR); });

    if (work.next_io_offset >= work.offset_end) {
        work.is_preload_phase = false;
        LOGINFO("We are done with the preload");
        return;
    }

    while (work.available_qs > 0) {
        --work.available_qs;
        do_write_io(work.next_io_offset);
        work.next_io_offset += io_size;
    }
}

static void issue_rw_io() {
    while (work.available_qs > 0) {
        --work.available_qs;
        ++work.nios_issued;
        auto offset = work.offset_start + (rand() % each_thread_size % io_size) * io_size;
        ((rand() % 100) < read_pct) ? do_read_io(offset) : do_write_io(offset);
    }
}

static void do_verify() {
    LOGINFO("All IOs completed for this thread, running verification");
    std::array< size_t, io_size / sizeof(size_t) > rbuf;
    for (size_t offset = work.offset_start; offset < work.offset_end; offset += io_size) {
        g_aio_iface->sync_read(g_iodev.get(), (char*)rbuf.data(), io_size, offset);
        for (auto i = 0; i < rbuf.size(); ++i) {
            assert(rbuf[i] == offset);
        }
    }
    LOGINFO("Verification succesful for this thread");
    runner.job_done();
}

static void on_io_completion(int64_t res, uint8_t* cookie) {
    // LOGINFO("An IO is completed");
    assert(cookie == (uint8_t*)&work);
    ++work.available_qs;
    if (work.is_preload_phase) {
        issue_preload();
    } else {
        ++work.nios_completed;
        if ((work.nios_completed % 10000) == 0) { LOGINFO("Total {} ios completed", work.nios_completed); }
        if (work.nios_issued < max_ios_per_thread) {
            issue_rw_io();
        } else if (work.nios_completed == max_ios_per_thread) {
            do_verify();
        }
    }
}

static void init_workload() {
    work.offset_start = next_available_range.fetch_add(each_thread_size);
    work.offset_end = work.offset_start + each_thread_size;
    work.next_io_offset = work.offset_start;
}

static void on_timeout(void* cookie) {
    uint64_t timeout_id = (uint64_t)cookie;
    LOGINFO("Received timeout for id = {}", timeout_id);
}

static void on_io_thread_state_change(bool started) {
    static std::atomic< uint64_t > _id = 0;

    if (started) {
        // sleep(2); // Wait for a second for fd to be opened and added before starting IO
        LOGINFO("New thread created, start workload on that thread");
        auto hdl1 = iomanager.schedule_thread_timer(1000000, false, (void*)_id.fetch_add(1), on_timeout);
        auto hdl2 = iomanager.schedule_thread_timer(1000001, false, (void*)_id.fetch_add(1), on_timeout);
        iomanager.cancel_thread_timer(hdl2);
        auto hdl3 = iomanager.schedule_thread_timer(50000000, true, (void*)_id.fetch_add(1), on_timeout);
        init_workload();
        issue_preload();
    } else {
        LOGINFO("This thread is about to exit");
    }
}

int main(int argc, char* argv[]) {
    SDS_OPTIONS_LOAD(argc, argv, logging);
    sds_logging::SetLogger("example");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    // Start the IOManager
    iomanager.start(1, nthreads, false, on_io_thread_state_change);

    // Create and add AIO Drive Interface to the IOManager, Also open the device
    // NOTE: We do not need to add the fd to the device, since either Read or Write IO will always be triggered
    // by the application on AIO fd (unlike network or other fd, where we will be pinged asynchornously)
    g_aio_iface = std::make_shared< AioDriveInterface >(on_io_completion);
    iomanager.add_interface(std::dynamic_pointer_cast< IOInterface >(g_aio_iface));

    // Wait for IO to finish on all threads.
    runner.wait();

    LOGINFO("IOManagerMetrics: {}", sisl::MetricsFarm::getInstance().get_result_in_json().dump(4));

    // Stop the IOManage for clean exit
    iomanager.stop();

    return 0;
}

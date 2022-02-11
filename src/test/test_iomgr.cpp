/*
 * Copyright 2018 by eBay Corporation
 */
#include <iomgr.hpp>
#include <drive_interface.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/utility/thread_factory.hpp>

extern "C" {
#include <spdk/env.h>
#include <spdk/thread.h>
}
#include <stdexcept>
using log_level = spdlog::level::level_enum;

SISL_LOGGING_INIT(IOMGR_LOG_MODS, flip)

SISL_OPTION_GROUP(test_iomgr,
                  (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"))

#define ENABLED_OPTIONS logging, iomgr, test_iomgr, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

using namespace iomgr;

// Constants
static constexpr size_t nthreads = 2;
static constexpr size_t total_dev_size = 512 * 1024 * 1024;
static constexpr size_t io_size = 4096;
static constexpr int read_pct = 50;
static constexpr size_t each_thread_size = total_dev_size / nthreads;
// static constexpr size_t max_ios_per_thread = 10000000;
static constexpr size_t max_ios_per_thread = 10000;
static const std::string dev_path = "/tmp/f1";

static io_device_ptr g_iodev = nullptr;
static iomgr::drive_attributes g_driveattr;

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

struct io_req {
    bool is_write{false};
    uint8_t* buf;
    std::array< size_t, io_size / sizeof(size_t) >* buf_arr;

    io_req() {
        buf = iomanager.iobuf_alloc(g_driveattr.align_size, io_size);
        buf_arr = (std::array< size_t, io_size / sizeof(size_t) >*)buf;
    }

    ~io_req() { iomanager.iobuf_free(buf); }
};

static thread_local Workload work;
static Runner runner;

static void do_write_io(size_t offset) {
    auto req = new io_req();
    req->buf_arr->fill(offset);

    // memset(wbuf, offset, io_size);
    g_iodev->drive_interface()->async_write(g_iodev.get(), (const char*)req->buf, io_size, offset, (uint8_t*)req);
    // LOGINFO("Write on Offset {}", offset);
}

static void do_read_io(size_t offset) {
    auto req = new io_req();
    g_iodev->drive_interface()->async_read(g_iodev.get(), (char*)req->buf, io_size, offset, (uint8_t*)req);
    // LOGINFO("Read on Offset {}", offset);
}

static void issue_preload() {
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
    auto sthread = sisl::named_thread("verify_thread", []() mutable {
        auto loop_type = SISL_OPTIONS["spdk"].as< bool >() ? TIGHT_LOOP : INTERRUPT_LOOP;
        iomanager.run_io_loop(loop_type, nullptr, [](bool is_started) {
            if (is_started) {
                uint8_t* rbuf = iomanager.iobuf_alloc(g_driveattr.align_size, io_size);
                for (size_t offset = work.offset_start; offset < work.offset_end; offset += io_size) {
                    g_iodev->drive_interface()->sync_read(g_iodev.get(), (char*)rbuf, io_size, offset);
                    for (auto i = 0u; i < io_size; ++i) {
                        assert(rbuf[i] == offset);
                    }
                }
                iomanager.iobuf_free(rbuf);
                LOGINFO("Verification successful for this thread");
                runner.job_done();
            }
        });
    });
    sthread.detach();
}

static void on_io_completion(int64_t res, uint8_t* cookie) {
    // LOGINFO("An IO is completed");
    io_req* req = (io_req*)cookie;
    delete req;

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
    LOGDEBUG("Received timeout for id = {}", timeout_id);
}

static void workload_on_thread([[maybe_unused]] io_thread_addr_t taddr) {
    static std::atomic< uint64_t > _id = 0;

    LOGINFO("New thread created, start workload on that thread");
    auto hdl1 = iomanager.schedule_thread_timer(1000000, false, (void*)_id.fetch_add(1), on_timeout);
    auto hdl2 = iomanager.schedule_thread_timer(1000001, false, (void*)_id.fetch_add(1), on_timeout);
    iomanager.cancel_timer(hdl2);
    auto hdl3 = iomanager.schedule_thread_timer(50000000, true, (void*)_id.fetch_add(1), on_timeout);
    init_workload();
    issue_preload();
}

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger("test_iomgr");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");

    // Start the IOManager
    iomanager.start(nthreads, SISL_OPTIONS["spdk"].as< bool >());
    std::stringstream ss;
    ss << iomgr::get_version();
    LOGINFO("IOManager ver. {}", ss.str());

    bool created = false;
    if (!std::filesystem::exists(std::filesystem::path{dev_path})) {
        LOGINFO("Device {} doesn't exists, creating a file for size {}", dev_path, total_dev_size);
        auto fd = ::open(dev_path.c_str(), O_RDWR | O_CREAT, 0666);
        assert(fd > 0);

        [[maybe_unused]] const auto ret = fallocate(fd, 0, 0, total_dev_size);
        assert(ret == 0);

        created = true;
    }
    g_iodev = iomgr::DriveInterface::open_dev(dev_path, O_CREAT | O_RDWR);
    g_driveattr = iomgr::DriveInterface::get_attributes(dev_path);
    g_iodev->drive_interface()->attach_completion_cb(on_io_completion);

    uint8_t* buf = iomanager.iobuf_alloc(g_driveattr.align_size, 8192);
    LOGINFO("Allocated iobuf size = {}", iomanager.iobuf_size(buf));
    iomanager.iobuf_free(buf);

    void* mempool = iomanager.create_mempool(io_size, 32);
    RELEASE_ASSERT_NE(mempool, nullptr, "Mempool was not created successfully");
    LOGINFO("Allocated mempool size = {}", io_size);
    uint8_t* mempool_buf = iomanager.iobuf_pool_alloc(g_driveattr.align_size, io_size);
    iomanager.iobuf_pool_free(mempool_buf, io_size);

    iomanager.run_on(thread_regex::all_io, workload_on_thread);

    // Wait for IO to finish on all threads.
    runner.wait();

    LOGINFO("IOManagerMetrics: {}", sisl::MetricsFarm::getInstance().get_result_in_json().dump(4));

    g_iodev->drive_interface()->close_dev(g_iodev);

    // Stop the IOManage for clean exit
    iomanager.stop();

    if (created) {
        LOGINFO("Device {} was created by this test, deleting the file", dev_path);
        std::filesystem::remove(std::filesystem::path{dev_path});
    }
    return 0;
}

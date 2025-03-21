/*
 * Copyright 2018 by eBay Corporation
 */
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/utility/thread_factory.hpp>

#ifdef __linux__
#include <fcntl.h>
#endif

#include <iomgr/iomgr.hpp>
#include <iomgr/io_environment.hpp>
#include <iomgr/drive_interface.hpp>

using log_level = spdlog::level::level_enum;

SISL_OPTION_GROUP(test_drive_interface,
                  (num_threads, "", "num_threads", "number of threads",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                  (num_fibers, "", "num_fibers", "number of fibers per thread",
                   ::cxxopts::value< uint32_t >()->default_value("4"), "number"),
                  (num_ios, "", "num_ios", "number of io operations",
                   ::cxxopts::value< uint64_t >()->default_value("10000"), "number"),
                  (read_pct, "", "read_pct", "Percentage of reads in io operations",
                   ::cxxopts::value< int >()->default_value("50"), "number"),
                  (dev_path, "", "dev_path", "drive path to test",
                   ::cxxopts::value< std::string >()->default_value("/tmp/iomgr_test_drive"), "path"),
                  (dev_size_mb, "", "dev_size_mb", "size of each device in MB",
                   ::cxxopts::value< uint64_t >()->default_value("100"), "number"),
                  (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"));

#define ENABLED_OPTIONS logging, iomgr, test_drive_interface, config
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

using namespace iomgr;

struct Workload {
    std::atomic< size_t > nios_issued{0};
    std::atomic< size_t > nios_completed{0};
    size_t offset_start{0};
    size_t offset_end{0};
    std::atomic< size_t > next_io_offset{0};
    size_t max_ios{10000};
    std::atomic< int64_t > available_qs{8};
    folly::Promise< folly::Unit > preload_completion;
    folly::Promise< folly::Unit > rw_completion;

    Workload() = default;
    Workload(Workload&& other) {
        nios_issued.store(other.nios_issued.load());
        nios_completed.store(other.nios_completed.load());
        offset_start = other.offset_start;
        offset_end = other.offset_end;
        next_io_offset.store(other.next_io_offset.load());
        max_ios = other.max_ios;
        available_qs.store(other.available_qs.load());
        preload_completion = std::move(other.preload_completion);
        rw_completion = std::move(other.rw_completion);
    }

    void reuse_ready() {
        nios_issued = 0;
        nios_completed = 0;
    }
};

class DriveTest : public ::testing::Test {
public:
    static constexpr size_t s_io_size{4096};
    static iomgr::drive_attributes s_driveattr;
    struct io_req {
        bool is_write{false};
        uint8_t* buf;
        std::array< size_t, DriveTest::s_io_size / sizeof(size_t) >* buf_arr;

        io_req() {
            buf = iomanager.iobuf_alloc(s_driveattr.align_size, DriveTest::s_io_size);
            buf_arr = reinterpret_cast< decltype(buf_arr) >(buf);
        }

        ~io_req() { iomanager.iobuf_free(buf); }
    };

public:
    void SetUp() override {
        m_dev_path = SISL_OPTIONS["dev_path"].as< std::string >();
        auto const dev_size = SISL_OPTIONS["dev_size_mb"].as< uint64_t >() * 1024 * 1024;
        m_nthreads = SISL_OPTIONS["num_threads"].as< uint32_t >();
        auto nfibers = SISL_OPTIONS["num_fibers"].as< uint32_t >();
        auto is_spdk = SISL_OPTIONS["spdk"].as< bool >();

        const std::filesystem::path file_path{m_dev_path};
        if (!std::filesystem::exists(file_path)) {
            LOGINFO("Device {} doesn't exists, creating a file for size {}", m_dev_path, dev_size);
            const auto fd{::open(m_dev_path.c_str(), O_RDWR | O_CREAT, 0666)};
            assert(fd > 0);
            ::close(fd);

            std::filesystem::resize_file(file_path, dev_size);
            m_created = true;
        }
        if (is_spdk && m_nthreads > 2) {
            LOGINFO("Spdk with more than 2 threads will cause overburden test systems, changing nthreads to 2");
            m_nthreads = 2;
        }

        m_each_thread_size = (dev_size - 1) / m_nthreads + 1;
        LOGINFO("Starting iomgr with {} threads, spdk: {}", m_nthreads, is_spdk);
        ioenvironment.with_iomgr(iomgr_params{.num_threads = m_nthreads, .is_spdk = is_spdk, .num_fibers = nfibers});

        std::stringstream iomgr_ver;
        iomgr_ver << iomgr::get_version();
        LOGINFO("IOManager ver. {}", iomgr_ver.str());
        m_iodev = iomgr::DriveInterface::open_dev(m_dev_path, O_CREAT | O_RDWR);
        s_driveattr = iomgr::DriveInterface::get_attributes(m_dev_path);
    }

    void TearDown() override {
        m_iodev->drive_interface()->close_dev(m_iodev);

        // Stop the IOManage for clean exit
        iomanager.stop();

        if (m_created) {
            LOGINFO("Device {} was created by this test, deleting the file", m_dev_path);
            std::filesystem::remove(std::filesystem::path{m_dev_path});
        }
    }

    void issue_preload(Workload* work) {
        while (work->available_qs.load() > 0) {
            --work->available_qs;

            auto offset = work->next_io_offset.load();
            auto* req = new io_req();
            req->buf_arr->fill(offset);

            LOGTRACE("Preload offset={}", offset);
            m_iodev->drive_interface()
                ->async_write(m_iodev.get(), r_cast< const char* >(req->buf), s_io_size, offset)
                .thenValue([work, this, req](auto&&) {
                    ++work->available_qs;
                    ++work->nios_completed;
                    delete req;

                    if (work->next_io_offset.load() < work->offset_end) {
                        issue_preload(work);
                    } else if (work->nios_completed.load() == work->nios_issued.load()) {
                        LOGINFO("We are done with the preload of size={} with num_ios={}",
                                s_io_size * work->nios_completed.load(), work->nios_completed.load());
                        work->preload_completion.setValue();
                    }
                });

            work->next_io_offset += s_io_size;
            ++work->nios_issued;
        }
    }

    void issue_rw_io(Workload* work) {
        static thread_local std::random_device rd{};
        static thread_local std::default_random_engine re{rd()};
        // 1 needed in order that end does not exceed each_thread_size, i.e. max_thread_offset * io_size +
        // io_size<=each_thread_size;
        size_t max_thread_offset = m_each_thread_size / s_io_size - 1;
        static int s_read_pct = SISL_OPTIONS["read_pct"].as< int >();

        while (work->available_qs.load() > 0) {
            --work->available_qs;
            ++work->nios_issued;

            std::uniform_int_distribution< size_t > thread_offset{0, max_thread_offset};
            const size_t offset = work->offset_start + thread_offset(re) * s_io_size;
            std::uniform_int_distribution< uint8_t > io_pct{0, 99};

            auto* req = new io_req();
            folly::Future< std::error_code > f = folly::Future< std::error_code >::makeEmpty();
            if (io_pct(re) < s_read_pct) {
                LOGTRACE("Read offset={}", offset);
                f = m_iodev->drive_interface()->async_read(m_iodev.get(), r_cast< char* >(req->buf), s_io_size, offset);
            } else {
                req->buf_arr->fill(offset);
                LOGTRACE("Write offset={}", offset);
                f = m_iodev->drive_interface()->async_write(m_iodev.get(), r_cast< const char* >(req->buf), s_io_size,
                                                            offset);
            }

            std::move(f).thenValue([work, this, req](auto&&) mutable {
                ++work->available_qs;
                ++work->nios_completed;
                delete req;

                if (work->nios_issued.load() < work->max_ios) {
                    issue_rw_io(work);
                } else if (work->nios_completed.load() == work->nios_issued.load()) {
                    LOGINFO("IOs completed (total_excluding_preload={}) for this thread", work->nios_completed.load());
                    work->rw_completion.setValue();
                }
            });
        }
    }

    void do_verify(Workload* work) {
        LOGINFO("Running verification");

        uint8_t* rbuf = iomanager.iobuf_alloc(s_driveattr.align_size, s_io_size);
        for (size_t offset{work->offset_start}; offset < work->offset_end; offset += s_io_size) {
            LOGTRACE("Verify offset={}", offset);
            m_iodev->drive_interface()->sync_read(m_iodev.get(), r_cast< char* >(rbuf), s_io_size, offset);
            for (size_t i{0}; i < s_io_size / sizeof(size_t); ++i) {
                assert((r_cast< uint64_t* >(rbuf))[i] == offset);
            }
        }
        iomanager.iobuf_free(rbuf);
        LOGINFO("Verification successful for this thread");
    }

    void issue_preload_regular_thread(Workload* work) {
        std::mutex q_mtx; // Needed only for non-io reactor thread handling
        std::condition_variable q_cv;

        do {
            {
                std::unique_lock lg{q_mtx};
                q_cv.wait(lg, [&]() { return work->available_qs.load() > 0; });
            }

            auto offset = work->next_io_offset.load();
            if (offset < work->offset_end) {
                auto* req = new io_req();
                req->buf_arr->fill(offset);
                --work->available_qs;

                LOGTRACE("Preload offset={}", offset);
                m_iodev->drive_interface()
                    ->async_write(m_iodev.get(), r_cast< const char* >(req->buf), s_io_size, offset)
                    .thenValue([work, this, req, &q_cv](auto&&) {
                        ++work->available_qs;
                        ++work->nios_completed;
                        delete req;
                        q_cv.notify_one();
                    });
                work->next_io_offset += s_io_size;
                ++work->nios_issued;
            }
        } while ((work->next_io_offset.load() < work->offset_end) ||
                 (work->nios_completed.load() < work->nios_issued.load()));

        LOGINFO("We are done with the preload of size={} with num_ios={}", s_io_size * work->nios_completed.load(),
                work->nios_completed.load());
    }

    void issue_rwload_regular_thread(Workload* work) {
        static thread_local std::random_device rd{};
        static thread_local std::default_random_engine re{rd()};
        // 1 needed in order that end does not exceed each_thread_size, i.e. max_thread_offset * io_size +
        // io_size<=each_thread_size;
        size_t max_thread_offset = m_each_thread_size / s_io_size - 1;
        static int s_read_pct = SISL_OPTIONS["read_pct"].as< int >();

        std::mutex q_mtx; // Needed only for non-io reactor thread handling
        std::condition_variable q_cv;
        do {
            {
                std::unique_lock lg{q_mtx};
                q_cv.wait(lg, [&]() { return work->available_qs.load() > 0; });
            }

            if (work->nios_issued.load() < work->max_ios) {
                --work->available_qs;

                std::uniform_int_distribution< size_t > thread_offset{0, max_thread_offset};
                const size_t offset = work->offset_start + thread_offset(re) * s_io_size;
                std::uniform_int_distribution< uint8_t > io_pct{0, 99};

                auto* req = new io_req();
                folly::Future< std::error_code > f = folly::Future< std::error_code >::makeEmpty();
                if (io_pct(re) < s_read_pct) {
                    LOGTRACE("Read offset={}", offset);
                    f = m_iodev->drive_interface()->async_read(m_iodev.get(), r_cast< char* >(req->buf), s_io_size,
                                                               offset);
                } else {
                    req->buf_arr->fill(offset);
                    LOGTRACE("Write offset={}", offset);
                    f = m_iodev->drive_interface()->async_write(m_iodev.get(), r_cast< const char* >(req->buf),
                                                                s_io_size, offset);
                }

                std::move(f).thenValue([work, this, req, &q_cv](auto&&) mutable {
                    ++work->available_qs;
                    ++work->nios_completed;
                    delete req;
                    q_cv.notify_one();
                });
                ++work->nios_issued;
            }
        } while ((work->nios_issued.load() < work->max_ios) ||
                 (work->nios_completed.load() < work->nios_issued.load()));

        LOGINFO("IOs completed (total_excluding_preload={}) for this thread", work->nios_completed.load());
    }

    void io_on_worker_threads() {
        std::mutex mtx;
        std::vector< Workload > work_list;
        uint32_t next_pick{0};

        m_next_available_range.store(0);
        for (uint32_t i{0}; i < m_nthreads; ++i) {
            Workload work;
            work.offset_start = m_next_available_range.fetch_add(m_each_thread_size);
            work.offset_end = work.offset_start + m_each_thread_size;
            work.next_io_offset = work.offset_start;
            work.max_ios = SISL_OPTIONS["num_ios"].as< uint64_t >() / m_nthreads;
            work_list.emplace_back(std::move(work));
        }

        iomanager.run_on_wait(reactor_regex::all_worker, [this, &mtx, &next_pick, &work_list]() {
            Workload* my_work;
            {
                std::unique_lock lg(mtx);
                my_work = &work_list[next_pick++];
            }

            issue_preload(my_work);

            my_work->preload_completion.getFuture().thenValue([my_work, this](auto&&) {
                my_work->reuse_ready();
                issue_rw_io(my_work);
            });
        });

        // Wait for all thread rw completion
        for (uint32_t i{0}; i < m_nthreads; ++i) {
            work_list[i].rw_completion.getFuture().wait();
        }

        // We will do sync read to do the verification
        next_pick = 0;
        iomanager.run_on_wait(reactor_regex::all_worker, fiber_regex::syncio_only,
                              [this, &mtx, &next_pick, &work_list]() {
                                  Workload* my_work;
                                  {
                                      std::unique_lock lg(mtx);
                                      my_work = &work_list[next_pick++];
                                  }
                                  do_verify(my_work);
                              });
    }

    void io_on_user_threads() {
        std::mutex mtx;
        std::vector< Workload > work_list;
        uint32_t next_pick{0};

        m_next_available_range.store(0);
        for (uint32_t i{0}; i < m_nthreads; ++i) {
            Workload work;
            work.offset_start = m_next_available_range.fetch_add(m_each_thread_size);
            work.offset_end = work.offset_start + m_each_thread_size;
            work.next_io_offset = work.offset_start;
            work.max_ios = SISL_OPTIONS["num_ios"].as< uint64_t >() / m_nthreads;
            work_list.emplace_back(std::move(work));
        }

        for (uint32_t i{0}; i < m_nthreads; ++i) {
            iomanager.create_reactor("user" + std::to_string(i + 1),
                                     SISL_OPTIONS["spdk"].as< bool >() ? TIGHT_LOOP : INTERRUPT_LOOP,
                                     SISL_OPTIONS["num_fibers"].as< uint32_t >(), [&](bool is_started) {
                                         if (is_started) {
                                             Workload* my_work;
                                             {
                                                 std::unique_lock lg(mtx);
                                                 my_work = &work_list[next_pick++];
                                             }

                                             issue_preload(my_work);

                                             my_work->preload_completion.getFuture().thenValue([my_work, this](auto&&) {
                                                 my_work->reuse_ready();
                                                 issue_rw_io(my_work);
                                             });
                                         }
                                     });
        }

        // Wait for all thread rw completion
        for (uint32_t i{0}; i < m_nthreads; ++i) {
            work_list[i].rw_completion.getFuture().wait();
        }

        // We will do sync read to do the verification
        next_pick = 0;
        iomanager.run_on_wait(reactor_regex::all_user, fiber_regex::syncio_only,
                              [this, &mtx, &next_pick, &work_list]() {
                                  Workload* my_work;
                                  {
                                      std::unique_lock lg(mtx);
                                      my_work = &work_list[next_pick++];
                                  }
                                  do_verify(my_work);
                              });
    }

    void io_on_regular_threads() {
        std::mutex mtx;
        std::vector< Workload > work_list;
        uint32_t next_pick{0};

        m_next_available_range.store(0);
        for (uint32_t i{0}; i < m_nthreads; ++i) {
            Workload work;
            work.offset_start = m_next_available_range.fetch_add(m_each_thread_size);
            work.offset_end = work.offset_start + m_each_thread_size;
            work.next_io_offset = work.offset_start;
            work.max_ios = SISL_OPTIONS["num_ios"].as< uint64_t >() / m_nthreads;
            work_list.emplace_back(std::move(work));
        }

        std::vector< std::thread > threads;
        for (uint32_t i{0}; i < m_nthreads; ++i) {
            threads.emplace_back(std::thread([&]() {
                Workload* my_work;
                {
                    std::unique_lock lg(mtx);
                    my_work = &work_list[next_pick++];
                }

                issue_preload_regular_thread(my_work);
                my_work->reuse_ready();
                issue_rwload_regular_thread(my_work);
                do_verify(my_work);
            }));
        }

        // Wait for all thread rw completion
        for (uint32_t i{0}; i < m_nthreads; ++i) {
            threads[i].join();
        }
    }

protected:
    io_device_ptr m_iodev{nullptr};
    std::atomic< size_t > m_next_available_range{0};
    size_t m_each_thread_size{0};
    bool m_created{false};
    uint32_t m_nthreads;
    std::string m_dev_path;
};

iomgr::drive_attributes DriveTest::s_driveattr;

TEST_F(DriveTest, io_on_different_threads) {
    io_on_worker_threads();
    io_on_user_threads();
    io_on_regular_threads();
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger("drive_test");
    spdlog::set_pattern("[%D %H:%M:%S.%f] [%l] [%t] %v");
    return RUN_ALL_TESTS();
}

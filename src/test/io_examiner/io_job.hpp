#pragma once

#include <filesystem>
#include <map>
#include <random>
#include <chrono>

#include <sisl/fds/bitset.hpp>
#include <sds_logging/logging.h>
#include <sds_options/options.h>
#include <sisl/utility/enum.hpp>
#include <nlohmann/json.hpp>
#include <fmt/format.h>
#include <sisl/fds/buffer.hpp>

#include "iomgr.hpp"
#include "job.hpp"

static constexpr uint64_t Ki{1024};
static constexpr uint64_t Mi{Ki * Ki};
static constexpr uint64_t Gi{Ki * Mi};

namespace iomgr {

ENUM(verify_type_t, uint8_t, csum, data, header, null);
ENUM(load_type_t, uint8_t, random, same, sequential);
VENUM(io_type_t, uint8_t, write = 0, read = 1, unmap = 2);
ENUM(buf_pattern_t, uint8_t, random, lbas);

using Clock = std::chrono::steady_clock;

struct IOJobCfg : public JobCfg {
public:
    IOJobCfg() = default;
    IOJobCfg(const IOJobCfg&) = default;
    IOJobCfg(IOJobCfg&&) noexcept = delete;
    IOJobCfg& operator=(const IOJobCfg&) = default;
    IOJobCfg& operator=(IOJobCfg&&) noexcept = delete;

    std::optional< uint64_t > max_num_ios;
    uint64_t max_io_size{1 * Mi};
    uint64_t qdepth{32u};
    uint64_t max_disk_capacity{10 * Gi};

    bool read_enable{true};
    bool unmap_enable{false};
    bool enable_crash_handler{true};
    bool read_verify{false};
    bool cleanup_enable{true};
    bool verify_only{false};
    bool pre_init_verify{true};

    verify_type_t verify_type{verify_type_t::csum}; // What type of verification on every reads
    load_type_t load_type{load_type_t::random};     // IO type (random, sequential, same)
    buf_pattern_t buf_pattern{buf_pattern_t::lbas}; // Buffer pattern to read/write verify (fill with lba, random)
    std::optional< uint32_t > io_blk_size;          // If not provided, use random blk_size, else use this blksize

    // Distribution of IO Patterms
    std::map< io_type_t, float > io_dist{{io_type_t::write, 34}, {io_type_t::read, 33}, {io_type_t::unmap, 33}};

    bool init{true};
    bool expected_init_fail{false};
    bool precreate_volume{true};
    bool expect_io_error{false};

public:
    bool verify_csum() const { return verify_type == verify_type_t::csum; }
    bool verify_data() const { return verify_type == verify_type_t::data; }
    bool verify_hdr() const { return verify_type == verify_type_t::header; }
    bool verify_type_set() const { return verify_type != verify_type_t::null; }
};

struct IOTestOutput {
public:
    IOTestOutput() = default;
    IOTestOutput(const IOTestOutput&) = delete;
    IOTestOutput(IOTestOutput&&) noexcept = delete;
    IOTestOutput& operator=(const IOTestOutput&) = delete;
    IOTestOutput& operator=(IOTestOutput&&) noexcept = delete;

    std::atomic< uint64_t > data_match_cnt = 0;
    std::atomic< uint64_t > csum_match_cnt = 0;
    std::atomic< uint64_t > hdr_only_match_cnt = 0;

    std::atomic< uint64_t > write_cnt = 0;
    std::atomic< uint64_t > read_cnt = 0;
    std::atomic< uint64_t > unmap_cnt = 0;
    std::atomic< uint64_t > read_err_cnt = 0;
    std::atomic< uint64_t > vol_create_cnt = 0;
    std::atomic< uint64_t > vol_del_cnt = 0;
    std::atomic< uint64_t > vol_mounted_cnt = 0;

    uint64_t io_count() const {
        return write_cnt.load(std::memory_order_relaxed) + read_cnt.load(std::memory_order_relaxed) +
            unmap_cnt.load(std::memory_order_relaxed);
    }

    void print(const char* work_type, bool metrics_dump = false) const {
        uint64_t v;
        fmt::memory_buffer buf;
        if ((v = write_cnt.load())) fmt::format_to(fmt::appender(buf), "write_cnt={} ", v);
        if ((v = read_cnt.load())) fmt::format_to(fmt::appender(buf), "read_cnt={} ", v);
        if ((v = unmap_cnt.load())) fmt::format_to(fmt::appender(buf), "unmap_cnt={} ", v);
        if ((v = read_err_cnt.load())) fmt::format_to(fmt::appender(buf), "read_err_cnt={} ", v);
        if ((v = data_match_cnt.load())) fmt::format_to(fmt::appender(buf), "data_match_cnt={} ", v);
        if ((v = csum_match_cnt.load())) fmt::format_to(fmt::appender(buf), "csum_match_cnt={} ", v);
        if ((v = hdr_only_match_cnt.load())) fmt::format_to(fmt::appender(buf), "hdr_only_match_cnt={} ", v);
        if ((v = vol_create_cnt.load())) fmt::format_to(fmt::appender(buf), "vol_create_cnt={} ", v);
        if ((v = vol_del_cnt.load())) fmt::format_to(fmt::appender(buf), "vol_del_cnt={} ", v);
        if ((v = vol_mounted_cnt.load())) fmt::format_to(fmt::appender(buf), "vol_mounted_cnt={} ", v);

        LOGINFO("{} Output: [{}]", work_type, buf.data());
        if (metrics_dump) LOGINFO("Metrics: {}", sisl::MetricsFarm::getInstance().get_result_in_json().dump(2));
    }

    std::string to_json() const {
        nlohmann::json j;
        j["IOExaminer"]["Writes"] = write_cnt.load(std::memory_order_relaxed);
        j["IOExaminer"]["Reads"] = read_cnt.load(std::memory_order_relaxed);
        j["IOExaminer"]["Unmaps"] = unmap_cnt.load(std::memory_order_relaxed);
        j["SysMetrics"] = sisl::MetricsFarm::getInstance().get_result_in_json();
        return j.dump(2);
    }
};

ENUM(lbas_choice_t, uint8_t, dont_care, atleast_one_valid, all_valid);
ENUM(lba_validate_t, uint8_t, dont_care, validate, invalidate);

class IOJobMetrics : public sisl::MetricsGroup {
public:
    IOJobMetrics() : sisl::MetricsGroup("IOJobMetrics", "Singleton") {
        REGISTER_COUNTER(iojob_read_count, "Read Operations");
        REGISTER_COUNTER(iojob_write_count, "Write Operations");
        REGISTER_COUNTER(iojob_unmap_count, "Unmap Operations");
        REGISTER_HISTOGRAM(iojob_read_latency, "Read latency");
        REGISTER_HISTOGRAM(iojob_write_latency, "Write latency");
        REGISTER_HISTOGRAM(iojob_unmap_latency, "Unmap latency");

        register_me_to_farm();
    }
    IOJobMetrics(const IOJobMetrics&) = delete;
    IOJobMetrics(IOJobMetrics&&) noexcept = delete;
    IOJobMetrics& operator=(const IOJobMetrics&) = delete;
    IOJobMetrics& operator=(IOJobMetrics&&) noexcept = delete;
    ~IOJobMetrics() { deregister_me_from_farm(); }
};

class IOJob : public Job {
public:
    IOJob(const std::shared_ptr< IOExaminer >& examiner, const IOJobCfg& cfg) :
            Job{examiner, static_cast< JobCfg >(cfg)},
            m_cfg{cfg},
            m_io_picker{
                {m_cfg.io_dist[io_type_t::write], m_cfg.io_dist[io_type_t::read], m_cfg.io_dist[io_type_t::unmap]}} {
        // Integrated mode is not supported until lambdas or per request completion routine can be passed to iomgr.
        // Otherwise the completion of client iomgr will go back to homestore layer.
        examiner->attach_completion_cb(bind_this(IOJob::on_completion, 2));
    }

    virtual ~IOJob() override = default;
    IOJob(const IOJob&) = delete;
    IOJob(IOJob&&) noexcept = delete;
    IOJob& operator=(const IOJob&) = delete;
    IOJob& operator=(IOJob&&) noexcept = delete;

    void run_one_iteration() override {
        RELEASE_ASSERT_GE(m_cfg.qdepth, iomanager.num_workers());

        while (m_outstanding_ios.load(std::memory_order_acquire) < (int64_t)m_cfg.qdepth) {
            switch (pick_io_type()) {
            case io_type_t::write:
                write_io();
                break;
            case io_type_t::read:
                read_io();
                break;
            case io_type_t::unmap:
                unmap_io();
                break;
            }
        }
    }

    bool time_to_stop() const override {
        return (m_cfg.max_num_ios.has_value() && (m_output.io_count() >= m_cfg.max_num_ios.value())) ||
            (get_elapsed_time_sec(m_start_time) > m_cfg.run_time);
    }

    bool is_job_done() const override { return (m_outstanding_ios.load(std::memory_order_acquire) == 0); }
    bool is_async_job() const override { return true; }
    std::string job_name() const { return "VolIOJob"; }
    std::string job_result() const { return sisl::MetricsFarm::getInstance().get_result_in_json().dump(2); }

protected:
    IOJobCfg m_cfg;
    IOJobMetrics m_metrics;
    IOTestOutput m_output;
    std::atomic< uint64_t > m_cur_vol{0};
    std::atomic< int64_t > m_outstanding_ios{0};
    std::discrete_distribution<> m_io_picker;

private:
    struct io_lba_range_t {
        io_lba_range_t() {}
        io_lba_range_t(bool valid, uint64_t vidx, uint64_t l, uint32_t n) :
                valid_io{valid}, vol_idx{vidx}, lba{l}, num_lbas{n} {}
        bool valid_io{false};
        uint64_t vol_idx{0};
        uint64_t lba{0};
        uint32_t num_lbas{0};
    };
    typedef std::function< io_lba_range_t(void) > LbaGeneratorType;
    typedef std::function< bool(const uint32_t, const uint64_t, const uint32_t) > IoFuncType;

    struct io_req_t {
        uint64_t lba{0};
        uint64_t nlbas{0};
        uint64_t verify_size{0};
        uint64_t verify_offset{0};
        uint8_t* buffer{nullptr};
        uint8_t* validate_buffer{nullptr};
        io_type_t op_type{io_type_t::write};
        std::shared_ptr< vol_info_t > vol_info;
        Clock::time_point start_time;
        bool done{false};

        ~io_req_t() {
            if (buffer) { iomanager.iobuf_free(buffer); }
            if (validate_buffer) { iomanager.iobuf_free(validate_buffer); }
        }
    };

private:
    std::shared_ptr< vol_info_t > pick_vol_round_robin(io_lba_range_t& r) {
        r.vol_idx = ++m_cur_vol % m_examiner->m_vol_info.size();
        auto vinfo = m_examiner->m_vol_info[r.vol_idx];
        return (vinfo && vinfo->m_vol_dev) ? vinfo : nullptr;
    }

    // *************** Different LBA Generator ******************
    io_lba_range_t same_lbas() { return io_lba_range_t{true, 0u, 1u, 100u}; }

    io_lba_range_t seq_lbas() {
        io_lba_range_t ret;
        const auto vinfo{pick_vol_round_robin(ret)};
        if (vinfo == nullptr) { return ret; }

        static thread_local std::random_device rd{};
        static thread_local std::default_random_engine engine{rd()};
        const uint32_t max_blks{static_cast< uint32_t >(m_cfg.max_io_size / vinfo->m_page_size)};
        std::uniform_int_distribution< uint32_t > nlbas_random{1, max_blks};
        ret.num_lbas = m_cfg.io_blk_size ? *m_cfg.io_blk_size / vinfo->m_page_size : nlbas_random(engine);
        ret.lba = vinfo->m_seq_lba_cursor.fetch_add(ret.num_lbas, std::memory_order_acq_rel) %
            (vinfo->m_max_vol_blks - ret.num_lbas);
        ret.valid_io = true;
        return ret;
    }

    io_lba_range_t do_get_rand_lbas(const lbas_choice_t lba_choice, const lba_validate_t validate_choice) {
        static thread_local std::random_device rd{};
        static thread_local std::default_random_engine engine{rd()};

        io_lba_range_t ret;
        const auto vinfo{pick_vol_round_robin(ret)};
        if (vinfo == nullptr) { return ret; }

        const uint32_t max_blks{static_cast< uint32_t >(m_cfg.max_io_size / vinfo->m_page_size)};
        // lba: [0, max_vol_blks - max_blks)
        std::uniform_int_distribution< uint64_t > lba_random{0, vinfo->m_max_vol_blks - max_blks - 1};
        // nlbas: [1, max_blks]
        std::uniform_int_distribution< uint32_t > nlbas_random{1, max_blks};

        // we won't be writing more then 128 blocks in one io
        uint32_t attempt{1};
        while (attempt <= 2u) {
            // can not support concurrent overlapping writes if whole data need to be verified
            std::unique_lock< std::mutex > lk{vinfo->m_mtx};
            if (lba_choice == lbas_choice_t::dont_care) {
                ret.lba = lba_random(engine);
                ret.num_lbas = nlbas_random(engine);
            } else {
                const auto start_lba = (attempt++ == 1u) ? lba_random(engine) : 0;
                std::tie(ret.lba, ret.num_lbas) = vinfo->get_next_valid_lbas(
                    start_lba, 1u, (lba_choice == lbas_choice_t::all_valid) ? nlbas_random(engine) : 1u);
                if ((lba_choice == lbas_choice_t::atleast_one_valid) && (ret.num_lbas)) {
                    ret.num_lbas = nlbas_random(engine);
                    std::uniform_int_distribution< uint32_t > pivot_random{0, ret.num_lbas - 1};
                    const auto pivot{pivot_random(engine)};
                    ret.lba = (ret.lba < pivot) ? 0 : ret.lba - pivot;
                    if ((ret.lba + ret.num_lbas) > vinfo->m_max_vol_blks) {
                        ret.num_lbas = vinfo->m_max_vol_blks - ret.lba;
                    }
                }
            }

            // check if someone is already doing writes/reads
            if (ret.num_lbas && vinfo->is_lbas_free(ret.lba, ret.num_lbas)) {
                vinfo->mark_lbas_busy(ret.lba, ret.num_lbas);
                if (validate_choice == lba_validate_t::validate) {
                    vinfo->validate_lbas(ret.lba, ret.num_lbas);
                } else if (validate_choice == lba_validate_t::invalidate) {
                    vinfo->invalidate_lbas(ret.lba, ret.num_lbas);
                }
                ret.valid_io = true;
                break;
            }
        }

        return ret;
    }

    io_lba_range_t readable_rand_lbas() {
        return do_get_rand_lbas(lbas_choice_t::atleast_one_valid, lba_validate_t::dont_care);
    }
    io_lba_range_t writeable_rand_lbas() {
        return do_get_rand_lbas(lbas_choice_t::dont_care, lba_validate_t::validate);
    }
    io_lba_range_t unmappable_rand_lbas() {
        return do_get_rand_lbas(lbas_choice_t::atleast_one_valid, lba_validate_t::invalidate);
    }

    // *************** Different IO Generator ******************
    bool write_io() {
        bool ret = false;
        const IoFuncType write_function{bind_this(IOJob::write_vol, 3)};
        switch (m_cfg.load_type) {
        case load_type_t::random:
            ret = run_io(bind_this(IOJob::writeable_rand_lbas, 0), write_function);
            break;
        case load_type_t::same:
            ret = run_io(bind_this(IOJob::same_lbas, 0), write_function);
            break;
        case load_type_t::sequential:
            ret = run_io(bind_this(IOJob::seq_lbas, 0), write_function);
            break;
        }
        return ret;
    }

    bool read_io() {
        const IoFuncType read_function{bind_this(IOJob::read_vol, 3)};
        bool ret = false;
        switch (m_cfg.load_type) {
        case load_type_t::random:
            ret = run_io(bind_this(IOJob::readable_rand_lbas, 0), read_function);
            break;
        case load_type_t::same:
            ret = run_io(bind_this(IOJob::same_lbas, 0), read_function);
            break;
        case load_type_t::sequential:
            assert(0);
            break;
        }
        return ret;
    }

    bool unmap_io() {
        const IoFuncType unmap_function{bind_this(IOJob::unmap_vol, 3)};
        bool ret = false;
        switch (m_cfg.load_type) {
        case load_type_t::random:
            ret = run_io(bind_this(IOJob::unmappable_rand_lbas, 0), unmap_function);
            break;
        case load_type_t::same:
            assert(0);
            break;
        case load_type_t::sequential:
            assert(0);
            break;
        }
        return ret;
    }

    bool run_io(const LbaGeneratorType& lba_generator, const IoFuncType& io_function) {
        const auto gen_lba{lba_generator()};
        return (gen_lba.valid_io) ? io_function(gen_lba.vol_idx, gen_lba.lba, gen_lba.num_lbas) : false;
    }

    io_type_t pick_io_type() {
        static thread_local std::random_device rd{};
        static thread_local std::default_random_engine engine{rd()};
        return static_cast< io_type_t >(m_io_picker(engine));
    }

    // *************** Actual Read/Write/Unmap methods ******************
    bool write_vol(const uint32_t vol_idx, const uint64_t lba, const uint32_t nlbas) {
        io_req_t* req{sisl::ObjectAllocator< io_req_t >::make_object()};
        req->vol_info = m_examiner->m_vol_info[vol_idx];
        req->lba = lba;
        req->nlbas = nlbas;
        req->op_type = io_type_t::write;

        uint64_t size{nlbas * req->vol_info->m_page_size};
        req->buffer = iomanager.iobuf_alloc(512, size);
        populate_buf(req->buffer, size, lba);

        LOGTRACE("Write op size={} lba={} outstanding_ios={}", size, lba,
                 m_outstanding_ios.load(std::memory_order_relaxed) + 1);
        COUNTER_INCREMENT(m_metrics, iojob_write_count, 1);
        req->start_time = Clock::now();
        auto& vol_dev = req->vol_info->m_vol_dev;
        vol_dev->drive_interface()->async_write(vol_dev.get(), reinterpret_cast< const char* >(req->buffer), size,
                                                lba * req->vol_info->m_page_size, reinterpret_cast< uint8_t* >(req));
        m_outstanding_ios.fetch_add(1, std::memory_order_acq_rel);
        return true;
    }

    bool read_vol(const uint32_t vol_idx, const uint64_t lba, const uint32_t nlbas) {
        io_req_t* req{sisl::ObjectAllocator< io_req_t >::make_object()};
        req->vol_info = m_examiner->m_vol_info[vol_idx];
        req->lba = lba;
        req->nlbas = nlbas;
        req->op_type = io_type_t::read;

        uint64_t size{nlbas * req->vol_info->m_page_size};
        req->buffer = iomanager.iobuf_alloc(512, size);

        LOGTRACE("Read op size={} lba={} outstanding_ios={}", size, lba,
                 m_outstanding_ios.load(std::memory_order_relaxed) + 1);
        COUNTER_INCREMENT(m_metrics, iojob_read_count, 1);
        req->start_time = Clock::now();
        auto& vol_dev = req->vol_info->m_vol_dev;
        vol_dev->drive_interface()->async_read(vol_dev.get(), reinterpret_cast< char* >(req->buffer), size,
                                               lba * req->vol_info->m_page_size, reinterpret_cast< uint8_t* >(req));
        m_outstanding_ios.fetch_add(1, std::memory_order_acq_rel);
        m_output.read_cnt.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool unmap_vol(const uint32_t vol_idx, const uint64_t lba, const uint32_t nlbas) {
        io_req_t* req{sisl::ObjectAllocator< io_req_t >::make_object()};
        req->vol_info = m_examiner->m_vol_info[vol_idx];
        req->lba = lba;
        req->nlbas = nlbas;
        req->op_type = io_type_t::unmap;

        LOGTRACE("Unmap op nlbas={} lba={} outstanding_ios={}", nlbas, lba,
                 m_outstanding_ios.load(std::memory_order_relaxed) + 1);
        COUNTER_INCREMENT(m_metrics, iojob_unmap_count, 1);
        req->start_time = Clock::now();
        auto& vol_dev = req->vol_info->m_vol_dev;
        vol_dev->drive_interface()->async_unmap(vol_dev.get(), nlbas * req->vol_info->m_page_size,
                                                lba * req->vol_info->m_page_size, reinterpret_cast< uint8_t* >(req));
        m_outstanding_ios.fetch_add(1, std::memory_order_acq_rel);

        return true;
    }

    void on_completion(int64_t res, uint8_t* cookie) {
        io_req_t* req = reinterpret_cast< io_req_t* >(cookie);
        if (req->op_type == io_type_t::read) {
            HISTOGRAM_OBSERVE(m_metrics, iojob_read_latency, get_elapsed_time_us(req->start_time));
        } else if (req->op_type == io_type_t::write) {
            HISTOGRAM_OBSERVE(m_metrics, iojob_write_latency, get_elapsed_time_us(req->start_time));
        } else {
            HISTOGRAM_OBSERVE(m_metrics, iojob_unmap_latency, get_elapsed_time_us(req->start_time));
        }

        {
            std::unique_lock< std::mutex > lk(req->vol_info->m_mtx);
            req->vol_info->mark_lbas_free(req->lba, req->nlbas);
        }
        sisl::ObjectAllocator< io_req_t >::deallocate(req);
        m_outstanding_ios.fetch_sub(1, std::memory_order_acq_rel);

        try_run_one_iteration();
    }

    // *************** Other Helper methods ******************
    void populate_buf(uint8_t* buf, const uint64_t size, const uint64_t start_lba) {
        static thread_local std::random_device rd{};
        static thread_local std::default_random_engine engine{rd()};
        static thread_local std::uniform_int_distribution< uint64_t > generator{};

        uint64_t current_lba{start_lba};
        for (uint64_t write_sz{0}; write_sz < size; write_sz += sizeof(uint64_t)) {
            uint64_t* write_buf{reinterpret_cast< uint64_t* >(buf + write_sz)};
            if (m_cfg.buf_pattern == buf_pattern_t::lbas) {
                *write_buf = current_lba++;
            } else {
                *write_buf = generator(engine);
            }
        }
    }
};
} // namespace iomgr

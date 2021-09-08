#pragma once

#include <filesystem>
#include <gtest/gtest.h>
#include <sds_logging/logging.h>
#include <isa-l/crc.h>

#include <sisl/fds/bitset.hpp>
#include <sisl/utility/enum.hpp>
#include "iomgr.hpp"

namespace iomgr {
static inline DriveInterface* drive_iface() { return IOManager::instance().default_drive_interface(); }

static constexpr uint16_t init_crc_16 = 0x8005;
struct vol_info_t {
private:
    friend class IOExaminer;
    friend class Job;
    friend class IOJob;

    io_device_ptr m_vol_dev;
    std::string m_vol_name;
    int m_shadow_fd;

    std::mutex m_mtx;
    std::unique_ptr< sisl::Bitset > m_pending_lbas_bm;
    std::unique_ptr< sisl::Bitset > m_hole_lbas_bm;

    uint32_t m_page_size;
    uint64_t m_max_vol_blks;
    uint64_t m_cur_checkpoint;

    std::atomic< uint64_t > m_seq_lba_cursor{0};
    size_t m_vol_idx{0};
    sisl::atomic_counter< uint64_t > m_ref_cnt{0};
    std::atomic< bool > m_vol_destroyed{false};

public:
    vol_info_t() = default;
    vol_info_t(const vol_info_t&) = delete;
    vol_info_t(vol_info_t&&) noexcept = delete;
    vol_info_t& operator=(const vol_info_t&) = delete;
    vol_info_t& operator=(vol_info_t&&) noexcept = delete;
    ~vol_info_t() = default;

    void mark_lbas_busy(const uint64_t start_lba, const uint32_t nlbas) {
        m_pending_lbas_bm->set_bits(start_lba, nlbas);
    }

    void mark_lbas_free(const uint64_t start_lba, const uint32_t nlbas) {
        m_pending_lbas_bm->reset_bits(start_lba, nlbas);
    }

    bool is_lbas_free(const uint64_t start_lba, const uint32_t nlbas) {
        return m_pending_lbas_bm->is_bits_reset(start_lba, nlbas);
    }

    void invalidate_lbas(const uint64_t start_lba, const uint32_t nlbas) { m_hole_lbas_bm->set_bits(start_lba, nlbas); }
    void validate_lbas(const uint64_t start_lba, const uint32_t nlbas) { m_hole_lbas_bm->reset_bits(start_lba, nlbas); }
    auto get_next_valid_lbas(const uint64_t start_lba, const uint32_t min_nlbas, const uint32_t max_nlbas) {
        const auto bb{m_hole_lbas_bm->get_next_contiguous_n_reset_bits(start_lba, std::nullopt, min_nlbas, max_nlbas)};
        return std::make_pair<>(bb.start_bit, bb.nbits);
    }
};

/**************** Common class created for all tests ***************/
class IOExaminer {
    friend class Job;
    friend class IOJob;

protected:
    std::atomic< size_t > m_outstanding_ios;
    std::vector< std::shared_ptr< vol_info_t > > m_vol_info;

    // std::condition_variable m_cv;
    std::condition_variable m_init_done_cv;
    std::mutex m_mutex;
    uint8_t* m_init_buf{nullptr};

    uint64_t m_max_vol_size{0};
    uint64_t m_max_vol_size_csum{0};
    bool m_integrated_mode{false};

public:
    IOExaminer(const uint32_t num_threads, const bool integrated_mode) : m_integrated_mode{integrated_mode} {
        RELEASE_ASSERT_EQ(integrated_mode, false, "Integrated mode not supported");
        m_vol_info.reserve(100);
        if (!integrated_mode) { iomanager.start(num_threads, false /* is_spdk */); }
    }

    virtual ~IOExaminer() {
        if (m_init_buf) { iomanager.iobuf_free(static_cast< uint8_t* >(m_init_buf)); }
        if (!m_integrated_mode) { iomanager.stop(); }
    }

    IOExaminer(const IOExaminer&) = delete;
    IOExaminer(IOExaminer&&) noexcept = delete;
    IOExaminer& operator=(const IOExaminer&) = delete;
    IOExaminer& operator=(IOExaminer&&) noexcept = delete;

    void add_device(const std::string& dev_name, const int oflags) {
        std::shared_ptr< vol_info_t > info = std::make_shared< vol_info_t >();

        info->m_vol_dev = drive_iface()->open_dev(dev_name.c_str(), drive_iface()->get_drive_type(dev_name), oflags);
        info->m_vol_name = std::filesystem::path(dev_name).filename();

        auto shadow_fname = "/tmp/" + info->m_vol_name + "_shadow";
        info->m_shadow_fd = open(shadow_fname.c_str(), O_RDWR);
        // init_shadow_file(info->m_shadow_fd);

        info->m_page_size =
            drive_iface()->get_attributes(info->m_vol_name, drive_iface()->get_drive_type(dev_name)).phys_page_size;
        info->m_max_vol_blks = drive_iface()->get_size(info->m_vol_dev.get()) / info->m_page_size;
        info->m_pending_lbas_bm = std::make_unique< sisl::Bitset >(info->m_max_vol_blks);
        info->m_hole_lbas_bm = std::make_unique< sisl::Bitset >(info->m_max_vol_blks);
        info->invalidate_lbas(0, info->m_max_vol_blks); // Punch hole for all.
        info->m_cur_checkpoint = 0;
        info->m_ref_cnt.increment(1);

        m_vol_info.push_back(std::move(info));
    }

private:
#if 0
    void init_shadow_file(const int fd) {
        // initialize the file
        uint8_t* init_csum_buf{nullptr};
        const uint16_t csum_zero{crc16_t10dif(init_crc_16, static_cast< const uint8_t* >(m_init_buf),
                                              drive_iface()->get_attributes().phys_page_size)};
        if (verify_csum()) {
            init_csum_buf = iomanager.iobuf_alloc(512, sizeof(uint16_t));
            *reinterpret_cast< uint16_t* >(init_csum_buf) = csum_zero;
        }
        const uint64_t offset_increment{tcfg.verify_csum() ? sizeof(uint16_t) : tcfg.max_io_size};
        const uint64_t max_offset{tcfg.verify_csum() ? max_vol_size_csum : max_vol_size};

        for (uint64_t offset{0}; offset < max_offset; offset += offset_increment) {
            uint64_t write_size = (offset + offset_increment > max_offset) ? max_offset - offset : offset_increment;
            write_vol_file(fd, static_cast< void* >(tcfg.verify_csum() ? init_csum_buf : init_buf), write_size,
                           static_cast< off_t >(offset));
        }
        if (init_csum_buf) { iomanager.iobuf_free(init_csum_buf); }
    }

    void remove_shadow_file(const std::string& vol_name) {
        auto shadow_fname = "/tmp/" + vol_name + "_shadow";
        remove(shadow_fname.c_str());
    }
#endif
};
} // namespace iomgr

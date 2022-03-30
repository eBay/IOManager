//
// Created by Kadayam, Hari on 2021-08-16.
//
#include <mntent.h>
#include <sys/stat.h>
#include <fstream>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/sysmacros.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>

#include <fmt/format.h>
#include <flip/flip.hpp>
#include <boost/algorithm/string.hpp>

#include "iomgr.hpp"
#include "drive_interface.hpp"
#include "kernel_drive_interface.hpp"
#include "spdk_drive_interface.hpp"

namespace iomgr {
std::unordered_map< std::string, drive_type > DriveInterface::s_dev_type;
std::mutex DriveInterface::s_dev_type_lookup_mtx;

std::unordered_map< std::string, drive_attributes > DriveInterface::s_dev_attrs;
std::mutex DriveInterface::s_dev_attrs_lookup_mtx;

static std::string get_mounted_device(const std::string& filename) {
    struct stat s;
    if (stat(filename.c_str(), &s) != 0) {
        throw std::system_error(errno, std::system_category(), "Unable to stat file " + filename);
    }
    dev_t dev = s.st_dev;

    FILE* fp;
    if ((fp = setmntent("/proc/mounts", "r")) == nullptr) {
        throw std::system_error(errno, std::system_category(), "Unable to open /proc/mounts path");
    }

    std::string mnt_dev;
    struct mntent* ent{nullptr};
    while ((ent = getmntent(fp)) != nullptr) {
        if (stat(ent->mnt_dir, &s) != 0) { continue; }
        if (s.st_dev == dev) {
            mnt_dev = ent->mnt_fsname;
            break;
        }
    }

    endmntent(fp);
    return mnt_dev;
}

static std::string get_major_minor(const std::string& devname) {
    struct stat statbuf;
    const int ret{::stat(devname.c_str(), &statbuf)};
    if (ret != 0) {
        LOGERROR("Unable to stat the path {}, ignoring to get major/minor, ret:{}", devname.c_str(), ret);
        return "";
    }
    return fmt::format("{}:{}", gnu_dev_major(statbuf.st_rdev), gnu_dev_minor(statbuf.st_rdev));
}

static bool is_rotational_device(const std::string& device) {
    int is_rotational = 0;
    const auto maj_min{get_major_minor(device)};
    if (!maj_min.empty()) {
        std::string sys_path = fmt::format("/sys/dev/block/{}/queue/rotational", maj_min);
        if (auto rot_file = std::ifstream(sys_path); rot_file.is_open()) { rot_file >> is_rotational; }
    }
    return (is_rotational == 1);
}

static uint64_t get_max_write_zeros(const std::string& devname) {
    uint64_t max_zeros{0};
    const auto maj_min{get_major_minor(devname)};
    if (!maj_min.empty()) {
        const auto p{fmt::format("/sys/dev/block/{}/queue/write_zeroes_max_bytes", maj_min)};
        if (auto max_zeros_file = std::ifstream(p); max_zeros_file.is_open()) {
            max_zeros_file >> max_zeros;
        } else {
            LOGERROR("Unable to open sys path={} to get write_zeros_max_bytes, assuming 0", p);
        }
    }
    return max_zeros;
}

// NOTE: This piece of code is taken from stackoverflow
// https://stackoverflow.com/questions/478898/how-do-i-execute-a-command-and-get-the-output-of-the-command-within-c-using-po
std::string exec_command(const std::string& cmd) {
    std::array< char, 128 > buffer;
    std::string result;
    std::unique_ptr< FILE, decltype(&pclose) > pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        LOGINFO("Unable to execute the command {}, perhaps command doesn't exists", cmd);
        return result;
    }
    while (std::fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

static std::string find_megacli_bin_path() {
    static std::vector< std::string > paths{"/usr/sbin/megacli", "/bin/megacli", "/usr/lib/megacli", "/bin/MegaCli64"};
    for (auto& p : paths) {
        if (std::filesystem::exists(p)) { return p; }
    }
    return std::string("");
}

static std::string get_raid_hdd_vendor_model() {
    // Find megacli bin in popular paths
    const auto megacli_bin = find_megacli_bin_path();
    if (megacli_bin.empty()) {
        LOGINFO("Megacli is not available in this host, ignoring hdd model detection");
        return megacli_bin;
    }

    // Run megacli command and parse to get the vendor number
    const auto summary_cmd = fmt::format("{} -ShowSummary -aALL", megacli_bin);
    const auto summary_out = exec_command(summary_cmd);
    if (summary_out.empty()) {
        LOGINFO("We are not able to find any raid/megacli command, ignorning");
        return summary_out;
    }

    // Parse the output to see number of enclosure and pick
    std::vector< std::string > summary_lines;
    boost::split(summary_lines, summary_out, boost::is_any_of("\n"));
    const std::regex pd_regex("^ +PD");
    const std::regex id_regex("^ +Product Id +: +(.+)");
    const std::regex vd_regex("^ +Virtual Drives");
    std::smatch base_match;
    bool pd_area{false};
    std::string product_id; // Output where we get model/product id
    for (auto& line : summary_lines) {
        if (pd_area) {
            if (std::regex_match(line, base_match, id_regex)) {
                // The first sub_match is the whole string; the next sub_match is the first parenthesized expression.
                if (base_match.size() == 2) {
                    product_id = base_match[1].str();
                    break;
                }
            } else if (std::regex_match(line, base_match, vd_regex)) {
                pd_area = false;
                break;
            }
        } else {
            if (std::regex_match(line, base_match, pd_regex)) { pd_area = true; }
        }
    }
    return product_id;
}

drive_type DriveInterface::detect_drive_type(const std::string& dev_name) {
    if (std::filesystem::is_regular_file(std::filesystem::status(dev_name))) {
        auto device = std::filesystem::path(get_mounted_device(dev_name)).filename();
        return is_rotational_device(device) ? drive_type::file_on_hdd : drive_type::file_on_nvme;
    } else if (std::filesystem::is_block_file(std::filesystem::status(dev_name))) {
        return is_rotational_device(dev_name) ? drive_type::block_hdd : drive_type::block_nvme;
    } else {
        return SpdkDriveInterface::detect_drive_type(dev_name);
    }
}

drive_type DriveInterface::get_drive_type(const std::string& dev_name) {
    std::unique_lock lg(s_dev_type_lookup_mtx);
    drive_type dtype;
    const auto it = s_dev_type.find(dev_name); // Lookup for already maintained information
    if (it != s_dev_type.end()) {
        dtype = it->second;
    } else {
        dtype = detect_drive_type(dev_name);
        LOGINFOMOD(iomgr, "Drive={} is detected to be drive_type={}", dev_name, dtype);
        s_dev_type.insert({dev_name, dtype});
    }

    return dtype;
}

void DriveInterface::emulate_drive_type(const std::string& dev_name, const drive_type dtype) {
    std::unique_lock lg(s_dev_type_lookup_mtx);
    auto [it, inserted] = s_dev_type.insert_or_assign(dev_name, dtype);
    if (inserted) {
        LOGINFOMOD(iomgr, "Emulating any upcoming open_dev of name={} as drive_type={}", dev_name, dtype);
    } else {
        LOGINFO("Changing device={} type as drive_type={}", dev_name, dtype);
    }
}

void DriveInterface::emulate_drive_attributes(const std::string& dev_name, const drive_attributes& attr) {
    std::unique_lock lg(s_dev_attrs_lookup_mtx);
    auto [it, inserted] = s_dev_attrs.insert_or_assign(dev_name, attr);
    if (inserted) {
        LOGINFOMOD(iomgr, "Emulating any upcoming open_dev of name={} with attributes={}", dev_name,
                   attr.to_json().dump(2));
    } else {
        LOGINFO("Changing device={} attributes to {}", dev_name, attr.to_json().dump(2));
    }
}

std::shared_ptr< DriveInterface > DriveInterface::get_iface_for_drive(const std::string& dev_name,
                                                                      const drive_type dtype) {
    drive_interface_type iface_type;
    if (iomanager.is_spdk_mode() && (dtype != drive_type::file_on_hdd) && (dtype != drive_type::block_hdd)) {
        iface_type = drive_interface_type::spdk;
    } else if (iomanager.is_uring_capable()) {
        iface_type = drive_interface_type::uring;
    } else {
        iface_type = drive_interface_type::aio;
    }
    return iomanager.get_drive_interface(iface_type);
}

drive_attributes DriveInterface::get_attributes(const std::string& dev_name) {
    std::unique_lock lg(s_dev_attrs_lookup_mtx);
    drive_attributes attrs;
    const auto it = s_dev_attrs.find(dev_name); // Lookup for already maintained information
    if (it != s_dev_attrs.end()) {
        attrs = it->second;
    } else {
        auto dtype = get_drive_type(dev_name);
        attrs = get_iface_for_drive(dev_name, dtype)->get_attributes(dev_name, dtype);
        s_dev_attrs.insert({dev_name, attrs});
    }
    return attrs;
}

io_device_ptr DriveInterface::open_dev(const std::string& dev_name, int oflags) {
    auto dtype = get_drive_type(dev_name);
    return get_iface_for_drive(dev_name, dtype)->open_dev(dev_name, dtype, oflags);
}

size_t DriveInterface::get_size(IODevice* iodev) { return iodev->drive_interface()->get_dev_size(iodev); }

/////////////////////////// KernelDriveInterface Section /////////////////////////////////////
size_t KernelDriveInterface::get_dev_size(IODevice* iodev) {
    if (std::filesystem::is_regular_file(std::filesystem::status(iodev->devname))) {
        struct stat buf;
        if (fstat(iodev->fd(), &buf) >= 0) { return buf.st_size; }
    } else {
        assert(std::filesystem::is_block_file(std::filesystem::status(iodev->devname)));
        size_t devsize;
        if (ioctl(iodev->fd(), BLKGETSIZE64, &devsize) >= 0) { return devsize; }
    }

    folly::throwSystemError(fmt::format("device stat failed for dev {} errno = {}", iodev->fd(), errno));
    return 0;
}

drive_attributes KernelDriveInterface::get_attributes(const std::string& devname, const drive_type drive_type) {
    // TODO: Get this information from SSD using /sys commands
    drive_attributes attr;
    attr.phys_page_size = 4096;
    attr.align_size = 512;
#ifndef NDEBUG
    attr.atomic_phys_page_size = 512;
#else
    attr.atomic_phys_page_size = 4096;
#endif
    attr.num_streams = 1;

    if ((drive_type == drive_type::block_hdd) || (drive_type == drive_type::file_on_hdd)) {
        // Try to find the underlying device type and see if any vendor data matches with our preconfigured settings
        static std::string model = get_raid_hdd_vendor_model();
        static std::unordered_map< std::string, uint32_t > s_num_streams_for_model = {
            {"HGST HUS726T6TAL", 128u}, // Western Digital
            {"ST6000NM021A-2R7", 128u}, // Seagate
        };

        if (SISL_OPTIONS.count("hdd_streams") > 0) {
            attr.num_streams = SISL_OPTIONS["hdd_streams"].as< uint32_t >();
            LOGINFO("Device={} uses overriden attribute for hdd streams={}", devname, attr.num_streams);
        } else if (!model.empty()) {
            const auto it = s_num_streams_for_model.find(model);
            if (it != s_num_streams_for_model.end()) {
                attr.num_streams = it->second;
                LOGINFO("Detected hdd product id as {}, setting num_streams as {}", model, it->second);
            } else {
                LOGINFO("Detected hdd product id as {}, but num_streams not configured for that, assuming default",
                        model);
            }
        }
    }
    return attr;
}

void KernelDriveInterface::init_write_zero_buf(const std::string& devname, const drive_type dev_type) {
#ifdef __linux__
    if ((dev_type == drive_type::block_nvme) && IM_DYNAMIC_CONFIG(aio.zeros_by_ioctl)) {
        if (m_max_write_zeros == std::numeric_limits< uint64_t >::max()) {
            m_max_write_zeros = get_max_write_zeros(devname);
        }
    } else {
        m_max_write_zeros = 0;
    }
#elif
    m_max_write_zeros = 0;
#endif

    if (m_max_write_zeros == 0) {
        if (!m_zero_buf) {
            m_zero_buf = std::unique_ptr< uint8_t, std::function< void(uint8_t* const) > >{
                sisl::AlignedAllocator::allocator().aligned_alloc(get_attributes(devname, dev_type).align_size,
                                                                  max_buf_size, sisl::buftag::common),
                [](uint8_t* const ptr) {
                    if (ptr) sisl::AlignedAllocator::allocator().aligned_free(ptr, sisl::buftag::common);
                }};
            if (m_zero_buf) std::memset(m_zero_buf.get(), 0, max_buf_size);
        }
    }
}

ssize_t KernelDriveInterface::sync_write(IODevice* iodev, const char* data, uint32_t size, uint64_t offset) {
    ssize_t written_size = 0;
    uint32_t resubmit_cnt = 0;
    while ((written_size != size) && resubmit_cnt <= IM_DYNAMIC_CONFIG(max_resubmit_cnt)) {
        written_size = pwrite(iodev->fd(), data, (ssize_t)size, (off_t)offset);
#ifdef _PRERELEASE
        auto flip_resubmit_cnt = flip::Flip::instance().get_test_flip< int >("write_sync_resubmit_io");
        if (flip_resubmit_cnt != boost::none && resubmit_cnt < (uint32_t)flip_resubmit_cnt.get()) { written_size = 0; }
#endif
        ++resubmit_cnt;
    }
    if (written_size != size) {
        folly::throwSystemError(fmt::format("Error during write offset={} write_size={} written_size={} errno={} fd={}",
                                            offset, size, written_size, errno, iodev->fd()));
    }

    return written_size;
}

ssize_t KernelDriveInterface::sync_writev(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                          uint64_t offset) {
    ssize_t written_size = 0;
    uint32_t resubmit_cnt = 0;
    while ((written_size != size) && resubmit_cnt <= IM_DYNAMIC_CONFIG(max_resubmit_cnt)) {
        written_size = pwritev(iodev->fd(), iov, iovcnt, offset);
#ifdef _PRERELEASE
        auto flip_resubmit_cnt = flip::Flip::instance().get_test_flip< int >("write_sync_resubmit_io");
        if (flip_resubmit_cnt != boost::none && resubmit_cnt < (uint32_t)flip_resubmit_cnt.get()) { written_size = 0; }
#endif
        ++resubmit_cnt;
    }
    if (written_size != size) {
        folly::throwSystemError(
            fmt::format("Error during writev offset={} write_size={} written_size={} iovcnt={} errno={} fd={}", offset,
                        size, written_size, iovcnt, errno, iodev->fd()));
    }

    return written_size;
}

ssize_t KernelDriveInterface::sync_read(IODevice* iodev, char* data, uint32_t size, uint64_t offset) {
    ssize_t read_size = 0;
    uint32_t resubmit_cnt = 0;
    while ((read_size != size) && resubmit_cnt <= IM_DYNAMIC_CONFIG(max_resubmit_cnt)) {
        read_size = pread(iodev->fd(), data, (ssize_t)size, (off_t)offset);
#ifdef _PRERELEASE
        auto flip_resubmit_cnt = flip::Flip::instance().get_test_flip< int >("read_sync_resubmit_io");
        if (flip_resubmit_cnt != boost::none && resubmit_cnt < (uint32_t)flip_resubmit_cnt.get()) { read_size = 0; }
#endif
        ++resubmit_cnt;
    }
    if (read_size != size) {
        folly::throwSystemError(fmt::format("Error during read offset={} to_read_size={} read_size={} errno={} fd={}",
                                            offset, size, read_size, errno, iodev->fd()));
    }

    return read_size;
}

ssize_t KernelDriveInterface::sync_readv(IODevice* iodev, const iovec* iov, int iovcnt, uint32_t size,
                                         uint64_t offset) {
    ssize_t read_size = 0;
    uint32_t resubmit_cnt = 0;
    while ((read_size != size) && resubmit_cnt <= IM_DYNAMIC_CONFIG(max_resubmit_cnt)) {
        read_size = preadv(iodev->fd(), iov, iovcnt, (off_t)offset);
#ifdef _PRERELEASE
        auto flip_resubmit_cnt = flip::Flip::instance().get_test_flip< int >("read_sync_resubmit_io");
        if (flip_resubmit_cnt != boost::none && resubmit_cnt < (uint32_t)flip_resubmit_cnt.get()) { read_size = 0; }
#endif
        ++resubmit_cnt;
    }
    if (read_size != size) {
        folly::throwSystemError(
            fmt::format("Error during readv offset={} to_read_size={} read_size={} iovcnt={} errno={} fd={}", offset,
                        size, read_size, iovcnt, errno, iodev->fd()));
    }

    return read_size;
}

void KernelDriveInterface::write_zero(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) {
    if ((iodev->dtype == drive_type::block_nvme) && (m_max_write_zeros != 0)) {
        write_zero_ioctl(iodev, size, offset, cookie);
    } else {
        write_zero_writev(iodev, size, offset, cookie);
    }
}

void KernelDriveInterface::write_zero_ioctl(const IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) {
    assert(m_max_write_zeros != 0);

#ifdef __linux__
    uint64_t range[2];
    int ret{0};
    while (size > 0) {
        uint64_t this_size = std::min(size, m_max_write_zeros);
        range[0] = offset;
        range[1] = this_size;
        ret = ioctl(iodev->fd(), BLKZEROOUT, range);
        if (ret != 0) {
            LOGERROR("Error in writing zeros at offset={} size={} errno={}", offset, this_size, errno);
            break;
        }
        offset += this_size;
        size -= this_size;
    }
    if (m_comp_cb) { m_comp_cb(((ret != 0) ? errno : 0), cookie); }
#endif
}

void KernelDriveInterface::write_zero_writev(IODevice* iodev, uint64_t size, uint64_t offset, uint8_t* cookie) {
    if (size == 0 || !m_zero_buf) {
        assert(false);
        return;
    }

    uint64_t total_sz_written = 0;
    while (total_sz_written < size) {
        const uint64_t sz_to_write{(size - total_sz_written) > max_zero_write_size ? max_zero_write_size
                                                                                   : (size - total_sz_written)};

        const auto iovcnt{(sz_to_write - 1) / max_buf_size + 1};

        std::vector< iovec > iov(iovcnt);
        for (uint32_t i = 0; i < iovcnt; ++i) {
            iov[i].iov_base = m_zero_buf.get();
            iov[i].iov_len = max_buf_size;
        }

        iov[iovcnt - 1].iov_len = sz_to_write - (max_buf_size * (iovcnt - 1));
        try {
            // returned written sz already asserted in sync_writev;
            sync_writev(iodev, &(iov[0]), iovcnt, sz_to_write, offset + total_sz_written);
        } catch (std::exception& e) {
            RELEASE_ASSERT(0, "Exception={} caught while doing sync_writev for devname={}, size={}", e.what(),
                           iodev->devname, size);
        }

        total_sz_written += sz_to_write;
    }

    assert(total_sz_written == size);

    if (m_comp_cb) { m_comp_cb(errno, cookie); }
}

} // namespace iomgr

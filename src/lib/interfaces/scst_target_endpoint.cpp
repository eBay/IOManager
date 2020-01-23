#include "DeviceIoMgr.h"

extern "C" {
#include <sys/epoll.h>
#include <unistd.h>
}

#include <boost/uuid/uuid_io.hpp>
#include <iomgr/iomgr.hpp>
#include <sds_logging/logging.h>

#include "ScstCommon.h"
#include "ScstTarget.h"
#include "ScstTask.h"

SDS_LOGGING_DECL(iomgr)

/// This is a hack to support SCST 3.3.x for the 4.15 kernel. It was
// pulled from the scst open source repo at Git SHA: "bf705eaf"
#define DEV_USER_VERSION_3_3                                                                                           \
    "3.3.0"                                                                                                            \
    "b03893a260c08b87cbc0a0f8c3b347d481bee1d7"                                                                         \
    "08d8e524e9ee6194aa18f995b591b04903c013eb"

constexpr char const* DEV_INTERFACE_VERSIONS[] = {DEV_USER_VERSION, DEV_USER_VERSION_3_3};

namespace connector::scst {

thread_local ScstTask* ready_task{nullptr};

DeviceIoMgr::DeviceIoMgr(node_id_t const& node_id, std::string const& device_name, volume_id_t const& vol_id) :
        ScstDevice(node_id, device_name, vol_id) {}

DeviceIoMgr::~DeviceIoMgr() = default;

void DeviceIoMgr::start(std::shared_ptr< iomgr::ioMgr > iomgr) {
    iomgr->add_fd(
        device(), [this](auto fd, auto cookie, auto event) { ioEvent(fd, cookie, event); }, EPOLLIN, 9, nullptr);
    iomgr_ = iomgr;
}

void DeviceIoMgr::registerDevice(uint8_t const device_type, uint32_t const logical_block_size) {
    // Open the SCST user device
    if (0 > openDevice()) {
        throw std::system_error(std::make_error_code(std::errc::bad_file_descriptor), "scst_handle");
    }

    auto const uniq_dev = unique_device_name(node_id, volume_id);
    int        res{-1};
    LOGINFOMOD(scst, "vol:{} blocksize:{} new SCST device", getName(), logical_block_size);

    for (auto const interface_version : DEV_INTERFACE_VERSIONS) {
        // REGISTER a device
        scst_user_dev_desc scst_descriptor{
            (unsigned long)interface_version, // Constant
            (unsigned long)"GPL",             // License string
            device_type,                      // Device type
            1,
            0,
            0,
            0, // SGV enabled
            {
                // SCST options
                SCST_USER_PARSE_STANDARD,                  // parse type
                SCST_USER_ON_FREE_CMD_IGNORE,              // command on-free type
                SCST_USER_MEM_REUSE_READ,                  // buffer reuse type
                SCST_USER_PARTIAL_TRANSFERS_NOT_SUPPORTED, // partial transfer type
                0,                                         // partial transfer length
                SCST_TST_0_SINGLE_TASK_SET,                // task set sharing
                0,                                         // task mgmt only (on fault)
                SCST_QUEUE_ALG_1_UNRESTRICTED_REORDER,     // maintain consistency in
                                                           // reordering
                SCST_QERR_0_ALL_RESUME,                    // fault does not abort all cmds
                1, 0, 0, 0,                                // TAS/SWP/DSENSE/ORDER MGMT,
                0                                          // Copy/Unmap support
            },
            logical_block_size, // Block size
            0,                  // PR cmd Notifications
            {},
            "00000000-0000-0000-0000-000000000000", // SGV Name (NodeID)
        };
        LOGDEBUG("Registering device: {} for {} with interface version: [{}]", uniq_dev, volume_id, interface_version);
        snprintf(scst_descriptor.name, SCST_MAX_NAME, "%s", uniq_dev.c_str());
        snprintf(scst_descriptor.sgv_name, SCST_MAX_NAME, "%s", to_string(node_id).c_str());
        res = send_ioctl(SCST_USER_REGISTER_DEVICE, &scst_descriptor);
        if (0 == res) return;
    }
    if (0 > res) {
        LOGERRORMOD(scst, "Failed to register device: {}", strerror(errno));
        throw std::system_error(errno, std::system_category(), "scst_registration");
    }
}

void DeviceIoMgr::execCompleteCmd(scst_user_get_cmd const* c) { LOGTRACEMOD(scst, "Complete: {}", c->cmd_h); }

void DeviceIoMgr::ioEvent(int descriptor, void*, uint32_t events) {
    LOGTRACEMOD(iomgr, "IO Event on {}, event set: {}", descriptor, events);
    if (device() == descriptor) {
        // Get the next command, and/or reply to any existing finished commands
        try {
            respondAndGet(events);
        } catch (std::system_error& e) { LOGWARNMOD(scst, "{}", e.what()); }
        return;
    }
}

void DeviceIoMgr::ready(ScstTask* task) { ready_task = task; }

thread_local bool in_gar{false};

void DeviceIoMgr::respondAndGet(uint32_t events) {
    if (in_gar) return; // Prevents recursive calls on this thread
    in_gar = true;
    do {
        scst_user_get_cmd cmd;
        cmd.preply = 0ul;
        Clock::time_point time_start;
        if (ready_task) {
            auto const& reply = *((scst_user_reply_cmd*)ready_task->getReply());
            LOGTRACEMOD(iomgr, "cmd:{} sc:{} result:{} responding", reply.cmd_h, reply.subcode, reply.result);
            cmd.preply = (uint64_t)&reply;
            cmd.cmd_h = reply.cmd_h;
            cmd.subcode = reply.subcode;
            if (ready_task->isCommon()) {
                // Do not delete our common_task when complete
                ready_task = nullptr;
            } else {
                auto exec_task = static_cast< ScstExecTask* >(ready_task);
                if (ScstExecTask::DataDir::none != exec_task->dataDir()) {
#ifndef NDEBUG
                    /// DEBUG CHECK
                    // We should have this task in our outstanding task list. If we don't
                    // it must have been aborted earlier so we should log a critical
                    // message
                    {
                        std::lock_guard< mutex_t > lg(outstanding_task_lock);
                        if (auto it = outstanding_tasks.find(cmd.cmd_h); outstanding_tasks.end() != it) {
                            outstanding_tasks.erase(it);
                        } else {
                            LOGFATAL("Could not find task {} in outstanding list...ABORTED?", cmd.cmd_h);
                        }
                    }
#endif
                    auto const time_elap = get_elapsed_time_ns(exec_task->start_time);
                    if (ScstExecTask::DataDir::out == exec_task->dataDir()) {
                        HISTOGRAM_OBSERVE(ScstDevice::device_metrics, device_read_lat_tx, time_elap);
                        COUNTER_DECREMENT(ScstDevice::device_metrics, device_read_outstanding, 1);
                    } else {
                        HISTOGRAM_OBSERVE(ScstDevice::device_metrics, device_write_lat_tx, time_elap);
                        COUNTER_DECREMENT(ScstDevice::device_metrics, device_write_outstanding, 1);
                    }
                } else {
                    HISTOGRAM_OBSERVE(ScstDevice::device_metrics, device_misc_lat_tx,
                                      get_elapsed_time_ns(exec_task->start_time));
                }
            }
        }

        int res = 0;
        do { // Make sure and finish the ioctl
            res = send_ioctl(SCST_USER_REPLY_AND_GET_CMD, &cmd);
        } while ((0 > res) && (EINTR == errno));
        if (ready_task) {
            delete ready_task;
            ready_task = nullptr;
        }
        time_start = Clock::now();
        cmd.preply = 0ul;

        if (events) {
            iomgr_->process_done(device(), events);
            iomgr_->fd_reschedule(device(), events);
            events = 0;
        }

        if (0 != res) {
            in_gar = false;
            switch (errno) {
            case ENOTTY:
            case EBADF:
                throw std::system_error(errno, std::system_category(),
                                        format(FMT_STRING("device_ioctl [{}:{}]"), volume_id, getName()));
            case EFAULT:
            case EINVAL:
                LOGERRORMOD(scst, "Invalid Arguments: {}", fmt_user_cmd(&cmd));
                throw std::system_error(errno, std::system_category(),
                                        format(FMT_STRING("device_ioctl [{}:{}]"), volume_id, getName()));
            case EAGAIN: return;
            default: LOGDEBUGMOD(scst, "Ignored error: {}", errno); return;
            }
        }

        execCmd(&cmd, time_start);
    } while (ready_task);
    in_gar = false;
}

void DeviceIoMgr::terminate() { stopping = true; }

} // namespace connector::scst

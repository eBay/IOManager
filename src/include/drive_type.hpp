
#pragma once

#include <utility/enum.hpp>

namespace iomgr {

ENUM(iomgr_drive_type, uint8_t,
     file,      // Works on top of file system
     block,     // Kernel block device
     raw_nvme,  // Raw Nvme device (which can be opened only thru spdk)
     memory,    // Non-persistent memory
     spdk_bdev, // A SDPK verion of bdev
     unknown    // Try to deduce it while loading
)

} // namespace iomgr


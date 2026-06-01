#pragma once

#include <string>

#include "drive_interface.hpp"
#include <iomgr/iomgr_types.hpp>

namespace iomgr {

class KernelDriveInterface : public DriveInterface {
public:
    KernelDriveInterface() = default;

protected:
    virtual size_t get_dev_size(IODevice* iodev) override;
    virtual drive_attributes get_attributes(const std::string& devname, const drive_type drive_type) override;
};
} // namespace iomgr

/**
 * Copyright eBay Corporation 2018
 */

#pragma once

#include <sds_logging/logging.h>

SDS_LOGGING_DECL(iomgr);

namespace iomgr {

void* iothread(void* iomgr);

} // namespace iomgr

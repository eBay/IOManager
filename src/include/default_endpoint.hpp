//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_DEFAULT_ENDPOINT_HPP
#define IOMGR_DEFAULT_ENDPOINT_HPP

#include "include/endpoint.hpp"

namespace iomgr {
class DefaultEndPoint : public EndPoint {
public:
    void on_io_thread_start(ioMgrThreadContext* iomgr_ctx) override { (void)iomgr_ctx; };
    void on_io_thread_stopped(ioMgrThreadContext* iomgr_ctx) override { (void)iomgr_ctx; };
};
} // namespace iomgr
#endif // IOMGR_DEFAULT_ENDPOINT_HPP

//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_ENDPOINT_HPP
#define IOMGR_ENDPOINT_HPP

#include <functional>

namespace iomgr {
typedef std::function< void(int64_t res, uint8_t* cookie) > endpoint_comp_cb_t;

class ioMgrThreadContext;
class EndPoint {
protected:
public:
    explicit EndPoint() {}
    virtual ~EndPoint() = default;

    virtual void on_io_thread_start(ioMgrThreadContext* ctx) = 0;
    virtual void on_io_thread_stopped(ioMgrThreadContext* ctx) = 0;
};

} // namespace iomgr
#endif // IOMGR_ENDPOINT_HPP

//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_INTERFACE_HPP
#define IOMGR_INTERFACE_HPP

#include <functional>

namespace iomgr {
typedef std::function< void(int64_t res, uint8_t* cookie) > io_interface_comp_cb_t;
typedef std::function< void(int nevents) > io_interface_batch_sentinel_cb_t;

class ioMgrThreadContext;
class IOInterface {
protected:
public:
    explicit IOInterface() {}
    virtual ~IOInterface() = default;

    virtual void on_io_thread_start(ioMgrThreadContext* ctx) = 0;
    virtual void on_io_thread_stopped(ioMgrThreadContext* ctx) = 0;
};

class DefaultIOInterface : public IOInterface {
public:
    virtual void on_io_thread_start(__attribute__((unused)) ioMgrThreadContext* ctx) {}
    virtual void on_io_thread_stopped(__attribute__((unused)) ioMgrThreadContext* ctx) {}
};
} // namespace iomgr
#endif // IOMGR_INTERFACE_HPP

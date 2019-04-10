//
// Created by Rishabh Mittal on 04/20/2018
//
#pragma once

#include <memory>
#include <functional>

namespace iomgr {

struct ioMgr;
struct ioMgrImpl;
struct fd_info;
using ev_callback = std::function< void(int fd, void* cookie, uint32_t events) >;

class EndPoint {
protected:
    std::shared_ptr< ioMgr > iomgr;

public:
    explicit EndPoint(std::shared_ptr< ioMgr > iomgr) : iomgr(iomgr) {}
    virtual ~EndPoint() = default;

    virtual void init_local() = 0;
    virtual void print_perf() = 0;
};

struct ioMgr {
    ioMgr(size_t const num_ep, size_t const num_threads);
    ~ioMgr();

    void     start();
    void     run_io_loop();
    void     add_ep(EndPoint* ep);
    fd_info* add_fd(int const fd, ev_callback cb, int const ev, int const pri, void* cookie);
    fd_info* add_local_fd(int const fd, ev_callback cb, int const ev, int const pri, void* cookie);

    void fd_reschedule(int const fd, uint32_t const event);
    void fd_reschedule(fd_info* info, uint32_t const event);

    void process_done(int const fd, int const ev);
    void process_done(fd_info* info, int const ev);

    void print_perf_cntrs();

private:
    std::unique_ptr< ioMgrImpl > _impl;
};

} // namespace iomgr

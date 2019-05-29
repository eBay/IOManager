#include "iomgr.hpp"
#include "iomgr.hpp"

namespace iomgr {

ioMgr::ioMgr(size_t const num_ep, size_t const num_threads) {
    _impl = std::make_unique< IOManager >(num_ep, num_threads);
}

ioMgr::~ioMgr() = default;

void ioMgr::start() { _impl->start(); }

void ioMgr::run_io_loop() { _impl->run_io_loop(); }

void ioMgr::add_ep(EndPoint* ep) { _impl->add_ep(ep); }

fd_info* ioMgr::add_fd(EndPoint* ep, int const fd, ev_callback cb, int const ev, int const pri, void* cookie) {
    return _impl->add_fd(ep, fd, cb, ev, pri, cookie);
}

fd_info* ioMgr::add_per_thread_fd(EndPoint* ep, int const fd, ev_callback cb, int const ev, int const pri, void* cookie) {
    return _impl->add_per_thread_fd(ep, fd, cb, ev, pri, cookie);
}

void ioMgr::print_perf_cntrs() { _impl->print_perf_cntrs(); }

void ioMgr::fd_reschedule(int const fd, uint32_t const event) { _impl->fd_reschedule(fd, event); }

void ioMgr::process_done(int const fd, int const ev) { _impl->process_done(fd, ev); }

} // namespace iomgr

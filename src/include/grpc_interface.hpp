//
// Created by Kadayam, Hari on 2021-08-05.
//
#pragma once

#include <vector>
#include <mutex>
#include <set>
#include <functional>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <grpc_internal/src/core/lib/iomgr/ev_posix.h>
#include <grpc_internal/src/core/lib/iomgr/port.h>
#include <grpc_internal/src/core/lib/iomgr/lockfree_event.h>

#include "include/reactor.hpp"
#include "include/io_interface.hpp"

struct grpc_pollset;
typedef boost::intrusive_ptr< ::grpc_pollset > safe_grpc_pollset;

struct grpc_fd : public boost::intrusive_ref_counter< grpc_fd > {
public:
    grpc_fd(int fd, const char* name, bool track_err);
    ~grpc_fd();

    void add_to_pollset(const safe_grpc_pollset& pollset);
    void remove_from_pollset(const safe_grpc_pollset& pollset);

public:
    iomgr::io_device_ptr m_iodev;

    grpc_core::LockfreeEvent m_read_closure;
    grpc_core::LockfreeEvent m_write_closure;
    grpc_core::LockfreeEvent m_error_closure;
    bool m_track_err;

    std::mutex m_pollsets_mtx;
    std::set< safe_grpc_pollset > m_pollsets; // All pollsets this fd is part of
};
typedef boost::intrusive_ptr< ::grpc_fd > safe_grpc_fd;

struct grpc_pollset_worker {
    iomgr::IOReactor* m_reactor{nullptr};
};

struct grpc_pollset : public boost::intrusive_ref_counter< grpc_pollset > {
    int m_epfd{-1};
    iomgr::io_device_ptr m_ep_iodev;
    std::set< safe_grpc_fd > m_gfds; // List of grpc fds that are part of this pollset

    gpr_mu m_mu;
    sisl::ActiveOnlyThreadBuffer< grpc_pollset_worker > m_workers;

    grpc_closure* m_shutdown_closure{nullptr};
    bool m_already_shutdown{false};
    int m_containing_pollset_set_count{0};
};

struct grpc_pollset_set {
    gpr_mu m_mu;
    grpc_pollset_set* parent;

    std::vector< safe_grpc_pollset > m_pollsets;
    std::vector< grpc_fd* > m_fds;
};

#if 0
namespace std {
template <>
struct hash< safe_grpc_fd > {
    size_t operator()(const safe_grpc_fd& sgfd) const noexcept { return std::hash< grpc_fd* >(sgfd.get()); }
};

template <>
struct hash< safe_grpc_pollset > {
    size_t operator()(const safe_grpc_pollset& sgps) const { return std::hash< grpc_pollset* >(sgps.get()); }
};
} // namespace std

bool operator==(const safe_grpc_fd& a, const safe_grpc_fd& b) { return (a.get() == b.get()); }
bool operator==(const safe_grpc_pollset& a, const safe_grpc_pollset& b) { return (a.get() == b.get()); }
#endif

namespace iomgr {
class GrpcInterface : public IOInterface {
public:
    GrpcInterface();
    std::string name() const override { return "grpc_interface"; }

    grpc_fd* fd_create(int fd, const char* name, bool track_err);
    int fd_wrapped_fd(grpc_fd* fd);
    void fd_orphan(grpc_fd* fd, grpc_closure* on_done, int* release_fd, const char* reason);
    void fd_shutdown(grpc_fd* fd, grpc_error* why);
    void fd_notify_on_read(grpc_fd* fd, grpc_closure* closure);
    void fd_notify_on_write(grpc_fd* fd, grpc_closure* closure);
    void fd_notify_on_error(grpc_fd* fd, grpc_closure* closure);
    void fd_set_readable(grpc_fd* fd);
    void fd_set_writable(grpc_fd* fd);
    void fd_set_error(grpc_fd* fd);
    bool fd_is_shutdown(grpc_fd* fd);

    void pollset_init(grpc_pollset* pollset, gpr_mu** mu);
    void pollset_shutdown(grpc_pollset* pollset, grpc_closure* closure);
    void pollset_destroy(grpc_pollset* pollset);
    grpc_error* pollset_work(grpc_pollset* pollset, grpc_pollset_worker** worker, grpc_millis deadline);
    grpc_error* pollset_kick(grpc_pollset* pollset, grpc_pollset_worker* specific_worker);
    void pollset_add_fd(grpc_pollset* pollset, struct grpc_fd* fd);
    grpc_pollset_set* pollset_set_create(void);
    void pollset_set_destroy(grpc_pollset_set* pollset_set);
    void pollset_set_add_pollset(grpc_pollset_set* pollset_set, grpc_pollset* pollset);
    void pollset_set_del_pollset(grpc_pollset_set* pollset_set, grpc_pollset* pollset);
    void pollset_set_add_pollset_set(grpc_pollset_set* bag, grpc_pollset_set* item);
    void pollset_set_del_pollset_set(grpc_pollset_set* bag, grpc_pollset_set* item);
    void pollset_set_add_fd(grpc_pollset_set* pollset_set, grpc_fd* fd);
    void pollset_set_del_fd(grpc_pollset_set* pollset_set, grpc_fd* fd);

    bool is_any_background_poller_thread(void);
    void shutdown_background_closure(void);
    void shutdown_engine(void);
    bool add_closure_to_background_poller(grpc_closure* closure, grpc_error* error);

private:
    void process_pollset_events(grpc_pollset* pollset, [[maybe_unused]] int e);

private:
    std::shared_mutex m_fds_mtx;
    std::set< safe_grpc_fd > m_gfds;
    std::shared_mutex m_pollsets_mtx;
    std::set< safe_grpc_pollset > m_pollsets;
};

static GrpcInterface* s_grpc_iface{nullptr};
[[maybe_unused]] static void set_grpc_interface(GrpcInterface* giface) { s_grpc_iface = giface; }
} // namespace iomgr

[[maybe_unused]] static inline iomgr::GrpcInterface* grpc_iface(void) { return iomgr::s_grpc_iface; }

static ::grpc_event_engine_vtable s_vtable = ::grpc_event_engine_vtable{
    sizeof(grpc_pollset),
    true,
    false,
    [](int fd, const char* name, bool track_err) -> grpc_fd* { return grpc_iface()->fd_create(fd, name, track_err); },
    [](grpc_fd* fd) -> int { return grpc_iface()->fd_wrapped_fd(fd); },
    [](grpc_fd* fd, grpc_closure* on_done, int* release_fd, const char* reason) {
        grpc_iface()->fd_orphan(fd, on_done, release_fd, reason);
    },
    [](grpc_fd* fd, grpc_error* why) { grpc_iface()->fd_shutdown(fd, why); },
    [](grpc_fd* fd, grpc_closure* closure) { grpc_iface()->fd_notify_on_read(fd, closure); },
    [](grpc_fd* fd, grpc_closure* closure) { grpc_iface()->fd_notify_on_write(fd, closure); },
    [](grpc_fd* fd, grpc_closure* closure) { grpc_iface()->fd_notify_on_error(fd, closure); },
    [](grpc_fd* fd) { grpc_iface()->fd_set_readable(fd); },
    [](grpc_fd* fd) { grpc_iface()->fd_set_writable(fd); },
    [](grpc_fd* fd) { grpc_iface()->fd_set_error(fd); },
    [](grpc_fd* fd) -> bool { return grpc_iface()->fd_is_shutdown(fd); },
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr

    /*bind_this(GrpcInterface::pollset_init, 2),
    bind_this(GrpcInterface::pollset_shutdown, 2),
    bind_this(GrpcInterface::pollset_destroy, 1),
    bind_this(GrpcInterface::pollset_work, 3),
    bind_this(GrpcInterface::pollset_kick, 2),
    bind_this(GrpcInterface::pollset_add_fd, 2),
    bind_this(GrpcInterface::pollset_set_create, 0),
    bind_this(GrpcInterface::pollset_set_destroy, 1), // destroy ==> unref 1 public ref
    bind_this(GrpcInterface::pollset_set_add_pollset, 2),
    bind_this(GrpcInterface::pollset_set_del_pollset, 2),
    bind_this(GrpcInterface::pollset_set_add_pollset_set, 2),
    bind_this(GrpcInterface::pollset_set_del_pollset_set, 2),
    bind_this(GrpcInterface::pollset_set_add_fd, 2),
    bind_this(GrpcInterface::pollset_set_del_fd, 2),
    bind_this(GrpcInterface::is_any_background_poller_thread, 0),
    bind_this(GrpcInterface::shutdown_background_closure, 0),
    bind_this(GrpcInterface::shutdown_engine, 0),
    bind_this(GrpcInterface::add_closure_to_background_poller, 2)}; */
};
[[maybe_unused]] static const struct ::grpc_event_engine_vtable* grpc_init_nuiomgr(bool explicitly_requested) {
    return &s_vtable;
}

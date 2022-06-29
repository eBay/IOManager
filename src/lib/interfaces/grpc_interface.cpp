//
// Created by Kadayam, Hari on 2021-08-05.
//
extern "C" {
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/types.h>
}

#include "include/grpc_interface.hpp"
#include "include/reactor.hpp"
#include "include/iomgr.hpp"
#include <grpc_internal/src/core/lib/gpr/spinlock.h>
#include <grpc_internal/src/core/lib/gprpp/sync.h>

grpc_fd::grpc_fd(int fd, const char* name, bool track_err) : m_track_err(track_err) {
    m_read_closure.InitEvent();
    m_write_closure.InitEvent();
    m_error_closure.InitEvent();
}

grpc_fd::~grpc_fd() {
    m_read_closure.DestroyEvent();
    m_write_closure.DestroyEvent();
    m_error_closure.DestroyEvent();
}

void grpc_fd::add_to_pollset(const safe_grpc_pollset& pollset) {
    std::unique_lock lk(m_pollsets_mtx);
    m_pollsets.insert(pollset);
}

void grpc_fd::remove_from_pollset(const safe_grpc_pollset& pollset) {
    std::unique_lock lk(m_pollsets_mtx);
    m_pollsets.erase(pollset);
}

static bool append_grpc_error(grpc_error** composite, grpc_error* error, const char* desc) {
    if (error == GRPC_ERROR_NONE) return true;
    if (*composite == GRPC_ERROR_NONE) { *composite = GRPC_ERROR_CREATE_FROM_COPIED_STRING(desc); }
    *composite = grpc_error_add_child(*composite, error);
    return false;
}

namespace iomgr {
GrpcInterface::GrpcInterface() : IOInterface() {
    set_grpc_interface(this);
    ::grpc_register_event_engine_factory("nuiomgr", grpc_init_nuiomgr, true /* add_to_head */);
}

grpc_fd* GrpcInterface::fd_create(int fd, const char* name, bool track_err) {
    grpc_fd* gfd = new grpc_fd(fd, name, track_err);
    gfd->m_iodev = alloc_io_device(fd, EPOLLIN | EPOLLOUT, 9, (void*)this, thread_regex::all_io, nullptr);
    gfd->m_iodev->devname = fmt::format("{}, fd=", name, fd);

    {
        std::unique_lock l(m_fds_mtx);
        m_gfds.insert(safe_grpc_fd(gfd));
    }
    return gfd;
}

int GrpcInterface::fd_wrapped_fd(grpc_fd* gfd) { return gfd->m_iodev->fd(); }
void GrpcInterface::fd_notify_on_read(grpc_fd* gfd, grpc_closure* closure) { gfd->m_read_closure.NotifyOn(closure); }
void GrpcInterface::fd_notify_on_write(grpc_fd* gfd, grpc_closure* closure) { gfd->m_write_closure.NotifyOn(closure); }
void GrpcInterface::fd_notify_on_error(grpc_fd* gfd, grpc_closure* closure) { gfd->m_error_closure.NotifyOn(closure); }
void GrpcInterface::fd_set_readable(grpc_fd* gfd) { gfd->m_read_closure.SetReady(); }
void GrpcInterface::fd_set_writable(grpc_fd* gfd) { gfd->m_write_closure.SetReady(); }
void GrpcInterface::fd_set_error(grpc_fd* gfd) { gfd->m_error_closure.SetReady(); }
bool GrpcInterface::fd_is_shutdown(grpc_fd* gfd) { return gfd->m_read_closure.IsShutdown(); }

void GrpcInterface::pollset_init(grpc_pollset* pollset, gpr_mu** mu) {
    grpc_error* error{GRPC_ERROR_NONE};

    new ((void*)pollset) grpc_pollset();
    gpr_mu_init(&pollset->m_mu);
    *mu = &pollset->m_mu;

    // Create a new linux pollset
    pollset->m_epfd = epoll_create1(0);
    if (pollset->m_epfd < 1) {
        LOGDFATAL("Creating epoll fd for grpc pollset init failed, errno={} error={}", errno, strerror(errno));
        append_grpc_error(&error, GRPC_OS_ERROR(errno, "epoll_create1"), "pollset_init");
        goto done;
    }

    // Create a wrapper iodevice to add it to reactor later
    pollset->m_ep_iodev = alloc_io_device(
        pollset->m_epfd, EPOLLIN | EPOLLOUT, 9, (void*)pollset, thread_regex::all_io,
        [this](IODevice* iodev, void* cookie, int events) { process_pollset_events((grpc_pollset*)cookie, events); });

    // Add the pollset to the global grpc_interface list
    {
        std::unique_lock lk(m_pollsets_mtx);
        m_pollsets.insert(safe_grpc_pollset(pollset));
    }

done:
    GRPC_LOG_IF_ERROR("pollset_init", error);
}

void GrpcInterface::pollset_add_fd(grpc_pollset* pollset, grpc_fd* gfd) {
    grpc_error* error{GRPC_ERROR_NONE};
    {
        grpc_core::MutexLockForGprMu lock(&pollset->m_mu);

        struct epoll_event ev;
        ev.events = static_cast< uint32_t >(EPOLLET | EPOLLEXCLUSIVE | EPOLLIN | EPOLLOUT);
        ev.data.ptr = (void*)gfd;
        if (epoll_ctl(pollset->m_epfd, EPOLL_CTL_ADD, gfd->m_iodev->fd(), &ev) == -1) {
            if (errno == EEXIST) {
                LOGTRACEMOD(iomgr, "Adding grpc fd={} to epoll fd={} returned already exists error, ignoring",
                            gfd->m_iodev->fd(), pollset->m_epfd);
            } else {
                LOGDFATAL("Adding fd {} to this thread's epoll fd {} failed, error = {}", gfd->m_iodev->fd(),
                          pollset->m_epfd, strerror(errno));
                append_grpc_error(&error, GRPC_OS_ERROR(errno, "epoll_ctl"), "pollset_add_fd");
            }
            goto done;
        }
        pollset->m_gfds.insert(safe_grpc_fd(gfd));
    }
    gfd->add_to_pollset(safe_grpc_pollset(pollset));

done:
    GRPC_LOG_IF_ERROR("pollset_add_fd", error);
}

grpc_error* GrpcInterface::pollset_work(grpc_pollset* pollset, grpc_pollset_worker** ppworker, grpc_core::Timestamp deadline) {
    grpc_error* error{GRPC_ERROR_NONE};

    if (!iomanager.am_i_io_reactor()) { iomanager.become_user_reactor(INTERRUPT_LOOP | USER_CONTROLLED_LOOP); }

    // First time worker is operating on this pollset, then create/add worker (using threadbuffer) and add all
    // devices in pollset to this reactor
    if (pollset->m_workers->m_reactor == nullptr) {
        pollset->m_workers->m_reactor = iomanager.this_reactor();
        add_to_my_reactor(pollset->m_ep_iodev, iomanager.this_reactor()->select_thread());
    }

    // TODO: Set this differently for spdk and epoll, because for spdk a separate poller needs to
    // add for deadline and call
    iomanager.this_reactor()->set_poll_interval(deadline.milliseconds_after_process_epoch());
    iomanager.this_reactor()->listen_once();

    if (ppworker) { *ppworker = pollset->m_workers.get(); }
    return error;
}

/////////// Private methods /////////////////////////
#if 0
void GrpcInterface::init_iface_thread_ctx(const io_thread_t& thr) override {
    if (t_cur_pollset) {
        add_to_my_reactor(t_cur_pollset->m_epfd, thr);
        t_cur_pollset = nullptr;
    }
}
#endif

static constexpr size_t GRPC_MAX_EPOLL_EVENTS{100};

void GrpcInterface::process_pollset_events(grpc_pollset* pollset, [[maybe_unused]] int e) {
    struct epoll_event events[GRPC_MAX_EPOLL_EVENTS];
    int num_fds{0};
    do {
        num_fds = epoll_wait(pollset->m_epfd, &events[0], GRPC_MAX_EPOLL_EVENTS, 0);
    } while (num_fds < 0 && errno == EINTR);

    for (auto i = 0; i < num_fds; ++i) {
        auto& ev = events[i];
        grpc_fd* gfd = (grpc_fd*)ev.data.ptr;

        bool cancel = (ev.events & EPOLLHUP) != 0;
        bool error = (ev.events & EPOLLERR) != 0;
        bool read_ev = (ev.events & (EPOLLIN | EPOLLPRI)) != 0;
        bool write_ev = (ev.events & EPOLLOUT) != 0;
        bool err_fallback = error && !gfd->m_track_err;

        if (error && !err_fallback) { fd_set_error(gfd); }
        if (read_ev || cancel || err_fallback) { fd_set_readable(gfd); }
        if (write_ev || cancel || err_fallback) { fd_set_writable(gfd); }
    }
}
} // namespace iomgr

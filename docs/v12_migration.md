# Migrating from IOManager v12 to v13

v13 is a ground-up modernization of the I/O path. The Folly-futures + stackful-fiber model, the SPDK
and libaio backends, and the Pistache HTTP server are gone; the drive path is now C++20/23 stackless
coroutines driven by `io_uring`, and the public API is a small, opaque-handle surface.

This document lists what changed and shows before/after for the calls a consumer is most likely to use.

## At a glance

| Area | v12 | v13 |
|---|---|---|
| Async model | `folly::Future<std::error_code>` + `.thenValue()` | C++20/23 coroutines — `co_await` an `io_op` |
| Concurrency runtime | stackful fibers (boost / folly `FiberManager`) | stackless coroutines on [sisl](https://github.com/eBay/sisl) `async` + [NVIDIA stdexec](https://github.com/NVIDIA/stdexec) (P2300) |
| Drive backend(s) | libaio + `io_uring` + SPDK bdev / NVMe-oF | `io_uring` only (hard error if unavailable) |
| SPDK / DPDK | supported | **removed** |
| HTTP server | `iomgr::HttpServer` (Pistache-era) | `sisl::HttpServer` |
| Error type | `std::error_code` / `int` / `int64_t` | `io_result = std::expected<size_t, std::error_condition>` |
| Drive handle | raw `IODevice*` + `DriveInterface*` | opaque `drive_handle` (RAII) |
| Drive I/O | `DriveInterface::async_*` / `sync_*` methods | `iomgr::async_*` free functions returning `io_op` |
| Batching | `part_of_batch` flag + `submit_batch()` | **removed** (the scheduler batches submissions automatically) |
| Timers | `schedule_*_timer` + manual `cancel_timer` | RAII `timer_token` via `schedule_recurring` / `schedule_oneshot` |
| Public headers | `DriveInterface`/`IODevice`/`IOInterface` installed | internal; public surface is 7 headers (PIMPL) |

## Removed APIs

These no longer exist (or are no longer public). Most map onto a v13 replacement below.

- **Fibers:** `io_fiber_t`, `FiberManagerLib`, `fiber_regex`, `iomanager.iofiber_self()`,
  `am_i_sync_io_capable()`, `sync_io_capable_fibers()`. The reactor is now a single stackless coroutine
  context.
- **Synchronous drive I/O:** `DriveInterface::sync_write/sync_read/sync_writev/sync_readv/sync_write_zero`.
  v13's drive interface is async-only (see [Synchronous I/O](#synchronous-io) below).
- **Batching:** `bool part_of_batch` parameters and `submit_batch()`.
- **SPDK / NVMe-oF / libaio** device types and `backing_dev_t` variant arms.
- **Backend types out of the public surface:** `DriveInterface`, `IODevice`, `IOInterface`,
  `GenericIOInterface` are now internal (`src/lib/`), not installed headers. A handful of `IOManager`
  methods that took/returned those types (`add_interface`, `remove_interface`, `generic_interface`,
  `get_io_wd`) are now private.

## Opening and closing a drive

`DriveInterface::open_dev` returned a raw, manually-closed `io_device_ptr`. v13 returns an opaque,
reference-counted `drive_handle` that closes the device when the last copy is dropped. Operational
failures come back as `std::unexpected`; precondition bugs still throw.

```cpp
// v12
iomgr::io_device_ptr iodev = iomgr::DriveInterface::open_dev(devname, oflags);
iomgr::DriveInterface* iface = iodev->drive_interface();
// ... use iface + iodev.get() ...
iface->close_dev(iodev);                                   // manual close

// v13
auto opened = iomgr::open_drive(devname, oflags);          // std::expected<drive_handle, std::error_condition>
if (!opened) { /* opened.error() */ }
iomgr::drive_handle drv = std::move(opened.value());
// ... use drv ...
drv.reset();                                               // RAII close (last handle drop). Release before iomanager::stop().
```

Device queries moved from `DriveInterface` statics to free functions:

| v12 | v13 |
|---|---|
| `DriveInterface::get_attributes(name)` | `iomgr::attributes_of(name)` |
| `DriveInterface::get_drive_type(name)` | `iomgr::type_of(name)` |
| `DriveInterface::get_size(iodev.get())` | `iomgr::size_of(drv)` |

## Issuing I/O

```cpp
// v12 — folly future, error_code, manual chaining
iface->async_write(iodev.get(), data, size, offset, /*part_of_batch=*/false)
     .thenValue([this](std::error_code ec) {
         if (ec) { /* fail */ }
         else    { /* ok   */ }
     });

// v13a — co_await inside a coroutine (linear control flow)
iomgr::io_result r = co_await iomgr::async_write(drv, data, size, offset);
if (!r) { /* r.error() is a std::error_condition */ }
else    { /* *r == bytes written */ }

// v13b — detach() bridges to a callback for non-coroutine callers (e.g. a folly::Future shim)
iomgr::detach(iomgr::async_write(drv, data, size, offset), [](iomgr::io_result r) {
    if (!r) { /* r.error() */ }
});
```

> ⚠️ **Inverted truthiness.** `std::error_code` is truthy on *failure*; `io_result` is truthy on
> *success*. `if (ec)` (error) becomes `if (!r)` (error), and the success value is the byte count.

The full set of free functions: `async_write`, `async_writev`, `async_read`, `async_readv`,
`async_unmap`, `async_write_zero`, `queue_fsync` — each takes a `const drive_handle&` and returns an
`io_op`.

### Synchronous I/O

v13's drive interface is async-only, so the v12 `sync_*` methods are gone. Until synchronous free
functions are added to the public surface, run an async op to completion where you need a blocking
call — e.g. for a cold-path superblock write, `detach()` the op and wait on the callback, or `co_await`
it from a reactor coroutine.

## Timers

```cpp
// v12 — raw handle, manual cancel (leaks if you forget)
iomgr::timer_handle_t h = iomanager.schedule_thread_timer(
    nanos, /*recurring=*/true, /*cookie=*/nullptr, [](void*) { tick(); });
// ... later, in a destructor or teardown path ...
iomanager.cancel_timer(h, /*wait=*/true);

// v13 — RAII token; the timer is cancelled when the token is dropped
iomgr::timer_token h = iomgr::schedule_recurring(std::chrono::nanoseconds{nanos}, [] { tick(); });
// global scope across reactors:
iomgr::timer_token g = iomgr::schedule_recurring(1s, iomgr::reactor_regex::all_worker, [] { tick(); });
// one-shot fire-and-forget (self-removes on fire — nothing to cancel):
iomgr::schedule_oneshot(5ms, [] { retry(); });
```

Store the `timer_token` as a member instead of a `timer_handle_t`; dropping it (or calling
`h.cancel()`) replaces the manual `cancel_timer`. The callback is now `std::function<void()>` — capture
any state you used to pass through the `void* cookie`.

> The low-level `schedule_thread_timer` / `schedule_global_timer` / `cancel_timer` methods still exist as
> the primitive that `timer.hpp` wraps, but new code should prefer the RAII API.

## HTTP server

```cpp
// v12
std::shared_ptr< iomgr::HttpServer > srv = ioenvironment.get_http_server();

// v13
std::shared_ptr< sisl::HttpServer > srv = ioenvironment.get_http_server();
```

`iomgr::HttpServer` (and `<iomgr/http_server.hpp>`) were a Pistache-era alias; use `sisl::HttpServer`
directly.

## What stayed the same

Reactor bring-up and the cross-reactor dispatch surface are unchanged:
`ioenvironment.with_iomgr(iomgr_params{...})`, `iomanager.stop()`, `iomanager.create_reactor(...)`,
`run_on_forget` / `run_on_wait`, the `iobuf_alloc` / `iobuf_free` aligned allocator, the `am_i_*_reactor`
predicates, and `reactor_regex` / `loop_type_t` scoping.

## Build changes

- **C++23** is required (for coroutines + the stdexec layer). Toolchains: GCC 13+, Clang 17+.
- `prepare.sh` is gone — build with `conan build -s:h build_type=Debug --build missing .` (see the
  [README](../README.md#-quick-start)).
- Dependencies: **sisl v14+** (now also the coroutine substrate) and **liburing**; **stdexec** is fetched
  via CMake FetchContent. SPDK, DPDK, libaio and Pistache are no longer dependencies.

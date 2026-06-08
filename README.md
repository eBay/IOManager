# IOManager

[![Conan Build](https://github.com/eBay/IOManager/actions/workflows/merge_build.yml/badge.svg?branch=dev%2Fv13.x)](https://github.com/eBay/IOManager/actions/workflows/merge_build.yml)
[![CodeCov](https://codecov.io/gh/szmyd/IOManager/branch/stable%2Fv4.x/graph/badge.svg)](https://codecov.io/gh/szmyd/IOManager)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> An asynchronous I/O reactor for Linux storage applications — C++20/23 coroutines over `io_uring`, with an epoll / tight-loop hybrid reactor.

IOManager multiplexes storage and event sources onto a small pool of reactor threads and lets an
application do async I/O **on the reactor threads themselves — non-blocking, never handed off to a separate thread pool.** As
of **v13** the drive path is built on C++20/23 stackless coroutines driven by `io_uring`; the older
Folly-futures, stackful-fiber, SPDK and libaio machinery has been removed.

> ⬆️ **Upgrading from v12?** See [docs/v12_migration.md](docs/v12_migration.md) for the full change
> list and before/after code.

## 🚀 Features

- **Hybrid reactor model** — interrupt (epoll) and tight-loop (polling) reactors run side by side, plus
  an *adaptive* mode that backs off to interrupt mode under no load.
- **Coroutine I/O** — drive operations are awaitables; `co_await` returns an `io_result`. Built on the
  [sisl](https://github.com/eBay/sisl) `async` substrate (`exec::task` driven over its `io_uring_scheduler`) and
  [NVIDIA stdexec](https://github.com/NVIDIA/stdexec) (P2300 sender/receiver).
- **`io_uring` drive backend** — the single, lock-free-submission drive backend; a per-loop reactor
  *sentinel* flushes queued SQEs and reaps CQEs, resuming suspended coroutines with no thread hops.
- **RAII everywhere** — `drive_handle` closes the device on last reference; `timer_token` cancels its
  timer when dropped; no manual `close_dev`/`cancel_timer` bookkeeping.
- **One error type** — `io_result = std::expected<size_t, std::error_condition>` across the public API.
- **Minimal PIMPL surface** — 7 installed headers; backend types (`DriveInterface`, `IODevice`, …) are
  internal and never leak into consumer builds.
- **Worker & user reactors** — iomgr-managed worker threads plus the ability for an application thread to
  *become* a reactor; cross-reactor work via `run_on_forget` / `run_on_wait`.
- **Aligned I/O buffers, per-thread metrics, and an I/O watchdog**, validated under ASan/TSan.

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [Reactor Model](#-reactor-model)
- [Asynchronous I/O](#-asynchronous-io)
- [Usage](#-usage)
- [Development](#-development)
- [Testing](#-testing)
- [Dependencies](#-dependencies)
- [Documentation](#-documentation)
- [License](#-license)

## 🏃 Quick Start

### Prerequisites

- Linux kernel with `io_uring` (5.6+)
- Conan 2.x (recipe also supports 1.60+)
- CMake 3.22+
- C++23 compiler (GCC 13+, Clang 17+)
- The [sisl](https://github.com/eBay/sisl) recipe (`sisl/<ver>@oss/dev`) available in your Conan cache
  or a configured remote

### Build & Test

```bash
git clone https://github.com/eBay/IOManager
cd IOManager
conan build -s:h build_type=Debug --build missing .
# the build runs the ctest suite automatically
```

To create the package for downstream consumers:

```bash
conan create -s:h build_type=Release --build missing .
```

### Build Options

```bash
# Release
conan build -s:h build_type=Release --build missing .

# Address / thread sanitizer
conan build -s:h build_type=Debug -o iomgr/*:sanitize=address --build missing .
conan build -s:h build_type=Debug -o iomgr/*:sanitize=thread  --build missing .

# Coverage
conan build -s:h build_type=Debug -o iomgr/*:coverage=True --build missing .

# Test scope: full | epoll_mode (default) | off
conan build -s:h build_type=Debug -o iomgr/*:testing=full --build missing .
```

## 🏗️ Architecture

```
IOManager/
├── src/
│   ├── include/iomgr/        # Public headers (installed)
│   │   ├── io_environment.hpp  # IOEnvironment: process bring-up (with_iomgr, HTTP, auth, ...)
│   │   ├── iomgr.hpp           # IOManager singleton: reactors, run_on_*, iobuf, lifecycle
│   │   ├── iomgr_types.hpp     # shared enums/aliases (reactor_regex, loop_type_t, drive_type)
│   │   ├── drive.hpp           # drive_handle + open/query/async I/O free functions
│   │   ├── io_op.hpp           # io_op awaitable + io_result
│   │   ├── timer.hpp           # RAII timer_token + schedule_recurring / schedule_oneshot
│   │   └── iomgr_flip.hpp      # fault-injection (flip) timer hooks
│   ├── lib/                  # Internal implementation (NOT installed)
│   │   ├── reactor/            # IOReactor event loop + per-thread state
│   │   ├── epoll/              # interrupt (epoll) reactor
│   │   └── interfaces/         # io_uring drive backend + generic device interface
│   └── test/                 # GoogleTest suites
├── CMakeLists.txt
└── conanfile.py
```

```text
   reactor threads (1 per system thread)
   ├─ interrupt (epoll)
   ├─ tight-loop / adaptive
   └─ user-controlled
        │
        │   each loop a sentinel runs io_uring poll_once:
        │   flush queued SQEs, reap CQEs, resume the awaiting coroutines
        ▼
   IOManager
   ├─ io_uring drive backend ──── async_* → io_op  (CQE-driven coroutines)
   └─ generic interface ───────── eventfd · generic fds · sockets · timers
        │
        ▼
   block devices / files (io_uring) · eventfd · generic fds · sockets
```

### Core Abstractions

| Type / entry point | Role |
|---|---|
| `ioenvironment.with_iomgr(...)` | Bring up the reactor pool (wraps `IOManager::start`) |
| `IOManager` (via the `iomanager` macro) | Reactor lifecycle, cross-reactor dispatch, I/O buffers |
| `IOReactor` | A single event-loop thread (interrupt, tight-loop, or adaptive) |
| `drive_handle` | Opaque, RAII handle to an open drive (`open_drive(...)`) |
| `io_op` | Awaitable for one async drive operation; `co_await` → `io_result` |
| `timer_token` | RAII handle for a recurring timer |

## ⚙️ Reactor Model

An **IO Reactor** is an event loop that owns one system thread and multiplexes every device and event
source assigned to it. Devices on the same reactor interact without locks. The loop mode is a
`loop_type_t` bitmask; interrupt and tight-loop reactors can run side by side in one process:

- **Interrupt loop** (`INTERRUPT_LOOP`) — sleeps in `epoll` until the kernel signals an event. Low CPU,
  ideal for control-plane and connection multiplexing.
- **Tight loop** (`TIGHT_LOOP`) — never sleeps; spins polling its sources for the lowest latency and
  highest per-core throughput, at the cost of a busy core.
- **Adaptive** (`ADAPTIVE_LOOP`) — polls under load and backs off toward interrupt mode when idle, to
  recover the CPU a pure tight loop would burn.
- **User-controlled** (`USER_CONTROLLED_LOOP`) — the application drives polling on its own cadence.

Reactors are further split into **worker** reactors (created and owned by IOManager at `start`) and
**user** reactors (an application thread that *becomes* a reactor via `become_user_reactor` / is created
with `create_reactor`, and can later relinquish that role).

The `io_uring` drive backend hooks each reactor with a lightweight **sentinel** callback: once per loop
iteration it flushes any SQEs queued since the last pass and reaps completed CQEs, resuming the
suspended coroutines that were awaiting them — all on the reactor thread, with no executor queue or
thread hop.

## 🧬 Asynchronous I/O

Drive I/O is expressed as coroutines. Each `async_*` call returns an `io_op` you can `co_await` (it
yields an `io_result`), or hand to `detach()` to bridge into non-coroutine code:

```cpp
#include <iomgr/drive.hpp>     // open_drive, async_*, drive_handle
#include <iomgr/io_op.hpp>     // io_op, io_result, detach

using namespace iomgr;

// In a coroutine: read, then write the same block elsewhere — linear control flow, no callbacks.
SomeTask copy_block(drive_handle src, drive_handle dst, char* buf, uint64_t off) {
    if (io_result r = co_await async_read(src, buf, 4096, off); !r) {
        co_return; // r.error() is a std::error_condition
    }
    co_await async_write(dst, buf, 4096, off);
}

// From non-coroutine code: detach() runs the op and invokes the callback on the completing reactor.
detach(async_write(drv, buf, 4096, off), [](io_result r) {
    if (!r) { /* handle r.error() */ }
});
```

The underlying coroutine / `io_uring` scheduler machinery (sisl's `io_uring_scheduler` driving `exec::task`,
stdexec senders) is fully hidden behind the `io_op` PIMPL — consumers never see, or need to depend on, stdexec.

## 🖥️ Usage

```cpp
#include <iomgr/io_environment.hpp>
#include <iomgr/iomgr.hpp>
#include <iomgr/drive.hpp>
#include <iomgr/timer.hpp>
#include <fcntl.h>

using namespace iomgr;
using namespace std::chrono_literals;

int main() {
    // Start one worker reactor.
    ioenvironment.with_iomgr(iomgr_params{.num_threads = 1});

    // open_drive returns std::expected; the handle closes the device when the last copy drops.
    auto drv = open_drive("/dev/nvme0n1", O_RDWR | O_DIRECT).value();
    const auto attr = attributes_of("/dev/nvme0n1");

    auto* buf = iomanager.iobuf_alloc(attr.align_size, 4096);

    // Fire an async write; free the buffer on completion.
    detach(async_write(drv, reinterpret_cast< const char* >(buf), 4096, 0), [buf](io_result) {
        iomanager.iobuf_free(buf);
    });

    // Recurring work on a reactor — cancelled automatically when the token is dropped.
    timer_token tick = schedule_recurring(1s, reactor_regex::all_worker, [] { /* periodic */ });

    // ... application runs ...

    drv.reset();        // RAII close, before stop()
    iomanager.stop();
}
```

## 🛠️ Development

### Code Style

- **Indentation:** 4 spaces  ·  **Line length:** 120  ·  **Pointers:** left-aligned (`Type* p`)
- **Standard:** C++23  ·  **Headers:** `#pragma once`
- Run `./apply-clang-format.sh` before submitting.

### Naming Conventions

| Element | Convention | Example |
|---|---|---|
| **Public I/O API** (`drive.hpp`, `io_op.hpp`, `timer.hpp`) | `lower_snake_case` | `drive_handle`, `io_op`, `io_result`, `open_drive`, `async_write`, `timer_token` |
| **Singleton access** | lowercase macros | `iomanager.run_on_forget(...)`, `ioenvironment.with_iomgr(...)` |
| **Internal classes** (`src/lib/`, not installed) | `PascalCase` | `IOReactor`, `DriveInterface`, `IODevice`, `GenericIOInterface` |
| Functions / methods | `snake_case` | `async_read`, `run_on_forget`, `schedule_recurring` |
| Members | `m_snake_case` | `m_reactors`, `m_num_workers` |

The new v13 I/O surface is uniformly `lower_snake_case`; the legacy `IOManager` core retains its
historical `PascalCase` and is reached through the `iomanager` / `ioenvironment` macros.

### Error Handling

The public surface uses one error type — bytes transferred on success, a `std::error_condition` on
failure; exceptions are reserved for precondition bugs.

```cpp
using io_result = std::expected< std::size_t, std::error_condition >;

io_result r = co_await async_read(drv, buf, len, off);
if (!r) {
    LOGERROR("read failed at {}: {}", off, r.error().message());
    // ... handle / propagate r.error() ...
}
```

## 🧪 Testing

Tests are GoogleTest suites under `src/test/` and run automatically as part of `conan build`. They
cover the drive path (`test_drive`, `test_write_zero`, including off-reactor completion), timers
(`test_timer`, including the `timer_token` RAII lifecycle), and messaging.

```bash
# Build + run the suite
conan build -s:h build_type=Debug --build missing .

# Under sanitizers
conan build -s:h build_type=Debug -o iomgr/*:sanitize=address --build missing .
conan build -s:h build_type=Debug -o iomgr/*:sanitize=thread  --build missing .
```

## 📦 Dependencies

### Core

- **[sisl](https://github.com/eBay/sisl)** (v14+) — logging, options, metrics, HTTP server, and the
  `async` coroutine substrate (`exec::task` and the `io_uring_scheduler` io_uring CQE bridge).
- **liburing** (2.1+) — `io_uring` access (Linux only).
- **[NVIDIA stdexec](https://github.com/NVIDIA/stdexec)** — P2300 sender/receiver; the structured-concurrency
  layer the drive path composes on (provided transitively via the sisl conan package).

### Test / Tooling

- **gtest**, **cpr** — test dependencies.
- **Conan** 2.x (1.60+ supported), **CMake** 3.22+, **GCC 13+ / Clang 17+**, **clang-format**.

## 📚 Documentation

- **[docs/v12_migration.md](docs/v12_migration.md)** — upgrading from IOManager v12 to v13.
- **[CHANGELOG.md](CHANGELOG.md)** — version history.

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).

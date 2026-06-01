/************************************************************************
 * Copyright 2017-2019 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **************************************************************************/
#pragma once

// INTERNAL launcher: starts a drive exec::task detached and invokes on_done(error_code) on completion.
// Kept out of the installed/public headers so stdexec does not leak to consumers -- the public surface
// is iomgr/io_op.hpp (an opaque awaitable that calls into this).

#include <memory>
#include <system_error>
#include <utility>

#include <stdexec/execution.hpp>
#include <exec/task.hpp>
#include <exec/inline_scheduler.hpp>

#include <iomgr/iomgr.hpp>

namespace iomgr::detail {

// A void-returning wrapper coroutine that awaits the drive task and hands the result to on_done. Using
// a plain coroutine (rather than a then/upon_* sender chain) keeps the sender graph shallow, avoiding a
// GCC coroutine-frame instantiation bug in stdexec's __connect_awaitable. The co_await is guarded: the
// scheduler's async_submit can complete on the ERROR channel (e.g. SQ exhaustion throws), which would
// otherwise reach start_detached's receiver and call std::terminate(); map it to an error_code so
// on_done always runs and the "completes only on the value channel" invariant holds.
template < typename OnDone >
exec::task< void > drive_wrapper(exec::task< std::error_code > task, OnDone on_done) {
    std::error_code ec;
    try {
        ec = co_await std::move(task);
    } catch (const std::system_error& e) { ec = e.code(); } catch (...) {
        ec = std::make_error_code(std::errc::io_error);
    }
    on_done(ec);
}

// write_env injects an inline scheduler so the sticky-affinity exec::task can be started without an
// enclosing scheduler context (it resumes inline on whatever reactor thread drives poll_once).
// start_detached owns the operation-state (heap, freed on completion).
template < typename OnDone >
void start_drive_task(exec::task< std::error_code > task, OnDone on_done) {
    stdexec::start_detached(stdexec::write_env(drive_wrapper(std::move(task), std::move(on_done)),
                                               stdexec::prop{stdexec::get_scheduler, exec::inline_scheduler{}}));
}

// Carries a move-only (task, on_done) across the off-reactor -> worker thread hop. Self-cleaning: if the
// marshalling message is dropped (target reactor shutting down) the lambda never runs, but the holder's
// destructor still fires, so on_done is invoked with a cancellation error instead of being stranded.
template < typename OnDone >
struct task_holder {
    exec::task< std::error_code > task;
    OnDone on_done;
    bool started{false};
    ~task_holder() {
        if (!started) { on_done(std::make_error_code(std::errc::operation_canceled)); }
    }
};

// Launch a drive exec::task detached; invoke on_done(result) on completion. The scheduler is
// reactor-local, so an off-reactor caller is marshalled onto a worker reactor and the task STARTED
// there. on_done runs on the reactor thread that reaped the completion -- keep it short.
template < typename OnDone >
void launch_drive_task(exec::task< std::error_code > task, OnDone on_done) {
    if (iomanager.this_reactor() != nullptr) {
        start_drive_task(std::move(task), std::move(on_done));
    } else {
        auto holder = std::make_shared< task_holder< OnDone > >(std::move(task), std::move(on_done));
        iomanager.run_on_forget(reactor_regex::random_worker, [holder]() {
            holder->started = true;
            start_drive_task(std::move(holder->task), std::move(holder->on_done));
        });
    }
}

} // namespace iomgr::detail

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

#include <new>
#include <system_error>
#include <utility>

#include <stdexec/execution.hpp>
#include <exec/task.hpp>
#include <exec/inline_scheduler.hpp>

#include <iomgr/iomgr.hpp>

namespace iomgr {
namespace detail {

// A void-returning wrapper coroutine that awaits the drive task and hands the result to on_done. The
// drive backends co_return the error_code on the value channel (failures included), so awaiting it is
// sufficient; using a plain coroutine here (rather than a then/upon_* sender chain) keeps the sender
// graph shallow, which avoids a GCC coroutine-frame instantiation bug in stdexec's __connect_awaitable.
//
// The co_await is guarded: the scheduler's async_submit can complete on the ERROR channel (e.g. SQ
// exhaustion throws std::system_error), which would otherwise propagate to start_detached's receiver
// and call std::terminate(). Map any such exception to an error_code so on_done always runs and the
// launcher's "completes only on the value channel" invariant holds.
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

// Start the wrapper detached. write_env injects an inline scheduler so the sticky-affinity exec::task
// can be started without an enclosing scheduler context (it resumes inline on whatever reactor thread
// drives poll_once). start_detached owns the operation-state (heap, freed on completion).
template < typename OnDone >
void start_drive_task(exec::task< std::error_code > task, OnDone on_done) {
    stdexec::start_detached(stdexec::write_env(drive_wrapper(std::move(task), std::move(on_done)),
                                               stdexec::prop{stdexec::get_scheduler, exec::inline_scheduler{}}));
}

// Carries a move-only (task, on_done) across the off-reactor -> worker thread hop. Self-cleaning: if the
// marshalling message is dropped (target reactor shutting down) the lambda never runs, but the holder's
// destructor still fires (the message frees its captured shared_ptr), so on_done is invoked with a
// cancellation error instead of being silently stranded -- and nothing leaks.
template < typename OnDone >
struct task_holder {
    exec::task< std::error_code > task;
    OnDone on_done;
    bool started{false};
    ~task_holder() {
        if (!started) { on_done(std::make_error_code(std::errc::operation_canceled)); }
    }
};

} // namespace detail

// Launch a drive `exec::task<std::error_code>` detached; invoke `on_done(result)` when it completes.
// The drive backends' scheduler is reactor-local, so an off-reactor caller is marshalled onto a worker
// reactor and the task is STARTED there (the task reads the reactor-local channel inside its body, so
// it must run on a reactor). `on_done` runs on the reactor thread that reaped the completion -- keep it
// short, or re-dispatch heavy work off the reactor.
template < typename OnDone >
void detach(exec::task< std::error_code > task, OnDone on_done) {
    if (iomanager.this_reactor() != nullptr) {
        detail::start_drive_task(std::move(task), std::move(on_done));
    } else {
        // exec::task and the closure are move-only; carry them across the thread hop via a shared holder.
        // A shared_ptr (not a raw new) is captured by-value into the run_on_forget lambda so the holder
        // is freed whether the message runs OR is dropped during shutdown; on the dropped path the
        // holder's destructor invokes on_done with a cancellation error (see task_holder).
        auto holder = std::make_shared< detail::task_holder< OnDone > >(std::move(task), std::move(on_done));
        iomanager.run_on_forget(reactor_regex::random_worker, [holder]() {
            holder->started = true;
            detail::start_drive_task(std::move(holder->task), std::move(holder->on_done));
        });
    }
}

} // namespace iomgr

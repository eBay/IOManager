/************************************************************************
 * Copyright 2026 eBay Inc.
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

// RAII timer wrapper over IOManager's schedule_*_timer / cancel_timer. timer_token owns a recurring
// timer's handle and cancels it on destruction; schedule_recurring() is the only producer of an
// active token. One-shot timers self-remove on fire, so schedule_oneshot() simply drops the handle.

#include <cstdint>

#include <iomgr/timer.hpp>
#include <iomgr/iomgr.hpp>

namespace iomgr {

namespace {
// Adapt a no-arg timer_callback to IOManager's void(void*) cookie-style callback.
inline timer_callback_t adapt(timer_callback&& cb) {
    return [cb = std::move(cb)](void*) { cb(); };
}
inline uint64_t to_nanos(std::chrono::nanoseconds d) { return static_cast< uint64_t >(d.count()); }
} // namespace

timer_token::timer_token(timer_token&& o) noexcept : _h{std::move(o._h)} { o._h.reset(); }

timer_token& timer_token::operator=(timer_token&& o) noexcept {
    if (this != &o) {
        cancel(); // release any timer we currently own before taking the new one
        _h = std::move(o._h);
        o._h.reset();
    }
    return *this;
}

timer_token::~timer_token() { cancel(); }

void timer_token::cancel(bool wait) noexcept {
    if (_h) {
        iomanager.cancel_timer(_h, wait);
        _h.reset();
    }
}

timer_token schedule_recurring(std::chrono::nanoseconds interval, timer_callback cb) {
    return timer_token{iomanager.schedule_thread_timer(to_nanos(interval), true /* recurring */, nullptr /* cookie */,
                                                       adapt(std::move(cb)))};
}

timer_token schedule_recurring(std::chrono::nanoseconds interval, reactor_regex scope, timer_callback cb,
                               bool wait_to_schedule) {
    return timer_token{iomanager.schedule_global_timer(to_nanos(interval), true /* recurring */, nullptr /* cookie */,
                                                       scope, adapt(std::move(cb)), wait_to_schedule)};
}

void schedule_oneshot(std::chrono::nanoseconds after, timer_callback cb) {
    // The returned handle is intentionally dropped: a one-shot fires once and pops itself from the
    // timer heap, so there is nothing to cancel and dropping the handle has no side effect.
    (void)iomanager.schedule_thread_timer(to_nanos(after), false /* recurring */, nullptr /* cookie */,
                                          adapt(std::move(cb)));
}

void schedule_oneshot(std::chrono::nanoseconds after, reactor_regex scope, timer_callback cb, bool wait_to_schedule) {
    (void)iomanager.schedule_global_timer(to_nanos(after), false /* recurring */, nullptr /* cookie */, scope,
                                          adapt(std::move(cb)), wait_to_schedule);
}

} // namespace iomgr

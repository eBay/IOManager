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
#pragma once

// RAII timer API.
//
// A *recurring* timer is owned by a move-only timer_token; it is cancelled when the token is
// destroyed (or overwritten by move-assignment). The recurring timer therefore cannot outlive the
// object that owns it -- this replaces the old schedule_*_timer()/cancel_timer() pair whose cleanup
// obligation lived only in a comment and leaked on any early return.
//
// A *one-shot* timer fires once and then removes itself, so there is nothing to own or cancel:
// schedule_oneshot() is fire-and-forget. (Cancelling a one-shot after it has fired would be a
// use-after-free, which is exactly why it does not hand back a cancellable handle.)

#include <chrono>
#include <functional>
#include <memory>

#include <iomgr/iomgr_types.hpp> // reactor_regex

namespace iomgr {

// Invoked when the timer fires. Capture any state you need; there is no cookie parameter.
using timer_callback = std::function< void() >;

class [[nodiscard]] timer_token {
public:
    timer_token() noexcept = default; // an inactive token (active() == false)
    timer_token(timer_token&&) noexcept;
    timer_token& operator=(timer_token&&) noexcept;
    timer_token(const timer_token&) = delete;
    timer_token& operator=(const timer_token&) = delete;
    ~timer_token(); // cancels the timer if still active

    // Cancel the timer now; idempotent. `wait` blocks until any in-flight callback has returned
    // (use it when the callback touches state about to be destroyed). After cancel(), active() is false.
    void cancel(bool wait = false) noexcept;

    bool active() const noexcept { return static_cast< bool >(_h); }
    explicit operator bool() const noexcept { return active(); }

private:
    // _h is an iomgr::timer_handle_t (std::shared_ptr<void>); null when inactive.
    explicit timer_token(std::shared_ptr< void > h) noexcept : _h{std::move(h)} {}
    friend timer_token schedule_recurring(std::chrono::nanoseconds, timer_callback);
    friend timer_token schedule_recurring(std::chrono::nanoseconds, reactor_regex, timer_callback, bool);
    std::shared_ptr< void > _h;
};

// ----- recurring timers (RAII: cancelled when the returned token is dropped) -----------------------
// On the CURRENT reactor: fires every `interval`, callback runs on this reactor thread.
timer_token schedule_recurring(std::chrono::nanoseconds interval, timer_callback cb);
// Across every reactor matching `scope`. If `wait_to_schedule`, blocks until armed on all of them.
timer_token schedule_recurring(std::chrono::nanoseconds interval, reactor_regex scope, timer_callback cb,
                               bool wait_to_schedule = false);

// ----- one-shot timers (fire-and-forget: fires once after `after`, then self-removes) -------------
// On the CURRENT reactor.
void schedule_oneshot(std::chrono::nanoseconds after, timer_callback cb);
// Across every reactor matching `scope`.
void schedule_oneshot(std::chrono::nanoseconds after, reactor_regex scope, timer_callback cb,
                      bool wait_to_schedule = false);

} // namespace iomgr

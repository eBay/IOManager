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

// liburing's inline io_uring_prep_openat2() does `sizeof(struct open_how)`, so <liburing.h> must be
// parsed with a COMPLETE struct open_how. liburing gets it from <linux/openat2.h>, but on some
// toolchains that header is not transitively pulled in before <liburing.h> (notably Ubuntu 22.04 /
// gcc 13), leaving open_how incomplete -> "invalid application of sizeof to incomplete type".
//
// Include this header BEFORE every <liburing.h>: it pulls the real <linux/openat2.h> when present
// (whose own include guard makes any later inclusion a no-op), and only hand-rolls the struct on the
// rare system that lacks the header entirely. (stable/v12.x carried an unconditional definition of
// this struct; this is the same workaround, guarded so it can't collide with the kernel header.)

#include <cstdint>

#if __has_include(<linux/openat2.h>)
#include <linux/openat2.h>
#endif

// RESOLVE_NO_MAGICLINKS is defined by <linux/openat2.h> in the same revision that defines open_how,
// so its absence means we did not get the struct from the kernel headers and must supply it.
#ifndef RESOLVE_NO_MAGICLINKS
struct open_how {
    uint64_t flags;
    uint64_t mode;
    uint64_t resolve;
};
#endif

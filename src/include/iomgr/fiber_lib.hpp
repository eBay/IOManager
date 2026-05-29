#pragma once

namespace iomgr {
class IOReactor;

// Lightweight stub representing a single execution context within a reactor.
// After the Boost.Fiber removal, each reactor has exactly one IOFiber (ordinal 0)
// that corresponds to its OS thread. The ordinal and reactor pointer are retained
// for API compatibility with callers that hold io_fiber_t handles.
struct IOFiber {
    IOReactor* reactor{nullptr};
    uint32_t ordinal{0};

    IOFiber() = default;
    IOFiber(IOReactor* r, uint32_t o) : reactor{r}, ordinal{o} {}
};

} // namespace iomgr

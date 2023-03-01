#pragma once
#include <sisl/fds/buffer.hpp>

namespace iomgr {
struct IOMgrAlignedAllocImpl : public sisl::AlignedAllocatorImpl {
    uint8_t* aligned_alloc(size_t align, size_t sz, const sisl::buftag tag) override;
    void aligned_free(uint8_t* b, const sisl::buftag tag) override;
    uint8_t* aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz = 0) override;
};
} // namespace iomgr
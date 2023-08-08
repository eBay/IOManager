#include <cstdint>
#include <iomgr/iomgr.hpp>
#include <sisl/fds/malloc_helper.hpp>
#include "epoll/epoll_mem.hpp"
#include "iomgr_config.hpp"

namespace iomgr {

uint8_t* IOMgrAlignedAllocImpl::aligned_alloc(size_t align, size_t size, const sisl::buftag tag) {
    return sisl::AlignedAllocatorImpl::aligned_alloc(align, size, tag);
}

void IOMgrAlignedAllocImpl::aligned_free(uint8_t* b, const sisl::buftag tag) {
    sisl::AlignedAllocatorImpl::aligned_free(b, tag);

    static std::atomic< uint64_t > num_frees{0};
    if (((num_frees.fetch_add(1, std::memory_order_relaxed) + 1) % IM_DYNAMIC_CONFIG(iomem.limit_check_freq)) == 0) {
        sisl::release_mem_if_needed(iomanager.soft_mem_threshold(), iomanager.aggressive_mem_threshold());
    }
}

uint8_t* IOMgrAlignedAllocImpl::aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz) {
    return sisl::AlignedAllocatorImpl::aligned_realloc(old_buf, align, new_sz, old_sz);
}
} // namespace iomgr
#include <iomgr/iomgr.hpp>
#include "mempool_spdk.hpp"

extern "C" {
#include <spdk/env_dpdk.h>
#include <spdk/env.h>
#include <spdk/init.h>
#include <spdk/rpc.h>
#include <rte_errno.h>
#include <rte_mempool.h>
#include <rte_malloc.h>
}

namespace iomgr {
////////////////////////////// Spdk Memory Allocator section ///////////////////////////////
SpdkAlignedAllocImpl::SpdkAlignedAllocImpl(cshared< SpdkMemPool >& pool) : sisl::AlignedAllocatorImpl(), m_pool{pool} {}

uint8_t* SpdkAlignedAllocImpl::aligned_alloc(size_t align, size_t size, [[maybe_unused]] const sisl::buftag tag) {
    auto buf = (uint8_t*)spdk_malloc(size, align, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
#ifdef _PRERELEASE
    sisl::AlignedAllocator::metrics().increment(tag, buf_size(buf));
#endif
    return buf;
}

void SpdkAlignedAllocImpl::aligned_free(uint8_t* b, [[maybe_unused]] const sisl::buftag tag) {
#ifdef _PRERELEASE
    sisl::AlignedAllocator::metrics().decrement(tag, buf_size(b));
#endif
    spdk_free(b);
}

uint8_t* SpdkAlignedAllocImpl::aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz) {
#ifdef _PRERELEASE
    sisl::AlignedAllocator::metrics().increment(sisl::buftag::common, new_sz - old_sz);
#endif
    return static_cast< uint8_t* >(spdk_realloc((void*)old_buf, new_sz, align));
}

uint8_t* SpdkAlignedAllocImpl::aligned_pool_alloc(const size_t align, const size_t sz, const sisl::buftag tag) {
    auto buf = static_cast< uint8_t* >(spdk_mempool_get(m_pool->get_mempool(sz)));
#ifdef _PRERELEASE
    if (buf) { sisl::AlignedAllocator::metrics().increment(tag, sz); }
#endif
    return buf;
}

void SpdkAlignedAllocImpl::aligned_pool_free(uint8_t* const b, const size_t sz, const sisl::buftag tag) {
#ifdef _PRERELEASE
    sisl::AlignedAllocator::metrics().decrement(tag, sz);
#endif
    RELEASE_ASSERT_NOTNULL((void*)b, "buffer is null while freeing");
    spdk_mempool_put(m_pool->get_mempool(sz), b);
}

size_t SpdkAlignedAllocImpl::buf_size(uint8_t* buf) const {
    size_t sz;
    [[maybe_unused]] const auto ret{rte_malloc_validate(buf, &sz)};
    assert(ret != -1);
    return sz;
}

////////////////////////////// Mempool section ///////////////////////////////
SpdkMemPool::SpdkMemPool() {
    for (size_t i{0}; i < max_mempool_count; ++i) {
        m_internal_pools[i] = nullptr;
    }
}

void SpdkMemPool::register_metrics(struct rte_mempool* mp) {
    std::string name = mp->name;
    std::unique_lock lg{m_mset_mtx};
    m_mempool_metrics_set.try_emplace(name, name, (const struct spdk_mempool*)mp);
}

void SpdkMemPool::metrics_populate() {
    rte_mempool_walk(
        [](struct rte_mempool* mp, void* arg) {
            SpdkMemPool* pthis = (SpdkMemPool*)arg;
            pthis->register_metrics(mp);
        },
        (void*)this);
}

spdk_mempool* SpdkMemPool::get_mempool(size_t size) {
    uint64_t idx = get_mempool_idx(size);
    return m_internal_pools[idx];
}

void* SpdkMemPool::create(size_t element_size, size_t element_count) {
    const uint64_t idx = get_mempool_idx(element_size);
    spdk_mempool* mempool = m_internal_pools[idx];
    if (mempool != nullptr) {
        if (spdk_mempool_count(mempool) == element_count) {
            return mempool;
        } else {
            spdk_mempool_free(mempool);
        }
    }
    std::string name = "iomgr_mempool_" + std::to_string(element_size);

    LOGINFO("Creating new mempool {} of element count {} and size {}", name, element_count, element_size);
    mempool = spdk_mempool_create(name.c_str(), element_count, element_size, 0, SPDK_ENV_SOCKET_ID_ANY);
    RELEASE_ASSERT(mempool != nullptr, "Failed to create new mempool of size={}, rte_errno={} {}", element_size,
                   rte_errno, rte_strerror(rte_errno));
    m_internal_pools[idx] = mempool;
    register_metrics(r_cast< rte_mempool* >(mempool));
    return mempool;
}

uint64_t SpdkMemPool::get_mempool_idx(size_t size) const {
    DEBUG_ASSERT_EQ(size % min_mempool_buf_size, 0, "Mempool size must be modulo mempool buf size");
    DEBUG_ASSERT_GE(size, min_mempool_buf_size,
                    "Mempool size must be greater than or equal to minimum mempool buf size");
    return sisl::logBase2(size / min_mempool_buf_size);
}

void SpdkMemPool::reset() {
    for (spdk_mempool* mempool : m_internal_pools) {
        if (mempool != nullptr) { spdk_mempool_free(mempool); }
    }
    m_internal_pools.fill(nullptr);
}

IOMempoolMetrics::IOMempoolMetrics(const std::string& pool_name, const struct spdk_mempool* mp) :
        sisl::MetricsGroup("IOMemoryPool", pool_name), m_mp{mp} {
    REGISTER_GAUGE(iomempool_obj_size, "Size of the entry for this mempool");
    REGISTER_GAUGE(iomempool_free_count, "Total count of objects which are free in this pool");
    REGISTER_GAUGE(iomempool_alloced_count, "Total count of objects which are alloced in this pool");
    REGISTER_GAUGE(iomempool_cache_size, "Total number of entries cached per lcore in this pool");

    register_me_to_farm();
    attach_gather_cb(std::bind(&IOMempoolMetrics::on_gather, this));

    // This is not going to change once created, so set it up avoiding atomic operations during gather
    GAUGE_UPDATE(*this, iomempool_obj_size, ((const struct rte_mempool*)m_mp)->elt_size);
    GAUGE_UPDATE(*this, iomempool_cache_size, ((const struct rte_mempool*)m_mp)->cache_size);
}

void IOMempoolMetrics::on_gather() {
    GAUGE_UPDATE(*this, iomempool_free_count, spdk_mempool_count(m_mp));
    GAUGE_UPDATE(*this, iomempool_alloced_count, rte_mempool_in_use_count((const struct rte_mempool*)m_mp));
}
} // namespace iomgr
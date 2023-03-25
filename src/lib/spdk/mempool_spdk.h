#pragma once
#include <string>
#include <unordered_map>
#include <mutex>

#include <sisl/metrics/metrics.hpp>

struct rte_mempool;
struct spdk_mempool;

namespace iomgr {

class IOMempoolMetrics : public sisl::MetricsGroup {
public:
    IOMempoolMetrics(const std::string& pool_name, const struct spdk_mempool* mp);
    ~IOMempoolMetrics() {
        detach_gather_cb();
        deregister_me_from_farm();
    }

    void on_gather();

private:
    const struct spdk_mempool* m_mp;
};

static constexpr uint64_t max_mempool_buf_size{256 * 1024};
static constexpr uint64_t min_mempool_buf_size{512};
static constexpr uint64_t max_mempool_count{sisl::logBase2(max_mempool_buf_size - min_mempool_buf_size)};

class SpdkMempool {
public:
    SpdkMempool();
    void register_metrics(struct ::rte_mempool* mp);
    void metrics_populate();
    spdk_mempool* get_mempool(size_t size);
    void* create(size_t element_size, size_t element_count);
    void reset();

private:
    uint64_t get_mempool_idx(size_t size) const;

private:
    std::array< spdk_mempool*, max_mempool_count > m_internal_pools;
    std::mutex m_mset_mtx;
    std::unordered_map< std::string, IOMempoolMetrics > m_mempool_metrics_set;
};

struct SpdkAlignedAllocImpl : public sisl::AlignedAllocatorImpl {
    uint8_t* aligned_alloc(size_t align, size_t sz, const sisl::buftag tag) override;
    void aligned_free(uint8_t* b, const sisl::buftag tag) override;
    uint8_t* aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz = 0) override;
    uint8_t* aligned_pool_alloc(const size_t align, const size_t sz, const sisl::buftag tag) override;
    void aligned_pool_free(uint8_t* const b, const size_t sz, const sisl::buftag tag) override;
    size_t buf_size(uint8_t* buf) const override;
};

} // namespace iomgr
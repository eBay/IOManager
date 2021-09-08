//
// Created by Kadayam, Hari on 05/26/20.
//
#include <sds_logging/logging.h>
#include "include/iomgr.hpp"
#include "include/spdk_nvmf_interface.hpp"
extern "C" {
#include <spdk/env.h>
#include <spdk/thread.h>
#include <spdk/string.h>
#include <spdk/nvmf.h>
#include <spdk/nvmf_transport.h>
}
#include <folly/Exception.h>
#include <sisl/fds/obj_allocator.hpp>
#include <sisl/fds/buffer.hpp>

namespace iomgr {

SpdkNvmfInterface::SpdkNvmfInterface(struct spdk_nvmf_tgt* tgt) : m_nvmf_tgt(tgt) {}

void SpdkNvmfInterface::init_iface_thread_ctx(const io_thread_t& thr) {
    // Create a poll group per thread and attach to the thread local of interface.
    auto nctx = std::make_unique< SpdkNvmfContext >();
    nctx->poll_group = spdk_nvmf_poll_group_create(m_nvmf_tgt);
    if (!nctx->poll_group) { throw std::runtime_error("Unable to create an spdk nvmf poll group"); }
    m_iface_thread_ctx[thr->thread_idx] = std::move(nctx);
}

void SpdkNvmfInterface::clear_iface_thread_ctx(const io_thread_t& thr) {
    auto nctx = static_cast< SpdkNvmfContext* >(m_iface_thread_ctx[thr->thread_idx].get());
    spdk_nvmf_poll_group_destroy(nctx->poll_group, nullptr, nullptr);
    m_iface_thread_ctx[thr->thread_idx].reset();
}

void SpdkNvmfInterface::init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) {
    auto nctx = static_cast< SpdkNvmfContext* >(m_iface_thread_ctx[thr->thread_idx].get());
    auto ret = spdk_nvmf_poll_group_add(nctx->poll_group, iodev->nvmf_qp());
    if (ret != 0) { throw std::runtime_error("Unable to add nvmf qpair to poll group"); }
}

void SpdkNvmfInterface::clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) {
    spdk_nvmf_poll_group_remove(iodev->nvmf_qp());
}
} // namespace iomgr

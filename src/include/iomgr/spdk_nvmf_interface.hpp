/************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 * Author/Developer(s): Harihara Kadayam
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
#include <iomgr/io_interface.hpp>

struct spdk_nvmf_poll_group;
struct spdk_nvmf_tgt;

namespace iomgr {
struct SpdkNvmfContext : public IOInterfaceThreadContext {
    struct spdk_nvmf_poll_group* poll_group;
};

class IOReactor;
class SpdkNvmfInterface : public IOInterface {
public:
    SpdkNvmfInterface(struct spdk_nvmf_tgt* tgt);
    std::string name() const override { return "spdk_nvmf_interface"; }

    [[nodiscard]] bool is_spdk_interface() const override { return true; }

private:
    void init_iface_thread_ctx(const io_thread_t& thr) override;
    void clear_iface_thread_ctx(const io_thread_t& thr) override;

    void init_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override;
    void clear_iodev_thread_ctx(const io_device_ptr& iodev, const io_thread_t& thr) override;

private:
    spdk_nvmf_tgt* m_nvmf_tgt; // TODO: Make this support a vector of targets which can be added dynamically.
};
} // namespace iomgr

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
#ifndef IOMGR_IOMGR_MSG_HPP
#define IOMGR_IOMGR_MSG_HPP

#include <iostream>
#include <folly/Traits.h>
#include <boost/fiber/all.hpp>
#include <sisl/fds/buffer.hpp>
#include <sisl/utility/enum.hpp>
#include <sisl/utility/atomic_counter.hpp>
#include <sisl/utility/obj_life_counter.hpp>
#include <iomgr/iomgr_types.hpp>
#include <iomgr/fiber_lib.hpp>

namespace iomgr {
using run_func_t = std::function< void(void) >;

struct iomgr_waitable_msg;
struct iomgr_msg : public sisl::ObjLifeCounter< iomgr_msg > {

    io_fiber_t m_dest_fiber{nullptr}; // Is this message heading to a specific fiber within reactor, defaults main fiber
    run_func_t m_method;

    template < class... Args >
    static iomgr_msg* create(Args&&... args) {
        return new iomgr_msg(std::forward< Args >(args)...);
    }

    static void free(iomgr_msg* msg) { delete msg; }

    virtual bool need_reply() const { return false; }
    virtual void completed() {}
    virtual iomgr_msg* clone() const { return new iomgr_msg{m_method}; }

protected:
    iomgr_msg() = default;
    iomgr_msg(const auto& fn) : m_method{fn} {}

    virtual ~iomgr_msg() = default;
};

struct iomgr_waitable_msg : public iomgr_msg {
    FiberManagerLib::Promise< bool > m_promise;

    template < class... Args >
    static iomgr_waitable_msg* create(Args&&... args) {
        return new iomgr_waitable_msg(std::forward< Args >(args)...);
    }

    iomgr_msg* clone() const override { return new iomgr_waitable_msg{m_method}; }
    bool need_reply() const override { return true; }
    void completed() { m_promise.setValue(true); }

protected:
    iomgr_waitable_msg(const auto& fn) : iomgr_msg(fn) {}
    virtual ~iomgr_waitable_msg() = default;
};

} // namespace iomgr

#if 0
namespace folly {
template <>
FOLLY_ASSUME_RELOCATABLE(iomgr::iomgr_msg);
} // namespace folly
#endif

#endif // IOMGR_IOMGR_MSG_HPP

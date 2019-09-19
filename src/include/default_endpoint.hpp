//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_DEFAULT_ENDPOINT_HPP
#define IOMGR_DEFAULT_ENDPOINT_HPP

#include "include/endpoint.hpp"

namespace iomgr {
class DefaultEndPoint : public EndPoint {
public:
    void on_thread_start() override {};
    void on_thread_exit() override {};
};
} // namespace iomgr
#endif //IOMGR_DEFAULT_ENDPOINT_HPP

//
// Created by Kadayam, Hari on 2019-04-25.
//

#ifndef IOMGR_ENDPOINT_HPP
#define IOMGR_ENDPOINT_HPP

namespace iomgr {
class EndPoint {
protected:
public:
    explicit EndPoint() {}
    virtual ~EndPoint() = default;

    virtual void on_thread_start() = 0;
    virtual void on_thread_exit() = 0;
};

}
#endif //IOMGR_ENDPOINT_HPP

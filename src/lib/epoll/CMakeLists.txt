cmake_minimum_required(VERSION 3.13)

add_library(iomgr_epoll OBJECT)
target_sources(iomgr_epoll PRIVATE
        iomgr_impl_epoll.cpp
        epoll_mem.cpp
        reactor_epoll.cpp
      )
target_link_libraries(iomgr_epoll ${COMMON_DEPS})
add_dependencies(iomgr_epoll iomgr_config)
cmake_minimum_required(VERSION 3.13)

add_library(iomgr_reactor OBJECT)
target_sources(iomgr_reactor PRIVATE
        reactor.cpp
      )
target_link_libraries(iomgr_reactor ${COMMON_DEPS})
add_dependencies(iomgr_reactor iomgr_config)
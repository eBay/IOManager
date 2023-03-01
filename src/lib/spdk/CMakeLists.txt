cmake_minimum_required(VERSION 3.13)

add_library(iomgr_spdk OBJECT)
target_sources(iomgr_spdk PRIVATE
        iomgr_impl_spdk.cpp
        mempool_spdk.cpp
        reactor_spdk.cpp
      )
target_link_libraries(iomgr_spdk ${COMMON_DEPS})
add_dependencies(iomgr_spdk iomgr_config)
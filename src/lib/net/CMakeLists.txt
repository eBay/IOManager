cmake_minimum_required(VERSION 3.13)

list(APPEND NET_SRC
        net_op.cpp
        accept_op.cpp
        connect_op.cpp
        )
add_library(iomgr_net OBJECT)
target_sources(iomgr_net PRIVATE ${NET_SRC})
target_link_libraries(iomgr_net ${COMMON_DEPS} stdexec::stdexec)
add_dependencies(iomgr_net iomgr_config)

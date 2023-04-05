cmake_minimum_required(VERSION 3.13)

add_library(iomgr_interfaces OBJECT)
target_sources(iomgr_interfaces PRIVATE
        aio_drive_interface.cpp
        drive_interface.cpp
        generic_interface.cpp
        spdk_drive_interface.cpp
        uring_drive_interface.cpp
        drive_iocb.cpp
      )
target_link_libraries(iomgr_interfaces ${COMMON_DEPS})
add_dependencies(iomgr_interfaces iomgr_config)
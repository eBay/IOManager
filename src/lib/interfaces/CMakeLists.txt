cmake_minimum_required(VERSION 3.13)

list(APPEND INTERFACE_SRC
        aio_drive_interface.cpp
        drive_interface.cpp
        generic_interface.cpp
        uring_drive_interface.cpp
        drive_iocb.cpp
        )
if (${spdk_FOUND})
        list(APPEND INTERFACE_SRC
                spdk_drive_interface.cpp
                )
endif ()
add_library(iomgr_interfaces OBJECT)
target_sources(iomgr_interfaces PRIVATE ${INTERFACE_SRC})
target_link_libraries(iomgr_interfaces ${COMMON_DEPS})
add_dependencies(iomgr_interfaces iomgr_config)

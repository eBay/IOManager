set (IOMGR_SRC
      iomgr.cpp
      reactor.cpp
      reactor_epoll.cpp
      reactor_spdk.cpp
      iomgr_timer.cpp
      interfaces/aio_drive_interface.cpp
      interfaces/spdk_drive_interface.cpp
      interfaces/generic_interface.cpp
      interfaces/spdk_nvmf_interface.cpp)

add_library (iomgr ${IOMGR_SRC})
target_link_libraries(iomgr ${CONAN_LIBS})

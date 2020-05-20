set (IOMGR_SRC
      iomgr.cpp
      io_thread.cpp
      io_thread_epoll.cpp
      io_thread_spdk.cpp
      iomgr_timer.cpp
      interfaces/aio_drive_interface.cpp
      interfaces/spdk_drive_interface.cpp
      interfaces/generic_interface.cpp)

add_library (iomgr ${IOMGR_SRC})
target_link_libraries(iomgr ${CONAN_LIBS} dpdk)

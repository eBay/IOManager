set (IOMGR_SRC
      iomgr.cpp
      io_thread.cpp
      iomgr_timer.cpp
      interfaces/aio_drive_interface.cpp
      interfaces/generic_interface.cpp)

add_library (iomgr ${IOMGR_SRC})
target_link_libraries(iomgr ${CONAN_LIBS} ${LIB_AIO} dpdk)

set (IOMGR_SRC
      iomgr.cpp
      io_thread.cpp
      interfaces/aio_drive_interface.cpp)

add_library (iomgr ${IOMGR_SRC})
target_link_libraries(iomgr ${CONAN_LIBS} ${LIB_AIO})

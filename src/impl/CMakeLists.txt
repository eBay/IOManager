set (IOMGR_SRC
      iomgr.cpp
      io_thread.cpp
      endpoints/aio_drive_endpoint.cpp)

add_library (iomgr ${IOMGR_SRC})
target_link_libraries(iomgr ${CONAN_LIBS} ${LIB_AIO})

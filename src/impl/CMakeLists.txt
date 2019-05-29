set (IOMGR_SRC
      iomgr.cpp
      io_thread.cpp
      endpoints/drive_endpoint.cpp)

add_library (iomgr ${IOMGR_SRC})

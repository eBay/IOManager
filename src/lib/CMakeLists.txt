set (IOMGR_SRC
      api.cpp
      iomgr.cpp
      io_thread.cpp)

add_library (iomgr ${IOMGR_SRC})

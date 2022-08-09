set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -Wno-sign-compare")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPACKAGE_VERSION=\\\"${CONAN_PACKAGE_VERSION}\\\"")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPACKAGE_NAME=\\\"${CONAN_PACKAGE_NAME}\\\"")
find_package (Threads REQUIRED)
find_library(LIB_AIO aio)

include_directories (BEFORE .. ../include)

set (IOMGR_SRC
      iomgr.cpp
      reactor.cpp
      reactor_epoll.cpp
      reactor_spdk.cpp
      iomgr_timer.cpp
      interfaces/drive_interface.cpp
      interfaces/aio_drive_interface.cpp
      interfaces/spdk_drive_interface.cpp
      interfaces/uring_drive_interface.cpp
      interfaces/generic_interface.cpp
      interfaces/spdk_nvmf_interface.cpp
      interfaces/grpc_interface.cpp
      io_environment.cpp
      )

add_library (iomgr STATIC ${IOMGR_SRC})

settings_gen_cpp(${CONAN_BIN_DIRS_FLATBUFFERS} ${CMAKE_CURRENT_BINARY_DIR}/generated/ iomgr iomgr_config.fbs)

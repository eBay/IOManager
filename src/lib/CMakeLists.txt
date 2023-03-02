cmake_minimum_required(VERSION 3.10)

set(PACKAGE_REVISION "0.0.0+unknown")
if (DEFINED CONAN_PACKAGE_NAME)
  set(PACKAGE_REVISION "${CONAN_PACKAGE_VERSION}")
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -Wno-sign-compare")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPACKAGE_VERSION=\\\"${PACKAGE_REVISION}\\\"")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPACKAGE_NAME=\\\"${PROJECT_NAME}\\\"")

find_package(flatbuffers REQUIRED)
find_package(grpc_internal REQUIRED)
find_package(sisl REQUIRED)
find_package(liburing REQUIRED)
find_package(zmarok-semver REQUIRED)

link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})

list(APPEND COMMON_DEPS
      liburing::liburing
      sisl::sisl
      spdk::spdk
    )

add_subdirectory(interfaces)
add_subdirectory(spdk)
add_subdirectory(epoll)

set(IOMGR_OBJECTS
    $<TARGET_OBJECTS:iomgr_config> 
    $<TARGET_OBJECTS:iomgr_interfaces> 
    $<TARGET_OBJECTS:iomgr_spdk> 
    $<TARGET_OBJECTS:iomgr_epoll>
    io_environment.cpp
    iomgr.cpp
    iomgr_timer.cpp
    reactor.cpp
)
add_library(iomgr_config OBJECT)

add_library (iomgr STATIC ${IOMGR_OBJECTS})

list(APPEND IOMGR_LIB_DEPS
      ${COMMON_DEPS}
      grpc_internal::grpc_internal
    )
if (${evhtp_FOUND})
    list(APPEND IOMGR_LIB_DEPS evhtp::evhtp)
endif()

target_link_libraries(iomgr ${IOMGR_LIB_DEPS})

include ("${sisl_INCLUDE_DIRS}/../cmake/settings_gen.cmake")
settings_gen_cpp($<TARGET_FILE:flatbuffers::flatc> ${CMAKE_CURRENT_BINARY_DIR}/generated iomgr_config iomgr_config.fbs)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

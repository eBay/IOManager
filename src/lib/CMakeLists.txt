cmake_minimum_required(VERSION 3.10)

set(PACKAGE_REVISION "0.0.0+unknown")
if (DEFINED CONAN_PACKAGE_NAME)
  set(PACKAGE_REVISION "${CONAN_PACKAGE_VERSION}")
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -Wno-sign-compare")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPACKAGE_VERSION=\\\"${PACKAGE_REVISION}\\\"")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPACKAGE_NAME=\\\"${PROJECT_NAME}\\\"")

find_package(flatbuffers REQUIRED)
find_package(sisl REQUIRED)
find_package(liburing REQUIRED)
find_package(Pistache REQUIRED)
find_package(zmarok-semver REQUIRED)

list(APPEND COMMON_DEPS
      liburing::liburing
      Pistache::Pistache
      sisl::sisl
    )
if (${spdk_FOUND})
    link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})
    list (APPEND COMMON_DEPS spdk::spdk)
endif ()

add_subdirectory(interfaces)
add_subdirectory(epoll)
if (${spdk_FOUND})
    add_subdirectory(spdk)
endif ()
add_subdirectory(reactor)

set(IOMGR_OBJECTS
    $<TARGET_OBJECTS:iomgr_config> 
    $<TARGET_OBJECTS:iomgr_interfaces> 
    $<TARGET_OBJECTS:iomgr_epoll>
    $<TARGET_OBJECTS:iomgr_reactor>
    io_environment.cpp
    iomgr.cpp
    iomgr_timer.cpp
    watchdog.cpp
    http_server.cpp
)
if (${spdk_FOUND})
    list (APPEND IOMGR_OBJECTS $<TARGET_OBJECTS:iomgr_spdk> )
endif()
add_library(iomgr_config OBJECT)

add_library(iomgr ${IOMGR_OBJECTS})

list(APPEND IOMGR_LIB_DEPS
      ${COMMON_DEPS}
    )
if (${grpc_internal_FOUND})
    list (APPEND IOMGR_LIB_DEPS grpc_internal::grpc_internal)
endif()

target_link_libraries(iomgr ${IOMGR_LIB_DEPS})

include ("${sisl_INCLUDE_DIRS}/../cmake/settings_gen.cmake")
settings_gen_cpp($<TARGET_FILE:flatbuffers::flatc> ${CMAKE_CURRENT_BINARY_DIR}/generated iomgr_config iomgr_config.fbs)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

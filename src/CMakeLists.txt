cmake_minimum_required(VERSION 3.10)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter")

find_package (Threads REQUIRED)
find_library(LIB_AIO aio)
find_package(sisl REQUIRED)
find_package(spdk)

if (${spdk_FOUND})
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DWITH_SPDK")
endif()

if ("${FIBER_IMPL}" STREQUAL "folly") 
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_FOLLY_FIBER")
endif()

include_directories (BEFORE lib/)
include_directories (BEFORE include/)
include_directories (BEFORE ${CMAKE_CURRENT_BINARY_DIR}/lib)

add_subdirectory (lib)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/lib)

add_subdirectory(test)

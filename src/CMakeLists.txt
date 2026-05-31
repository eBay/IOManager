cmake_minimum_required(VERSION 3.10)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter")

find_package (Threads REQUIRED)
find_package(sisl REQUIRED)
find_package(liburing QUIET)


include_directories (BEFORE lib/)
include_directories (BEFORE include/)
include_directories (BEFORE ${CMAKE_CURRENT_BINARY_DIR}/include)
include_directories (BEFORE ${CMAKE_CURRENT_BINARY_DIR}/lib)

add_subdirectory (lib)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/lib)

add_subdirectory(test)

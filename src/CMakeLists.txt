cmake_minimum_required(VERSION 3.10)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -fconcepts")

find_package (Threads REQUIRED)
find_library(LIB_AIO aio)
find_package(sisl REQUIRED)
find_package(spdk REQUIRED)

include_directories (BEFORE . include)

add_subdirectory (lib)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/lib)
include_directories (BEFORE ${CONAN_INCLUDE_DIRS_SISL}/sisl)

add_subdirectory(test)

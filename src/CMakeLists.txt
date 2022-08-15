cmake_minimum_required(VERSION 3.11)

find_package (Threads REQUIRED)
find_library(LIB_AIO aio)
find_package(sisl REQUIRED)
find_package(spdk REQUIRED)

include_directories (BEFORE . include)

add_subdirectory (lib)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/lib)

add_subdirectory(test)

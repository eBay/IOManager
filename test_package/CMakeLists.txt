cmake_minimum_required(VERSION 3.11)
project(test_package LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
find_package (Threads REQUIRED)
find_package (iomgr REQUIRED)

link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})

add_executable(test_package)
target_sources(test_package PRIVATE test_package.cpp)
target_link_libraries(test_package
                      iomgr::iomgr
                      ${spdk_LIBRARY_LIST}
                      ${dpdk_LIBRARY_LIST}
                    )

project(PackageTest CXX)
cmake_minimum_required(VERSION 2.8.12)

set (CMAKE_CXX_STANDARD 17)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

find_package (Threads REQUIRED)

include_directories(BEFORE .. ../src/include)
add_executable(test_iomgr test_iomgr.cpp)
target_link_libraries(test_iomgr iomgr ${CMAKE_THREAD_LIBS_INIT})

# CTest is a testing tool that can be used to test your project.
# enable_testing()
# add_test(NAME example
#          WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
#          COMMAND example)

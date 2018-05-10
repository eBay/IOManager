project(PackageTest CXX)
cmake_minimum_required(VERSION 2.8.12)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

find_package (Threads REQUIRED)
find_library(LIB_TCMALLOC tcmalloc REQUIRED)

add_executable(package_test package_test.cpp)
target_link_libraries(package_test
                       ${CONAN_LIBS}
                       ${LIB_TCMALLOC}
                       ${CMAKE_THREAD_LIBS_INIT})

# CTest is a testing tool that can be used to test your project.
# enable_testing()
# add_test(NAME example
#          WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
#          COMMAND example)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")

find_package (Threads REQUIRED)

set(TEST_IOMGR_FILES 
    test_main.cpp
    iomgr_test.cpp
    )
enable_testing()
add_executable(test_iomgr ${TEST_IOMGR_FILES})

target_link_libraries(test_iomgr
                       iomgr
                       ${CONAN_LIBS}
                       ${CMAKE_THREAD_LIBS_INIT})

# CTest is a testing tool that can be used to test your project.
# add_test(NAME TestIOMgr COMMAND test_iomgr)

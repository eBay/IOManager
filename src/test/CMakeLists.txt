set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -fconcepts")

find_package (Threads REQUIRED)

set(TEST_IOMGR_FILES 
    test_iomgr.cpp
    )
enable_testing()

include_directories (BEFORE .. ../include)

add_executable(test_iomgr ${TEST_IOMGR_FILES})

target_link_libraries(test_iomgr ${CONAN_LIBS} iomgr sisl)

# CTest is a testing tool that can be used to test your project.
# add_test(NAME TestIOMgr COMMAND test_iomgr)

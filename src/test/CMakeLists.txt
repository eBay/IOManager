set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -fconcepts")

find_package (Threads REQUIRED)


enable_testing()

include_directories (BEFORE .. ../include)

if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "spdk_mode") OR 
        ("${CONAN_TEST_TARGET}" STREQUAL "epoll_mode"))
    set(TEST_IOMGR_FILES test_iomgr.cpp)
    add_executable(test_iomgr ${TEST_IOMGR_FILES})
    target_link_libraries(test_iomgr ${CONAN_LIBS} iomgr sisl flip)

    if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "epoll_mode"))
        add_test(NAME TestIOMgr-Epoll COMMAND test_iomgr)
    endif()

# TODO: Disabled till we get the spdk test_iomgr failure is fixed
#   if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "spdk_mode"))
#       add_test(NAME TestIOMgr-Spdk COMMAND test_iomgr --spdk true)
#   endif()
endif()

if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "spdk_mode") OR 
        ("${CONAN_TEST_TARGET}" STREQUAL "epoll_mode"))
    set(TEST_MSG_FILES test_msg.cpp)
    add_executable(test_msg ${TEST_MSG_FILES})
    target_link_libraries(test_msg ${CONAN_LIBS} iomgr sisl flip)

    if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "epoll_mode"))
        add_test(NAME TestMsg-Epoll COMMAND test_msg)
    endif()

    if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "spdk_mode"))
        add_test(NAME TestMsg-Spdk COMMAND test_msg --spdk true)
    endif()
endif()

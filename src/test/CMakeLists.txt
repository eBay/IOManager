set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -fconcepts")

find_package (Threads REQUIRED)


enable_testing()

include_directories (BEFORE .. ../include)

if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "spdk_mode") OR 
        ("${CONAN_TEST_TARGET}" STREQUAL "epoll_mode"))
    set(TEST_IOMGR_FILES test_iomgr.cpp)
    add_executable(test_iomgr ${TEST_IOMGR_FILES})
    target_link_libraries(test_iomgr ${CONAN_LIBS} iomgr sisl flip)

    set(TEST_MSG_FILES test_msg.cpp)
    add_executable(test_msg ${TEST_MSG_FILES})
    target_link_libraries(test_msg ${CONAN_LIBS} iomgr sisl flip)

    set(TEST_IOJOB_FILES test_io_job.cpp)
    add_executable(test_iojob ${TEST_IOJOB_FILES})
    target_link_libraries(test_iojob ${CONAN_LIBS} iomgr sisl flip isal atomic)
    
    set(TEST_WRITEZERO_FILES test_write_zero.cpp)
    add_executable(test_write_zero ${TEST_WRITEZERO_FILES})
    target_link_libraries(test_write_zero ${CONAN_LIBS} iomgr sisl flip)

    set(TEST_TIMER_FILES test_timer.cpp)
    add_executable(test_timer ${TEST_TIMER_FILES})
    target_link_libraries(test_timer ${CONAN_LIBS} iomgr sisl flip)

    if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "epoll_mode"))
        add_test(NAME TestIOMgr-Epoll COMMAND test_iomgr)
        add_test(NAME TestMsg-Epoll COMMAND test_msg)
        add_test(NAME TestIOJob-Epoll COMMAND test_iojob)
        add_test(NAME TestTimer-Epoll COMMAND test_timer)
        add_test(NAME TestWriteZero-Epoll COMMAND test_write_zero)
    endif()

    if (("${CONAN_TEST_TARGET}" STREQUAL "full") OR ("${CONAN_TEST_TARGET}" STREQUAL "spdk_mode"))
        # TODO: Disabled till we get the spdk test_iomgr failure is fixed
        #add_test(NAME TestIOMgr-Spdk COMMAND test_iomgr --spdk true)
        add_test(NAME TestTimer-Spdk COMMAND test_timer --spdk true)

        add_test(NAME TestMsg-Spdk COMMAND test_msg --spdk true)
        SET_TESTS_PROPERTIES(TestMsg-Spdk PROPERTIES DEPENDS TestTimer-Spdk)
    endif()
endif()

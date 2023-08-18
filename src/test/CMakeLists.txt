cmake_minimum_required(VERSION 3.10)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -fconcepts")

find_package (GTest REQUIRED)
find_package (cpr REQUIRED)

link_directories(${spdk_LIB_DIRS} ${dpdk_LIB_DIRS})
include_directories (BEFORE .. ../include)

set (TEST_DEPS
        iomgr 
        sisl::sisl
        ${spdk_LIBRARY_LIST}
        ${dpdk_LIBRARY_LIST}
        GTest::gmock
    )

if (("${CMAKE_TEST_TARGET}" STREQUAL "full") OR ("${CMAKE_TEST_TARGET}" STREQUAL "spdk_mode") OR 
        ("${CMAKE_TEST_TARGET}" STREQUAL "epoll_mode"))
    #set(TEST_IOMGR_FILES test_iomgr.cpp)
    #add_executable(test_iomgr ${TEST_IOMGR_FILES})
    #target_link_libraries(test_iomgr ${TEST_DEPS} )

    set(TEST_MSG_FILES test_msg.cpp)
    add_executable(test_msg ${TEST_MSG_FILES})
    target_link_libraries(test_msg ${TEST_DEPS} )

    set(TEST_IOJOB_FILES test_io_job.cpp)
    add_executable(test_iojob ${TEST_IOJOB_FILES})
    target_link_libraries(test_iojob ${TEST_DEPS} atomic )
    
    set(TEST_WRITEZERO_FILES test_write_zero.cpp)
    add_executable(test_write_zero ${TEST_WRITEZERO_FILES})
    target_link_libraries(test_write_zero ${TEST_DEPS} )

    set(TEST_TIMER_FILES test_timer.cpp)
    add_executable(test_timer ${TEST_TIMER_FILES})
    target_link_libraries(test_timer ${TEST_DEPS} )

    set(TEST_HTTP_SERVER_SOURCES test_http_server.cpp)
    add_executable(test_http_server ${TEST_HTTP_SERVER_SOURCES})
    target_link_libraries(test_http_server ${TEST_DEPS} cpr::cpr)
    add_test(NAME TestHttpServer COMMAND test_http_server)

    set(TEST_DRIVE_FILES test_drive_interface.cpp)
    add_executable(test_drive ${TEST_DRIVE_FILES})
    target_link_libraries(test_drive ${TEST_DEPS} )

    set(TEST_SHARED_MUTEX_FILES test_fiber_shared_mutex.cpp)
    add_executable(test_shared_mutex ${TEST_SHARED_MUTEX_FILES})
    target_link_libraries(test_shared_mutex ${TEST_DEPS} )

    #set(TEST_HTTP_SERVER_SOURCES test_http_server.cpp)
    #add_executable(test_http_server ${TEST_HTTP_SERVER_SOURCES})
    #target_link_libraries(test_http_server ${TEST_DEPS})
    #add_test(NAME TestHttpServer COMMAND test_http_server)

    if (("${CMAKE_TEST_TARGET}" STREQUAL "full") OR ("${CMAKE_TEST_TARGET}" STREQUAL "epoll_mode"))
        add_test(NAME TestTimer-Epoll COMMAND test_timer)
        add_test(NAME TestIOJob-Epoll COMMAND test_iojob)
        add_test(NAME TestWriteZero-Epoll COMMAND test_write_zero --dev /tmp/test_wz_epoll)
        add_test(NAME TestDrive-Epoll COMMAND test_drive --dev_path /tmp/iomgr_test_drive_epoll)
        add_test(NAME TestMsg-Epoll COMMAND test_msg)
        add_test(NAME test_shared_mutex-Epoll COMMAND test_shared_mutex)
        SET_TESTS_PROPERTIES(TestMsg-Epoll PROPERTIES DEPENDS TestWriteZero-Epoll)
    endif()

    if (("${CMAKE_TEST_TARGET}" STREQUAL "full") OR ("${CMAKE_TEST_TARGET}" STREQUAL "spdk_mode"))
        add_test(NAME TestTimer-Spdk COMMAND test_timer --spdk true --iova-mode va)

        add_test(NAME TestWriteZero-Spdk COMMAND test_write_zero --spdk true --iova-mode va --dev /tmp/test_wz_spdk)
        SET_TESTS_PROPERTIES(TestWriteZero-Spdk PROPERTIES DEPENDS TestTimer-Spdk)

        add_test(NAME TestMsg-Spdk COMMAND test_msg --spdk true --iova-mode va)
        SET_TESTS_PROPERTIES(TestMsg-Spdk PROPERTIES DEPENDS TestWriteZero-Spdk)

        add_test(NAME TestDrive-Spdk COMMAND test_drive --spdk true --iova-mode va --dev_path /tmp/iomgr_test_drive_spdk)
        SET_TESTS_PROPERTIES(TestDrive-Spdk PROPERTIES DEPENDS TestMsg-Spdk)

        add_test(NAME TestIOJob-Spdk COMMAND test_iojob --spdk true --iova-mode va)
        SET_TESTS_PROPERTIES(TestIOJob-Spdk PROPERTIES DEPENDS TestDrive-Spdk)
    endif()
endif()

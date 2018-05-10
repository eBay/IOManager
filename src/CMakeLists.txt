set (CPP_17_FLAGS "-std=c++1z -fPIC")
set (CPP_WARNINGS "-Wall -Wextra -Werror")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPP_17_FLAGS} ${CPP_WARNINGS}")

find_package (Threads REQUIRED)

include_directories (BEFORE include)

add_subdirectory (lib)

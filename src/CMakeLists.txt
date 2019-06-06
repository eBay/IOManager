set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")

find_package (Threads REQUIRED)

include_directories (BEFORE include)

add_subdirectory (lib)
add_subdirectory (test)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-empty-body -Wno-deprecated-copy -Wno-unused-parameter -fconcepts")

find_package (Threads REQUIRED)
find_library(LIB_AIO aio)

include_directories (BEFORE . include)

add_subdirectory (lib)

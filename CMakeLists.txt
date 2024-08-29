# CMakeList.txt : CMake project for newsql, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("newsql")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

#include directories
include_directories(include)
link_directories(lib)

file(GLOB_RECURSE NEWSQL_SRC "src/*.cpp" "src/*.h" 
  "src/scan/*.cpp" "src/scan/*.h"
  "src/manager/*.cpp" "src/manager/*.h")

# Add source to this project's executable.
add_executable (newsql ${NEWSQL_SRC})
target_link_libraries(newsql LINK_PUBLIC ${CMAKE_SOURCE_DIR}/lib/spdlog.lib)

file(GLOB_RECURSE TEST_SRC 
  "tests/*.cpp" "tests/*.h" 
  "src/manager/*.cpp" "src/manager/*.h"
  "src/*.h" "src/scan/*.cpp" "src/scan/*.h" 
  "src/query.cpp" "src/parser.cpp" "src/HeapFileIterator.cpp")

foreach(file ${TEST_SRC})
    message(STATUS "${file}")
endforeach()

# Add the Catch2 test executable
add_executable(testsql ${TEST_SRC} "include/catch.hpp")
target_include_directories(testsql PRIVATE include)
target_link_libraries(testsql LINK_PUBLIC ${CMAKE_SOURCE_DIR}/lib/spdlog.lib)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET newsql PROPERTY CXX_STANDARD 20)
  set_property(TARGET testsql PROPERTY CXX_STANDARD 20)
endif()

# TODO: Add tests and install targets if needed.


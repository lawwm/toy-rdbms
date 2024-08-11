# CMakeList.txt : CMake project for newsql, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("newsql")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Ensure exceptions are enabled
# if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
#     add_compile_options(-fexceptions)
# endif()

#include directories
include_directories(include)

file(GLOB_RECURSE NEWSQL_SRC "src/*.cpp" "src/*.h")

# Add source to this project's executable.
add_executable (newsql ${NEWSQL_SRC} "tests/test_executor.cpp")


file(GLOB_RECURSE TEST_SRC "tests/*.cpp" "tests/*.h" "src/*.h" "src/query.cpp" "src/scan.cpp" "src/parser.cpp" "src/buffer.cpp" "src/planner.cpp")

foreach(file ${TEST_SRC})
    message(STATUS "${file}")
endforeach()

# Add the Catch2 test executable
add_executable(testsql ${TEST_SRC} "include/catch.hpp" "tests/test_executor.cpp")
target_include_directories(testsql PRIVATE include)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET newsql PROPERTY CXX_STANDARD 20)
  set_property(TARGET testsql PROPERTY CXX_STANDARD 20)
endif()

# TODO: Add tests and install targets if needed.

# CMakeList.txt : CMake project for newsql, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("newsql")

#include directories
include_directories(include)

# Add source to this project's executable.
add_executable (newsql "src/main.cpp"  "src/parser.cpp" "src/query.h")



# Add the Catch2 test executable
file(GLOB headers "src/parser.h" "src/query.h")
file(GLOB srcs "tests/test_main.cpp" "tests/test_parser.cpp" "src/parser.cpp")

add_executable(testsql ${srcs})
target_include_directories(testsql PRIVATE include)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET newsql PROPERTY CXX_STANDARD 20)
  set_property(TARGET testsql PROPERTY CXX_STANDARD 20)
endif()

# TODO: Add tests and install targets if needed.

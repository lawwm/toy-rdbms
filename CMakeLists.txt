# CMakeList.txt : CMake project for newsql, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("newsql")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Ensure exceptions are enabled
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(-fexceptions)
endif()

#include directories
include_directories(include)

# Add source to this project's executable.
add_executable (newsql "src/main.cpp"  "src/parser.cpp" "src/query.h" "src/buffer.h" "src/buffer.cpp" "tests/test_buffer.cpp")



# Add the Catch2 test executable
file(GLOB headers "src/parser.h" "src/query.h" "src/buffer.h")
file(GLOB srcs "tests/test_main.cpp" "tests/test_parser.cpp" "src/parser.cpp" "src/buffer.cpp" "tests/test_buffer.cpp")


add_executable(testsql ${srcs})
target_include_directories(testsql PRIVATE include)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET newsql PROPERTY CXX_STANDARD 20)
  set_property(TARGET testsql PROPERTY CXX_STANDARD 20)
endif()

# TODO: Add tests and install targets if needed.

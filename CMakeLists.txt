# CMakeList.txt : CMake project for newsql, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("newsql")

#include directories
include_directories(include)

# Enable Hot Reload for MSVC compilers if supported.
#if (POLICY CMP0141)
#  cmake_policy(SET CMP0141 NEW)
#  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
#endif()


# Add source to this project's executable.
add_executable (newsql "src/main.cpp"  "src/parser.h" "src/parser.cpp")

# Add the Catch2 test executable
add_executable(testsql "tests/test_main.cpp" "src/parser.h" "src/parser.cpp")
target_include_directories(testsql PRIVATE include)

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET newsql PROPERTY CXX_STANDARD 20)
  set_property(TARGET testsql PROPERTY CXX_STANDARD 20)
endif()

# TODO: Add tests and install targets if needed.

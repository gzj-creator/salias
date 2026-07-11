cmake_minimum_required(VERSION 3.25)

project(salias VERSION 0.0.1 LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT APPLE)
  message(FATAL_ERROR "salias currently supports Linux and macOS.")
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(BUILD_TESTING "Build salias tests" OFF)
option(SALIAS_BUILD_BENCHMARKS "Build salias benchmarks" OFF)
option(SALIAS_BUILD_EXAMPLES "Build salias examples" ON)
option(SALIAS_ENABLE_ASAN_UBSAN "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(SALIAS_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

include(CTest)
include(GNUInstallDirs)

set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(
    "$<$<CONFIG:Debug>:-Og>"
    "$<$<CONFIG:Release>:-O3>"
    "$<$<CONFIG:Release>:-flto>"
    "$<$<CONFIG:Release>:-march=native>"
  )
  add_link_options("$<$<CONFIG:Release>:-flto>")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(warnings)
include(sanitizers)

add_subdirectory(src/core)
add_subdirectory(src/salias)

if(SALIAS_BUILD_EXAMPLES)
  add_subdirectory(example)
endif()

if(SALIAS_BUILD_BENCHMARKS)
  add_subdirectory(bench)
  add_subdirectory(tools/final_bench)
endif()

if(BUILD_TESTING)
  add_subdirectory(test)
endif()

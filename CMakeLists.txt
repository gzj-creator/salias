cmake_minimum_required(VERSION 3.25)

project(salias VERSION 6.1.0 LANGUAGES CXX)

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
include(CMakePackageConfigHelpers)

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

if(BUILD_TESTING AND SALIAS_ENABLE_TSAN)
  add_executable(salias_tsan_runtime_probe test/support/tsan_runtime_probe.cpp)
  salias_enable_warnings(salias_tsan_runtime_probe)
  salias_enable_sanitizers(salias_tsan_runtime_probe)
  set(SALIAS_TEST_TSAN_PROBE "$<TARGET_FILE:salias_tsan_runtime_probe>")
endif()

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
  add_test(
    NAME salias_version_consistency
    COMMAND
      "${CMAKE_COMMAND}"
      "-DSALIAS_SOURCE_DIR=${PROJECT_SOURCE_DIR}"
      -P "${PROJECT_SOURCE_DIR}/cmake/check_version_consistency.cmake"
  )
  add_subdirectory(test)
endif()

install(
  TARGETS salias salias_core
  EXPORT saliasCoreTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  COMPONENT core
)
install(
  DIRECTORY "${PROJECT_SOURCE_DIR}/src/salias/include/salias/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/salias"
  COMPONENT core
)
install(
  DIRECTORY "${PROJECT_SOURCE_DIR}/src/core/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/salias/core"
  COMPONENT core
  FILES_MATCHING PATTERN "*.hpp"
)
install(
  EXPORT saliasCoreTargets
  FILE saliasCoreTargets.cmake
  NAMESPACE salias::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/salias"
  COMPONENT core
)
configure_package_config_file(
  "${PROJECT_SOURCE_DIR}/cmake/saliasConfig.cmake.in"
  "${PROJECT_BINARY_DIR}/saliasConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/salias"
)
write_basic_package_version_file(
  "${PROJECT_BINARY_DIR}/saliasConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion
)
install(
  FILES
    "${PROJECT_BINARY_DIR}/saliasConfig.cmake"
    "${PROJECT_BINARY_DIR}/saliasConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/salias"
  COMPONENT core
)

if(BUILD_TESTING)
  set(salias_install_test_command
    "${CMAKE_COMMAND}"
    "-DSALIAS_BUILD_DIR=${PROJECT_BINARY_DIR}"
    "-DSALIAS_SOURCE_DIR=${PROJECT_SOURCE_DIR}"
    "-DSALIAS_TEST_CONFIG=$<CONFIG>"
    "-DSALIAS_CMAKE_GENERATOR=${CMAKE_GENERATOR}"
    "-DSALIAS_CMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
    "-DSALIAS_CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    "-DSALIAS_CMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
    "-DSALIAS_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}"
    "-DSALIAS_TEST_LAUNCHER=${PROJECT_SOURCE_DIR}/cmake/salias_test_launcher.sh"
  )
  if(DEFINED CMAKE_C_COMPILER AND NOT CMAKE_C_COMPILER STREQUAL "")
    list(APPEND salias_install_test_command "-DSALIAS_CMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
  endif()
  if(DEFINED CMAKE_GENERATOR_PLATFORM AND NOT CMAKE_GENERATOR_PLATFORM STREQUAL "")
    list(APPEND salias_install_test_command
      "-DSALIAS_CMAKE_GENERATOR_PLATFORM=${CMAKE_GENERATOR_PLATFORM}")
  endif()
  if(DEFINED CMAKE_GENERATOR_TOOLSET AND NOT CMAKE_GENERATOR_TOOLSET STREQUAL "")
    list(APPEND salias_install_test_command
      "-DSALIAS_CMAKE_GENERATOR_TOOLSET=${CMAKE_GENERATOR_TOOLSET}")
  endif()
  if(DEFINED CMAKE_GENERATOR_INSTANCE AND NOT CMAKE_GENERATOR_INSTANCE STREQUAL "")
    list(APPEND salias_install_test_command
      "-DSALIAS_CMAKE_GENERATOR_INSTANCE=${CMAKE_GENERATOR_INSTANCE}")
  endif()
  if(DEFINED CMAKE_TOOLCHAIN_FILE AND NOT CMAKE_TOOLCHAIN_FILE STREQUAL "")
    list(APPEND salias_install_test_command
      "-DSALIAS_CMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
  endif()
  if(DEFINED SALIAS_TEST_TSAN_PROBE AND NOT SALIAS_TEST_TSAN_PROBE STREQUAL "")
    list(APPEND salias_install_test_command
      "-DSALIAS_TEST_TSAN_PROBE=${SALIAS_TEST_TSAN_PROBE}")
  endif()
  list(APPEND salias_install_test_command
    -P "${PROJECT_SOURCE_DIR}/cmake/install_consumer_test.cmake")
  add_test(NAME salias_install_consumer COMMAND ${salias_install_test_command})
  set_tests_properties(salias_install_consumer PROPERTIES TIMEOUT 600)
endif()

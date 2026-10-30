cmake_minimum_required(VERSION 3.25)

project(salias_install_consumer LANGUAGES CXX)

include(CTest)
enable_testing()

find_package(salias CONFIG REQUIRED COMPONENTS core)

add_executable(salias_install_consumer main.cpp)
target_compile_features(salias_install_consumer PRIVATE cxx_std_23)
target_link_libraries(salias_install_consumer PRIVATE salias::salias)

if(DEFINED SALIAS_TEST_LAUNCHER AND NOT SALIAS_TEST_LAUNCHER STREQUAL "")
  set(salias_install_launch_prefix "${SALIAS_TEST_LAUNCHER}")
  if(DEFINED SALIAS_TEST_TSAN_PROBE AND NOT SALIAS_TEST_TSAN_PROBE STREQUAL "")
    set(salias_install_launch_prefix
      "${SALIAS_TEST_LAUNCHER};--tsan-probe;${SALIAS_TEST_TSAN_PROBE};--")
  endif()
  add_test(
    NAME salias_install_consumer
    COMMAND ${salias_install_launch_prefix} "$<TARGET_FILE:salias_install_consumer>"
  )
else()
  add_test(NAME salias_install_consumer COMMAND salias_install_consumer)
endif()
set_tests_properties(salias_install_consumer PROPERTIES TIMEOUT 120)

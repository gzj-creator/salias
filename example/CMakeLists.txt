add_executable(salias_mpsc_subscriber mpsc_subscriber.cpp)
target_link_libraries(salias_mpsc_subscriber PRIVATE salias)
salias_enable_warnings(salias_mpsc_subscriber)
salias_enable_sanitizers(salias_mpsc_subscriber)

add_executable(salias_mpsc_publisher mpsc_publisher.cpp)
target_link_libraries(salias_mpsc_publisher PRIVATE salias)
salias_enable_warnings(salias_mpsc_publisher)
salias_enable_sanitizers(salias_mpsc_publisher)

add_executable(salias_minimal_fifo minimal_fifo.cpp)
target_link_libraries(salias_minimal_fifo PRIVATE salias)
salias_enable_warnings(salias_minimal_fifo)
salias_enable_sanitizers(salias_minimal_fifo)

add_executable(salias_minimal_ordered minimal_ordered.cpp)
target_link_libraries(salias_minimal_ordered PRIVATE salias)
salias_enable_warnings(salias_minimal_ordered)
salias_enable_sanitizers(salias_minimal_ordered)

if(BUILD_TESTING)
  find_program(SALIAS_BASH_EXECUTABLE bash REQUIRED)
  set(SALIAS_TEST_LAUNCHER "${PROJECT_SOURCE_DIR}/cmake/salias_test_launcher.sh")
  set(salias_example_launch_prefix "${SALIAS_TEST_LAUNCHER}")
  if(DEFINED SALIAS_TEST_TSAN_PROBE AND NOT SALIAS_TEST_TSAN_PROBE STREQUAL "")
    set(salias_example_launch_prefix
      "${SALIAS_TEST_LAUNCHER};--tsan-probe;${SALIAS_TEST_TSAN_PROBE};--")
  endif()
  add_test(
    NAME salias_example_smoke
    COMMAND
      ${salias_example_launch_prefix}
      "${SALIAS_BASH_EXECUTABLE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/example_smoke.sh"
      "$<TARGET_FILE:salias_mpsc_subscriber>"
      "$<TARGET_FILE:salias_mpsc_publisher>"
  )
  add_test(
    NAME salias_minimal_fifo
    COMMAND ${salias_example_launch_prefix} "$<TARGET_FILE:salias_minimal_fifo>"
  )
  add_test(
    NAME salias_minimal_ordered
    COMMAND ${salias_example_launch_prefix} "$<TARGET_FILE:salias_minimal_ordered>"
  )
  set_tests_properties(
    salias_example_smoke salias_minimal_fifo salias_minimal_ordered
    PROPERTIES TIMEOUT 120
  )
endif()

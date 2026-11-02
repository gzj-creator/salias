# 示例按使用方式分组：
# - simple/：通用 Channel 的入门与语义示例，底层是 per-producer MagicRing；
# - concurrency/：FixedChannel 的固定槽并发拓扑，按拓扑选择 Fast*Ring。
function(salias_add_example target source)
  add_executable(${target} ${source})
  target_link_libraries(${target} PRIVATE salias)
  salias_enable_warnings(${target})
  salias_enable_sanitizers(${target})
endfunction()

salias_add_example(salias_example_simple_fifo_mpsc simple/fifo_mpsc.cpp)
salias_add_example(salias_example_simple_fifo_mpsc_publisher simple/fifo_mpsc_publisher.cpp)
salias_add_example(salias_example_simple_fifo_mpsc_subscriber simple/fifo_mpsc_subscriber.cpp)
salias_add_example(salias_example_simple_ordered_mpsc simple/ordered_mpsc.cpp)
salias_add_example(salias_example_simple_fifo_mpmc simple/fifo_mpmc.cpp)
salias_add_example(salias_example_simple_ordered_mpmc simple/ordered_mpmc.cpp)
salias_add_example(salias_example_simple_latest_mpmc simple/latest_mpmc.cpp)

salias_add_example(salias_example_concurrency_spsc concurrency/spsc.cpp)
salias_add_example(salias_example_concurrency_mpsc concurrency/mpsc.cpp)
salias_add_example(salias_example_concurrency_spmc concurrency/spmc.cpp)
salias_add_example(salias_example_concurrency_mpmc concurrency/mpmc.cpp)

if(BUILD_TESTING)
  find_program(SALIAS_BASH_EXECUTABLE bash REQUIRED)
  set(SALIAS_TEST_LAUNCHER "${PROJECT_SOURCE_DIR}/cmake/salias_test_launcher.sh")
  set(salias_example_launch_prefix "${SALIAS_TEST_LAUNCHER}")
  if(DEFINED SALIAS_TEST_TSAN_PROBE AND NOT SALIAS_TEST_TSAN_PROBE STREQUAL "")
    set(salias_example_launch_prefix
      "${SALIAS_TEST_LAUNCHER};--tsan-probe;${SALIAS_TEST_TSAN_PROBE};--")
  endif()

  add_test(
    NAME salias_example_simple_fifo_mpsc_ipc
    COMMAND
      ${salias_example_launch_prefix}
      "${SALIAS_BASH_EXECUTABLE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/example_smoke.sh"
      "$<TARGET_FILE:salias_example_simple_fifo_mpsc_subscriber>"
      "$<TARGET_FILE:salias_example_simple_fifo_mpsc_publisher>"
  )

  foreach(example
      salias_example_simple_fifo_mpsc
      salias_example_simple_ordered_mpsc
      salias_example_simple_fifo_mpmc
      salias_example_simple_ordered_mpmc
      salias_example_simple_latest_mpmc
      salias_example_concurrency_spsc
      salias_example_concurrency_mpsc
      salias_example_concurrency_spmc
      salias_example_concurrency_mpmc)
    add_test(
      NAME ${example}
      COMMAND ${salias_example_launch_prefix} "$<TARGET_FILE:${example}>"
    )
  endforeach()

  set_tests_properties(
    salias_example_simple_fifo_mpsc_ipc
    salias_example_simple_fifo_mpsc
    salias_example_simple_ordered_mpsc
    salias_example_simple_fifo_mpmc
    salias_example_simple_ordered_mpmc
    salias_example_simple_latest_mpmc
    salias_example_concurrency_spsc
    salias_example_concurrency_mpsc
    salias_example_concurrency_spmc
    salias_example_concurrency_mpmc
    PROPERTIES TIMEOUT 120
  )
endif()

find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

add_executable(salias_platform_tests
  platform/futex_test.cpp
  platform/mapping_test.cpp
)
target_link_libraries(salias_platform_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_platform_tests)
salias_enable_sanitizers(salias_platform_tests)

if(SALIAS_ENABLE_TSAN)
  find_program(SALIAS_SETARCH_EXECUTABLE setarch REQUIRED)
  add_test(
    NAME salias_platform_tests
    COMMAND
      "${SALIAS_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
      "$<TARGET_FILE:salias_platform_tests>"
  )
else()
  gtest_discover_tests(salias_platform_tests)
endif()

add_executable(salias_ring_tests
  ring/atomic_cell_test.cpp
  ring/magic_ring_test.cpp
)
target_link_libraries(salias_ring_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_ring_tests)
salias_enable_sanitizers(salias_ring_tests)

if(SALIAS_ENABLE_TSAN)
  add_test(
    NAME salias_ring_tests
    COMMAND
      "${SALIAS_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
      "$<TARGET_FILE:salias_ring_tests>"
  )
else()
  gtest_discover_tests(salias_ring_tests)
endif()

add_executable(salias_frame_tests
  frame/codec_test.cpp
)
target_link_libraries(salias_frame_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_frame_tests)
salias_enable_sanitizers(salias_frame_tests)

if(SALIAS_ENABLE_TSAN)
  add_test(
    NAME salias_frame_tests
    COMMAND
      "${SALIAS_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
      "$<TARGET_FILE:salias_frame_tests>"
  )
else()
  gtest_discover_tests(salias_frame_tests)
endif()

add_executable(salias_flow_tests
  flow/spsc_flow_test.cpp
)
target_link_libraries(salias_flow_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_flow_tests)
salias_enable_sanitizers(salias_flow_tests)

if(SALIAS_ENABLE_TSAN)
  add_test(
    NAME salias_flow_tests
    COMMAND
      "${SALIAS_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
      "$<TARGET_FILE:salias_flow_tests>"
  )
else()
  gtest_discover_tests(salias_flow_tests)
endif()

add_executable(salias_wait_tests
  wait/wait_strategy_test.cpp
)
target_link_libraries(salias_wait_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_wait_tests)
salias_enable_sanitizers(salias_wait_tests)

if(SALIAS_ENABLE_TSAN)
  add_test(
    NAME salias_wait_tests
    COMMAND
      "${SALIAS_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
      "$<TARGET_FILE:salias_wait_tests>"
  )
else()
  gtest_discover_tests(salias_wait_tests)
endif()

add_executable(salias_channel_tests
  channel/broadcast_channel_test.cpp
  channel/bulk_channel_test.cpp
  channel/mpsc_channel_test.cpp
  channel/spsc_channel_test.cpp
)
target_link_libraries(salias_channel_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_channel_tests)
salias_enable_sanitizers(salias_channel_tests)

if(SALIAS_ENABLE_TSAN)
  add_test(
    NAME salias_channel_tests
    COMMAND
      "${SALIAS_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
      "$<TARGET_FILE:salias_channel_tests>"
  )
else()
  gtest_discover_tests(salias_channel_tests)
endif()

add_executable(salias_metrics_tests
  metrics/counters_test.cpp
)
target_link_libraries(salias_metrics_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_metrics_tests)
salias_enable_sanitizers(salias_metrics_tests)

if(SALIAS_ENABLE_TSAN)
  add_test(
    NAME salias_metrics_tests
    COMMAND
      "${SALIAS_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
      "$<TARGET_FILE:salias_metrics_tests>"
  )
else()
  gtest_discover_tests(salias_metrics_tests)
endif()

add_executable(salias_api_tests
  api/channel_api_test.cpp
)
target_link_libraries(salias_api_tests PRIVATE salias GTest::gtest_main)

salias_enable_warnings(salias_api_tests)
salias_enable_sanitizers(salias_api_tests)

if(SALIAS_ENABLE_TSAN)
  add_test(
    NAME salias_api_tests
    COMMAND
      "${SALIAS_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
      "$<TARGET_FILE:salias_api_tests>"
  )
else()
  gtest_discover_tests(salias_api_tests)
endif()

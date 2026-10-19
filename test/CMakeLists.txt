set(SALIAS_TEST_LAUNCHER "${PROJECT_SOURCE_DIR}/cmake/salias_test_launcher.sh")
if(NOT EXISTS "${SALIAS_TEST_LAUNCHER}")
  message(FATAL_ERROR "salias test launcher is missing: ${SALIAS_TEST_LAUNCHER}")
endif()
set(salias_test_launch_prefix "${SALIAS_TEST_LAUNCHER}")
if(DEFINED SALIAS_TEST_TSAN_PROBE AND NOT SALIAS_TEST_TSAN_PROBE STREQUAL "")
  set(salias_test_launch_prefix
    "${SALIAS_TEST_LAUNCHER};--tsan-probe;${SALIAS_TEST_TSAN_PROBE};--")
endif()

function(salias_register_test name)
  add_test(NAME "${name}" COMMAND ${salias_test_launch_prefix} ${ARGN})
  set_tests_properties("${name}" PROPERTIES TIMEOUT 120)
endfunction()

add_executable(salias_batch_length_contract_test
  api/batch_length_contract_test.cpp
)
target_link_libraries(salias_batch_length_contract_test PRIVATE salias)
salias_enable_warnings(salias_batch_length_contract_test)
salias_enable_sanitizers(salias_batch_length_contract_test)
salias_register_test(salias_batch_length_contract_test
  "$<TARGET_FILE:salias_batch_length_contract_test>")

add_executable(salias_config_contract_test
  api/config_contract_test.cpp
)
target_link_libraries(salias_config_contract_test PRIVATE salias)
salias_enable_warnings(salias_config_contract_test)
salias_enable_sanitizers(salias_config_contract_test)
salias_register_test(salias_config_contract_test "$<TARGET_FILE:salias_config_contract_test>")

add_executable(salias_capacity_contract_test
  api/capacity_contract_test.cpp
)
target_link_libraries(salias_capacity_contract_test PRIVATE salias)
salias_enable_warnings(salias_capacity_contract_test)
salias_enable_sanitizers(salias_capacity_contract_test)
salias_register_test(salias_capacity_contract_test "$<TARGET_FILE:salias_capacity_contract_test>")

add_executable(salias_allocation_contract_test
  api/allocation_contract_test.cpp
)
target_link_libraries(salias_allocation_contract_test PRIVATE salias)
salias_enable_warnings(salias_allocation_contract_test)
salias_enable_sanitizers(salias_allocation_contract_test)
salias_register_test(salias_allocation_contract_test "$<TARGET_FILE:salias_allocation_contract_test>")

add_executable(salias_commit_visibility_contract_test
  api/commit_visibility_contract_test.cpp
)
target_link_libraries(salias_commit_visibility_contract_test PRIVATE salias)
salias_enable_warnings(salias_commit_visibility_contract_test)
salias_enable_sanitizers(salias_commit_visibility_contract_test)
salias_register_test(salias_commit_visibility_contract_test
  "$<TARGET_FILE:salias_commit_visibility_contract_test>")

add_executable(salias_poll_storage_contract_test api/poll_storage_contract_test.cpp)
target_link_libraries(salias_poll_storage_contract_test PRIVATE salias)
salias_enable_warnings(salias_poll_storage_contract_test)
salias_enable_sanitizers(salias_poll_storage_contract_test)
salias_register_test(salias_poll_storage_contract_test
  "$<TARGET_FILE:salias_poll_storage_contract_test>")

find_package(GTest CONFIG QUIET)
if(NOT GTest_FOUND)
  message(WARNING
    "GTest was not found; skipping salias test targets. "
    "Install GTest or set GTest_DIR to build the tests."
  )
  return()
endif()
add_executable(salias_latency_histogram_tests
  tools/latency_histogram_test.cpp
)
target_include_directories(salias_latency_histogram_tests PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(salias_latency_histogram_tests PRIVATE GTest::gtest_main)

salias_enable_warnings(salias_latency_histogram_tests)
salias_enable_sanitizers(salias_latency_histogram_tests)

salias_register_test(salias_latency_histogram_tests
  "$<TARGET_FILE:salias_latency_histogram_tests>")

add_executable(salias_platform_tests
  platform/mapping_test.cpp
  platform/futex_wait_test.cpp
)
target_link_libraries(salias_platform_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_platform_tests)
salias_enable_sanitizers(salias_platform_tests)

salias_register_test(salias_platform_tests "$<TARGET_FILE:salias_platform_tests>")

add_executable(salias_ring_tests
  ring/atomic_cell_test.cpp
  ring/magic_ring_test.cpp
  ring/ringbuffer_test.cpp
)
target_link_libraries(salias_ring_tests PRIVATE salias_core GTest::gtest_main)
target_include_directories(salias_ring_tests PRIVATE "${PROJECT_SOURCE_DIR}/src/salias/include")

salias_enable_warnings(salias_ring_tests)
salias_enable_sanitizers(salias_ring_tests)

salias_register_test(salias_ring_tests "$<TARGET_FILE:salias_ring_tests>")

add_executable(salias_frame_tests
  frame/codec_test.cpp
)
target_link_libraries(salias_frame_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_frame_tests)
salias_enable_sanitizers(salias_frame_tests)

salias_register_test(salias_frame_tests "$<TARGET_FILE:salias_frame_tests>")

add_executable(salias_flow_tests
  flow/spsc_flow_test.cpp
)
target_link_libraries(salias_flow_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_flow_tests)
salias_enable_sanitizers(salias_flow_tests)

salias_register_test(salias_flow_tests "$<TARGET_FILE:salias_flow_tests>")

add_executable(salias_channel_tests
  channel/hybrid_mpsc_channel_test.cpp
  channel/shared_latest_channel_test.cpp
  channel/shared_reliable_channel_test.cpp
)
target_link_libraries(salias_channel_tests PRIVATE salias_core GTest::gtest_main)

salias_enable_warnings(salias_channel_tests)
salias_enable_sanitizers(salias_channel_tests)

salias_register_test(salias_channel_tests "$<TARGET_FILE:salias_channel_tests>")

add_executable(salias_api_tests
  api/channel_api_test.cpp
  api/latest_subscription_test.cpp
)
target_link_libraries(salias_api_tests PRIVATE salias GTest::gtest_main)

salias_enable_warnings(salias_api_tests)
salias_enable_sanitizers(salias_api_tests)

salias_register_test(salias_api_tests "$<TARGET_FILE:salias_api_tests>")

add_library(salias_test_keyed STATIC "${PROJECT_SOURCE_DIR}/src/salias/src/latest_by_key.cpp")
target_compile_features(salias_test_keyed PUBLIC cxx_std_23)
target_include_directories(salias_test_keyed
  PUBLIC "${PROJECT_SOURCE_DIR}/src/salias/include"
  PRIVATE "${PROJECT_SOURCE_DIR}/src"
)
target_compile_definitions(salias_test_keyed PRIVATE SALIAS_TEST_HOOKS=1)
target_link_libraries(salias_test_keyed PUBLIC salias_core Threads::Threads)
salias_enable_warnings(salias_test_keyed)
salias_enable_sanitizers(salias_test_keyed)

add_executable(salias_latest_by_key_tests
  api/latest_by_key_test.cpp
)
target_compile_definitions(salias_latest_by_key_tests PRIVATE SALIAS_TEST_HOOKS=1)
target_link_libraries(salias_latest_by_key_tests PRIVATE salias_test_keyed GTest::gtest_main)

salias_enable_warnings(salias_latest_by_key_tests)
salias_enable_sanitizers(salias_latest_by_key_tests)

salias_register_test(salias_latest_by_key_tests "$<TARGET_FILE:salias_latest_by_key_tests>")

add_executable(salias_priority_tests
  api/priority_channel_test.cpp
)
target_link_libraries(salias_priority_tests PRIVATE salias GTest::gtest_main)

salias_enable_warnings(salias_priority_tests)
salias_enable_sanitizers(salias_priority_tests)

salias_register_test(salias_priority_tests "$<TARGET_FILE:salias_priority_tests>")

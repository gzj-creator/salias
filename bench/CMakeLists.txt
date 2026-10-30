find_package(Threads REQUIRED)

add_executable(salias_hotpath_ab hotpath_ab.cpp)
target_link_libraries(salias_hotpath_ab PRIVATE salias Threads::Threads)
salias_enable_warnings(salias_hotpath_ab)
salias_enable_sanitizers(salias_hotpath_ab)

add_executable(salias_receiver_scan receiver_scan.cpp)
target_link_libraries(salias_receiver_scan PRIVATE salias)
salias_enable_warnings(salias_receiver_scan)
salias_enable_sanitizers(salias_receiver_scan)

add_executable(salias_round2_probe round2_probe.cpp)
target_link_libraries(salias_round2_probe PRIVATE salias_extras Threads::Threads)
salias_enable_warnings(salias_round2_probe)
salias_enable_sanitizers(salias_round2_probe)

add_executable(salias_fast_ring_compare fast_ring_compare.cpp)
target_link_libraries(salias_fast_ring_compare PRIVATE salias_core Threads::Threads)
salias_enable_warnings(salias_fast_ring_compare)
salias_enable_sanitizers(salias_fast_ring_compare)

add_executable(salias_fast_ring_latency fast_ring_latency.cpp)
target_link_libraries(salias_fast_ring_latency PRIVATE salias_core Threads::Threads)
salias_enable_warnings(salias_fast_ring_latency)
salias_enable_sanitizers(salias_fast_ring_latency)

add_executable(salias_fixed_channel_poll_compare fixed_channel_poll_compare.cpp)
target_link_libraries(salias_fixed_channel_poll_compare PRIVATE salias Threads::Threads)
salias_enable_warnings(salias_fixed_channel_poll_compare)
salias_enable_sanitizers(salias_fixed_channel_poll_compare)

find_package(benchmark CONFIG QUIET)

if(benchmark_FOUND)
  add_executable(salias_bench_round2 round2.cpp)
  target_link_libraries(salias_bench_round2 PRIVATE salias_extras benchmark::benchmark Threads::Threads)
  salias_enable_warnings(salias_bench_round2)
  salias_enable_sanitizers(salias_bench_round2)

  add_executable(salias_bench_latest_by_key latest_by_key.cpp)
  target_link_libraries(salias_bench_latest_by_key PRIVATE salias_extras benchmark::benchmark Threads::Threads)
  salias_enable_warnings(salias_bench_latest_by_key)
  salias_enable_sanitizers(salias_bench_latest_by_key)

  add_executable(salias_bench_channel_stress channel_stress.cpp)
  target_link_libraries(salias_bench_channel_stress PRIVATE salias_extras benchmark::benchmark)

  salias_enable_warnings(salias_bench_channel_stress)
  salias_enable_sanitizers(salias_bench_channel_stress)

  if(BUILD_TESTING)
    add_test(NAME salias_bench_channel_stress
             COMMAND salias_bench_channel_stress --benchmark_min_time=0.01)
  endif()
else()
  message(WARNING
    "Google Benchmark was not found; skipping salias_bench_channel_stress. "
    "Install benchmark or set benchmark_DIR to build this target."
  )
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(salias_ipc_compare ../tools/aeron_compare/salias_ipc_compare.cpp)
  target_link_libraries(salias_ipc_compare PRIVATE salias)

  salias_enable_warnings(salias_ipc_compare)
  salias_enable_sanitizers(salias_ipc_compare)
else()
  message(WARNING "salias_ipc_compare is Linux-only; skipping this target.")
endif()

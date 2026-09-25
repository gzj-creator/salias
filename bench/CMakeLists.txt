find_package(Threads REQUIRED)

add_executable(salias_hotpath_ab hotpath_ab.cpp)
target_link_libraries(salias_hotpath_ab PRIVATE salias Threads::Threads)
salias_enable_warnings(salias_hotpath_ab)
salias_enable_sanitizers(salias_hotpath_ab)

add_executable(salias_receiver_scan receiver_scan.cpp)
target_link_libraries(salias_receiver_scan PRIVATE salias)
salias_enable_warnings(salias_receiver_scan)
salias_enable_sanitizers(salias_receiver_scan)

find_package(benchmark CONFIG QUIET)

if(benchmark_FOUND)
  add_executable(salias_bench_channel_stress channel_stress.cpp)
  target_link_libraries(salias_bench_channel_stress PRIVATE salias benchmark::benchmark)

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

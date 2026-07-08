find_package(benchmark CONFIG REQUIRED)

add_executable(salias_bench_smoke smoke.cpp)
target_link_libraries(salias_bench_smoke PRIVATE salias benchmark::benchmark_main)

salias_enable_warnings(salias_bench_smoke)
salias_enable_sanitizers(salias_bench_smoke)

add_executable(salias_bench_channel_stress channel_stress.cpp)
target_link_libraries(salias_bench_channel_stress PRIVATE salias benchmark::benchmark)

salias_enable_warnings(salias_bench_channel_stress)
salias_enable_sanitizers(salias_bench_channel_stress)

add_executable(salias_bench_compare channel_compare.cpp)
target_link_libraries(salias_bench_compare PRIVATE salias)

salias_enable_warnings(salias_bench_compare)
salias_enable_sanitizers(salias_bench_compare)

add_executable(salias_ipc_compare ../tools/aeron_compare/salias_ipc_compare.cpp)
target_link_libraries(salias_ipc_compare PRIVATE salias)

salias_enable_warnings(salias_ipc_compare)
salias_enable_sanitizers(salias_ipc_compare)

if(BUILD_TESTING)
  add_test(NAME salias_bench_smoke COMMAND salias_bench_smoke --benchmark_min_time=0.001)
  add_test(NAME salias_bench_channel_stress
           COMMAND salias_bench_channel_stress --benchmark_min_time=0.01)
endif()

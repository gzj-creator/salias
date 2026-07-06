find_package(benchmark CONFIG REQUIRED)

add_executable(salias_bench_smoke smoke.cpp)
target_link_libraries(salias_bench_smoke PRIVATE salias benchmark::benchmark_main)

salias_enable_warnings(salias_bench_smoke)
salias_enable_sanitizers(salias_bench_smoke)

if(BUILD_TESTING)
  add_test(NAME salias_bench_smoke COMMAND salias_bench_smoke --benchmark_min_time=0.001)
endif()

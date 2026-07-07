#include <benchmark/benchmark.h>

// 用于确认 benchmark 框架接线正确的最小 benchmark。
static void BM_empty(benchmark::State& state) {
  for (auto _ : state) {
  }
}

BENCHMARK(BM_empty);

#pragma once

#include <atomic>
#include <cstdint>

#include "latency_histogram.hpp"

namespace salias::tools::latency {

// Open-loop scheduling retains the original arrival time when a publisher falls behind.
inline std::uint64_t planned_arrival_ns(std::uint64_t start_ns, std::uint64_t sequence,
                                       std::uint64_t rate, std::uint32_t producer,
                                       std::uint32_t producers) noexcept {
  return start_ns + (sequence / rate) * 1'000'000'000ull +
         (sequence % rate) * 1'000'000'000ull / rate +
         static_cast<std::uint64_t>(producer) * (1'000'000'000ull / rate) / producers;
}

inline void wait_for_arrival(std::uint64_t planned_ns) noexcept {
  while (monotonic_now_ns() < planned_ns) {
#if defined(__i386__) || defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield");
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
  }
}

}  // namespace salias::tools::latency

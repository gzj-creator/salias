#pragma once

#include <atomic>
#include <cstdint>
#include <thread>

#include "core/wait/cpu_relax.hpp"

namespace salias::wait {

class Yielding {
 public:
  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
    int spins = 0;
    while (std::atomic_ref<std::uint32_t>(*word).load(std::memory_order_acquire) == expected) {
      if (++spins < kSpinBeforeYield) {
        cpu_relax();
      } else {
        std::this_thread::yield();
      }
    }
  }

  void wake(std::uint32_t*) noexcept {}
  void reset() noexcept {}

 private:
  static constexpr int kSpinBeforeYield = 64;
};

}  // namespace salias::wait

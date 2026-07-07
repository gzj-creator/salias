#pragma once

#include <atomic>
#include <cstdint>
#include <thread>

#include "core/wait/cpu_relax.hpp"

namespace salias::wait {

class Yielding {
 public:
  // 先短暂自旋，再让出线程，直到 *word 与 expected 不同。
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

  // 空唤醒；yield 等待方直接观察 word。
  void wake(std::uint32_t*) noexcept {}
  // 空重置；Yielding 没有可变状态。
  void reset() noexcept {}

 private:
  static constexpr int kSpinBeforeYield = 64;
};

}  // namespace salias::wait

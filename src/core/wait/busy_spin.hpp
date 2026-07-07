#pragma once

#include <atomic>
#include <cstdint>

namespace salias::wait {

struct BusySpin {
  // 持续自旋，直到 *word 与 expected 不同。
  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
    while (std::atomic_ref<std::uint32_t>(*word).load(std::memory_order_acquire) == expected) {
    }
  }

  // 空唤醒；忙等方直接观察 word。
  void wake(std::uint32_t*) noexcept {}
  // 空重置；BusySpin 没有内部状态。
  void reset() noexcept {}
};

}  // namespace salias::wait

#pragma once

#include <atomic>
#include <cstdint>

#include "core/wait/cpu_relax.hpp"

namespace salias::wait {

struct SpinPause {
  constexpr bool needs_wake() const noexcept { return false; }

  // 使用 cpu_relax() 提示自旋，直到 *word 与 expected 不同。
  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
    while (std::atomic_ref<std::uint32_t>(*word).load(std::memory_order_acquire) == expected) {
      cpu_relax();
    }
  }

  // 空唤醒；自旋等待方直接观察 word。
  void wake(std::uint32_t*) noexcept {}
  // 空重置；SpinPause 没有内部状态。
  void reset() noexcept {}
};

}  // namespace salias::wait

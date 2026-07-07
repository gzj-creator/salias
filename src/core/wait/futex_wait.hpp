#pragma once

#include <atomic>
#include <cstdint>

#include "core/platform/futex.hpp"
#include "core/wait/cpu_relax.hpp"

namespace salias::wait {

class FutexWait {
 public:
  // 创建先短暂自旋、再退化到 futex 睡眠的等待策略。
  explicit FutexWait(int spin_budget = 128) noexcept : spin_budget_(spin_budget) {}

  // 等待直到 *word 与 expected 不同；先有限自旋，再进入 futex wait。
  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
    for (int i = 0; i < spin_budget_; ++i) {
      if (std::atomic_ref<std::uint32_t>(*word).load(std::memory_order_acquire) != expected) {
        return;
      }
      cpu_relax();
    }

    // 安全性：platform::Futex 会在睡眠前原子复查 *word == expected。
    // 调用方必须先写入新 word 值再 wake()，因此错过唤醒会退化为立即 EAGAIN 返回。
    static_cast<void>(platform::Futex(word).wait(expected));
  }

  // 唤醒阻塞在 word 上的一个等待者。
  void wake(std::uint32_t* word) noexcept {
    static_cast<void>(platform::Futex(word).wake_one());
  }

  // 重置等待策略状态；当前没有需要清理的瞬态状态。
  void reset() noexcept {}

 private:
  int spin_budget_;
};

}  // namespace salias::wait

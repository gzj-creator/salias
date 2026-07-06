#pragma once

#include <atomic>
#include <cstdint>

#include "core/platform/futex.hpp"
#include "core/wait/cpu_relax.hpp"

namespace salias::wait {

class FutexWait {
 public:
  explicit FutexWait(int spin_budget = 128) noexcept : spin_budget_(spin_budget) {}

  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
    for (int i = 0; i < spin_budget_; ++i) {
      if (std::atomic_ref<std::uint32_t>(*word).load(std::memory_order_acquire) != expected) {
        return;
      }
      cpu_relax();
    }

    // SAFETY: platform::Futex atomically rechecks *word == expected before sleeping. Callers must
    // store the new word value before wake(), so missed wakeups become immediate EAGAIN returns.
    static_cast<void>(platform::Futex(word).wait(expected));
  }

  void wake(std::uint32_t* word) noexcept {
    static_cast<void>(platform::Futex(word).wake_one());
  }

  void reset() noexcept {}

 private:
  int spin_budget_;
};

}  // namespace salias::wait

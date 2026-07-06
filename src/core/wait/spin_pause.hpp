#pragma once

#include <atomic>
#include <cstdint>

#include "core/wait/cpu_relax.hpp"

namespace salias::wait {

struct SpinPause {
  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
    while (std::atomic_ref<std::uint32_t>(*word).load(std::memory_order_acquire) == expected) {
      cpu_relax();
    }
  }

  void wake(std::uint32_t*) noexcept {}
  void reset() noexcept {}
};

}  // namespace salias::wait

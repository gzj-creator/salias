#pragma once

#include <atomic>
#include <cstdint>

namespace salias::wait {

struct BusySpin {
  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
    while (std::atomic_ref<std::uint32_t>(*word).load(std::memory_order_acquire) == expected) {
    }
  }

  void wake(std::uint32_t*) noexcept {}
  void reset() noexcept {}
};

}  // namespace salias::wait

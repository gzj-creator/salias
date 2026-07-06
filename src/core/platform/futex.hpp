#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

#include "core/platform/error.hpp"

namespace salias::platform {

class Futex {
 public:
  // Non-owning wrapper over a 4-byte aligned futex word. For cross-process wakeups, the word must
  // live in MAP_SHARED memory and all value changes must use atomic operations or the futex syscall.
  explicit Futex(std::uint32_t* word) noexcept;

  // Sleeps only if the kernel still observes *word == expected. EAGAIN, EINTR, and timeout are
  // reported as Ok because callers are expected to re-check the guarded condition.
  PlatformError wait(
      std::uint32_t expected,
      std::optional<std::chrono::nanoseconds> timeout = std::nullopt) noexcept;
  // Wakes at most one waiter. Returns the kernel wake count, or -1 on syscall/precondition failure.
  int wake_one() noexcept;
  // Wakes all current waiters. Returns the kernel wake count, or -1 on syscall/precondition failure.
  int wake_all() noexcept;

 private:
  std::uint32_t* word_;
};

}  // namespace salias::platform

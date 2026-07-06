#include "core/platform/futex.hpp"

#include <errno.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <climits>
#include <cstdint>
#include <ctime>
#include <limits>

namespace salias::platform {

namespace {

bool is_aligned_word(const std::uint32_t* word) noexcept {
  return word != nullptr &&
         (reinterpret_cast<std::uintptr_t>(word) % alignof(std::uint32_t)) == 0;
}

timespec to_timespec(std::chrono::nanoseconds timeout) noexcept {
  if (timeout.count() < 0) {
    timeout = std::chrono::nanoseconds::zero();
  }
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
  const auto nanos = timeout - seconds;
  return timespec{.tv_sec = seconds.count(), .tv_nsec = static_cast<long>(nanos.count())};
}

}  // namespace

Futex::Futex(std::uint32_t* word) noexcept : word_(word) {}

PlatformError Futex::wait(std::uint32_t expected,
                          std::optional<std::chrono::nanoseconds> timeout) noexcept {
  if (!is_aligned_word(word_)) {
    return PlatformError::FutexFailed;
  }

  timespec timeout_value{};
  timespec* timeout_ptr = nullptr;
  if (timeout.has_value()) {
    timeout_value = to_timespec(*timeout);
    timeout_ptr = &timeout_value;
  }

  // SAFETY: word_ is checked for non-null 4-byte alignment above. Callers must place it in shared
  // memory for cross-process use; FUTEX_WAIT atomically verifies *word_ == expected before sleeping.
  const long rc = ::syscall(SYS_futex, word_, FUTEX_WAIT, expected, timeout_ptr, nullptr, 0);
  if (rc == 0) {
    return PlatformError::Ok;
  }

  switch (errno) {
    case EAGAIN:
    case EINTR:
    case ETIMEDOUT:
      return PlatformError::Ok;
    default:
      return PlatformError::FutexFailed;
  }
}

int Futex::wake_one() noexcept {
  if (!is_aligned_word(word_)) {
    return -1;
  }

  // SAFETY: word_ is checked for non-null 4-byte alignment above. FUTEX_WAKE does not dereference
  // user memory in this process beyond the kernel's futex word access contract.
  const long rc = ::syscall(SYS_futex, word_, FUTEX_WAKE, 1, nullptr, nullptr, 0);
  if (rc < 0 || rc > std::numeric_limits<int>::max()) {
    return -1;
  }
  return static_cast<int>(rc);
}

int Futex::wake_all() noexcept {
  if (!is_aligned_word(word_)) {
    return -1;
  }

  // SAFETY: same alignment and lifetime preconditions as wake_one(); INT_MAX requests all current
  // waiters on this futex word.
  const long rc = ::syscall(SYS_futex, word_, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0);
  if (rc < 0 || rc > std::numeric_limits<int>::max()) {
    return -1;
  }
  return static_cast<int>(rc);
}

}  // namespace salias::platform

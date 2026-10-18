/**
 * @file src/core/wait/futex_wait.hpp
 * @brief Process-shared futex wait primitives used by the adaptive receiver.
 *
 * The channel keeps a monotonically changing 32 bit epoch in shared memory.  Receivers snapshot
 * that epoch before scanning and compare-wait on it after the scan.  Linux's futex_waitv lets a
 * priority receiver wait on all of its streams with one absolute, monotonic one millisecond
 * deadline.  Unsupported kernels and non-Linux platforms use the bounded sleep fallback in the
 * caller; they never turn a notification failure into an unbounded wait.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <span>
#include <thread>

#if defined(__linux__)
#include <cerrno>
#include <ctime>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "core/wait/cpu_relax.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::wait {

/// A compare-wait token for one shared epoch word.
struct FutexWaitToken {
  std::uint32_t* word = nullptr;
  std::uint32_t expected = 0;
};

enum class FutexWaitResult { Woken, Unsupported, PlatformFail };

namespace detail {

#if defined(__linux__)
inline FutexWaitResult classify_futex_waitv_error(int error) noexcept {
  if (error == EAGAIN || error == EINTR || error == ETIMEDOUT) {
    return FutexWaitResult::Woken;
  }
  // EPERM commonly means a seccomp profile disabled futex_waitv. Treat that as a missing
  // backend and use the explicit one millisecond polling fallback, like ENOSYS.
  if (error == ENOSYS || error == EOPNOTSUPP || error == EPERM) {
    return FutexWaitResult::Unsupported;
  }
  return FutexWaitResult::PlatformFail;
}

inline FutexWaitResult futex_wait(std::uint32_t* word, std::uint32_t expected,
                                  const timespec* timeout = nullptr) noexcept {
  if (word == nullptr) {
    return FutexWaitResult::PlatformFail;
  }
  const long result = ::syscall(SYS_futex, word, FUTEX_WAIT, expected, timeout, nullptr, 0);
  if (result == 0) {
    return FutexWaitResult::Woken;
  }
  // EAGAIN means the epoch changed between our snapshot and entering the kernel; EINTR and
  // ETIMEDOUT are ordinary wake paths.  The caller always rescans after all of them.
  return errno == EAGAIN || errno == EINTR || errno == ETIMEDOUT
             ? FutexWaitResult::Woken
             : FutexWaitResult::PlatformFail;
}

inline void futex_wake_all(std::uint32_t* word) noexcept {
  if (word != nullptr) {
    static_cast<void>(::syscall(SYS_futex, word, FUTEX_WAKE, std::numeric_limits<int>::max(),
                                nullptr, nullptr, 0));
  }
}

inline timespec monotonic_deadline(std::chrono::milliseconds timeout) noexcept {
  timespec now{};
  ::clock_gettime(CLOCK_MONOTONIC, &now);
  const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(timeout).count();
  now.tv_sec += static_cast<time_t>(nanoseconds / 1'000'000'000LL);
  now.tv_nsec += static_cast<long>(nanoseconds % 1'000'000'000LL);
  if (now.tv_nsec >= 1'000'000'000L) {
    ++now.tv_sec;
    now.tv_nsec -= 1'000'000'000L;
  }
  return now;
}
#endif

}  // namespace detail

/**
 * Adaptive process-shared wait strategy.
 *
 * The strategy starts disabled.  `activate()` is a sticky, one-way transition for a channel; until
 * then producer commits do not perform a wait-word RMW or a kernel wake.  The active flag lives in
 * the same shared control mapping as the epoch, so a consumer activation is observed by producers
 * in other processes.
 */
class AdaptiveFutex {
 public:
  AdaptiveFutex() noexcept = default;

  void bind(std::uint32_t* active) noexcept { active_ = active; }

  bool activate() noexcept {
    if (active_ == nullptr) {
      return false;
    }
    auto active = std::atomic_ref<std::uint32_t>(*active_);
    if (active.load(std::memory_order_acquire) == 0) {
      std::uint32_t expected = 0;
      static_cast<void>(active.compare_exchange_strong(expected, 1, std::memory_order_seq_cst,
                                                       std::memory_order_acquire));
    }
    return true;
  }

  bool needs_wake() const noexcept {
    return active_ != nullptr &&
           std::atomic_ref<std::uint32_t>(*active_).load(std::memory_order_acquire) != 0;
  }

  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
#if defined(__linux__)
    // FUTEX_WAIT (unlike futex_waitv) takes a relative timeout.  Bound this single-stream path
    // to one millisecond; the outer receive loop snapshots and rescans after every timeout. The
    // legacy WaitStrategy contract returns void, so an unexpected syscall error takes a bounded
    // fallback pause instead of silently retrying the failing syscall in a hot loop.
    const timespec timeout{0, 1'000'000};
    if (detail::futex_wait(word, expected, &timeout) == FutexWaitResult::PlatformFail) {
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
#else
    // macOS has no process-shared futex. Use the bounded polling fallback rather than pretending
    // that an event backend exists. The caller performs a complete data scan after this pause.
    (void)word;
    (void)expected;
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
#endif
  }

  void wake(std::uint32_t* word) noexcept {
#if defined(__linux__)
    detail::futex_wake_all(word);
#else
    (void)word;
#endif
  }

  void reset() noexcept {}

  /** Wait on a deduplicated token set until an absolute one millisecond safety deadline. */
  static FutexWaitResult waitv(
      std::span<const FutexWaitToken> tokens,
      std::chrono::milliseconds timeout = std::chrono::milliseconds{1}) noexcept {
#if defined(__linux__) && defined(SYS_futex_waitv)
    // Linux limits this vector to 128 entries.  The caller supplies one token per stream; skip
    // malformed and duplicate addresses without changing the caller's token storage.
    struct futex_waitv waiters[128]{};
    std::uint32_t count = 0;
    for (const auto& token : tokens) {
      if (token.word == nullptr) {
        continue;
      }
      const auto address = reinterpret_cast<std::uintptr_t>(token.word);
      if ((address & (alignof(std::uint32_t) - 1)) != 0) {
        return FutexWaitResult::PlatformFail;
      }
      bool duplicate = false;
      for (std::uint32_t i = 0; i < count; ++i) {
        if (waiters[i].uaddr == address) {
          duplicate = true;
          break;
        }
      }
      if (duplicate) {
        continue;
      }
      if (count == 128) {
        return FutexWaitResult::Unsupported;
      }
      waiters[count].uaddr = address;
      waiters[count].val = token.expected;
      waiters[count].flags = FUTEX_32;
      ++count;
    }
    if (count == 0) {
      return FutexWaitResult::Unsupported;
    }
    const timespec deadline = detail::monotonic_deadline(timeout);
    const long result = ::syscall(SYS_futex_waitv, waiters, count, 0, &deadline, CLOCK_MONOTONIC);
    if (result >= 0) {
      return FutexWaitResult::Woken;
    }
    // EAGAIN (epoch changed), EINTR, and ETIMEDOUT return for a full rescan. Unsupported kernels
    // or seccomp-disabled syscalls use the explicit caller fallback; other errors are surfaced.
    return detail::classify_futex_waitv_error(errno);
#else
    (void)tokens;
    (void)timeout;
    return FutexWaitResult::Unsupported;
#endif
  }

 private:
  std::uint32_t* active_ = nullptr;
};

static_assert(WaitStrategy<AdaptiveFutex>);

}  // namespace salias::wait

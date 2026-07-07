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

// 检查 Linux futex 对非空 32 位对齐 word 的前置条件。
bool is_aligned_word(const std::uint32_t* word) noexcept {
  return word != nullptr &&
         (reinterpret_cast<std::uintptr_t>(word) % alignof(std::uint32_t)) == 0;
}

// 将 chrono 时长转换为 FUTEX_WAIT 使用的相对 timespec。
timespec to_timespec(std::chrono::nanoseconds timeout) noexcept {
  if (timeout.count() < 0) {
    timeout = std::chrono::nanoseconds::zero();
  }
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
  const auto nanos = timeout - seconds;
  return timespec{.tv_sec = seconds.count(), .tv_nsec = static_cast<long>(nanos.count())};
}

}  // namespace

// 保存非持有的 futex word 指针。
Futex::Futex(std::uint32_t* word) noexcept : word_(word) {}

// 当内核仍观察到 expected 值时进入等待。
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

  // 安全性：上面已检查 word_ 非空且 4 字节对齐。
  // 跨进程使用时调用方必须把它放在共享内存中；FUTEX_WAIT 会在睡眠前原子校验 *word_ == expected。
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

// 唤醒 futex word 上最多一个等待者。
int Futex::wake_one() noexcept {
  if (!is_aligned_word(word_)) {
    return -1;
  }

  // 安全性：上面已检查 word_ 非空且 4 字节对齐。
  // FUTEX_WAKE 只按内核 futex word 访问约定触碰用户内存。
  const long rc = ::syscall(SYS_futex, word_, FUTEX_WAKE, 1, nullptr, nullptr, 0);
  if (rc < 0 || rc > std::numeric_limits<int>::max()) {
    return -1;
  }
  return static_cast<int>(rc);
}

// 唤醒 futex word 上所有当前等待者。
int Futex::wake_all() noexcept {
  if (!is_aligned_word(word_)) {
    return -1;
  }

  // 安全性：对齐和生命周期前置条件与 wake_one() 相同；INT_MAX 表示请求唤醒该 word 上的所有当前等待者。
  const long rc = ::syscall(SYS_futex, word_, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0);
  if (rc < 0 || rc > std::numeric_limits<int>::max()) {
    return -1;
  }
  return static_cast<int>(rc);
}

}  // namespace salias::platform

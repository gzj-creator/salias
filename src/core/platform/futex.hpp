#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

#include "core/platform/error.hpp"

namespace salias::platform {

class Futex {
 public:
  // 绑定非持有的 4 字节对齐 futex word；跨进程使用时 word 必须位于 MAP_SHARED 内存。
  // 所有值变更必须通过原子操作或 futex 系统调用完成。
  explicit Futex(std::uint32_t* word) noexcept;

  // 仅当内核仍观察到 *word == expected 时睡眠。
  // EAGAIN、EINTR 和超时返回 Ok，由调用者重新检查受保护条件。
  PlatformError wait(
      std::uint32_t expected,
      std::optional<std::chrono::nanoseconds> timeout = std::nullopt) noexcept;
  // 唤醒最多一个等待者；返回内核唤醒数量，系统调用或前置条件失败时返回 -1。
  int wake_one() noexcept;
  // 唤醒所有当前等待者；返回内核唤醒数量，系统调用或前置条件失败时返回 -1。
  int wake_all() noexcept;

 private:
  std::uint32_t* word_;
};

}  // namespace salias::platform

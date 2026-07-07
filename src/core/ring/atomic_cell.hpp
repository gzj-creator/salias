#pragma once

#include <atomic>
#include <cstdint>

namespace salias::ring {

// 对已有 64 位单元执行 acquire 读取，支持位于共享内存中的单元。
// 安全性：cell 必须 8 字节对齐，并在调用期间存活；所有参与进程/线程都必须用兼容原子操作访问。
inline std::uint64_t load_acquire(const std::uint64_t& cell) noexcept {
  auto& mutable_cell = const_cast<std::uint64_t&>(cell);
  return std::atomic_ref<std::uint64_t>(mutable_cell).load(std::memory_order_acquire);
}

// 以 release 语义发布 64 位单元，供上层交接位置使用。
// 安全性：cell 必须 8 字节对齐，且不能被并发的非原子访问触碰。
inline void store_release(std::uint64_t& cell, std::uint64_t value) noexcept {
  std::atomic_ref<std::uint64_t>(cell).store(value, std::memory_order_release);
}

}  // namespace salias::ring

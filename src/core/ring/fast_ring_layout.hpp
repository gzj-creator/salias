/**
 * @file src/core/ring/fast_ring_layout.hpp
 * @brief Fast Ring 共享布局的公共原子访问、对齐与尺寸工具。
 * @details 控制器和游标按 128 B cache line 隔离，payload slot 固定步长，避免
 *          producer 与 consumer 反复修改同一 cache line。
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>

namespace salias::ring::fast_ring_detail {

inline constexpr std::size_t kCacheLine = 128;
inline constexpr std::size_t kIdentityOffset = 0;
inline constexpr std::size_t kIdentityReadyOffset = 32;

constexpr std::size_t align_up(std::size_t value, std::size_t alignment) noexcept {
  return (value + alignment - 1) & ~(alignment - 1);
}

inline std::atomic_ref<std::uint64_t> atomic_at(std::byte* base, std::size_t offset) noexcept {
  return std::atomic_ref<std::uint64_t>(*reinterpret_cast<std::uint64_t*>(base + offset));
}

inline bool storage_aligned(std::span<std::byte> storage) noexcept {
  return reinterpret_cast<std::uintptr_t>(storage.data()) % kCacheLine == 0;
}

}  // namespace salias::ring::fast_ring_detail

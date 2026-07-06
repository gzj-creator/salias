#pragma once

#include <atomic>
#include <cstdint>

namespace salias::ring {

// Applies acquire semantics to an existing 64-bit cell, including cells located in shared memory.
// SAFETY: cell must be 8-byte aligned, live for the duration of the call, and all participating
// processes/threads must access it through compatible atomic operations.
inline std::uint64_t load_acquire(const std::uint64_t& cell) noexcept {
  auto& mutable_cell = const_cast<std::uint64_t&>(cell);
  return std::atomic_ref<std::uint64_t>(mutable_cell).load(std::memory_order_acquire);
}

// Publishes a 64-bit cell with release semantics. Used by higher layers for position handoff.
// SAFETY: cell must be 8-byte aligned and not concurrently accessed non-atomically.
inline void store_release(std::uint64_t& cell, std::uint64_t value) noexcept {
  std::atomic_ref<std::uint64_t>(cell).store(value, std::memory_order_release);
}

}  // namespace salias::ring

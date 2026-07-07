#include "core/ring/magic_ring.hpp"

#include <utility>

namespace salias::ring {

namespace {

// 判断 value 是否为非零 2 的幂。
bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace

// 校验双映射并将其转换为持有映射的 ring。
MagicRing::CreateResult MagicRing::create(platform::Mapping mapping) noexcept {
  if (mapping.as_ptr() == nullptr || mapping.len() == 0) {
    return std::unexpected(RingError::ZeroLen);
  }
  if (!is_power_of_two(mapping.len())) {
    return std::unexpected(RingError::NotPowerOfTwo);
  }

  auto* const base = mapping.as_ptr();
  const std::size_t cap = mapping.len();
  return MagicRing(std::move(mapping), base, cap);
}

// 从双映射 ring 返回连续只读 span。
std::span<const std::byte> MagicRing::slice(std::uint64_t pos, std::size_t len) const noexcept {
  if (base_ == nullptr || len > cap_) {
    return {};
  }
  // 安全性：create() 只接受 2 的幂容量，L0 保证 [base_, base_ + 2*cap_) 已映射。
  // 当 len <= cap_ 时，即使跨越逻辑容量边界，base_ + (pos & mask()) + len 也有效。
  return {base_ + (pos & mask()), len};
}

// 从双映射 ring 返回连续可写 span。
std::span<std::byte> MagicRing::slice_mut(std::uint64_t pos, std::size_t len) noexcept {
  if (base_ == nullptr || len > cap_) {
    return {};
  }
  // 安全性：地址连续性约束与 slice() 相同；写入所有权由上层协议保证，L1 不建模。
  return {base_ + (pos & mask()), len};
}

// 保存已校验的映射、起始指针和逻辑容量。
MagicRing::MagicRing(platform::Mapping mapping, std::byte* base, std::size_t cap) noexcept
    : base_(base), cap_(cap), mapping_(std::move(mapping)) {}

}  // namespace salias::ring

#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "core/platform/mapping.hpp"
#include "core/ring/error.hpp"

namespace salias::ring {

class MagicRing {
 public:
  using CreateResult = std::expected<MagicRing, RingError>;

  // 接管有效的 L0 双映射；映射长度作为逻辑容量，第二段虚拟地址用于跨尾连续访问。
  static CreateResult create(platform::Mapping mapping) noexcept;

  // 创建空 ring 句柄。
  MagicRing() noexcept = default;
  // 从另一个 ring 转移映射所有权。
  MagicRing(MagicRing&&) noexcept = default;
  // 移动赋值映射所有权。
  MagicRing& operator=(MagicRing&&) noexcept = default;
  // 禁止拷贝；ring 独占映射所有权。
  MagicRing(const MagicRing&) = delete;
  // 禁止拷贝赋值；ring 独占映射所有权。
  MagicRing& operator=(const MagicRing&) = delete;

  // 返回单段逻辑环形容量。
  std::size_t capacity() const noexcept { return cap_; }
  // 返回 2 的幂容量对应的索引掩码；空 ring 返回 0。
  std::size_t mask() const noexcept { return cap_ == 0 ? 0 : cap_ - 1; }
  // 返回 len 是否能放入单个逻辑环形容量。
  bool fits(std::size_t len) const noexcept { return len <= cap_; }

  // 约束：len 必须不超过 capacity()；调用者负责位置所有权和发布顺序。
  // L1 只保证地址连续性。
  std::span<const std::byte> slice(std::uint64_t pos, std::size_t len) const noexcept;
  // 约束：len 必须不超过 capacity()，且调用者必须拥有该逻辑写入区间。
  std::span<std::byte> slice_mut(std::uint64_t pos, std::size_t len) noexcept;

 private:
  // 保存 create() 校验后的映射所有权、起始地址与容量。
  MagicRing(platform::Mapping mapping, std::byte* base, std::size_t cap) noexcept;

  std::byte* base_ = nullptr;
  std::size_t cap_ = 0;
  platform::Mapping mapping_;
};

}  // namespace salias::ring

#include "core/ring/magic_ring.hpp"

#include <utility>

namespace salias::ring {

namespace {

bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace

MagicRing::CreateResult MagicRing::create(platform::Mapping mapping) noexcept {
  if (mapping.as_ptr() == nullptr || mapping.len() == 0) {
    return MagicRing::CreateResult::failure(RingError::ZeroLen);
  }
  if (!is_power_of_two(mapping.len())) {
    return MagicRing::CreateResult::failure(RingError::NotPowerOfTwo);
  }

  auto* const base = mapping.as_ptr();
  const std::size_t cap = mapping.len();
  return MagicRing::CreateResult::success(MagicRing(std::move(mapping), base, cap));
}

std::span<const std::byte> MagicRing::slice(std::uint64_t pos, std::size_t len) const noexcept {
  if (base_ == nullptr || len > cap_) {
    return {};
  }
  // SAFETY: create() only accepts power-of-two capacities and L0 guarantees the address range
  // [base_, base_ + 2*cap_) is mapped. For len <= cap_, base_ + (pos & mask()) + len is valid even
  // when it crosses the logical capacity boundary.
  return {base_ + (pos & mask()), len};
}

std::span<std::byte> MagicRing::slice_mut(std::uint64_t pos, std::size_t len) noexcept {
  if (base_ == nullptr || len > cap_) {
    return {};
  }
  // SAFETY: same address-continuity invariant as slice(); mutation ownership is a higher-layer
  // protocol concern and is intentionally not modeled in L1.
  return {base_ + (pos & mask()), len};
}

MagicRing::MagicRing(platform::Mapping mapping, std::byte* base, std::size_t cap) noexcept
    : base_(base), cap_(cap), mapping_(std::move(mapping)) {}

}  // namespace salias::ring

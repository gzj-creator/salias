#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "core/platform/mapping.hpp"
#include "core/platform/result.hpp"
#include "core/ring/error.hpp"

namespace salias::ring {

class MagicRing {
 public:
  using CreateResult = platform::Result<MagicRing, RingError>;

  // Takes ownership of a valid L0 double mapping. The mapping length becomes the logical ring
  // capacity; the second virtual segment remains available for contiguous wraparound spans.
  static CreateResult create(platform::Mapping mapping) noexcept;

  MagicRing() noexcept = default;
  MagicRing(MagicRing&&) noexcept = default;
  MagicRing& operator=(MagicRing&&) noexcept = default;
  MagicRing(const MagicRing&) = delete;
  MagicRing& operator=(const MagicRing&) = delete;

  std::size_t capacity() const noexcept { return cap_; }
  std::size_t mask() const noexcept { return cap_ == 0 ? 0 : cap_ - 1; }
  bool fits(std::size_t len) const noexcept { return len <= cap_; }

  // SAFETY: len must be <= capacity(). Callers are responsible for position ownership and
  // release/acquire publication; L1 only guarantees address continuity.
  std::span<const std::byte> slice(std::uint64_t pos, std::size_t len) const noexcept;
  // SAFETY: len must be <= capacity(), and the caller must own the writable logical range.
  std::span<std::byte> slice_mut(std::uint64_t pos, std::size_t len) noexcept;

 private:
  MagicRing(platform::Mapping mapping, std::byte* base, std::size_t cap) noexcept;

  std::byte* base_ = nullptr;
  std::size_t cap_ = 0;
  platform::Mapping mapping_;
};

}  // namespace salias::ring

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "core/flow/error.hpp"
#include "core/flow/position.hpp"
#include "core/platform/result.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias::flow {

struct Claim {
  std::span<std::byte> payload;
  std::uint64_t start_pos = 0;
  std::uint32_t payload_len = 0;
  std::uint32_t meta = 0;
};

// SPSC producer side. It is a non-owning view over a MagicRing and position cells; the caller must
// keep the ring and Positions storage alive. This class assumes exactly one writer owns producer.
class Producer {
 public:
  using ClaimResult = platform::Result<Claim, FlowError>;

  Producer(ring::MagicRing& ring, Positions positions) noexcept;

  // Reserves writable payload bytes without publishing them. The returned region is private to this
  // producer until commit() performs the release-store to the producer position.
  ClaimResult claim(std::uint32_t payload_len) noexcept;

  // Writes the frame header and publishes the claimed frame. Payload bytes must already be written.
  void commit(const Claim& claim) noexcept;

 private:
  ring::MagicRing* ring_;
  Positions positions_;
  std::uint64_t cached_head_ = 0;
};

}  // namespace salias::flow

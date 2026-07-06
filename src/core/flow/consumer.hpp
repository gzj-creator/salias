#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

#include "core/flow/position.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias::flow {

struct Message {
  std::span<const std::byte> payload;
  std::uint64_t position = 0;
  std::uint64_t next_position = 0;
  std::uint32_t meta = 0;
};

// SPSC consumer side. It never owns the ring or position storage. poll() observes producer position
// with acquire semantics and advance() releases consumer progress back to the producer.
class Consumer {
 public:
  Consumer(const ring::MagicRing& ring, Positions positions) noexcept;

  std::optional<Message> poll() noexcept;
  void advance(std::uint64_t new_head) noexcept;

 private:
  const ring::MagicRing* ring_;
  Positions positions_;
  std::uint64_t cached_tail_ = 0;
};

}  // namespace salias::flow

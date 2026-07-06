#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace salias {

// Borrowed view over a received message. The payload points into the owning Channel's ring and is
// valid until that storage is released or overwritten according to channel protocol.
struct Message {
  std::span<const std::byte> payload;
  std::uint64_t position = 0;
  std::uint64_t next_position = 0;
  std::uint32_t meta = 0;
};

}  // namespace salias

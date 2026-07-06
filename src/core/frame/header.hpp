#pragma once

#include <cstddef>
#include <cstdint>

namespace salias::frame {

inline constexpr std::size_t kHeaderSize = 8;
inline constexpr std::size_t kFrameAlign = 8;

// Frame metadata flags. Standard one-frame messages use BEGIN | END | COMMITTED.
enum Flags : std::uint32_t {
  FLAG_BEGIN = 1u << 0,
  FLAG_END = 1u << 1,
  FLAG_PADDING = 1u << 2,
  FLAG_COMMITTED = 1u << 3,
};

// Fixed 8-byte on-ring header. Fields are written in host little-endian order because salias is
// same-host Linux IPC only; heterogeneous byte-order transport is intentionally outside L2 scope.
struct FrameHeader {
  std::uint32_t len;
  std::uint32_t meta;
};

static_assert(sizeof(FrameHeader) == kHeaderSize);

}  // namespace salias::frame

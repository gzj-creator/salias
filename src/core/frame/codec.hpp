#pragma once

#include <cstring>
#include <span>

#include "core/frame/header.hpp"

namespace salias::frame {

// Rounds byte counts up to the fixed frame alignment. The alignment is a power of two, so this is
// deliberately mask-based and has no division in the hot path.
inline constexpr std::size_t align_up(std::size_t n) noexcept {
  return (n + (kFrameAlign - 1)) & ~(kFrameAlign - 1);
}

// Total bytes occupied in the ring by a standard frame: 8-byte header plus aligned payload.
inline constexpr std::size_t frame_len(std::size_t payload_len) noexcept {
  return kHeaderSize + align_up(payload_len);
}

// Slot width for fixed-size/no-header mode. L2 only defines the size calculation; L3/L5 decide
// whether a channel is allowed to use this mode.
inline constexpr std::size_t fixed_slot(std::size_t record_size) noexcept {
  return align_up(record_size);
}

// Encodes a header into caller-owned storage. This is a pure byte-layout function: no allocation,
// no I/O, and no synchronization. Publication ordering is supplied by L3's release store.
inline void encode_header(std::span<std::byte, kHeaderSize> dst, std::uint32_t len,
                          std::uint32_t meta) noexcept {
  std::memcpy(dst.data(), &len, sizeof(len));
  std::memcpy(dst.data() + sizeof(len), &meta, sizeof(meta));
}

// Decodes a header from caller-owned storage. The caller must guarantee that src contains exactly
// kHeaderSize readable bytes, typically after L3 has observed producer position with acquire.
inline FrameHeader decode_header(std::span<const std::byte, kHeaderSize> src) noexcept {
  FrameHeader header{};
  std::memcpy(&header.len, src.data(), sizeof(header.len));
  std::memcpy(&header.meta, src.data() + sizeof(header.len), sizeof(header.meta));
  return header;
}

inline std::uint32_t flags(const FrameHeader& header) noexcept {
  return header.meta & 0xFFu;
}

inline std::uint32_t seq(const FrameHeader& header) noexcept {
  return header.meta >> 8;
}

}  // namespace salias::frame

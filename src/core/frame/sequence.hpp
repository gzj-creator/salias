#pragma once

#include <cstdint>

namespace salias::frame {

inline constexpr std::uint32_t kSequenceLowBits = 24;
inline constexpr std::uint32_t kSequenceLowMask = (1u << kSequenceLowBits) - 1u;

// Return the sequence bits stored in the high 24 bits of the 8-byte frame header metadata word.
inline constexpr std::uint32_t sequence_low(std::uint64_t sequence) noexcept {
  return static_cast<std::uint32_t>(sequence) & kSequenceLowMask;
}

// Extract the low sequence bits from a frame metadata word.
inline constexpr std::uint32_t sequence_low_from_meta(std::uint32_t meta) noexcept {
  return (meta >> 8) & kSequenceLowMask;
}

// Split a 64-bit global sequence across the 24-bit metadata field and side metadata.
inline void encode_sequence(std::uint64_t sequence, std::uint32_t& meta_inout,
                            std::uint64_t& sequence_high_out) noexcept {
  meta_inout = (sequence_low(sequence) << 8) | (meta_inout & 0xFFu);
  sequence_high_out = sequence >> kSequenceLowBits;
}

// Rebuild a 64-bit global sequence from the frame metadata word and side metadata.
inline constexpr std::uint64_t decode_sequence(std::uint32_t meta,
                                               std::uint64_t sequence_high) noexcept {
  return (sequence_high << kSequenceLowBits) | sequence_low_from_meta(meta);
}

}  // namespace salias::frame

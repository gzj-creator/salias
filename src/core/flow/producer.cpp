#include "core/flow/producer.hpp"

#include <array>
#include <cstring>

#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/ring/atomic_cell.hpp"

namespace salias::flow {

namespace {

std::uint32_t standard_meta(std::uint64_t start_pos, std::size_t cap) noexcept {
  const auto generation = static_cast<std::uint32_t>((start_pos / cap) & 0x00FF'FFFFu);
  return (generation << 8) | frame::FLAG_BEGIN | frame::FLAG_END | frame::FLAG_COMMITTED;
}

bool has_capacity(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                  std::size_t need) noexcept {
  const std::uint64_t used = tail - head;
  return used <= capacity && need <= capacity - used;
}

}  // namespace

Producer::Producer(ring::MagicRing& ring, Positions positions) noexcept
    : ring_(&ring), positions_(positions) {}

Producer::ClaimResult Producer::claim(std::uint32_t payload_len) noexcept {
  if (ring_ == nullptr || positions_.producer == nullptr || positions_.consumer == nullptr ||
      positions_.cap == 0) {
    return Producer::ClaimResult::failure(FlowError::MessageTooLarge);
  }

  const std::size_t need = frame::frame_len(payload_len);
  if (need > positions_.cap) {
    return Producer::ClaimResult::failure(FlowError::MessageTooLarge);
  }

  const std::uint64_t tail = *positions_.producer;
  if (!has_capacity(positions_.cap, tail, cached_head_, need)) {
    // SAFETY: consumer position is written by the consumer with release semantics in advance().
    // This acquire load observes that release and any prior consumer reads before reusing space.
    cached_head_ = ring::load_acquire(*positions_.consumer);
    if (!has_capacity(positions_.cap, tail, cached_head_, need)) {
      return Producer::ClaimResult::failure(FlowError::BackPressured);
    }
  }

  return Producer::ClaimResult::success(Claim{
      .payload = ring_->slice_mut(tail + frame::kHeaderSize, payload_len),
      .start_pos = tail,
      .payload_len = payload_len,
      .meta = standard_meta(tail, positions_.cap),
  });
}

void Producer::commit(const Claim& claim) noexcept {
  std::array<std::byte, frame::kHeaderSize> header{};
  frame::encode_header(header, claim.payload_len, claim.meta);
  auto header_dst = ring_->slice_mut(claim.start_pos, frame::kHeaderSize);
  std::memcpy(header_dst.data(), header.data(), header.size());

  // SAFETY: payload and header bytes are written before this release-store. Consumers acquire-load
  // producer position before reading the frame, forming the SPSC publication happens-before edge.
  ring::store_release(*positions_.producer, claim.start_pos + frame::frame_len(claim.payload_len));
}

}  // namespace salias::flow

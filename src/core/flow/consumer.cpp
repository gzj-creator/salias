#include "core/flow/consumer.hpp"

#include <array>
#include <cstring>

#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/ring/atomic_cell.hpp"

namespace salias::flow {

Consumer::Consumer(const ring::MagicRing& ring, Positions positions) noexcept
    : ring_(&ring), positions_(positions) {}

std::optional<Message> Consumer::poll() noexcept {
  if (ring_ == nullptr || positions_.producer == nullptr || positions_.consumer == nullptr) {
    return std::nullopt;
  }

  const std::uint64_t head = *positions_.consumer;
  if (head >= cached_tail_) {
    // SAFETY: producer commit() release-stores producer position after writing header and payload.
    // This acquire load makes those bytes visible before decode_header() and payload reads.
    cached_tail_ = ring::load_acquire(*positions_.producer);
    if (head == cached_tail_) {
      return std::nullopt;
    }
  }

  auto header_src = ring_->slice(head, frame::kHeaderSize);
  std::array<std::byte, frame::kHeaderSize> header_bytes{};
  std::memcpy(header_bytes.data(), header_src.data(), header_bytes.size());
  const frame::FrameHeader header = frame::decode_header(header_bytes);
  const std::uint64_t next = head + frame::frame_len(header.len);

  return Message{
      .payload = ring_->slice(head + frame::kHeaderSize, header.len),
      .position = head,
      .next_position = next,
      .meta = header.meta,
  };
}

void Consumer::advance(std::uint64_t new_head) noexcept {
  // SAFETY: advancing consumer position releases space back to the producer. Producer claim()
  // acquire-loads this cell before deciding that old bytes may be overwritten.
  ring::store_release(*positions_.consumer, new_head);
}

}  // namespace salias::flow

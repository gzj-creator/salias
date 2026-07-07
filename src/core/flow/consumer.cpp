#include "core/flow/consumer.hpp"

#include <array>
#include <cstring>

#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/ring/atomic_cell.hpp"

namespace salias::flow {

// 保存非持有的 ring 引用和共享位置单元。
Consumer::Consumer(const ring::MagicRing& ring, Positions positions) noexcept
    : ring_(&ring), positions_(positions) {}

// 读取下一条可用帧，但不发布消费者进度。
std::optional<Message> Consumer::poll() noexcept {
  if (ring_ == nullptr || positions_.producer == nullptr || positions_.consumer == nullptr) {
    return std::nullopt;
  }

  const std::uint64_t head = *positions_.consumer;
  if (head >= cached_tail_) {
    // 安全性：生产者 commit() 写完 header 和 payload 后以 release 语义发布位置。
    // 这里的 acquire load 保证 decode_header() 和读取 payload 前可见这些字节。
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

// 发布消费者 head，使生产者可回收 ring 空间。
void Consumer::advance(std::uint64_t new_head) noexcept {
  // 安全性：推进消费者位置会把空间 release 给生产者。
  // Producer::claim() 决定覆盖旧字节前会 acquire-load 该单元。
  ring::store_release(*positions_.consumer, new_head);
}

}  // namespace salias::flow

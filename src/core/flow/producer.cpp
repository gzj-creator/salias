#include "core/flow/producer.hpp"

#include <array>
#include <cstring>

#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/ring/atomic_cell.hpp"

namespace salias::flow {

namespace {

// 根据起始位置编码标准已提交 SPSC 帧 metadata。
std::uint32_t standard_meta(std::uint64_t start_pos, std::size_t cap) noexcept {
  const auto generation = static_cast<std::uint32_t>((start_pos / cap) & 0x00FF'FFFFu);
  return (generation << 8) | frame::FLAG_BEGIN | frame::FLAG_END | frame::FLAG_COMMITTED;
}

// 判断 tail/head 窗口是否还能容纳请求的帧大小。
bool has_capacity(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                  std::size_t need) noexcept {
  const std::uint64_t used = tail - head;
  return used <= capacity && need <= capacity - used;
}

}  // namespace

// 保存非持有的 ring 引用和共享位置单元。
Producer::Producer(ring::MagicRing& ring, Positions positions) noexcept
    : ring_(&ring), positions_(positions) {}

// 在 ring 剩余容量足够时预留可写 payload 区域。
Producer::ClaimResult Producer::claim(std::uint32_t payload_len) noexcept {
  if (ring_ == nullptr || positions_.producer == nullptr || positions_.consumer == nullptr ||
      positions_.cap == 0) {
    return std::unexpected(FlowError::MessageTooLarge);
  }

  const std::size_t need = frame::frame_len(payload_len);
  if (need > positions_.cap) {
    return std::unexpected(FlowError::MessageTooLarge);
  }

  const std::uint64_t tail = *positions_.producer;
  if (!has_capacity(positions_.cap, tail, cached_head_, need)) {
    // 安全性：消费者在 advance() 中以 release 语义写入位置。
    // 这里的 acquire load 能在复用空间前观察到该发布和消费者此前的读取。
    cached_head_ = ring::load_acquire(*positions_.consumer);
    if (!has_capacity(positions_.cap, tail, cached_head_, need)) {
      return std::unexpected(FlowError::BackPressured);
    }
  }

  return Claim{
      .payload = ring_->slice_mut(tail + frame::kHeaderSize, payload_len),
      .start_pos = tail,
      .payload_len = payload_len,
      .meta = standard_meta(tail, positions_.cap),
  };
}

// 写入帧头并发布生产者位置。
void Producer::commit(const Claim& claim) noexcept {
  std::array<std::byte, frame::kHeaderSize> header{};
  frame::encode_header(header, claim.payload_len, claim.meta);
  auto header_dst = ring_->slice_mut(claim.start_pos, frame::kHeaderSize);
  std::memcpy(header_dst.data(), header.data(), header.size());

  // 安全性：payload 和 header 在此 release-store 前已写完。
  // 消费者读取帧前会 acquire-load 生产者位置，形成 SPSC 发布的 happens-before 边。
  ring::store_release(*positions_.producer, claim.start_pos + frame::frame_len(claim.payload_len));
}

}  // namespace salias::flow

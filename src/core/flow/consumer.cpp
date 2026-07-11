/**
 * @file src/core/flow/consumer.cpp
 * @brief 实现 SPSC 消费端：读取已提交帧并推进消费位置。
 * @details 位于 L3 flow 子层，组合 L1 ring(MagicRing) 的内存视图与 L0 atomic_cell
 *          的 acquire/release 原子访问，构成 SPSC 单消费者读取路径。核心不变式：
 *          生产者 commit() 以 release 写 tail，消费者 poll() 以 acquire 读 tail，
 *          二者配对保证帧头与 payload 在被读取前已完整可见。线程模型：单消费者独占。
 */
#include "core/flow/consumer.hpp"

#include <array>
#include <cstring>

#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/ring/atomic_cell.hpp"

namespace salias::flow {  // flow 层：环形缓冲之上的生产/消费位置与流控逻辑

/// @brief 保存非持有的 ring 引用和共享位置单元。
Consumer::Consumer(const ring::MagicRing& ring, Positions positions) noexcept
    : ring_(&ring), positions_(positions) {}

/// @brief 读取下一条可用帧，但不发布消费者进度。
std::optional<Message> Consumer::poll() noexcept {
  // 未绑定 ring 或位置单元时无可消费数据，安全返回空。
  if (ring_ == nullptr || positions_.producer == nullptr || positions_.consumer == nullptr) {
    return std::nullopt;
  }

  const std::uint64_t head = *positions_.consumer;
  if (head >= cached_tail_) {
    // 安全性：生产者 commit() 写完 header 和 payload 后以 release 语义发布位置。
    // 这里的 acquire load 保证 decode_header() 和读取 payload 前可见这些字节。
    cached_tail_ = ring::load_acquire(*positions_.producer);
    if (head == cached_tail_) {
      // head 追上 tail：缓冲为空，生产者尚未提交新帧。
      return std::nullopt;
    }
  }

  // 将帧头从 ring(可能为共享内存)拷贝到栈缓冲，再解码；避免对共享内存做复杂访问。
  auto header_src = ring_->slice(head, frame::kHeaderSize);
  std::array<std::byte, frame::kHeaderSize> header_bytes{};
  std::memcpy(header_bytes.data(), header_src.data(), header_bytes.size());
  const frame::FrameHeader header = frame::decode_header(header_bytes);
  // 下一帧位置按 frame_len 对齐(含帧头+padding)，保证环绕边界正确。
  const std::uint64_t next = head + frame::frame_len(header.len);

  return Message{
      .payload = ring_->slice(head + frame::kHeaderSize, header.len),
      .position = head,
      .next_position = next,
      .meta = header.meta,
  };
}

/// @brief 发布消费者 head，使生产者可回收 ring 空间。
void Consumer::advance(std::uint64_t new_head) noexcept {
  // 安全性：推进消费者位置会把空间 release 给生产者。
  // Producer::claim() 决定覆盖旧字节前会 acquire-load 该单元。
  ring::store_release(*positions_.consumer, new_head);
}

}  // namespace salias::flow

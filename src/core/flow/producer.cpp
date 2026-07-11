/**
 * @file src/core/flow/producer.cpp
 * @brief 实现 SPSC 生产端：预留可写区域、写入帧头并发布生产者位置。
 * @details 位于 L3 flow 子层，组合 L1 ring(MagicRing) 的可写内存视图、L2 frame
 *          的帧头编解码与 L0 atomic_cell 的 acquire/release 原子访问，构成 SPSC
 *          单生产者写入路径。核心不变式：claim→写 payload→commit 两阶段发布，
 *          commit 以 release store 推进 tail，与消费者 poll() 的 acquire load 配对，
 *          保证消费者可见帧时帧头与 payload 完整。背压：空间不足时返回 BackPressured。
 *          线程模型：单生产者独占。
 */
#include "core/flow/producer.hpp"

#include <array>
#include <cstring>

#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/ring/atomic_cell.hpp"

namespace salias::flow {  // flow 层：环形缓冲之上的生产/消费位置与流控逻辑

namespace {  // 匿名命名空间：仅本翻译单元可见的内部辅助函数

/// @brief 根据起始位置编码标准已提交 SPSC 帧 metadata。
/// @param start_pos 帧在 ring 中的起始字节偏移。
/// @param cap ring 容量(字节)，用于推算 generation(环绕世代号)。
/// @return 编码后的 32 位 metadata：generation 占高 24 位，标志位占低 8 位。
/// @details generation = start_pos / cap，表示该位置经历过多少次环绕写，
///          消费者可据此校验读到的帧是否为最新世代(检测覆盖)。低 8 位同时置
///          BEGIN/END/COMMITTED 标志，表示单帧完整且已提交。
std::uint32_t standard_meta(std::uint64_t start_pos, std::size_t cap) noexcept {
  // generation 取低 24 位，足以表示约 1600 万次环绕(2^24)。
  const auto generation = static_cast<std::uint32_t>((start_pos / cap) & 0x00FF'FFFFu);
  return (generation << 8) | frame::FLAG_BEGIN | frame::FLAG_END | frame::FLAG_COMMITTED;
}

/// @brief 判断 tail/head 窗口是否还能容纳请求的帧大小。
/// @param capacity ring 总容量(字节)。
/// @param tail 生产者下一个写入位置(单调递增)。
/// @param head 消费者下一个读取位置(单调递增)。
/// @param need 请求的字节数(含帧头+对齐)。
/// @retval true  剩余空间足够。
/// @retval false 空间不足(背压)。
/// @details used = tail - head 为当前已用空间(无符号减法，因 tail>=head 恒成立)。
///          必须同时满足 used<=capacity 且 need<=capacity-used，避免环绕越界。
bool has_capacity(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                  std::size_t need) noexcept {
  const std::uint64_t used = tail - head;
  return used <= capacity && need <= capacity - used;
}

}  // namespace

/// @brief 保存非持有的 ring 引用和共享位置单元。
Producer::Producer(ring::MagicRing& ring, Positions positions) noexcept
    : ring_(&ring), positions_(positions) {}

/// @brief 在 ring 剩余容量足够时预留可写 payload 区域。
Producer::ClaimResult Producer::claim(std::uint32_t payload_len) noexcept {
  // 未绑定或容量非法：按不可恢复错误返回(而非背压)。
  if (ring_ == nullptr || positions_.producer == nullptr || positions_.consumer == nullptr ||
      positions_.cap == 0) {
    return std::unexpected(FlowError::MessageTooLarge);
  }

  // need 含帧头+payload+对齐 padding，超过 ring 总容量则永远无法写入。
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
      // 重新读取后仍不足：消费者尚未腾出空间，触发背压。
      return std::unexpected(FlowError::BackPressured);
    }
  }

  // 预留成功：slice_mut 跳过帧头位置，直接给出 payload 区(帧头由 commit 写入)。
  return Claim{
      .payload = ring_->slice_mut(tail + frame::kHeaderSize, payload_len),
      .start_pos = tail,
      .payload_len = payload_len,
      .meta = standard_meta(tail, positions_.cap),
  };
}

/// @brief 写入帧头并发布生产者位置。
void Producer::commit(const Claim& claim) noexcept {
  // 帧头先在栈缓冲编码，再一次性 memcpy 进 ring，避免对共享内存的多字节非原子写。
  std::array<std::byte, frame::kHeaderSize> header{};
  frame::encode_header(header, claim.payload_len, claim.meta);
  auto header_dst = ring_->slice_mut(claim.start_pos, frame::kHeaderSize);
  std::memcpy(header_dst.data(), header.data(), header.size());

  // 安全性：payload 和 header 在此 release-store 前已写完。
  // 消费者读取帧前会 acquire-load 生产者位置，形成 SPSC 发布的 happens-before 边。
  // 推进 tail 为 start_pos + frame_len，含对齐，保证下一帧起始对齐。
  ring::store_release(*positions_.producer, claim.start_pos + frame::frame_len(claim.payload_len));
}

}  // namespace salias::flow

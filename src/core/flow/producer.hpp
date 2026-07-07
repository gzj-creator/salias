#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "core/flow/error.hpp"
#include "core/flow/position.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias::flow {

struct Claim {
  std::span<std::byte> payload;
  std::uint64_t start_pos = 0;
  std::uint32_t payload_len = 0;
  std::uint32_t meta = 0;
};

// SPSC 生产端；它只借用 MagicRing 和位置单元，调用方必须保证 ring 与 Positions 存活。
// 该类假定恰好一个写入者拥有 producer。
class Producer {
 public:
  using ClaimResult = std::expected<Claim, FlowError>;

  // 绑定非持有的 ring 与生产者/消费者位置单元。
  Producer(ring::MagicRing& ring, Positions positions) noexcept;

  // 预留可写 payload 区域但不发布；返回区域在 commit() 前仅归当前生产者使用。
  ClaimResult claim(std::uint32_t payload_len) noexcept;

  // 写入帧头并发布已预留的帧；调用前 payload 必须已经写完。
  void commit(const Claim& claim) noexcept;

 private:
  ring::MagicRing* ring_;
  Positions positions_;
  std::uint64_t cached_head_ = 0;
};

}  // namespace salias::flow

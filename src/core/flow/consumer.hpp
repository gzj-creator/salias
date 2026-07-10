#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

#include "core/flow/position.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias::flow {

struct Message {
  std::span<const std::byte> payload;
  std::uint64_t position = 0;
  std::uint64_t next_position = 0;
  std::uint32_t meta = 0;
  std::uint64_t sequence = 0;
  std::uint32_t producer_id = 0;
};

// SPSC 消费端；它不拥有 ring 或位置存储。
// poll() 以 acquire 语义观察生产者位置，advance() 将消费者进度 release 给生产者。
class Consumer {
 public:
  // 绑定非持有的 ring 与生产者/消费者位置单元。
  Consumer(const ring::MagicRing& ring, Positions positions) noexcept;

  // 非阻塞读取下一条已提交帧，但不推进消费者位置。
  std::optional<Message> poll() noexcept;
  // 发布消费者进度，使生产者可以复用已释放的环形空间。
  void advance(std::uint64_t new_head) noexcept;

 private:
  const ring::MagicRing* ring_;
  Positions positions_;
  std::uint64_t cached_tail_ = 0;
};

}  // namespace salias::flow

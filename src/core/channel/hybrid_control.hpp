#pragma once

#include <atomic>
#include <cstdint>

namespace salias::channel {

enum class Order : std::uint8_t {
  Fifo,
  Ordered,
};

// Shared sequencing state for the hybrid MPSC design. Each hot field sits on its own
// 128-byte line so producers and the single consumer do not false-share progress words.
struct alignas(128) HybridSharedControl {
  alignas(128) std::atomic<std::uint64_t> global_seq{0};
  alignas(128) std::atomic<std::uint64_t> consumer_seq{0};
  alignas(128) std::uint32_t wait_word = 0;
  std::uint32_t num_producers = 0;
  // 发布流控窗口（字节，0 表示满环）；限制生产者领先消费者的在途字节数。
  std::uint64_t publication_window = 0;
};

static_assert(alignof(HybridSharedControl) == 128);

}  // namespace salias::channel

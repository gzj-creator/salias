#pragma once

#include <cstddef>
#include <cstdint>

namespace salias::metrics {

inline constexpr std::uint32_t kMagic = 0x53414C31;
inline constexpr std::uint32_t kVersion = 1;
inline constexpr std::uint32_t kSlotStride = 64;

// counters 区域开头的固定 ABI header。
// 未来版本只能追加字段；重排字段会破坏独立 mmap 该区域的 reader。
struct alignas(64) MetaHeader {
  std::uint32_t magic;
  std::uint32_t version;
  std::uint32_t counter_count;
  std::uint32_t slot_stride;
  std::uint64_t created_unix_nanos;
  std::byte _pad[64 - 24];
};

// 一个可观测 counter。
// 每个槽位独占一条 cache line，避免无关生产者、消费者和维护 counter 之间发生 false sharing。
struct alignas(64) CounterSlot {
  std::uint64_t value;
  std::uint32_t type_id;
  std::uint32_t owner_channel;
  char label[64 - 16];
};

static_assert(sizeof(MetaHeader) == 64);
static_assert(sizeof(CounterSlot) == 64);

enum class CounterType : std::uint32_t {
  ProducerPos = 1,
  ConsumerPos = 2,
  BackPressureCount = 3,
  ErrorCount = 4,
  BytesPublished = 5,
  MessagesPublished = 6,
  LagBytes = 7,
  WakeSyscalls = 8,
};

// 返回容纳 counter_count 个槽位所需的 counters 区域字节数。
inline constexpr std::size_t region_size(std::uint32_t counter_count) noexcept {
  return sizeof(MetaHeader) + static_cast<std::size_t>(counter_count) * sizeof(CounterSlot);
}

}  // namespace salias::metrics

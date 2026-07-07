#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string_view>

#include "core/metrics/error.hpp"
#include "core/metrics/layout.hpp"

namespace salias::metrics {

// 调用者提供的 counters 区域上的可写视图。
// 该区域在测试中可以是普通内存，在生产中可以是 MAP_SHARED 内存；Counters 不拥有也不调整该内存。
class Counters {
 public:
  using Result = std::expected<Counters, MetricsError>;

  // 在调用者提供的区域内初始化 MetaHeader 和清零槽位。
  static Result create_in(std::span<std::byte> region, std::uint32_t counter_count) noexcept;

  // 将已有已初始化区域打开为可写视图。
  static Result view(std::span<std::byte> region) noexcept;

  // 返回当前视图是否引用兼容的已初始化 counters 区域。
  bool valid() const noexcept;
  // 返回可写 counter 槽位；视图无效时返回空 span。
  std::span<CounterSlot> slots() noexcept;
  // 返回只读 counter 槽位；视图无效时返回空 span。
  std::span<const CounterSlot> slots() const noexcept;

  // 定义槽位元数据，并按固定 label 缓冲区截断标签。
  MetricsError define(std::uint32_t slot, CounterType type, std::uint32_t owner_channel,
                      std::string_view label) noexcept;
  // 以 relaxed 顺序原子递增一个 counter 槽位。
  MetricsError incr(std::uint32_t slot, std::uint64_t by = 1) noexcept;
  // 以 release 语义写入 counter 值，供位置类计数器发布进度。
  MetricsError set_release(std::uint32_t slot, std::uint64_t value) noexcept;

 private:
  // 使用已校验的 header 和 slot 指针构造视图。
  Counters(MetaHeader* header, CounterSlot* slots) noexcept;

  MetaHeader* header_ = nullptr;
  CounterSlot* slots_ = nullptr;
};

}  // namespace salias::metrics

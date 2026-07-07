#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string_view>

#include "core/metrics/error.hpp"
#include "core/metrics/layout.hpp"

namespace salias::metrics {

struct OwnedReaderRegion;

// counters 区域上的只读观察视图。
// 该视图把区域视为不可信数据；调用方必须先检查 Result 和 valid()，再信任槽位 metadata 或值。
class CountersReader {
 public:
  using Result = std::expected<CountersReader, MetricsError>;

  // 打开已有内存 counters 区域的只读视图。
  static Result view(std::span<const std::byte> region) noexcept;
  // 只读映射 counters 文件，并返回持有映射生命周期的 reader。
  static Result open(std::string_view path);

  // 返回 reader 是否引用兼容的 counters 元数据。
  bool valid() const noexcept;
  // 返回只读 counter 槽位；无效时返回空 span。
  std::span<const CounterSlot> slots() const noexcept;
  // 原子读取 counter 值；reader 无效或槽位越界时返回 0。
  std::uint64_t value(std::uint32_t slot) const noexcept;

 private:
  // 校验原始区域字节，并可附加映射所有权。
  static Result from_region(std::span<const std::byte> region,
                            std::shared_ptr<const OwnedReaderRegion> owner = {}) noexcept;
  // 使用已校验的 header/slot 指针和可选所有者状态构造 reader。
  CountersReader(const MetaHeader* header, const CounterSlot* slots,
                 std::shared_ptr<const OwnedReaderRegion> owner = {}) noexcept;

  const MetaHeader* header_ = nullptr;
  const CounterSlot* slots_ = nullptr;
  std::shared_ptr<const OwnedReaderRegion> owner_;
};

}  // namespace salias::metrics

#include "core/metrics/counters.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>

namespace salias::metrics {

namespace {

// 返回紧跟 metadata header 后的第一个 counter 槽位。
CounterSlot* slots_after(MetaHeader* header) noexcept {
  return reinterpret_cast<CounterSlot*>(reinterpret_cast<std::byte*>(header) + sizeof(MetaHeader));
}

// 判断区域是否能容纳 header 和指定数量的槽位。
bool region_has_size(std::size_t size, std::uint32_t counter_count) noexcept {
  return size >= region_size(counter_count);
}

}  // namespace

// 在调用者提供的存储中初始化可写 counters 区域。
Counters::Result Counters::create_in(std::span<std::byte> region,
                                     std::uint32_t counter_count) noexcept {
  if (!region_has_size(region.size(), counter_count)) {
    return std::unexpected(MetricsError::RegionTooSmall);
  }

  std::fill(region.begin(), region.begin() + static_cast<std::ptrdiff_t>(region_size(counter_count)),
            std::byte{0});
  auto* header = reinterpret_cast<MetaHeader*>(region.data());
  header->magic = kMagic;
  header->version = kVersion;
  header->counter_count = counter_count;
  header->slot_stride = kSlotStride;
  header->created_unix_nanos = 0;

  return Counters(header, slots_after(header));
}

// 校验并打开已有可写 counters 区域。
Counters::Result Counters::view(std::span<std::byte> region) noexcept {
  if (region.size() < sizeof(MetaHeader)) {
    return std::unexpected(MetricsError::RegionTooSmall);
  }

  auto* header = reinterpret_cast<MetaHeader*>(region.data());
  if (header->magic != kMagic) {
    return std::unexpected(MetricsError::BadMagic);
  }
  if (header->version != kVersion || header->slot_stride != kSlotStride) {
    return std::unexpected(MetricsError::BadVersion);
  }
  if (!region_has_size(region.size(), header->counter_count)) {
    return std::unexpected(MetricsError::RegionTooSmall);
  }

  return Counters(header, slots_after(header));
}

// 检查当前视图是否仍引用兼容 metadata。
bool Counters::valid() const noexcept {
  return header_ != nullptr && slots_ != nullptr && header_->magic == kMagic &&
         header_->version == kVersion && header_->slot_stride == kSlotStride;
}

// 返回有效 counters 视图的可写槽位。
std::span<CounterSlot> Counters::slots() noexcept {
  if (!valid()) {
    return {};
  }
  return {slots_, header_->counter_count};
}

// 返回有效 counters 视图的只读槽位。
std::span<const CounterSlot> Counters::slots() const noexcept {
  if (!valid()) {
    return {};
  }
  return {slots_, header_->counter_count};
}

// 定义 counter 槽位的 metadata 和 label。
MetricsError Counters::define(std::uint32_t slot, CounterType type, std::uint32_t owner_channel,
                              std::string_view label) noexcept {
  if (!valid() || slot >= header_->counter_count) {
    return MetricsError::SlotOutOfRange;
  }

  CounterSlot& counter = slots_[slot];
  counter.type_id = static_cast<std::uint32_t>(type);
  counter.owner_channel = owner_channel;
  std::fill(std::begin(counter.label), std::end(counter.label), '\0');
  const std::size_t copy_len = std::min(label.size(), sizeof(counter.label) - 1);
  std::memcpy(counter.label, label.data(), copy_len);
  return MetricsError::Ok;
}

// 原子递增一个 counter 槽位。
MetricsError Counters::incr(std::uint32_t slot, std::uint64_t by) noexcept {
  if (!valid() || slot >= header_->counter_count) {
    return MetricsError::SlotOutOfRange;
  }

  // 安全性：固定布局保证 CounterSlot::value 为 8 字节对齐。
  // metrics counter 不发布消息字节或其它数据依赖，因此 relaxed 递增足够。
  std::atomic_ref<std::uint64_t>(slots_[slot].value).fetch_add(by, std::memory_order_relaxed);
  return MetricsError::Ok;
}

// 以 release 顺序写入 counter 值。
MetricsError Counters::set_release(std::uint32_t slot, std::uint64_t value) noexcept {
  if (!valid() || slot >= header_->counter_count) {
    return MetricsError::SlotOutOfRange;
  }

  // 安全性：位置 counter 也可能作为数据面同步单元。
  // release 保留 flow/channel 层以及外部 reader 依赖的“先写数据、再发布位置”约定。
  std::atomic_ref<std::uint64_t>(slots_[slot].value).store(value, std::memory_order_release);
  return MetricsError::Ok;
}

// 保存已校验的 header 和 slot 指针。
Counters::Counters(MetaHeader* header, CounterSlot* slots) noexcept : header_(header), slots_(slots) {}

}  // namespace salias::metrics

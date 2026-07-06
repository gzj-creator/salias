#include "core/metrics/counters.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>

namespace salias::metrics {

namespace {

CounterSlot* slots_after(MetaHeader* header) noexcept {
  return reinterpret_cast<CounterSlot*>(reinterpret_cast<std::byte*>(header) + sizeof(MetaHeader));
}

bool region_has_size(std::size_t size, std::uint32_t counter_count) noexcept {
  return size >= region_size(counter_count);
}

}  // namespace

Counters::Result Counters::create_in(std::span<std::byte> region,
                                     std::uint32_t counter_count) noexcept {
  if (!region_has_size(region.size(), counter_count)) {
    return Counters::Result::failure(MetricsError::RegionTooSmall);
  }

  std::fill(region.begin(), region.begin() + static_cast<std::ptrdiff_t>(region_size(counter_count)),
            std::byte{0});
  auto* header = reinterpret_cast<MetaHeader*>(region.data());
  header->magic = kMagic;
  header->version = kVersion;
  header->counter_count = counter_count;
  header->slot_stride = kSlotStride;
  header->created_unix_nanos = 0;

  return Counters::Result::success(Counters(header, slots_after(header)));
}

Counters::Result Counters::view(std::span<std::byte> region) noexcept {
  if (region.size() < sizeof(MetaHeader)) {
    return Counters::Result::failure(MetricsError::RegionTooSmall);
  }

  auto* header = reinterpret_cast<MetaHeader*>(region.data());
  if (header->magic != kMagic) {
    return Counters::Result::failure(MetricsError::BadMagic);
  }
  if (header->version != kVersion || header->slot_stride != kSlotStride) {
    return Counters::Result::failure(MetricsError::BadVersion);
  }
  if (!region_has_size(region.size(), header->counter_count)) {
    return Counters::Result::failure(MetricsError::RegionTooSmall);
  }

  return Counters::Result::success(Counters(header, slots_after(header)));
}

bool Counters::valid() const noexcept {
  return header_ != nullptr && slots_ != nullptr && header_->magic == kMagic &&
         header_->version == kVersion && header_->slot_stride == kSlotStride;
}

std::span<CounterSlot> Counters::slots() noexcept {
  if (!valid()) {
    return {};
  }
  return {slots_, header_->counter_count};
}

std::span<const CounterSlot> Counters::slots() const noexcept {
  if (!valid()) {
    return {};
  }
  return {slots_, header_->counter_count};
}

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

MetricsError Counters::incr(std::uint32_t slot, std::uint64_t by) noexcept {
  if (!valid() || slot >= header_->counter_count) {
    return MetricsError::SlotOutOfRange;
  }

  // SAFETY: CounterSlot::value is 8-byte aligned by the fixed layout. Atomic relaxed increments are
  // sufficient because metrics counters do not publish message bytes or other data dependencies.
  std::atomic_ref<std::uint64_t>(slots_[slot].value).fetch_add(by, std::memory_order_relaxed);
  return MetricsError::Ok;
}

MetricsError Counters::set_release(std::uint32_t slot, std::uint64_t value) noexcept {
  if (!valid() || slot >= header_->counter_count) {
    return MetricsError::SlotOutOfRange;
  }

  // SAFETY: Position counters may also be data-plane synchronization cells. Release preserves the
  // write-before-position contract used by flow/channel layers and by external readers.
  std::atomic_ref<std::uint64_t>(slots_[slot].value).store(value, std::memory_order_release);
  return MetricsError::Ok;
}

Counters::Counters(MetaHeader* header, CounterSlot* slots) noexcept : header_(header), slots_(slots) {}

}  // namespace salias::metrics

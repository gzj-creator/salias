#include "core/metrics/reader.hpp"

#include <atomic>

namespace salias::metrics {

namespace {

const CounterSlot* slots_after(const MetaHeader* header) noexcept {
  return reinterpret_cast<const CounterSlot*>(reinterpret_cast<const std::byte*>(header) +
                                             sizeof(MetaHeader));
}

bool region_has_size(std::size_t size, std::uint32_t counter_count) noexcept {
  return size >= region_size(counter_count);
}

}  // namespace

CountersReader::Result CountersReader::view(std::span<const std::byte> region) noexcept {
  if (region.size() < sizeof(MetaHeader)) {
    return CountersReader::Result::failure(MetricsError::RegionTooSmall);
  }

  auto* header = reinterpret_cast<const MetaHeader*>(region.data());
  if (header->magic != kMagic) {
    return CountersReader::Result::failure(MetricsError::BadMagic);
  }
  if (header->version != kVersion || header->slot_stride != kSlotStride) {
    return CountersReader::Result::failure(MetricsError::BadVersion);
  }
  if (!region_has_size(region.size(), header->counter_count)) {
    return CountersReader::Result::failure(MetricsError::RegionTooSmall);
  }

  return CountersReader::Result::success(CountersReader(header, slots_after(header)));
}

bool CountersReader::valid() const noexcept {
  return header_ != nullptr && slots_ != nullptr && header_->magic == kMagic &&
         header_->version == kVersion && header_->slot_stride == kSlotStride;
}

std::span<const CounterSlot> CountersReader::slots() const noexcept {
  if (!valid()) {
    return {};
  }
  return {slots_, header_->counter_count};
}

std::uint64_t CountersReader::value(std::uint32_t slot) const noexcept {
  if (!valid() || slot >= header_->counter_count) {
    return 0;
  }

  // SAFETY: CounterSlot::value is 8-byte aligned by layout. Acquire lets observers see position
  // stores that were published with set_release().
  return std::atomic_ref<const std::uint64_t>(slots_[slot].value).load(std::memory_order_acquire);
}

CountersReader::CountersReader(const MetaHeader* header, const CounterSlot* slots) noexcept
    : header_(header), slots_(slots) {}

}  // namespace salias::metrics

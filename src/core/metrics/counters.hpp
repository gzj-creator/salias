#pragma once

#include <cstdint>
#include <span>
#include <string_view>

#include "core/metrics/error.hpp"
#include "core/metrics/layout.hpp"
#include "core/platform/result.hpp"

namespace salias::metrics {

// Mutable writer view over a caller-provided counters region. The region may be regular memory in
// tests or MAP_SHARED memory in production. Counters never own or resize this memory.
class Counters {
 public:
  using Result = platform::Result<Counters, MetricsError>;

  // Initializes a region with a MetaHeader and zeroed slots.
  static Result create_in(std::span<std::byte> region, std::uint32_t counter_count) noexcept;

  // Opens an existing initialized region as a mutable writer.
  static Result view(std::span<std::byte> region) noexcept;

  bool valid() const noexcept;
  std::span<CounterSlot> slots() noexcept;
  std::span<const CounterSlot> slots() const noexcept;

  MetricsError define(std::uint32_t slot, CounterType type, std::uint32_t owner_channel,
                      std::string_view label) noexcept;
  MetricsError incr(std::uint32_t slot, std::uint64_t by = 1) noexcept;
  MetricsError set_release(std::uint32_t slot, std::uint64_t value) noexcept;

 private:
  Counters(MetaHeader* header, CounterSlot* slots) noexcept;

  MetaHeader* header_ = nullptr;
  CounterSlot* slots_ = nullptr;
};

}  // namespace salias::metrics

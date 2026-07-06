#pragma once

#include <cstdint>
#include <span>

#include "core/metrics/error.hpp"
#include "core/metrics/layout.hpp"
#include "core/platform/result.hpp"

namespace salias::metrics {

// Read-only observer view over a counters region. It treats the region as untrusted data: callers
// must check the Result and valid() before trusting slot metadata or values.
class CountersReader {
 public:
  using Result = platform::Result<CountersReader, MetricsError>;

  static Result view(std::span<const std::byte> region) noexcept;

  bool valid() const noexcept;
  std::span<const CounterSlot> slots() const noexcept;
  std::uint64_t value(std::uint32_t slot) const noexcept;

 private:
  CountersReader(const MetaHeader* header, const CounterSlot* slots) noexcept;

  const MetaHeader* header_ = nullptr;
  const CounterSlot* slots_ = nullptr;
};

}  // namespace salias::metrics

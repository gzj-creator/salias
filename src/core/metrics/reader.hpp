#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

#include "core/metrics/error.hpp"
#include "core/metrics/layout.hpp"
#include "core/platform/result.hpp"

namespace salias::metrics {

struct OwnedReaderRegion;

// Read-only observer view over a counters region. It treats the region as untrusted data: callers
// must check the Result and valid() before trusting slot metadata or values.
class CountersReader {
 public:
  using Result = platform::Result<CountersReader, MetricsError>;

  static Result view(std::span<const std::byte> region) noexcept;
  static Result open(std::string_view path);

  bool valid() const noexcept;
  std::span<const CounterSlot> slots() const noexcept;
  std::uint64_t value(std::uint32_t slot) const noexcept;

 private:
  static Result from_region(std::span<const std::byte> region,
                            std::shared_ptr<const OwnedReaderRegion> owner = {}) noexcept;
  CountersReader(const MetaHeader* header, const CounterSlot* slots,
                 std::shared_ptr<const OwnedReaderRegion> owner = {}) noexcept;

  const MetaHeader* header_ = nullptr;
  const CounterSlot* slots_ = nullptr;
  std::shared_ptr<const OwnedReaderRegion> owner_;
};

}  // namespace salias::metrics

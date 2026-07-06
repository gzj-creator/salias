#include "core/metrics/reader.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>

namespace salias::metrics {

struct OwnedReaderRegion {
  OwnedReaderRegion(const std::byte* base_value, std::size_t len_value, int fd_value) noexcept
      : base(base_value), len(len_value), fd(fd_value) {}

  OwnedReaderRegion(const OwnedReaderRegion&) = delete;
  OwnedReaderRegion& operator=(const OwnedReaderRegion&) = delete;
  ~OwnedReaderRegion() noexcept {
    if (base != nullptr && len != 0) {
      static_cast<void>(::munmap(const_cast<std::byte*>(base), len));
    }
    if (fd >= 0) {
      static_cast<void>(::close(fd));
    }
  }

  const std::byte* base = nullptr;
  std::size_t len = 0;
  int fd = -1;
};

namespace {

const CounterSlot* slots_after(const MetaHeader* header) noexcept {
  return reinterpret_cast<const CounterSlot*>(reinterpret_cast<const std::byte*>(header) +
                                             sizeof(MetaHeader));
}

bool region_has_size(std::size_t size, std::uint32_t counter_count) noexcept {
  return size >= region_size(counter_count);
}

}  // namespace

CountersReader::Result CountersReader::from_region(
    std::span<const std::byte> region,
    std::shared_ptr<const OwnedReaderRegion> owner) noexcept {
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

  return CountersReader::Result::success(CountersReader(header, slots_after(header), owner));
}

CountersReader::Result CountersReader::view(std::span<const std::byte> region) noexcept {
  return from_region(region);
}

CountersReader::Result CountersReader::open(std::string_view path) {
  const std::string owned_path(path);
  const int fd = ::open(owned_path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return CountersReader::Result::failure(MetricsError::OpenFailed);
  }

  struct stat stat_buf {};
  if (::fstat(fd, &stat_buf) != 0 || stat_buf.st_size < 0) {
    static_cast<void>(::close(fd));
    return CountersReader::Result::failure(MetricsError::OpenFailed);
  }
  const auto len = static_cast<std::size_t>(stat_buf.st_size);
  if (len < sizeof(MetaHeader)) {
    static_cast<void>(::close(fd));
    return CountersReader::Result::failure(MetricsError::RegionTooSmall);
  }

  // SAFETY: fd is opened read-only and len comes from fstat(). The returned mapping is held alive
  // by OwnedReaderRegion for as long as any copied CountersReader references it.
  void* mapped = ::mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) {
    static_cast<void>(::close(fd));
    return CountersReader::Result::failure(MetricsError::MapFailed);
  }

  auto owner =
      std::make_shared<OwnedReaderRegion>(static_cast<const std::byte*>(mapped), len, fd);
  return from_region(std::span<const std::byte>(owner->base, owner->len), owner);
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

CountersReader::CountersReader(const MetaHeader* header, const CounterSlot* slots,
                               std::shared_ptr<const OwnedReaderRegion> owner) noexcept
    : header_(header), slots_(slots), owner_(std::move(owner)) {}

}  // namespace salias::metrics

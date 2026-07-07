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
  // 保存只读 mmap 区域及其后端 fd 的所有权。
  OwnedReaderRegion(const std::byte* base_value, std::size_t len_value, int fd_value) noexcept
      : base(base_value), len(len_value), fd(fd_value) {}

  // 禁止拷贝；该区域持有 mmap 和 fd。
  OwnedReaderRegion(const OwnedReaderRegion&) = delete;
  // 禁止拷贝赋值；该区域持有 mmap 和 fd。
  OwnedReaderRegion& operator=(const OwnedReaderRegion&) = delete;
  // 解除映射并关闭后端 fd。
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

// 返回紧跟 metadata header 后的第一个 counter 槽位。
const CounterSlot* slots_after(const MetaHeader* header) noexcept {
  return reinterpret_cast<const CounterSlot*>(reinterpret_cast<const std::byte*>(header) +
                                             sizeof(MetaHeader));
}

// 判断区域是否能容纳 header 和指定数量的槽位。
bool region_has_size(std::size_t size, std::uint32_t counter_count) noexcept {
  return size >= region_size(counter_count);
}

}  // namespace

// 校验原始区域字节并返回 reader。
CountersReader::Result CountersReader::from_region(
    std::span<const std::byte> region,
    std::shared_ptr<const OwnedReaderRegion> owner) noexcept {
  if (region.size() < sizeof(MetaHeader)) {
    return std::unexpected(MetricsError::RegionTooSmall);
  }

  auto* header = reinterpret_cast<const MetaHeader*>(region.data());
  if (header->magic != kMagic) {
    return std::unexpected(MetricsError::BadMagic);
  }
  if (header->version != kVersion || header->slot_stride != kSlotStride) {
    return std::unexpected(MetricsError::BadVersion);
  }
  if (!region_has_size(region.size(), header->counter_count)) {
    return std::unexpected(MetricsError::RegionTooSmall);
  }

  return CountersReader(header, slots_after(header), owner);
}

// 基于调用者持有的内存打开 reader。
CountersReader::Result CountersReader::view(std::span<const std::byte> region) noexcept {
  return from_region(region);
}

// 按路径打开并 mmap counters 文件。
CountersReader::Result CountersReader::open(std::string_view path) {
  const std::string owned_path(path);
  const int fd = ::open(owned_path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return std::unexpected(MetricsError::OpenFailed);
  }

  struct stat stat_buf {};
  if (::fstat(fd, &stat_buf) != 0 || stat_buf.st_size < 0) {
    static_cast<void>(::close(fd));
    return std::unexpected(MetricsError::OpenFailed);
  }
  const auto len = static_cast<std::size_t>(stat_buf.st_size);
  if (len < sizeof(MetaHeader)) {
    static_cast<void>(::close(fd));
    return std::unexpected(MetricsError::RegionTooSmall);
  }

  // 安全性：fd 以只读方式打开，len 来自 fstat()。
  // 返回的映射由 OwnedReaderRegion 持有，只要任一拷贝出的 CountersReader 引用它就保持存活。
  void* mapped = ::mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) {
    static_cast<void>(::close(fd));
    return std::unexpected(MetricsError::MapFailed);
  }

  auto owner =
      std::make_shared<OwnedReaderRegion>(static_cast<const std::byte*>(mapped), len, fd);
  return from_region(std::span<const std::byte>(owner->base, owner->len), owner);
}

// 检查 reader 是否引用兼容 metadata。
bool CountersReader::valid() const noexcept {
  return header_ != nullptr && slots_ != nullptr && header_->magic == kMagic &&
         header_->version == kVersion && header_->slot_stride == kSlotStride;
}

// 返回有效 reader 的只读槽位。
std::span<const CounterSlot> CountersReader::slots() const noexcept {
  if (!valid()) {
    return {};
  }
  return {slots_, header_->counter_count};
}

// 原子读取一个 counter 值。
std::uint64_t CountersReader::value(std::uint32_t slot) const noexcept {
  if (!valid() || slot >= header_->counter_count) {
    return 0;
  }

  // 安全性：布局保证 CounterSlot::value 为 8 字节对齐。
  // acquire 让观察方看到通过 set_release() 发布的位置写入。
  auto& mutable_value = const_cast<std::uint64_t&>(slots_[slot].value);
  return std::atomic_ref<std::uint64_t>(mutable_value).load(std::memory_order_acquire);
}

// 保存已校验的 metadata/slot 指针和可选映射所有权。
CountersReader::CountersReader(const MetaHeader* header, const CounterSlot* slots,
                               std::shared_ptr<const OwnedReaderRegion> owner) noexcept
    : header_(header), slots_(slots), owner_(std::move(owner)) {}

}  // namespace salias::metrics

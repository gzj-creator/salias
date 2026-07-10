#include "core/platform/mapping.hpp"

#include <errno.h>
#include <fcntl.h>
#include <linux/memfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <limits>
#include <utility>

namespace salias::platform {

namespace {

constexpr std::size_t kHugePage2MiB = std::size_t{2} * 1024 * 1024;
constexpr std::size_t kHugePage1GiB = std::size_t{1024} * 1024 * 1024;

// 判断 value 是否为非零 2 的幂。
bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

// 校验页对齐、2 的幂大小和溢出边界。
bool is_valid_size(std::size_t size) noexcept {
  const long page_size = ::sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    return false;
  }
  if (!is_power_of_two(size)) {
    return false;
  }
  if (size % static_cast<std::size_t>(page_size) != 0) {
    return false;
  }
  if (size > (std::numeric_limits<std::size_t>::max() / 2)) {
    return false;
  }
  if (size > static_cast<std::size_t>(std::numeric_limits<off_t>::max())) {
    return false;
  }
  return true;
}

// 返回显式大页请求对应的页大小；None 表示不要求额外对齐。
std::size_t huge_page_size(HugePage huge) noexcept {
  switch (huge) {
    case HugePage::None:
      return 0;
    case HugePage::Size2MB:
      return kHugePage2MiB;
    case HugePage::Size1GB:
      return kHugePage1GiB;
  }
  return 0;
}

// 返回 memfd_create 的 MFD_HUGE_* 编码。
int huge_memfd_flag(HugePage huge) noexcept {
  switch (huge) {
    case HugePage::None:
      return 0;
    case HugePage::Size2MB:
      return MFD_HUGE_2MB;
    case HugePage::Size1GB:
      return MFD_HUGE_1GB;
  }
  return 0;
}

// 显式大页请求要求 ring 容量按所选大页大小对齐。
bool is_valid_huge_size(std::size_t size, HugePage huge) noexcept {
  const std::size_t page_size = huge_page_size(huge);
  return page_size == 0 || size % page_size == 0;
}

// 向上对齐虚拟地址；alignment 必须是 2 的幂。
std::uintptr_t align_up(std::uintptr_t value, std::size_t alignment) noexcept {
  const auto mask = static_cast<std::uintptr_t>(alignment - 1);
  if (value > std::numeric_limits<std::uintptr_t>::max() - mask) {
    return 0;
  }
  return (value + mask) & ~mask;
}

// 为 MAP_FIXED 双映射预留连续虚拟地址。大页 fd 需要大页粒度对齐的起始地址。
void* reserve_aligned_range(std::size_t mapped_len, std::size_t alignment) noexcept {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  if (raw_page_size <= 0) {
    return MAP_FAILED;
  }

  const auto system_page_size = static_cast<std::size_t>(raw_page_size);
  if (alignment <= system_page_size) {
    return ::mmap(nullptr, mapped_len, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  }

  if (mapped_len > std::numeric_limits<std::size_t>::max() - alignment) {
    return MAP_FAILED;
  }

  const std::size_t reserved_len = mapped_len + alignment;
  void* const reserved =
      ::mmap(nullptr, reserved_len, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (reserved == MAP_FAILED) {
    return MAP_FAILED;
  }

  const auto raw_addr = reinterpret_cast<std::uintptr_t>(reserved);
  const std::uintptr_t aligned_addr = align_up(raw_addr, alignment);
  if (aligned_addr == 0) {
    static_cast<void>(::munmap(reserved, reserved_len));
    return MAP_FAILED;
  }

  const std::size_t prefix_len = static_cast<std::size_t>(aligned_addr - raw_addr);
  if (prefix_len != 0) {
    static_cast<void>(::munmap(reserved, prefix_len));
  }

  auto* const aligned = reinterpret_cast<std::byte*>(aligned_addr);
  const std::size_t suffix_len = reserved_len - prefix_len - mapped_len;
  if (suffix_len != 0) {
    static_cast<void>(::munmap(aligned + mapped_len, suffix_len));
  }

  return aligned;
}

// 为 ring 后端存储创建 close-on-exec 匿名 memfd。
int create_memfd(HugePage huge) noexcept {
  int flags = MFD_CLOEXEC;
  if (huge != HugePage::None) {
    flags |= MFD_HUGETLB | huge_memfd_flag(huge);
  }

  const long fd = ::syscall(SYS_memfd_create, "salias-ring", flags);
  if (fd < 0 || fd > std::numeric_limits<int>::max()) {
    return -1;
  }
  return static_cast<int>(fd);
}

// 当 fd 表示已打开描述符时关闭它。
void close_if_open(int fd) noexcept {
  if (fd >= 0) {
    static_cast<void>(::close(fd));
  }
}

}  // namespace

// 将一个已持有 fd 映射到两段相邻虚拟地址区间。
Mapping::CreateResult Mapping::map_owned_fd(int fd, std::size_t size, bool self_check,
                                            std::size_t alignment, bool huge_requested) noexcept {
  const std::size_t mapped_len = size * 2;
  // 安全性：这里只预留不可访问的虚拟地址区间；不传入既有地址，由内核选择空闲连续洞。
  // 后续再用两个固定共享映射替换该预留区间。
  void* const reserved = reserve_aligned_range(mapped_len, alignment);
  if (reserved == MAP_FAILED) {
    close_if_open(fd);
    return std::unexpected(PlatformError::ReserveFailed);
  }

  auto* const base = static_cast<std::byte*>(reserved);

  // 安全性：base 指向上面的 PROT_NONE 预留区，且至少覆盖 size 字节。
  // MAP_FIXED 只刻意替换该预留子区间，并映射 fd offset 0。
  void* const first =
      ::mmap(base, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED | MAP_POPULATE, fd, 0);
  if (first == MAP_FAILED || first != base) {
    static_cast<void>(::munmap(base, mapped_len));
    close_if_open(fd);
    if (huge_requested) {
      return std::unexpected(PlatformError::HugePageUnavailable);
    }
    return std::unexpected(PlatformError::MapFixedFailed);
  }

  // 安全性：base + size 仍位于同一段 2*size 预留区间内。
  // 再次映射同一 fd offset 0 会创建同一物理页的第二个虚拟别名。
  void* const second = ::mmap(base + size, size, PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_FIXED | MAP_POPULATE, fd, 0);
  if (second == MAP_FAILED || second != base + size) {
    static_cast<void>(::munmap(base, mapped_len));
    close_if_open(fd);
    if (huge_requested) {
      return std::unexpected(PlatformError::HugePageUnavailable);
    }
    return std::unexpected(PlatformError::MapFixedFailed);
  }

  if (self_check) {
    // 安全性：两个别名都已映射为可写，并指向 fd offset 0。
    // 分别触碰 byte 0 是就地运行时检查，用于确认两段虚拟地址共享同一后端页。
    const std::byte original = base[0];
    base[0] = std::byte{0x5A};
    const bool aliases_same_physical_page = base[size] == std::byte{0x5A};
    base[0] = original;
    if (!aliases_same_physical_page) {
      static_cast<void>(::munmap(base, mapped_len));
      close_if_open(fd);
      return std::unexpected(PlatformError::MapFixedFailed);
    }
  }

  return Mapping(base, size, fd);
}

// 创建新的 memfd 后端双映射。
Mapping::CreateResult Mapping::create(const MapOptions& options) noexcept {
  if (!is_valid_size(options.size)) {
    return std::unexpected(PlatformError::InvalidSize);
  }
  if (!is_valid_huge_size(options.size, options.huge)) {
    return std::unexpected(PlatformError::InvalidSize);
  }
  if (options.numa_node >= 0) {
    return std::unexpected(PlatformError::NumaUnavailable);
  }

  const int fd = create_memfd(options.huge);
  if (fd < 0) {
    if (options.huge != HugePage::None) {
      return std::unexpected(PlatformError::HugePageUnavailable);
    }
    return std::unexpected(PlatformError::MemfdCreateFailed);
  }

  if (::ftruncate(fd, static_cast<off_t>(options.size)) != 0) {
    close_if_open(fd);
    if (options.huge != HugePage::None) {
      return std::unexpected(PlatformError::HugePageUnavailable);
    }
    return std::unexpected(PlatformError::FtruncateFailed);
  }

  const std::size_t alignment = huge_page_size(options.huge);
  return map_owned_fd(fd, options.size, true, alignment, options.huge != HugePage::None);
}

// 基于 dup 后的共享 fd 创建双映射。
Mapping::CreateResult Mapping::map_shared_fd(int fd, const MapOptions& options) noexcept {
  if (fd < 0 || !is_valid_size(options.size) || !is_valid_huge_size(options.size, options.huge)) {
    return std::unexpected(PlatformError::InvalidSize);
  }
  if (options.numa_node >= 0) {
    return std::unexpected(PlatformError::NumaUnavailable);
  }

  const int owned_fd = ::dup(fd);
  if (owned_fd < 0) {
    return std::unexpected(PlatformError::MemfdCreateFailed);
  }

  const std::size_t alignment = huge_page_size(options.huge);
  return map_owned_fd(owned_fd, options.size, false, alignment, options.huge != HugePage::None);
}

// 保存持有映射的起始地址、逻辑长度和 fd。
Mapping::Mapping(std::byte* base, std::size_t len, int fd) noexcept
    : base_(base), len_(len), fd_(fd) {}

// 从 other 转移映射所有权并清空来源对象。
Mapping::Mapping(Mapping&& other) noexcept : base_(other.base_), len_(other.len_), fd_(other.fd_) {
  other.base_ = nullptr;
  other.len_ = 0;
  other.fd_ = -1;
}

// 释放当前所有权并从 other 转移映射所有权。
Mapping& Mapping::operator=(Mapping&& other) noexcept {
  if (this != &other) {
    reset();
    base_ = other.base_;
    len_ = other.len_;
    fd_ = other.fd_;
    other.base_ = nullptr;
    other.len_ = 0;
    other.fd_ = -1;
  }
  return *this;
}

// 释放所有持有的映射和描述符。
Mapping::~Mapping() noexcept { reset(); }

// 解除双映射、关闭 fd，并将句柄重置为空。
void Mapping::reset() noexcept {
  if (base_ != nullptr && len_ != 0) {
    // 安全性：Mapping 只拥有 create() 返回的连续 2*len_ 区间。
    // 移动操作会清空来源对象，因此最多解除映射一次。
    static_cast<void>(::munmap(base_, len_ * 2));
  }
  close_if_open(fd_);
  base_ = nullptr;
  len_ = 0;
  fd_ = -1;
}

}  // namespace salias::platform

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

// 为 ring 后端存储创建 close-on-exec 匿名 memfd。
int create_memfd() noexcept {
  const long fd = ::syscall(SYS_memfd_create, "salias-ring", MFD_CLOEXEC);
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
Mapping::CreateResult Mapping::map_owned_fd(int fd, std::size_t size, bool self_check) noexcept {
  const std::size_t mapped_len = size * 2;
  // 安全性：这里只预留不可访问的虚拟地址区间；不传入既有地址，由内核选择空闲连续洞。
  // 后续再用两个固定共享映射替换该预留区间。
  void* const reserved =
      ::mmap(nullptr, mapped_len, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (reserved == MAP_FAILED) {
    close_if_open(fd);
    return std::unexpected(PlatformError::ReserveFailed);
  }

  auto* const base = static_cast<std::byte*>(reserved);

  // 安全性：base 指向上面的 PROT_NONE 预留区，且至少覆盖 size 字节。
  // MAP_FIXED 只刻意替换该预留子区间，并映射 fd offset 0。
  void* const first = ::mmap(base, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
  if (first == MAP_FAILED || first != base) {
    static_cast<void>(::munmap(base, mapped_len));
    close_if_open(fd);
    return std::unexpected(PlatformError::MapFixedFailed);
  }

  // 安全性：base + size 仍位于同一段 2*size 预留区间内。
  // 再次映射同一 fd offset 0 会创建同一物理页的第二个虚拟别名。
  void* const second =
      ::mmap(base + size, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
  if (second == MAP_FAILED || second != base + size) {
    static_cast<void>(::munmap(base, mapped_len));
    close_if_open(fd);
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
  if (options.huge != HugePage::None) {
    return std::unexpected(PlatformError::HugePageUnavailable);
  }
  if (options.numa_node >= 0) {
    return std::unexpected(PlatformError::NumaUnavailable);
  }

  const int fd = create_memfd();
  if (fd < 0) {
    return std::unexpected(PlatformError::MemfdCreateFailed);
  }

  if (::ftruncate(fd, static_cast<off_t>(options.size)) != 0) {
    close_if_open(fd);
    return std::unexpected(PlatformError::FtruncateFailed);
  }

  return map_owned_fd(fd, options.size, true);
}

// 基于 dup 后的共享 fd 创建双映射。
Mapping::CreateResult Mapping::map_shared_fd(int fd, const MapOptions& options) noexcept {
  if (fd < 0 || !is_valid_size(options.size)) {
    return std::unexpected(PlatformError::InvalidSize);
  }
  if (options.huge != HugePage::None) {
    return std::unexpected(PlatformError::HugePageUnavailable);
  }
  if (options.numa_node >= 0) {
    return std::unexpected(PlatformError::NumaUnavailable);
  }

  const int owned_fd = ::dup(fd);
  if (owned_fd < 0) {
    return std::unexpected(PlatformError::MemfdCreateFailed);
  }

  return map_owned_fd(owned_fd, options.size, false);
}

// 保存持有映射的起始地址、逻辑长度和 fd。
Mapping::Mapping(std::byte* base, std::size_t len, int fd) noexcept
    : base_(base), len_(len), fd_(fd) {}

// 从 other 转移映射所有权并清空来源对象。
Mapping::Mapping(Mapping&& other) noexcept
    : base_(other.base_), len_(other.len_), fd_(other.fd_) {
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

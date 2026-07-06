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

bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

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

int create_memfd() noexcept {
  const long fd = ::syscall(SYS_memfd_create, "salias-ring", MFD_CLOEXEC);
  if (fd < 0 || fd > std::numeric_limits<int>::max()) {
    return -1;
  }
  return static_cast<int>(fd);
}

void close_if_open(int fd) noexcept {
  if (fd >= 0) {
    static_cast<void>(::close(fd));
  }
}

}  // namespace

Mapping::CreateResult Mapping::create(const MapOptions& options) noexcept {
  if (!is_valid_size(options.size)) {
    return Mapping::CreateResult::failure(PlatformError::InvalidSize);
  }
  if (options.huge != HugePage::None) {
    return Mapping::CreateResult::failure(PlatformError::HugePageUnavailable);
  }
  if (options.numa_node >= 0) {
    return Mapping::CreateResult::failure(PlatformError::NumaUnavailable);
  }

  const int fd = create_memfd();
  if (fd < 0) {
    return Mapping::CreateResult::failure(PlatformError::MemfdCreateFailed);
  }

  if (::ftruncate(fd, static_cast<off_t>(options.size)) != 0) {
    close_if_open(fd);
    return Mapping::CreateResult::failure(PlatformError::FtruncateFailed);
  }

  const std::size_t mapped_len = options.size * 2;
  // SAFETY: This reserves an inaccessible virtual range only. No existing address is passed, so
  // the kernel chooses a free contiguous hole that we later replace with two fixed shared mappings.
  void* const reserved =
      ::mmap(nullptr, mapped_len, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (reserved == MAP_FAILED) {
    close_if_open(fd);
    return Mapping::CreateResult::failure(PlatformError::ReserveFailed);
  }

  auto* const base = static_cast<std::byte*>(reserved);

  // SAFETY: base points to the PROT_NONE reservation above and covers at least options.size bytes.
  // MAP_FIXED deliberately replaces only that reserved subrange with fd offset 0.
  void* const first =
      ::mmap(base, options.size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
  if (first == MAP_FAILED || first != base) {
    static_cast<void>(::munmap(base, mapped_len));
    close_if_open(fd);
    return Mapping::CreateResult::failure(PlatformError::MapFixedFailed);
  }

  // SAFETY: base + options.size is still within the same reserved 2*size range. Mapping the same
  // fd offset 0 creates the required second virtual alias of the same physical pages.
  void* const second = ::mmap(base + options.size, options.size, PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_FIXED, fd, 0);
  if (second == MAP_FAILED || second != base + options.size) {
    static_cast<void>(::munmap(base, mapped_len));
    close_if_open(fd);
    return Mapping::CreateResult::failure(PlatformError::MapFixedFailed);
  }

  // SAFETY: both aliases are mapped writable above and point at fd offset 0. Touching byte 0 in
  // each alias is an in-band runtime check that the two virtual ranges share the same backing page.
  const std::byte original = base[0];
  base[0] = std::byte{0x5A};
  const bool aliases_same_physical_page = base[options.size] == std::byte{0x5A};
  base[0] = original;
  if (!aliases_same_physical_page) {
    static_cast<void>(::munmap(base, mapped_len));
    close_if_open(fd);
    return Mapping::CreateResult::failure(PlatformError::MapFixedFailed);
  }

  return Mapping::CreateResult::success(Mapping(base, options.size, fd));
}

Mapping::Mapping(std::byte* base, std::size_t len, int fd) noexcept
    : base_(base), len_(len), fd_(fd) {}

Mapping::Mapping(Mapping&& other) noexcept
    : base_(other.base_), len_(other.len_), fd_(other.fd_) {
  other.base_ = nullptr;
  other.len_ = 0;
  other.fd_ = -1;
}

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

Mapping::~Mapping() noexcept { reset(); }

void Mapping::reset() noexcept {
  if (base_ != nullptr && len_ != 0) {
    // SAFETY: Mapping owns exactly the contiguous 2*len_ range returned by create(). Move
    // operations null the source object, so this unmaps at most once.
    static_cast<void>(::munmap(base_, len_ * 2));
  }
  close_if_open(fd_);
  base_ = nullptr;
  len_ = 0;
  fd_ = -1;
}

}  // namespace salias::platform

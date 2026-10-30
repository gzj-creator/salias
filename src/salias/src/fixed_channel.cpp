/**
 * @file src/salias/src/fixed_channel.cpp
 * @brief FixedChannel 的具名共享内存控制段、ring 数据段与 schema 校验实现。
 */
#include "salias/fixed_channel.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

namespace salias::detail {

namespace {

inline constexpr std::uint64_t kFixedMagic = 0x53414C4649584544ull;  // "SALFIXED"
inline constexpr std::uint32_t kFixedVersion = 1;
inline constexpr std::size_t kControlSize = 4096;
inline constexpr std::size_t kMaxNameLength = 128;
inline constexpr std::size_t kHugePage2MiB = std::size_t{2} * 1024 * 1024;
inline constexpr std::size_t kHugePage1GiB = std::size_t{1} * 1024 * 1024 * 1024;
inline constexpr const char* kDefaultHugetlbfsDir = "/dev/hugepages";
inline constexpr std::uint32_t kFlagHugetlbfs = 1u;
inline constexpr std::uint32_t kHugeShift = 1u;
inline constexpr std::uint32_t kHugeMask = 0x6u;

#if defined(MAP_POPULATE)
inline constexpr int kPopulateFlag = MAP_POPULATE;
#else
inline constexpr int kPopulateFlag = 0;
#endif

struct alignas(64) Control {
  std::uint64_t magic = 0;
  std::uint32_t version = 0;
  std::uint32_t flags = 0;
  std::uint64_t schema_id = 0;
  std::uint32_t schema_version = 0;
  std::uint32_t payload_size = 0;
  std::uint64_t ring_storage_size = 0;
  std::uint64_t capacity_slots = 0;
  std::uint32_t producer_count = 0;
  std::uint32_t consumer_count = 0;
  std::uint32_t ready = 0;
  std::uint32_t reserved = 0;
  std::uint64_t ring_mapping_size = 0;
};

static_assert(offsetof(Control, ready) < 64);

bool valid_name(std::string_view name) noexcept {
  if (name.empty() || name.size() > kMaxNameLength) {
    return false;
  }
  for (const unsigned char character : name) {
    if ((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
        (character >= '0' && character <= '9') || character == '-' || character == '_' ||
        character == '.') {
      continue;
    }
    return false;
  }
  return true;
}

std::uint64_t name_hash(std::string_view name) noexcept {
  constexpr std::uint64_t offset_basis = 14695981039346656037ull;
  constexpr std::uint64_t prime = 1099511628211ull;
  std::uint64_t hash = offset_basis;
  for (const unsigned char character : name) {
    hash ^= character;
    hash *= prime;
  }
  return hash;
}

std::string encoded_name(std::string_view name) {
  constexpr std::array<char, 16> hex_digits = {'0', '1', '2', '3', '4', '5', '6', '7',
                                               '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::array<char, 16> encoded{};
  std::uint64_t hash = name_hash(name);
  for (std::size_t index = encoded.size(); index != 0; --index) {
    encoded[index - 1] = hex_digits[hash & 0x0fu];
    hash >>= 4;
  }
  return std::string(encoded.data(), encoded.size());
}

std::string control_name(std::string_view name) {
  return "/salias-fixed-" + encoded_name(name) + "-c";
}

std::string ring_shm_name(std::string_view name) {
  return "/salias-fixed-" + encoded_name(name) + "-r";
}

std::string hugetlbfs_dir() {
  if (const char* value = std::getenv("SALIAS_HUGETLBFS_DIR");
      value != nullptr && value[0] != '\0') {
    return value;
  }
  return kDefaultHugetlbfsDir;
}

std::string huge_ring_path(std::string_view name) {
  std::string directory = hugetlbfs_dir();
  if (!directory.empty() && directory.back() == '/') {
    directory.pop_back();
  }
  return directory + "/salias-fixed-" + encoded_name(name) + "-r";
}

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

std::uint32_t flags_for(HugePage huge) noexcept {
  switch (huge) {
    case HugePage::None:
      return 0;
    case HugePage::Size2MB:
      return kFlagHugetlbfs | (1u << kHugeShift);
    case HugePage::Size1GB:
      return kFlagHugetlbfs | (2u << kHugeShift);
  }
  return 0;
}

std::optional<HugePage> huge_from_flags(std::uint32_t flags) noexcept {
  constexpr std::uint32_t known_mask = kFlagHugetlbfs | kHugeMask;
  if ((flags & ~known_mask) != 0) {
    return std::nullopt;
  }
  const bool hugetlbfs = (flags & kFlagHugetlbfs) != 0;
  const std::uint32_t code = (flags & kHugeMask) >> kHugeShift;
  if (!hugetlbfs) {
    return code == 0 ? std::optional<HugePage>{HugePage::None} : std::nullopt;
  }
  if (code == 1) {
    return HugePage::Size2MB;
  }
  if (code == 2) {
    return HugePage::Size1GB;
  }
  return std::nullopt;
}

std::size_t mapping_size_for(std::size_t storage_size, HugePage huge) noexcept {
  std::size_t alignment = huge_page_size(huge);
  if (alignment == 0) {
    const long page = ::sysconf(_SC_PAGESIZE);
    if (page <= 0) {
      return 0;
    }
    alignment = static_cast<std::size_t>(page);
  }
  if (storage_size == 0 || alignment == 0 ||
      storage_size > std::numeric_limits<std::size_t>::max() - (alignment - 1)) {
    return 0;
  }
  return (storage_size + alignment - 1) & ~(alignment - 1);
}

void close_if_open(int fd) noexcept {
  if (fd >= 0) {
    static_cast<void>(::close(fd));
  }
}

void unlink_shm_name(const std::string& name) noexcept {
  if (!name.empty()) {
    static_cast<void>(::shm_unlink(name.c_str()));
  }
}

void unlink_ring_name(const std::string& name, bool hugetlbfs) noexcept {
  if (name.empty()) {
    return;
  }
  if (hugetlbfs) {
    static_cast<void>(::unlink(name.c_str()));
  } else {
    static_cast<void>(::shm_unlink(name.c_str()));
  }
}

int create_sized_shm(const std::string& name, std::size_t size) noexcept {
  const int fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    return -1;
  }
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
    close_if_open(fd);
    static_cast<void>(::shm_unlink(name.c_str()));
    return -1;
  }
  return fd;
}

int create_sized_file(const std::string& path, std::size_t size) noexcept {
  const int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    return -1;
  }
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
    close_if_open(fd);
    static_cast<void>(::unlink(path.c_str()));
    return -1;
  }
  return fd;
}

std::byte* map_region(int fd, std::size_t size) noexcept {
  void* mapped = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | kPopulateFlag, fd, 0);
  return mapped == MAP_FAILED ? nullptr : static_cast<std::byte*>(mapped);
}

bool valid_layout(const FixedLayout& layout) noexcept {
  return layout.payload_size != 0 && layout.ring_storage_size != 0 && layout.capacity_slots >= 2 &&
         (layout.capacity_slots & (layout.capacity_slots - 1)) == 0 && layout.producer_count != 0 &&
         layout.consumer_count != 0;
}

bool valid_schema(const SchemaDescriptor& schema, const FixedLayout& layout) noexcept {
  return schema.typed && schema.schema_id != 0 && schema.version != 0 && schema.payload_size != 0 &&
         schema.payload_size == layout.payload_size;
}

}  // namespace

struct FixedSharedSegment::State {
  std::byte* control = nullptr;
  std::byte* ring = nullptr;
  std::size_t ring_storage_size = 0;
  std::size_t ring_mapping_size = 0;
  std::string control_name;
  std::string ring_name;
  bool owns_names = false;
  bool hugetlbfs = false;

  ~State() noexcept {
    if (ring != nullptr) {
      static_cast<void>(::munmap(ring, ring_mapping_size));
    }
    if (control != nullptr) {
      static_cast<void>(::munmap(control, kControlSize));
    }
    if (owns_names) {
      unlink_shm_name(control_name);
      unlink_ring_name(ring_name, hugetlbfs);
    }
  }
};

FixedSharedSegment::FixedSharedSegment() = default;
FixedSharedSegment::FixedSharedSegment(std::shared_ptr<State> state) noexcept
    : state_(std::move(state)) {}
FixedSharedSegment::FixedSharedSegment(FixedSharedSegment&&) noexcept = default;
FixedSharedSegment& FixedSharedSegment::operator=(FixedSharedSegment&&) noexcept = default;
FixedSharedSegment::~FixedSharedSegment() = default;

Result<FixedSharedSegment> FixedSharedSegment::create(std::string_view name, HugePage huge,
                                                      SchemaDescriptor schema, FixedLayout layout) {
  if (!valid_name(name) || !valid_layout(layout) || !valid_schema(schema, layout)) {
    return std::unexpected(Error::BadConfig);
  }
  const std::size_t mapping_size = mapping_size_for(layout.ring_storage_size, huge);
  if (mapping_size == 0 ||
      layout.ring_storage_size >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()) ||
      mapping_size > static_cast<std::size_t>(std::numeric_limits<off_t>::max())) {
    return std::unexpected(Error::BadConfig);
  }

  const std::string control_path = control_name(name);
  const std::string data_path = huge == HugePage::None ? ring_shm_name(name) : huge_ring_path(name);
  int control_fd = create_sized_shm(control_path, kControlSize);
  if (control_fd < 0) {
    return std::unexpected(Error::PlatformFail);
  }
  std::byte* control = map_region(control_fd, kControlSize);
  close_if_open(control_fd);
  if (control == nullptr) {
    unlink_shm_name(control_path);
    return std::unexpected(Error::PlatformFail);
  }

  int ring_fd = huge == HugePage::None ? create_sized_shm(data_path, mapping_size)
                                       : create_sized_file(data_path, mapping_size);
  if (ring_fd < 0) {
    static_cast<void>(::munmap(control, kControlSize));
    unlink_shm_name(control_path);
    return std::unexpected(Error::PlatformFail);
  }
  std::byte* ring = map_region(ring_fd, mapping_size);
  close_if_open(ring_fd);
  if (ring == nullptr) {
    static_cast<void>(::munmap(control, kControlSize));
    unlink_shm_name(control_path);
    unlink_ring_name(data_path, huge != HugePage::None);
    return std::unexpected(Error::PlatformFail);
  }

  std::memset(control, 0, kControlSize);
  auto* shared = reinterpret_cast<Control*>(control);
  shared->magic = kFixedMagic;
  shared->version = kFixedVersion;
  shared->flags = flags_for(huge);
  shared->schema_id = schema.schema_id;
  shared->schema_version = schema.version;
  shared->payload_size = schema.payload_size;
  shared->ring_storage_size = layout.ring_storage_size;
  shared->capacity_slots = layout.capacity_slots;
  shared->producer_count = layout.producer_count;
  shared->consumer_count = layout.consumer_count;
  shared->ring_mapping_size = mapping_size;

  try {
    auto state = std::make_shared<State>();
    state->control_name = control_path;
    state->ring_name = data_path;
    state->owns_names = true;
    state->hugetlbfs = huge != HugePage::None;
    state->control = control;
    state->ring = ring;
    state->ring_storage_size = static_cast<std::size_t>(layout.ring_storage_size);
    state->ring_mapping_size = mapping_size;
    return FixedSharedSegment(std::move(state));
  } catch (const std::bad_alloc&) {
    static_cast<void>(::munmap(ring, mapping_size));
    static_cast<void>(::munmap(control, kControlSize));
    unlink_shm_name(control_path);
    unlink_ring_name(data_path, huge != HugePage::None);
    return std::unexpected(Error::OutOfMemory);
  }
}

Result<FixedSharedSegment> FixedSharedSegment::connect(std::string_view name,
                                                       SchemaDescriptor schema,
                                                       FixedLayout layout) {
  if (!valid_name(name) || !valid_layout(layout) || !valid_schema(schema, layout)) {
    return std::unexpected(Error::BadConfig);
  }
  const std::string control_path = control_name(name);
  std::byte* control = nullptr;
  for (int open_attempt = 0; open_attempt < 100000 && control == nullptr; ++open_attempt) {
    const int fd = ::shm_open(control_path.c_str(), O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
      std::this_thread::yield();
      continue;
    }
    struct stat status {};
    if (::fstat(fd, &status) != 0 || status.st_size != static_cast<off_t>(kControlSize)) {
      close_if_open(fd);
      std::this_thread::yield();
      continue;
    }
    control = map_region(fd, kControlSize);
    close_if_open(fd);
    if (control == nullptr) {
      return std::unexpected(Error::PlatformFail);
    }

    auto* shared = reinterpret_cast<Control*>(control);
    for (int ready_attempt = 0; ready_attempt < 100000; ++ready_attempt) {
      if (std::atomic_ref<std::uint32_t>(shared->ready).load(std::memory_order_acquire) == 1) {
        break;
      }
      std::this_thread::yield();
    }
    if (std::atomic_ref<std::uint32_t>(shared->ready).load(std::memory_order_acquire) != 1) {
      static_cast<void>(::munmap(control, kControlSize));
      return std::unexpected(Error::NotFound);
    }
  }
  if (control == nullptr) {
    return std::unexpected(Error::NotFound);
  }

  auto* shared = reinterpret_cast<Control*>(control);
  const auto cleanup_control = [&]() { static_cast<void>(::munmap(control, kControlSize)); };
  if (shared->magic != kFixedMagic || shared->version != kFixedVersion) {
    cleanup_control();
    return std::unexpected(Error::VersionMismatch);
  }
  if (shared->schema_id != schema.schema_id || shared->schema_version != schema.version ||
      shared->payload_size != schema.payload_size) {
    cleanup_control();
    return std::unexpected(Error::SchemaMismatch);
  }
  if (shared->ring_storage_size != layout.ring_storage_size ||
      shared->capacity_slots != layout.capacity_slots ||
      shared->producer_count != layout.producer_count ||
      shared->consumer_count != layout.consumer_count) {
    cleanup_control();
    return std::unexpected(Error::BadConfig);
  }
  const auto huge = huge_from_flags(shared->flags);
  if (!huge || shared->ring_mapping_size != mapping_size_for(layout.ring_storage_size, *huge)) {
    cleanup_control();
    return std::unexpected(Error::VersionMismatch);
  }
  if (shared->ring_mapping_size > std::numeric_limits<std::size_t>::max()) {
    cleanup_control();
    return std::unexpected(Error::BadConfig);
  }

  const std::string data_path =
      *huge == HugePage::None ? ring_shm_name(name) : huge_ring_path(name);
  const int ring_fd = *huge == HugePage::None
                          ? ::shm_open(data_path.c_str(), O_RDWR | O_CLOEXEC, 0600)
                          : ::open(data_path.c_str(), O_RDWR | O_CLOEXEC);
  if (ring_fd < 0) {
    cleanup_control();
    return std::unexpected(Error::NotFound);
  }
  struct stat ring_status {};
  const auto ring_mapping_size = static_cast<std::size_t>(shared->ring_mapping_size);
  if (::fstat(ring_fd, &ring_status) != 0 ||
      ring_status.st_size != static_cast<off_t>(ring_mapping_size)) {
    close_if_open(ring_fd);
    cleanup_control();
    return std::unexpected(Error::VersionMismatch);
  }
  std::byte* ring = map_region(ring_fd, ring_mapping_size);
  close_if_open(ring_fd);
  if (ring == nullptr) {
    cleanup_control();
    return std::unexpected(Error::PlatformFail);
  }

  try {
    auto state = std::make_shared<State>();
    state->control_name = control_path;
    state->ring_name = data_path;
    state->owns_names = false;
    state->hugetlbfs = *huge != HugePage::None;
    state->control = control;
    state->ring = ring;
    state->ring_storage_size = static_cast<std::size_t>(layout.ring_storage_size);
    state->ring_mapping_size = ring_mapping_size;
    return FixedSharedSegment(std::move(state));
  } catch (const std::bad_alloc&) {
    static_cast<void>(::munmap(ring, ring_mapping_size));
    cleanup_control();
    return std::unexpected(Error::OutOfMemory);
  }
}

std::span<std::byte> FixedSharedSegment::ring_storage() const noexcept {
  if (state_ == nullptr || state_->ring == nullptr) {
    return {};
  }
  return std::span<std::byte>(state_->ring, state_->ring_storage_size);
}

void FixedSharedSegment::publish_ready() noexcept {
  if (state_ != nullptr && state_->control != nullptr) {
    auto* shared = reinterpret_cast<Control*>(state_->control);
    std::atomic_ref<std::uint32_t>(shared->ready).store(1, std::memory_order_release);
  }
}

Error map_fixed_ring_error(ring::FastRingError error) noexcept {
  switch (error) {
    case ring::FastRingError::Ok:
      return Error::Ok;
    case ring::FastRingError::BackPressured:
      return Error::BackPressured;
    case ring::FastRingError::PayloadSizeMismatch:
      return Error::PayloadSizeMismatch;
    case ring::FastRingError::BadMagic:
    case ring::FastRingError::IncompatibleLayout:
    case ring::FastRingError::AlreadyInitialized:
      return Error::VersionMismatch;
    case ring::FastRingError::InvalidArgument:
    case ring::FastRingError::UnalignedStorage:
    case ring::FastRingError::StorageTooSmall:
    case ring::FastRingError::Empty:
    case ring::FastRingError::ClaimOutstanding:
    case ring::FastRingError::ReadOutstanding:
    case ring::FastRingError::NoOutstandingClaim:
    case ring::FastRingError::NoOutstandingRead:
      return Error::BadConfig;
  }
  return Error::BadConfig;
}

}  // namespace salias::detail

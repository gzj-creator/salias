/**
 * @file src/salias/src/latest_by_key.cpp
 * @brief Named shared-memory storage and crash-released locking for LatestByKeyChannel.
 */
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

#include "salias/detail/latest_by_key_storage.hpp"

namespace salias::detail {
namespace {

inline constexpr std::uint64_t kStorageMagic = 0x53414C4B4C415445ULL;  // "SALKLATE"
inline constexpr std::uint32_t kStorageVersion = 1;
inline constexpr std::size_t kMaxNameLength = 128;
inline constexpr std::size_t kCacheLineBytes = 64;
inline constexpr std::size_t kMinEventCount = 64;
inline constexpr std::size_t kRegistryCleanupThreshold = 64;
inline constexpr std::size_t kSharedAlignment =
    std::max(kCacheLineBytes, std::atomic_ref<std::uint64_t>::required_alignment);

struct alignas(kSharedAlignment) SharedHeader {
  std::uint64_t magic;
  std::uint32_t version;
  std::uint32_t ready;
  std::uint64_t total_size;
  std::uint64_t slot_count;
  std::uint64_t key_size;
  std::uint64_t value_size;
  std::uint64_t key_schema;
  std::uint64_t value_schema;
  std::uint64_t name_hash;
  std::uint64_t event_count;
  std::uint64_t event_sequence;
  char name[kMaxNameLength + 1];
};

struct alignas(std::atomic_ref<std::uint64_t>::required_alignment) SharedSlotPrefix {
  std::uint64_t commit;
  std::uint64_t hash;
};

static_assert(sizeof(SharedSlotPrefix) == 2 * sizeof(std::uint64_t));

struct alignas(std::atomic_ref<std::uint64_t>::required_alignment) SharedEvent {
  std::uint64_t sequence;
  std::uint64_t slot_index;
};

static_assert(sizeof(SharedEvent) == 2 * sizeof(std::uint64_t));

struct Layout {
  std::size_t data_offset = 0;
  std::size_t key_offset = 0;
  std::size_t value0_offset = 0;
  std::size_t value1_offset = 0;
  std::size_t stride = 0;
  std::size_t event_offset = 0;
  std::size_t event_count = 0;
  std::size_t total_size = 0;
};

std::uint64_t hash_bytes(std::string_view bytes) noexcept {
  constexpr std::uint64_t offset = 14695981039346656037ULL;
  constexpr std::uint64_t prime = 1099511628211ULL;
  std::uint64_t hash = offset;
  for (const unsigned char byte : bytes) {
    hash ^= byte;
    hash *= prime;
  }
  return hash;
}

std::string storage_name(std::string_view name) {
  constexpr std::array<char, 16> digits = {'0', '1', '2', '3', '4', '5', '6', '7',
                                           '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::array<char, digits.size()> encoded{};
  constexpr std::uint64_t hex_digit_mask = digits.size() - 1;
  std::uint64_t hash = hash_bytes(name);
  for (std::size_t index = encoded.size(); index != 0; --index) {
    encoded[index - 1] = digits[hash & hex_digit_mask];
    hash >>= 4;
  }
  return "/salias-kl-" + std::string(encoded.data(), encoded.size());
}

bool valid_name(std::string_view name) noexcept {
  if (name.empty() || name.size() > kMaxNameLength) {
    return false;
  }
  return std::all_of(name.begin(), name.end(), [](unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
           c == '-' || c == '.';
  });
}

bool checked_add(std::size_t left, std::size_t right, std::size_t& out) noexcept {
  if (right > std::numeric_limits<std::size_t>::max() - left) {
    return false;
  }
  out = left + right;
  return true;
}

bool checked_multiply(std::size_t left, std::size_t right, std::size_t& out) noexcept {
  if (left != 0 && right > std::numeric_limits<std::size_t>::max() / left) {
    return false;
  }
  out = left * right;
  return true;
}

bool align_up(std::size_t value, std::size_t alignment, std::size_t& out) noexcept {
  const std::size_t remainder = value % alignment;
  return remainder == 0 ? (out = value, true) : checked_add(value, alignment - remainder, out);
}

bool make_layout(std::size_t slot_count, std::size_t key_size, std::size_t value_size,
                 Layout& layout) noexcept {
  if (slot_count == 0 || slot_count > std::numeric_limits<std::uint32_t>::max() || key_size == 0 ||
      value_size == 0 || !align_up(sizeof(SharedHeader), kSharedAlignment, layout.data_offset)) {
    return false;
  }
  layout.key_offset = sizeof(SharedSlotPrefix);
  if (!checked_add(layout.key_offset, key_size, layout.value0_offset) ||
      !checked_add(layout.value0_offset, value_size, layout.value1_offset)) {
    return false;
  }
  std::size_t end = 0;
  if (!checked_add(layout.value1_offset, value_size, end) ||
      !align_up(end, std::atomic_ref<std::uint64_t>::required_alignment, layout.stride)) {
    return false;
  }
  std::size_t slots_size = 0;
  if (!checked_multiply(slot_count, layout.stride, slots_size) ||
      !checked_add(layout.data_offset, slots_size, layout.event_offset) ||
      !align_up(layout.event_offset, alignof(SharedEvent), layout.event_offset)) {
    return false;
  }
  layout.event_count = std::max(slot_count, kMinEventCount);
  std::size_t event_bytes = 0;
  return checked_multiply(layout.event_count, sizeof(SharedEvent), event_bytes) &&
         checked_add(layout.event_offset, event_bytes, layout.total_size) &&
         layout.total_size <= static_cast<std::size_t>(std::numeric_limits<off_t>::max());
}

bool lock_file(int fd, bool wait) noexcept {
  const int operation = LOCK_EX | (wait ? 0 : LOCK_NB);
  while (::flock(fd, operation) != 0) {
    if (errno == EINTR && wait) {
      continue;
    }
    return false;
  }
  return true;
}

std::shared_ptr<std::mutex> local_mutex_for(const std::string& shm_name) {
  static std::mutex registry_mutex;
  static std::unordered_map<std::string, std::weak_ptr<std::mutex>> registry;
  std::lock_guard lock(registry_mutex);
  if (auto existing = registry[shm_name].lock()) {
    return existing;
  }
  if (registry.size() > kRegistryCleanupThreshold) {
    std::erase_if(registry, [](const auto& entry) { return entry.second.expired(); });
  }
  auto created = std::make_shared<std::mutex>();
  registry[shm_name] = created;
  return created;
}

void unlock_file(int fd) noexcept { static_cast<void>(::flock(fd, LOCK_UN)); }

void close_fd(int fd) noexcept {
  if (fd >= 0) {
    static_cast<void>(::close(fd));
  }
}

}  // namespace

struct LatestByKeyStorage {
  int fd = -1;
  pid_t fd_pid = 0;
  void* mapping = MAP_FAILED;
  std::size_t mapping_size = 0;
  std::size_t data_offset = 0;
  std::size_t slot_count = 0;
  std::size_t stride = 0;
  std::size_t event_offset = 0;
  std::size_t event_count = 0;
  std::size_t key_offset = 0;
  std::size_t value0_offset = 0;
  std::size_t value1_offset = 0;
  std::string shm_name;
  pid_t owner_pid = 0;
  std::shared_ptr<std::mutex> local_mutex;

  Error ensure_process_fd() noexcept {
    const pid_t current_pid = ::getpid();
    if (fd_pid == current_pid) {
      return Error::Ok;
    }
    const int reopened = ::shm_open(shm_name.c_str(), O_RDWR | O_CLOEXEC, 0600);
    if (reopened < 0) {
      return errno == ENOENT ? Error::NotFound : Error::PlatformFail;
    }
    close_fd(fd);
    fd = reopened;
    fd_pid = current_pid;
    return Error::Ok;
  }

  ~LatestByKeyStorage() noexcept {
    if (mapping != MAP_FAILED) {
      static_cast<void>(::munmap(mapping, mapping_size));
    }
    close_fd(fd);
    if (owner_pid == ::getpid() && !shm_name.empty()) {
      static_cast<void>(::shm_unlink(shm_name.c_str()));
    }
  }
};

namespace {

Result<std::shared_ptr<LatestByKeyStorage>> map_storage(
    int fd, std::string shm_name, bool owner, const Layout& layout, std::size_t slot_count,
    std::size_t key_size, std::size_t value_size, std::uint64_t key_schema,
    std::uint64_t value_schema, std::string_view public_name, bool initialize) {
  void* mapping = ::mmap(nullptr, layout.total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (mapping == MAP_FAILED) {
    if (owner) {
      static_cast<void>(::shm_unlink(shm_name.c_str()));
    }
    return std::unexpected(Error::PlatformFail);
  }

  try {
    if (initialize) {
      std::memset(mapping, 0, layout.total_size);
      auto* header = std::construct_at(static_cast<SharedHeader*>(mapping));
      header->magic = kStorageMagic;
      header->version = kStorageVersion;
      header->total_size = layout.total_size;
      header->slot_count = slot_count;
      header->key_size = key_size;
      header->value_size = value_size;
      header->key_schema = key_schema;
      header->value_schema = value_schema;
      header->name_hash = hash_bytes(public_name);
      header->event_count = layout.event_count;
      std::memcpy(header->name, public_name.data(), public_name.size());
      auto* slot_bytes = static_cast<std::byte*>(mapping) + layout.data_offset;
      for (std::size_t index = 0; index < slot_count; ++index) {
        std::construct_at(reinterpret_cast<SharedSlotPrefix*>(slot_bytes + index * layout.stride));
      }
      auto* event_bytes = static_cast<std::byte*>(mapping) + layout.event_offset;
      for (std::size_t index = 0; index < layout.event_count; ++index) {
        std::construct_at(
            reinterpret_cast<SharedEvent*>(event_bytes + index * sizeof(SharedEvent)));
      }
      std::atomic_ref<std::uint32_t>(header->ready).store(1, std::memory_order_release);
    } else {
      const auto* header = static_cast<const SharedHeader*>(mapping);
      auto* ready = const_cast<std::uint32_t*>(&header->ready);
      if (std::atomic_ref<std::uint32_t>(*ready).load(std::memory_order_acquire) != 1 ||
          header->magic != kStorageMagic || header->version != kStorageVersion) {
        static_cast<void>(::munmap(mapping, layout.total_size));
        return std::unexpected(Error::VersionMismatch);
      }
      const auto name_end = std::find(std::begin(header->name), std::end(header->name), '\0');
      const auto name_length = static_cast<std::size_t>(name_end - std::begin(header->name));
      if (name_end == std::end(header->name) || name_length != public_name.size() ||
          std::memcmp(header->name, public_name.data(), name_length) != 0 ||
          header->name_hash != hash_bytes(public_name)) {
        static_cast<void>(::munmap(mapping, layout.total_size));
        return std::unexpected(Error::VersionMismatch);
      }
      if (header->total_size != layout.total_size || header->slot_count != slot_count ||
          header->key_size != key_size || header->value_size != value_size ||
          header->key_schema != key_schema || header->value_schema != value_schema ||
          header->event_count != layout.event_count) {
        static_cast<void>(::munmap(mapping, layout.total_size));
        return std::unexpected(Error::BadConfig);
      }
    }

    auto process_mutex = local_mutex_for(shm_name);
    auto storage = std::make_shared<LatestByKeyStorage>();
    storage->shm_name = std::move(shm_name);
    storage->local_mutex = std::move(process_mutex);
    storage->mapping_size = layout.total_size;
    storage->data_offset = layout.data_offset;
    storage->slot_count = slot_count;
    storage->stride = layout.stride;
    storage->event_offset = layout.event_offset;
    storage->event_count = layout.event_count;
    storage->key_offset = layout.key_offset;
    storage->value0_offset = layout.value0_offset;
    storage->value1_offset = layout.value1_offset;
    storage->owner_pid = owner ? ::getpid() : 0;
    storage->fd_pid = ::getpid();
    storage->mapping = mapping;
    storage->fd = fd;
    return storage;
  } catch (const std::bad_alloc&) {
    static_cast<void>(::munmap(mapping, layout.total_size));
    if (owner) {
      static_cast<void>(::shm_unlink(shm_name.c_str()));
    }
    return std::unexpected(Error::OutOfMemory);
  }
}

}  // namespace

Result<std::shared_ptr<LatestByKeyStorage>> create_latest_by_key_storage(
    std::string_view name, std::size_t slot_count, std::size_t key_size, std::size_t value_size,
    std::uint64_t key_schema, std::uint64_t value_schema) {
  if (!valid_name(name)) {
    return std::unexpected(Error::BadConfig);
  }
  Layout layout;
  if (!make_layout(slot_count, key_size, value_size, layout)) {
    return std::unexpected(Error::BadConfig);
  }

  try {
    const std::string shm_name = storage_name(name);
    for (int attempt = 0; attempt < 2; ++attempt) {
      const int fd = ::shm_open(shm_name.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
      if (fd >= 0) {
        if (!lock_file(fd, true) || ::ftruncate(fd, static_cast<off_t>(layout.total_size)) != 0) {
          close_fd(fd);
          static_cast<void>(::shm_unlink(shm_name.c_str()));
          return std::unexpected(Error::PlatformFail);
        }
        auto created = map_storage(fd, shm_name, true, layout, slot_count, key_size, value_size,
                                   key_schema, value_schema, name, true);
        unlock_file(fd);
        if (!created) {
          close_fd(fd);
        }
        return created;
      }
      if (errno != EEXIST || attempt != 0) {
        return std::unexpected(Error::PlatformFail);
      }

      // A process killed before publishing ready leaves no usable channel. Reclaim only such
      // incomplete segments; an active creator holds the same kernel lock during initialization.
      const int existing = ::shm_open(shm_name.c_str(), O_RDWR | O_CLOEXEC, 0600);
      if (existing < 0 || !lock_file(existing, false)) {
        close_fd(existing);
        return std::unexpected(Error::PlatformFail);
      }
      struct stat info {};
      bool incomplete = false;
      if (::fstat(existing, &info) == 0) {
        if (info.st_size >= static_cast<off_t>(sizeof(SharedHeader))) {
          void* header_map = ::mmap(nullptr, sizeof(SharedHeader), PROT_READ | PROT_WRITE,
                                    MAP_SHARED, existing, 0);
          if (header_map != MAP_FAILED) {
            auto* header = static_cast<SharedHeader*>(header_map);
            incomplete =
                std::atomic_ref<std::uint32_t>(header->ready).load(std::memory_order_acquire) != 1;
            static_cast<void>(::munmap(header_map, sizeof(SharedHeader)));
          }
        }
      }
      if (incomplete) {
        static_cast<void>(::shm_unlink(shm_name.c_str()));
      }
      unlock_file(existing);
      close_fd(existing);
      if (!incomplete) {
        return std::unexpected(Error::PlatformFail);
      }
    }
  } catch (const std::bad_alloc&) {
    return std::unexpected(Error::OutOfMemory);
  }
  return std::unexpected(Error::PlatformFail);
}

Result<std::shared_ptr<LatestByKeyStorage>> connect_latest_by_key_storage(
    std::string_view name, std::size_t slot_count, std::size_t key_size, std::size_t value_size,
    std::uint64_t key_schema, std::uint64_t value_schema) {
  if (!valid_name(name)) {
    return std::unexpected(Error::BadConfig);
  }
  Layout layout;
  if (!make_layout(slot_count, key_size, value_size, layout)) {
    return std::unexpected(Error::BadConfig);
  }

  try {
    const std::string shm_name = storage_name(name);
    const int fd = ::shm_open(shm_name.c_str(), O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
      return std::unexpected(errno == ENOENT ? Error::NotFound : Error::PlatformFail);
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    for (;;) {
      if (!lock_file(fd, true)) {
        close_fd(fd);
        return std::unexpected(Error::PlatformFail);
      }
      struct stat info {};
      if (::fstat(fd, &info) != 0) {
        unlock_file(fd);
        close_fd(fd);
        return std::unexpected(Error::PlatformFail);
      }
      if (info.st_size == 0 && std::chrono::steady_clock::now() < deadline) {
        unlock_file(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      if (info.st_size != static_cast<off_t>(layout.total_size)) {
        unlock_file(fd);
        close_fd(fd);
        return std::unexpected(Error::VersionMismatch);
      }
      auto connected = map_storage(fd, shm_name, false, layout, slot_count, key_size, value_size,
                                   key_schema, value_schema, name, false);
      unlock_file(fd);
      if (!connected) {
        close_fd(fd);
      }
      return connected;
    }
  } catch (const std::bad_alloc&) {
    return std::unexpected(Error::OutOfMemory);
  }
}

Error with_latest_by_key_table(const std::shared_ptr<LatestByKeyStorage>& storage,
                               LatestByKeyTableCallback callback, void* context,
                               bool wait_for_lock) noexcept {
  if (!storage || callback == nullptr) {
    return Error::BadConfig;
  }

  if (!storage->local_mutex) {
    return Error::BadConfig;
  }
  std::unique_lock local_lock(*storage->local_mutex, std::defer_lock);
  if (wait_for_lock) {
    local_lock.lock();
  } else if (!local_lock.try_lock()) {
    return Error::BackPressured;
  }
  const Error reopened = storage->ensure_process_fd();
  if (reopened != Error::Ok) {
    return reopened;
  }
  if (!lock_file(storage->fd, wait_for_lock)) {
    return wait_for_lock ? Error::PlatformFail
                         : ((errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR)
                                ? Error::BackPressured
                                : Error::PlatformFail);
  }

  auto* bytes = static_cast<std::byte*>(storage->mapping) + storage->data_offset;
  const LatestByKeyTableView table{
      .slots = bytes,
      .events = static_cast<std::byte*>(storage->mapping) + storage->event_offset,
      .event_sequence = &static_cast<SharedHeader*>(storage->mapping)->event_sequence,
      .slot_count = storage->slot_count,
      .event_count = storage->event_count,
      .stride = storage->stride,
      .key_offset = storage->key_offset,
      .value0_offset = storage->value0_offset,
      .value1_offset = storage->value1_offset,
  };
  const Error result = callback(table, context);
  unlock_file(storage->fd);
  return result;
}

}  // namespace salias::detail

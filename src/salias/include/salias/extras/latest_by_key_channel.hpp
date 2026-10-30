/**
 * @file src/salias/include/salias/extras/latest_by_key_channel.hpp
 * @brief Fixed-capacity, cross-process latest-value conflation with exact object-byte keys.
 */
#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "salias/extras/detail/latest_by_key_storage.hpp"

namespace salias::extras {

/// @brief Owned snapshot of one key's current value and per-key update version.
template <class Key, class Value>
struct LatestByKeyMessage {
  Key key;
  Value value;
  std::uint64_t version = 0;
};

namespace latest_by_key_detail {

inline constexpr std::uint64_t kOccupied = 1;
inline constexpr std::uint64_t kActiveBuffer = 2;
inline constexpr unsigned kVersionShift = 2;
inline constexpr std::uint64_t kMaxVersion =
    std::numeric_limits<std::uint64_t>::max() >> kVersionShift;
inline constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
inline constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

constexpr std::uint64_t version_of(std::uint64_t commit) noexcept {
  return commit >> kVersionShift;
}

constexpr std::uint32_t active_buffer_of(std::uint64_t commit) noexcept {
  return static_cast<std::uint32_t>((commit & kActiveBuffer) >> 1);
}

constexpr std::uint64_t make_commit(std::uint64_t version, std::uint32_t active) noexcept {
  return (version << kVersionShift) | (static_cast<std::uint64_t>(active) << 1) | kOccupied;
}

inline Error append_event(detail::LatestByKeyTableView table, std::size_t slot_index) noexcept {
  const std::uint64_t current =
      std::atomic_ref<std::uint64_t>(*table.event_sequence).load(std::memory_order_relaxed);
  if (current == std::numeric_limits<std::uint64_t>::max()) {
    return Error::SequenceExhausted;
  }
  const std::uint64_t next = current + 1;
  auto event = table.event(next % table.event_count);
  std::atomic_ref<std::uint64_t>(*event.sequence).store(0, std::memory_order_release);
  *event.slot_index = slot_index;
  std::atomic_ref<std::uint64_t>(*event.sequence).store(next, std::memory_order_release);
  std::atomic_ref<std::uint64_t>(*table.event_sequence).store(next, std::memory_order_release);
  return Error::Ok;
}

inline std::uint64_t hash_key(std::span<const std::byte> bytes) noexcept {
  std::uint64_t hash = kFnvOffsetBasis;
  for (const std::byte byte : bytes) {
    hash ^= std::to_integer<std::uint8_t>(byte);
    hash *= kFnvPrime;
  }
  return hash;
}

template <class T>
consteval std::uint64_t type_schema() {
#if defined(__clang__) || defined(__GNUC__)
  constexpr std::string_view signature = __PRETTY_FUNCTION__;
#else
  constexpr std::string_view signature = __FUNCSIG__;
#endif
  std::uint64_t hash = kFnvOffsetBasis;
  for (const char byte : signature) {
    hash ^= static_cast<unsigned char>(byte);
    hash *= kFnvPrime;
  }
  return hash;
}

template <class T, std::uint64_t SchemaVersion>
consteval std::uint64_t versioned_type_schema() {
  if constexpr (SchemaVersion == 0) {
    return type_schema<T>();
  }

  std::uint64_t hash = type_schema<T>();
  constexpr unsigned schema_bits = std::numeric_limits<std::uint64_t>::digits;
  constexpr unsigned byte_bits = std::numeric_limits<std::uint8_t>::digits;
  for (unsigned shift = 0; shift < schema_bits; shift += byte_bits) {
    hash ^= static_cast<std::uint8_t>(SchemaVersion >> shift);
    hash *= kFnvPrime;
  }
  return hash;
}

template <class T>
using ObjectBytes = std::array<std::byte, sizeof(T)>;

template <class T>
ObjectBytes<T> to_bytes(const T& value) noexcept {
  return std::bit_cast<ObjectBytes<T>>(value);
}

template <class Key, class Value>
struct OfferContext {
  const std::byte* key = nullptr;
  const std::byte* value = nullptr;
  std::uint64_t hash = 0;
};

template <class Key, class Value>
Error offer(detail::LatestByKeyTableView table, void* opaque) noexcept {
  const auto& context = *static_cast<OfferContext<Key, Value>*>(opaque);
  const std::size_t start = context.hash % table.slot_count;
  for (std::size_t probe = 0; probe < table.slot_count; ++probe) {
    const std::size_t index = (start + probe) % table.slot_count;
    auto slot = table.slot(index);
    const std::uint64_t commit =
        std::atomic_ref<std::uint64_t>(*slot.commit).load(std::memory_order_acquire);
    if ((commit & kOccupied) == 0) {
      const Error event_result = append_event(table, index);
      if (event_result != Error::Ok) {
        return event_result;
      }
      slot.hash[0] = context.hash;
      std::memcpy(slot.key, context.key, sizeof(Key));
      std::memcpy(slot.values[0], context.value, sizeof(Value));
      std::atomic_ref<std::uint64_t>(*slot.commit)
          .store(make_commit(1, 0), std::memory_order_release);
      return Error::Ok;
    }
    if (*slot.hash != context.hash || std::memcmp(slot.key, context.key, sizeof(Key)) != 0) {
      continue;
    }

    const std::uint64_t version = version_of(commit);
    if (version == 0) {
      return Error::VersionMismatch;
    }
    if (version == kMaxVersion) {
      return Error::SequenceExhausted;
    }
    const Error event_result = append_event(table, index);
    if (event_result != Error::Ok) {
      return event_result;
    }
    const std::uint32_t inactive = active_buffer_of(commit) ^ 1U;
    std::memcpy(slot.values[inactive], context.value, sizeof(Value));
    std::atomic_ref<std::uint64_t>(*slot.commit)
        .store(make_commit(version + 1, inactive), std::memory_order_release);
    return Error::Ok;
  }
  return Error::CapacityExceeded;
}

template <class Key, class Value>
struct Cursor {
  explicit Cursor(std::shared_ptr<detail::LatestByKeyStorage> storage_value, std::size_t capacity)
      : storage(std::move(storage_value)),
        seen(capacity, 0),
        initial_version(capacity, 0),
        initial_pending(capacity, false),
        scan_remaining(capacity) {}

  std::shared_ptr<detail::LatestByKeyStorage> storage;
  std::vector<std::uint64_t> seen;
  std::vector<std::uint64_t> initial_version;
  std::vector<bool> initial_pending;
  std::size_t next_slot = 0;
  std::uint64_t event_sequence = 0;
  std::size_t scan_remaining = 0;
  std::uint64_t conflated = 0;
};

template <class Key, class Value>
struct InitializeCursorContext {
  Cursor<Key, Value>* cursor = nullptr;
};

template <class Key, class Value>
Error initialize_cursor(detail::LatestByKeyTableView table, void* opaque) noexcept {
  auto& cursor = *static_cast<InitializeCursorContext<Key, Value>*>(opaque)->cursor;
  cursor.event_sequence =
      std::atomic_ref<std::uint64_t>(*table.event_sequence).load(std::memory_order_acquire);
  for (std::size_t index = 0; index < table.slot_count; ++index) {
    const auto slot = table.slot(index);
    const std::uint64_t commit =
        std::atomic_ref<std::uint64_t>(*slot.commit).load(std::memory_order_acquire);
    const std::uint64_t version = version_of(commit);
    const bool occupied = (commit & kOccupied) != 0;
    if (occupied && version == 0) {
      return Error::VersionMismatch;
    }
    cursor.seen[index] = occupied ? version - 1 : 0;
    cursor.initial_version[index] = occupied ? version : 0;
    cursor.initial_pending[index] = occupied;
  }
  return Error::Ok;
}

template <class Key, class Value>
struct ReceiveContext {
  Cursor<Key, Value>* cursor = nullptr;
  ObjectBytes<Key>* key = nullptr;
  ObjectBytes<Value>* value = nullptr;
  std::uint64_t* version = nullptr;
  bool found = false;
};

template <class Key, class Value>
Error receive(detail::LatestByKeyTableView table, void* opaque) noexcept {
  auto& context = *static_cast<ReceiveContext<Key, Value>*>(opaque);
  auto& cursor = *context.cursor;

  const auto deliver_slot = [&](std::size_t index) noexcept -> Error {
    const auto slot = table.slot(index);
    const std::uint64_t commit =
        std::atomic_ref<std::uint64_t>(*slot.commit).load(std::memory_order_acquire);
    if ((commit & kOccupied) == 0) {
      return Error::Ok;
    }
    const std::uint64_t version = version_of(commit);
    if (version == 0) {
      return Error::VersionMismatch;
    }
    if (version == cursor.seen[index]) {
      return Error::Ok;
    }

    const auto active = active_buffer_of(commit);
    std::memcpy(context.key->data(), slot.key, sizeof(Key));
    std::memcpy(context.value->data(), slot.values[active], sizeof(Value));
    const std::uint64_t lost =
        cursor.initial_pending[index]
            ? version - cursor.initial_version[index]
            : (version > cursor.seen[index] + 1 ? version - cursor.seen[index] - 1 : 0);
    if (lost != 0) {
      cursor.conflated = lost > std::numeric_limits<std::uint64_t>::max() - cursor.conflated
                             ? std::numeric_limits<std::uint64_t>::max()
                             : cursor.conflated + lost;
    }
    cursor.seen[index] = version;
    cursor.initial_pending[index] = false;
    *context.version = version;
    context.found = true;
    return Error::Ok;
  };

  for (;;) {
    if (cursor.scan_remaining != 0) {
      while (cursor.scan_remaining != 0) {
        const std::size_t index = cursor.next_slot;
        cursor.next_slot = (index + 1) % table.slot_count;
        --cursor.scan_remaining;
        const Error result = deliver_slot(index);
        if (result != Error::Ok || context.found) {
          return result;
        }
      }
    }

    const std::uint64_t current =
        std::atomic_ref<std::uint64_t>(*table.event_sequence).load(std::memory_order_acquire);
    if (current < cursor.event_sequence) {
      return Error::VersionMismatch;
    }
    if (current == cursor.event_sequence) {
      return Error::Ok;
    }
    if (current - cursor.event_sequence > table.event_count) {
      cursor.event_sequence = current;
      cursor.next_slot = 0;
      cursor.scan_remaining = table.slot_count;
      continue;
    }

    std::uint64_t next = cursor.event_sequence + 1;
    for (;;) {
      const auto event = table.event(next % table.event_count);
      if (*event.sequence != next || *event.slot_index >= table.slot_count) {
        cursor.event_sequence = current;
        cursor.next_slot = 0;
        cursor.scan_remaining = table.slot_count;
        break;
      }
      const Error result = deliver_slot(static_cast<std::size_t>(*event.slot_index));
      cursor.event_sequence = next;
      if (result != Error::Ok || context.found) {
        return result;
      }
      if (next == current) {
        return Error::Ok;
      }
      ++next;
    }
  }
}

}  // namespace latest_by_key_detail

/// @brief Publisher endpoint for a LatestByKeyChannel.
template <class Key, class Value>
class LatestByKeyPublisher {
 public:
  LatestByKeyPublisher() noexcept = default;

  /**
   * @brief Insert a new key or replace the latest value for an existing key.
   * @retval true The snapshot is now visible to subscribers.
   * @retval Error::BackPressured Another process/thread currently owns the table lock.
   * @retval Error::CapacityExceeded All slots contain different keys.
   * @retval Error::SequenceExhausted The key version or channel event sequence is exhausted.
   * @retval Error::BadConfig This is a default or moved-from endpoint.
   */
  Result<bool> offer(const Key& key, const Value& value) noexcept {
    if (!storage_) {
      return std::unexpected(Error::BadConfig);
    }
    const auto key_bytes = latest_by_key_detail::to_bytes(key);
    const auto value_bytes = latest_by_key_detail::to_bytes(value);
    latest_by_key_detail::OfferContext<Key, Value> context{
        .key = key_bytes.data(),
        .value = value_bytes.data(),
        .hash = latest_by_key_detail::hash_key(key_bytes),
    };
    const Error result = detail::with_latest_by_key_table(
        storage_, &latest_by_key_detail::offer<Key, Value>, &context);
    if (result != Error::Ok) {
      return std::unexpected(result);
    }
    return true;
  }

  explicit operator bool() const noexcept { return static_cast<bool>(storage_); }

 private:
  explicit LatestByKeyPublisher(std::shared_ptr<detail::LatestByKeyStorage> storage) noexcept
      : storage_(std::move(storage)) {}

  std::shared_ptr<detail::LatestByKeyStorage> storage_;

  template <class, class, std::size_t, std::uint64_t>
  friend class LatestByKeyChannel;
};

/// @brief Independent cursor over the latest snapshots retained by a latest-by-key channel.
template <class Key, class Value, std::size_t Capacity>
class LatestByKeySubscriber {
 public:
  using message_type = LatestByKeyMessage<Key, Value>;

  LatestByKeySubscriber() noexcept = default;

  /**
   * @brief Read one changed key as an owned copy without blocking.
   * @return The next snapshot, or an empty optional when no key changed.
   * @retval Error::BackPressured The shared table is temporarily locked.
   * @retval Error::BadConfig This is a default or moved-from endpoint.
   * @note Each key is returned at most once per observed version; intermediate updates are
   *       conflated. The scan starts after the previously returned slot and has no cross-key order.
   */
  Result<std::optional<LatestByKeyMessage<Key, Value>>> try_recv_checked() noexcept {
    if (!cursor_) {
      return std::unexpected(Error::BadConfig);
    }
    if (cursor_->scan_remaining == 0 &&
        detail::latest_by_key_table_unchanged(cursor_->storage, cursor_->event_sequence)) {
      return std::optional<LatestByKeyMessage<Key, Value>>{};
    }
    latest_by_key_detail::ObjectBytes<Key> key_bytes{};
    latest_by_key_detail::ObjectBytes<Value> value_bytes{};
    std::uint64_t version = 0;
    latest_by_key_detail::ReceiveContext<Key, Value> context{
        .cursor = cursor_.get(), .key = &key_bytes, .value = &value_bytes, .version = &version};
    const Error result = detail::with_latest_by_key_table(
        cursor_->storage, &latest_by_key_detail::receive<Key, Value>, &context);
    if (result != Error::Ok) {
      return std::unexpected(result);
    }
    if (!context.found) {
      return std::optional<LatestByKeyMessage<Key, Value>>{};
    }
    return std::optional<LatestByKeyMessage<Key, Value>>{
        std::in_place, LatestByKeyMessage<Key, Value>{std::bit_cast<Key>(key_bytes),
                                                      std::bit_cast<Value>(value_bytes), version}};
  }

  /** @brief Convenience receive; checked errors are folded into an empty result. */
  std::optional<LatestByKeyMessage<Key, Value>> try_recv() noexcept {
    auto received = try_recv_checked();
    return received ? std::move(*received) : std::nullopt;
  }

  /**
   * @brief Wait until one changed key is available or an unrecoverable receive error occurs.
   * @return The next owned snapshot.
   * @retval Error::BadConfig This is a default or moved-from endpoint.
   * @retval Error::PlatformFail A shared-memory operation failed.
   * @retval Error::VersionMismatch The shared table state is inconsistent.
   * @note Lock contention is retried internally; use try_recv_checked() for nonblocking access.
   */
  Result<LatestByKeyMessage<Key, Value>> recv() noexcept {
    for (;;) {
      auto received = try_recv_checked();
      if (!received) {
        if (received.error() != Error::BackPressured) {
          return std::unexpected(received.error());
        }
      } else if (*received) {
        return std::move(**received);
      }
      std::this_thread::yield();
    }
  }

  /** @return Updates overwritten before this endpoint observed them, saturating at UINT64_MAX. */
  std::uint64_t conflated_updates() const noexcept { return cursor_ ? cursor_->conflated : 0; }

  explicit operator bool() const noexcept { return static_cast<bool>(cursor_); }

 private:
  explicit LatestByKeySubscriber(
      std::shared_ptr<latest_by_key_detail::Cursor<Key, Value>> cursor) noexcept
      : cursor_(std::move(cursor)) {}

  std::shared_ptr<latest_by_key_detail::Cursor<Key, Value>> cursor_;

  template <class, class, std::size_t, std::uint64_t>
  friend class LatestByKeyChannel;
};

/**
 * @brief Bounded cross-process latest-value table with exact-key conflation.
 * @tparam Key Trivially copyable, non-pointer key with unique object representations; exact bytes
 *         identify a key.
 * @tparam Value Trivially copyable, self-contained value. It and its members must not contain
 *         process-local pointers.
 * @tparam Capacity Maximum number of distinct keys retained by the channel.
 * @tparam SchemaVersion Application-managed key/value schema version; zero preserves automatic
 *         type-signature checking only.
 *
 * Each key owns one fixed slot. Repeated updates replace that key's current value and are
 * independently conflated for each subscriber. New subscribers receive one current snapshot per
 * existing key. A bounded event log avoids rescanning the full key table on every receive; lagging
 * subscribers rescan the current snapshot after event-log overrun. Updates to different keys have
 * no total ordering guarantee. A subscriber returns copied values, not borrowed shared-memory
 * views. When all slots contain distinct keys, a new key returns CapacityExceeded; updating an
 * existing key still succeeds. The shared table uses a robust process-shared mutex on Linux and
 * a crash-released OS file lock on macOS; operations on the table are serialized and contention
 * is reported as BackPressured. Empty polls use an atomic fast path. After fork, endpoints may be
 * used in the child only when all threads using this channel were quiescent at fork time.
 */
template <class Key, class Value, std::size_t Capacity, std::uint64_t SchemaVersion = 0>
class LatestByKeyChannel {
  static_assert(std::is_trivially_copyable_v<Key>, "Key must be trivially copyable");
  static_assert(!std::is_pointer_v<Key> && !std::is_member_pointer_v<Key>,
                "Key cannot be a process-local pointer");
  static_assert(std::has_unique_object_representations_v<Key>,
                "Key must have unique object representations");
  static_assert(std::is_trivially_copyable_v<Value>, "Value must be trivially copyable");
  static_assert(!std::is_pointer_v<Value> && !std::is_member_pointer_v<Value>,
                "Value cannot be a process-local pointer");
  static_assert(Capacity > 0, "LatestByKeyChannel capacity must be greater than zero");
  static_assert(Capacity <= std::numeric_limits<std::uint32_t>::max(),
                "LatestByKeyChannel capacity exceeds the supported slot count");

 public:
  using CreateResult = Result<LatestByKeyChannel>;
  using Publisher = LatestByKeyPublisher<Key, Value>;
  using Subscriber = LatestByKeySubscriber<Key, Value, Capacity>;
  using Message = LatestByKeyMessage<Key, Value>;

  /** @brief Create and own a new named shared-memory latest-by-key table. */
  static CreateResult create(std::string_view name) {
    auto storage = detail::create_latest_by_key_storage(
        name, Capacity, sizeof(Key), sizeof(Value),
        latest_by_key_detail::versioned_type_schema<Key, SchemaVersion>(),
        latest_by_key_detail::versioned_type_schema<Value, SchemaVersion>());
    if (!storage) {
      return std::unexpected(storage.error());
    }
    return LatestByKeyChannel(std::move(*storage));
  }

  /** @brief Connect to a table and verify capacity, key/value type schemas, and schema version. */
  static CreateResult connect(std::string_view name) {
    auto storage = detail::connect_latest_by_key_storage(
        name, Capacity, sizeof(Key), sizeof(Value),
        latest_by_key_detail::versioned_type_schema<Key, SchemaVersion>(),
        latest_by_key_detail::versioned_type_schema<Value, SchemaVersion>());
    if (!storage) {
      return std::unexpected(storage.error());
    }
    return LatestByKeyChannel(std::move(*storage));
  }

  LatestByKeyChannel(LatestByKeyChannel&&) noexcept = default;
  LatestByKeyChannel& operator=(LatestByKeyChannel&&) noexcept = default;
  LatestByKeyChannel(const LatestByKeyChannel&) = default;
  LatestByKeyChannel& operator=(const LatestByKeyChannel&) = default;

  /** @brief Create a publisher endpoint sharing this table. */
  Publisher publisher() const noexcept { return Publisher(storage_); }

  /**
   * @brief Create a subscriber, waiting for an in-progress table operation to finish.
   * @note Each new subscriber receives one current snapshot for every occupied key. Copies of the
   *       returned endpoint share one cursor and must not be polled concurrently.
   */
  Subscriber subscriber() const {
    auto created = try_subscriber_impl(true);
    if (!created) {
      if (created.error() == Error::OutOfMemory) {
        throw std::bad_alloc();
      }
      return {};
    }
    return std::move(*created);
  }

  /**
   * @brief Nonblocking subscriber creation.
   * @retval Error::BackPressured A table operation is active.
   * @retval Error::OutOfMemory The local cursor could not be allocated.
   */
  Result<Subscriber> try_subscriber() const { return try_subscriber_impl(false); }

  /// @return Maximum number of distinct retained keys, fixed at compile time.
  static constexpr std::size_t capacity() noexcept { return Capacity; }

 private:
  explicit LatestByKeyChannel(std::shared_ptr<detail::LatestByKeyStorage> storage) noexcept
      : storage_(std::move(storage)) {}

  Result<Subscriber> try_subscriber_impl(bool wait_for_lock) const {
    try {
      auto cursor = std::make_shared<latest_by_key_detail::Cursor<Key, Value>>(storage_, Capacity);
      latest_by_key_detail::InitializeCursorContext<Key, Value> context{.cursor = cursor.get()};
      const Error result = detail::with_latest_by_key_table(
          storage_, &latest_by_key_detail::initialize_cursor<Key, Value>, &context, wait_for_lock);
      if (result != Error::Ok) {
        return std::unexpected(result);
      }
      return Subscriber(std::move(cursor));
    } catch (const std::bad_alloc&) {
      return std::unexpected(Error::OutOfMemory);
    }
  }

  std::shared_ptr<detail::LatestByKeyStorage> storage_;
};

}  // namespace salias::extras

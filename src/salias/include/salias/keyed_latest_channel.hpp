/**
 * @file src/salias/include/salias/keyed_latest_channel.hpp
 * @brief Fixed-capacity, cross-process latest-value conflation keyed by exact object bytes.
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

#include "salias/detail/keyed_latest_storage.hpp"

namespace salias {

/// @brief Owned snapshot of one key's current value and per-key update version.
template <class Key, class Value>
struct KeyedLatestMessage {
  Key key;
  Value value;
  std::uint64_t version = 0;
};

namespace keyed_latest_detail {

inline constexpr std::uint64_t kOccupied = 1;
inline constexpr std::uint64_t kActiveBuffer = 2;
inline constexpr unsigned kVersionShift = 2;
inline constexpr std::uint64_t kMaxVersion =
    std::numeric_limits<std::uint64_t>::max() >> kVersionShift;

constexpr std::uint64_t version_of(std::uint64_t commit) noexcept {
  return commit >> kVersionShift;
}

constexpr std::uint32_t active_buffer_of(std::uint64_t commit) noexcept {
  return static_cast<std::uint32_t>((commit & kActiveBuffer) >> 1);
}

constexpr std::uint64_t make_commit(std::uint64_t version, std::uint32_t active) noexcept {
  return (version << kVersionShift) | (static_cast<std::uint64_t>(active) << 1) | kOccupied;
}

inline Error append_event(detail::KeyedLatestTableView table, std::size_t slot_index) noexcept {
  const std::uint64_t current =
      std::atomic_ref<std::uint64_t>(*table.event_sequence).load(std::memory_order_relaxed);
  if (current == std::numeric_limits<std::uint64_t>::max()) {
    return Error::SequenceExhausted;
  }
  const std::uint64_t next = current + 1;
  auto event = table.event(next % table.event_count);
  *event.slot_index = slot_index;
  event.sequence[0] = next;
  std::atomic_ref<std::uint64_t>(*table.event_sequence).store(next, std::memory_order_release);
  return Error::Ok;
}

inline std::uint64_t hash_key(std::span<const std::byte> bytes) noexcept {
  constexpr std::uint64_t offset = 14695981039346656037ull;
  constexpr std::uint64_t prime = 1099511628211ull;
  std::uint64_t hash = offset;
  for (const std::byte byte : bytes) {
    hash ^= std::to_integer<std::uint8_t>(byte);
    hash *= prime;
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
  constexpr std::uint64_t offset = 14695981039346656037ull;
  constexpr std::uint64_t prime = 1099511628211ull;
  std::uint64_t hash = offset;
  for (const char byte : signature) {
    hash ^= static_cast<unsigned char>(byte);
    hash *= prime;
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
Error offer(detail::KeyedLatestTableView table, void* opaque) noexcept {
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
    const std::uint32_t inactive = active_buffer_of(commit) ^ 1u;
    std::memcpy(slot.values[inactive], context.value, sizeof(Value));
    std::atomic_ref<std::uint64_t>(*slot.commit)
        .store(make_commit(version + 1, inactive), std::memory_order_release);
    return Error::Ok;
  }
  return Error::CapacityExceeded;
}

template <class Key, class Value>
struct Cursor {
  explicit Cursor(std::shared_ptr<detail::KeyedLatestStorage> storage_value, std::size_t capacity)
      : storage(std::move(storage_value)),
        seen(capacity, 0),
        initial_version(capacity, 0),
        initial_pending(capacity, false) {}

  std::shared_ptr<detail::KeyedLatestStorage> storage;
  std::vector<std::uint64_t> seen;
  std::vector<std::uint64_t> initial_version;
  std::vector<bool> initial_pending;
  std::size_t next_slot = 0;
  std::uint64_t event_sequence = 0;
  bool scan_pending = true;
  std::uint64_t conflated = 0;
};

template <class Key, class Value>
struct InitializeCursorContext {
  Cursor<Key, Value>* cursor = nullptr;
};

template <class Key, class Value>
Error initialize_cursor(detail::KeyedLatestTableView table, void* opaque) noexcept {
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
Error receive(detail::KeyedLatestTableView table, void* opaque) noexcept {
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
    if (cursor.scan_pending) {
      const std::size_t scan_start = cursor.next_slot;
      for (std::size_t offset = 0; offset < table.slot_count; ++offset) {
        const std::size_t index = (scan_start + offset) % table.slot_count;
        const Error result = deliver_slot(index);
        if (result != Error::Ok || context.found) {
          if (context.found) {
            cursor.next_slot = (index + 1) % table.slot_count;
          }
          return result;
        }
      }
      cursor.next_slot = (scan_start + 1) % table.slot_count;
      cursor.scan_pending = false;
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
      cursor.scan_pending = true;
      continue;
    }

    std::uint64_t next = cursor.event_sequence + 1;
    for (;;) {
      const auto event = table.event(next % table.event_count);
      if (*event.sequence != next || *event.slot_index >= table.slot_count) {
        cursor.event_sequence = current;
        cursor.next_slot = 0;
        cursor.scan_pending = true;
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

}  // namespace keyed_latest_detail

/// @brief Publisher endpoint for a KeyedLatestChannel.
template <class Key, class Value>
class KeyedLatestPublisher {
 public:
  KeyedLatestPublisher() noexcept = default;

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
    const auto key_bytes = keyed_latest_detail::to_bytes(key);
    const auto value_bytes = keyed_latest_detail::to_bytes(value);
    keyed_latest_detail::OfferContext<Key, Value> context{
        .key = key_bytes.data(),
        .value = value_bytes.data(),
        .hash = keyed_latest_detail::hash_key(key_bytes),
    };
    const Error result = detail::with_keyed_latest_table(
        storage_, &keyed_latest_detail::offer<Key, Value>, &context);
    if (result != Error::Ok) {
      return std::unexpected(result);
    }
    return true;
  }

  explicit operator bool() const noexcept { return static_cast<bool>(storage_); }

 private:
  explicit KeyedLatestPublisher(std::shared_ptr<detail::KeyedLatestStorage> storage) noexcept
      : storage_(std::move(storage)) {}

  std::shared_ptr<detail::KeyedLatestStorage> storage_;

  template <class, class, std::size_t>
  friend class KeyedLatestChannel;
};

/// @brief Independent cursor over the latest snapshots retained by a keyed channel.
template <class Key, class Value, std::size_t Capacity>
class KeyedLatestSubscriber {
 public:
  using message_type = KeyedLatestMessage<Key, Value>;

  KeyedLatestSubscriber() noexcept = default;

  /**
   * @brief Read one changed key as an owned copy without blocking.
   * @return The next snapshot, or an empty optional when no key changed.
   * @retval Error::BackPressured The shared table is temporarily locked.
   * @retval Error::BadConfig This is a default or moved-from endpoint.
   * @note Each key is returned at most once per observed version; intermediate updates are
   *       conflated. The scan starts after the previously returned slot and has no cross-key order.
   */
  Result<std::optional<KeyedLatestMessage<Key, Value>>> try_recv_checked() noexcept {
    if (!cursor_) {
      return std::unexpected(Error::BadConfig);
    }
    keyed_latest_detail::ObjectBytes<Key> key_bytes{};
    keyed_latest_detail::ObjectBytes<Value> value_bytes{};
    std::uint64_t version = 0;
    keyed_latest_detail::ReceiveContext<Key, Value> context{
        .cursor = cursor_.get(), .key = &key_bytes, .value = &value_bytes, .version = &version};
    const Error result = detail::with_keyed_latest_table(
        cursor_->storage, &keyed_latest_detail::receive<Key, Value>, &context);
    if (result != Error::Ok) {
      return std::unexpected(result);
    }
    if (!context.found) {
      return std::optional<KeyedLatestMessage<Key, Value>>{};
    }
    return std::optional<KeyedLatestMessage<Key, Value>>{
        std::in_place, KeyedLatestMessage<Key, Value>{std::bit_cast<Key>(key_bytes),
                                                      std::bit_cast<Value>(value_bytes), version}};
  }

  /** @brief Convenience receive; lock contention is reported as temporarily empty. */
  std::optional<KeyedLatestMessage<Key, Value>> try_recv() noexcept {
    auto received = try_recv_checked();
    return received ? std::move(*received) : std::nullopt;
  }

  /** @brief Spin/yield until one changed key is available. */
  KeyedLatestMessage<Key, Value> recv() noexcept {
    for (;;) {
      if (auto message = try_recv()) {
        return std::move(*message);
      }
      std::this_thread::yield();
    }
  }

  /** @return Updates overwritten before this endpoint observed them, saturating at UINT64_MAX. */
  std::uint64_t conflated_updates() const noexcept { return cursor_ ? cursor_->conflated : 0; }

  explicit operator bool() const noexcept { return static_cast<bool>(cursor_); }

 private:
  explicit KeyedLatestSubscriber(
      std::shared_ptr<keyed_latest_detail::Cursor<Key, Value>> cursor) noexcept
      : cursor_(std::move(cursor)) {}

  std::shared_ptr<keyed_latest_detail::Cursor<Key, Value>> cursor_;

  template <class, class, std::size_t>
  friend class KeyedLatestChannel;
};

/**
 * @brief Bounded cross-process latest-value table with exact-key conflation.
 * @tparam Key Trivially copyable, non-pointer key with unique object representations; exact bytes
 *         identify a key.
 * @tparam Value Trivially copyable, self-contained value. It and its members must not contain
 *         process-local pointers.
 * @tparam Capacity Maximum number of distinct keys retained by the channel.
 *
 * Each key owns one fixed slot. Repeated updates replace that key's current value and are
 * independently conflated for each subscriber. New subscribers receive one current snapshot per
 * existing key. A bounded event log avoids rescanning the full key table on every receive; lagging
 * subscribers rescan the current snapshot after event-log overrun. Updates to different keys have
 * no total ordering guarantee. A subscriber returns copied values, not borrowed shared-memory
 * views. When all slots contain distinct keys, a new key returns CapacityExceeded; updating an
 * existing key still succeeds. The shared table is protected by a crash-released OS file lock;
 * lock contention is reported as BackPressured.
 */
template <class Key, class Value, std::size_t Capacity>
class KeyedLatestChannel {
  static_assert(std::is_trivially_copyable_v<Key>, "Key must be trivially copyable");
  static_assert(!std::is_pointer_v<Key> && !std::is_member_pointer_v<Key>,
                "Key cannot be a process-local pointer");
  static_assert(std::has_unique_object_representations_v<Key>,
                "Key must have unique object representations");
  static_assert(std::is_trivially_copyable_v<Value>, "Value must be trivially copyable");
  static_assert(!std::is_pointer_v<Value> && !std::is_member_pointer_v<Value>,
                "Value cannot be a process-local pointer");
  static_assert(Capacity > 0, "KeyedLatestChannel capacity must be greater than zero");
  static_assert(Capacity <= std::numeric_limits<std::uint32_t>::max(),
                "KeyedLatestChannel capacity exceeds the supported slot count");

 public:
  using CreateResult = Result<KeyedLatestChannel>;
  using Publisher = KeyedLatestPublisher<Key, Value>;
  using Subscriber = KeyedLatestSubscriber<Key, Value, Capacity>;
  using Message = KeyedLatestMessage<Key, Value>;

  /** @brief Create and own a new named shared-memory keyed table. */
  static CreateResult create(std::string_view name) {
    auto storage = detail::create_keyed_latest_storage(name, Capacity, sizeof(Key), sizeof(Value),
                                                       keyed_latest_detail::type_schema<Key>(),
                                                       keyed_latest_detail::type_schema<Value>());
    if (!storage) {
      return std::unexpected(storage.error());
    }
    return KeyedLatestChannel(std::move(*storage));
  }

  /** @brief Connect to a table and verify capacity and key/value type schemas. */
  static CreateResult connect(std::string_view name) {
    auto storage = detail::connect_keyed_latest_storage(name, Capacity, sizeof(Key), sizeof(Value),
                                                        keyed_latest_detail::type_schema<Key>(),
                                                        keyed_latest_detail::type_schema<Value>());
    if (!storage) {
      return std::unexpected(storage.error());
    }
    return KeyedLatestChannel(std::move(*storage));
  }

  KeyedLatestChannel(KeyedLatestChannel&&) noexcept = default;
  KeyedLatestChannel& operator=(KeyedLatestChannel&&) noexcept = default;
  KeyedLatestChannel(const KeyedLatestChannel&) = default;
  KeyedLatestChannel& operator=(const KeyedLatestChannel&) = default;

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
  explicit KeyedLatestChannel(std::shared_ptr<detail::KeyedLatestStorage> storage) noexcept
      : storage_(std::move(storage)) {}

  Result<Subscriber> try_subscriber_impl(bool wait_for_lock) const {
    try {
      auto cursor = std::make_shared<keyed_latest_detail::Cursor<Key, Value>>(storage_, Capacity);
      keyed_latest_detail::InitializeCursorContext<Key, Value> context{.cursor = cursor.get()};
      const Error result = detail::with_keyed_latest_table(
          storage_, &keyed_latest_detail::initialize_cursor<Key, Value>, &context, wait_for_lock);
      if (result != Error::Ok) {
        return std::unexpected(result);
      }
      return Subscriber(std::move(cursor));
    } catch (const std::bad_alloc&) {
      return std::unexpected(Error::OutOfMemory);
    }
  }

  std::shared_ptr<detail::KeyedLatestStorage> storage_;
};

}  // namespace salias

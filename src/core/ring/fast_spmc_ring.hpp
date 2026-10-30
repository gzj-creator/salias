/**
 * @file src/core/ring/fast_spmc_ring.hpp
 * @brief 固定 payload、固定消费者数量的跨进程 SPMC 快速 ring。
 * @details producer 在 slot 内写 payload 和完成序号，每个 consumer 直接轮询自己的
 *          下一 slot。consumer 独立游标只用于可靠释放空间；producer 缓存最慢游标，
 *          仅在全环时刷新。任一慢 consumer 都会形成背压。
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <limits>
#include <span>

#include "core/ring/fast_ring_error.hpp"
#include "core/ring/fast_ring_layout.hpp"

namespace salias::ring {

namespace fast_spmc_detail {

inline constexpr std::uint64_t kMagic = 0x53414653504D4332ull;  // "SAFSPMC2"
inline constexpr std::uint32_t kVersion = 2;
inline constexpr std::size_t kCursorOffset = fast_ring_detail::kCacheLine;
inline constexpr std::size_t kSlotSequenceOffset = 0;
inline constexpr std::size_t kSlotPayloadOffset = sizeof(std::uint64_t);

constexpr std::size_t slot_stride(std::size_t payload_size) noexcept {
  return fast_ring_detail::align_up(kSlotPayloadOffset + payload_size, 64);
}

template <std::size_t PayloadSize, std::size_t Capacity, std::size_t ConsumerCount>
struct Layout {
  static constexpr std::size_t kSlotStride = slot_stride(PayloadSize);
  static constexpr std::size_t kDataOffset =
      kCursorOffset + ConsumerCount * fast_ring_detail::kCacheLine;
  static constexpr std::size_t kStorageSize = kDataOffset + kSlotStride * Capacity;

  static constexpr std::size_t cursor_offset(std::size_t consumer) noexcept {
    return kCursorOffset + consumer * fast_ring_detail::kCacheLine;
  }

  static constexpr std::size_t slot_offset(std::uint64_t position) noexcept {
    return kDataOffset + (position & (Capacity - 1)) * kSlotStride;
  }
};

}  // namespace fast_spmc_detail

/**
 * @brief 单生产者、固定数量消费者的跨进程广播 ring。
 * @tparam PayloadSize 固定 payload 字节数。
 * @tparam CapacitySlotCount 每环 slot 数，必须为大于等于 2 的 2 的幂。
 * @tparam ConsumerCount 固定消费者数量；每个消费者接收所有消息。
 */
template <std::size_t PayloadSize, std::size_t CapacitySlotCount, std::size_t ConsumerCount>
class FastSpmcRing {
  static_assert(PayloadSize > 0, "FastSpmcRing payload size must be greater than zero");
  static_assert(CapacitySlotCount >= 2, "FastSpmcRing requires at least two slots");
  static_assert(std::has_single_bit(CapacitySlotCount),
                "FastSpmcRing slot capacity must be a power of two");
  static_assert(ConsumerCount > 0, "FastSpmcRing requires at least one consumer");

 public:
  using Layout = fast_spmc_detail::Layout<PayloadSize, CapacitySlotCount, ConsumerCount>;
  using CreateResult = std::expected<FastSpmcRing, FastRingError>;

  static constexpr std::size_t payload_size() noexcept { return PayloadSize; }
  static constexpr std::size_t capacity_slots() noexcept { return CapacitySlotCount; }
  static constexpr std::size_t consumer_count() noexcept { return ConsumerCount; }
  static constexpr std::size_t required_storage_size() noexcept { return Layout::kStorageSize; }

  static CreateResult create(std::span<std::byte> storage) noexcept {
    if (storage.size() < required_storage_size()) {
      return std::unexpected(FastRingError::StorageTooSmall);
    }
    if (!fast_ring_detail::storage_aligned(storage)) {
      return std::unexpected(FastRingError::UnalignedStorage);
    }
    auto ready = fast_ring_detail::atomic_at(
        storage.data(), fast_ring_detail::kIdentityOffset + fast_ring_detail::kIdentityReadyOffset);
    if (ready.load(std::memory_order_acquire) == 1) {
      return std::unexpected(FastRingError::AlreadyInitialized);
    }

    std::memset(storage.data(), 0, required_storage_size());
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset) =
        fast_spmc_detail::kMagic;
    *reinterpret_cast<std::uint32_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 8) =
        fast_spmc_detail::kVersion;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 16) =
        PayloadSize;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 24) =
        CapacitySlotCount;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 40) =
        ConsumerCount;
    ready.store(1, std::memory_order_release);
    return FastSpmcRing(storage.data());
  }

  static CreateResult attach(std::span<std::byte> storage) noexcept {
    if (storage.size() < required_storage_size()) {
      return std::unexpected(FastRingError::StorageTooSmall);
    }
    if (!fast_ring_detail::storage_aligned(storage)) {
      return std::unexpected(FastRingError::UnalignedStorage);
    }
    auto ready = fast_ring_detail::atomic_at(
        storage.data(), fast_ring_detail::kIdentityOffset + fast_ring_detail::kIdentityReadyOffset);
    if (ready.load(std::memory_order_acquire) != 1) {
      return std::unexpected(FastRingError::IncompatibleLayout);
    }
    if (*reinterpret_cast<const std::uint64_t*>(
            storage.data() + fast_ring_detail::kIdentityOffset) != fast_spmc_detail::kMagic) {
      return std::unexpected(FastRingError::BadMagic);
    }
    if (*reinterpret_cast<const std::uint32_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                8) != fast_spmc_detail::kVersion ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                16) != PayloadSize ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                24) != CapacitySlotCount ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                40) != ConsumerCount) {
      return std::unexpected(FastRingError::IncompatibleLayout);
    }
    return FastSpmcRing(storage.data());
  }

  class Producer {
   public:
    using ClaimResult = std::expected<std::span<std::byte>, FastRingError>;

    explicit Producer(std::byte* storage) noexcept : storage_(storage) {
      slowest_cache_ = load_slowest();
    }

    ClaimResult try_claim() noexcept {
      if (claim_outstanding_) {
        return std::unexpected(FastRingError::ClaimOutstanding);
      }
      if (published_ - slowest_cache_ >= CapacitySlotCount) {
        slowest_cache_ = load_slowest();
        if (published_ - slowest_cache_ >= CapacitySlotCount) {
          return std::unexpected(FastRingError::BackPressured);
        }
      }
      const auto next_index = (published_ + 1) & (CapacitySlotCount - 1);
      __builtin_prefetch(storage_ + Layout::kDataOffset + next_index * Layout::kSlotStride, 1, 3);
      claim_outstanding_ = true;
      return std::span<std::byte>(
          storage_ + Layout::slot_offset(published_) + fast_spmc_detail::kSlotPayloadOffset,
          PayloadSize);
    }

    std::expected<void, FastRingError> commit() noexcept {
      if (!claim_outstanding_) {
        return std::unexpected(FastRingError::NoOutstandingClaim);
      }
      claim_outstanding_ = false;
      auto sequence = fast_ring_detail::atomic_at(
          storage_, Layout::slot_offset(published_) + fast_spmc_detail::kSlotSequenceOffset);
      const auto position = published_ + 1;
      sequence.store(position, std::memory_order_release);
      published_ = position;
      return {};
    }

    std::expected<void, FastRingError> try_push(std::span<const std::byte> payload) noexcept {
      if (payload.size() != PayloadSize) {
        return std::unexpected(FastRingError::PayloadSizeMismatch);
      }
      auto claim = try_claim();
      if (!claim) {
        return std::unexpected(claim.error());
      }
      std::memcpy(claim->data(), payload.data(), PayloadSize);
      return commit();
    }

    std::uint64_t published_position() const noexcept { return published_; }

   private:
    std::uint64_t load_slowest() const noexcept {
      auto slowest = std::numeric_limits<std::uint64_t>::max();
      for (std::size_t consumer = 0; consumer < ConsumerCount; ++consumer) {
        slowest =
            std::min(slowest, fast_ring_detail::atomic_at(storage_, Layout::cursor_offset(consumer))
                                  .load(std::memory_order_acquire));
      }
      return slowest;
    }

    std::byte* storage_;
    std::uint64_t published_ = 0;
    std::uint64_t slowest_cache_ = 0;
    bool claim_outstanding_ = false;
  };

  class Consumer {
   public:
    using ReadResult = std::expected<std::span<const std::byte>, FastRingError>;

    Consumer(std::byte* storage, std::size_t consumer_index) noexcept
        : storage_(storage), consumer_index_(consumer_index) {
      consumed_ = fast_ring_detail::atomic_at(storage_, Layout::cursor_offset(consumer_index_))
                      .load(std::memory_order_acquire);
    }

    ReadResult try_read() noexcept {
      if (read_outstanding_) {
        return std::unexpected(FastRingError::ReadOutstanding);
      }
      const auto sequence =
          fast_ring_detail::atomic_at(
              storage_, Layout::slot_offset(consumed_) + fast_spmc_detail::kSlotSequenceOffset)
              .load(std::memory_order_acquire);
      if (sequence != consumed_ + 1) {
        return std::unexpected(FastRingError::Empty);
      }
      read_outstanding_ = true;
      return std::span<const std::byte>(
          storage_ + Layout::slot_offset(consumed_) + fast_spmc_detail::kSlotPayloadOffset,
          PayloadSize);
    }

    std::expected<void, FastRingError> release(bool publish = true) noexcept {
      if (!read_outstanding_) {
        return std::unexpected(FastRingError::NoOutstandingRead);
      }
      read_outstanding_ = false;
      ++consumed_;
      dirty_ = true;
      if (publish) {
        publish_consumed();
      }
      return {};
    }

    std::expected<void, FastRingError> flush_release() noexcept {
      if (read_outstanding_) {
        return std::unexpected(FastRingError::ReadOutstanding);
      }
      publish_consumed();
      return {};
    }

    std::expected<void, FastRingError> try_pop(std::span<std::byte> destination) noexcept {
      if (destination.size() != PayloadSize) {
        return std::unexpected(FastRingError::PayloadSizeMismatch);
      }
      auto read = try_read();
      if (!read) {
        return std::unexpected(read.error());
      }
      std::memcpy(destination.data(), read->data(), PayloadSize);
      return release();
    }

    std::uint64_t consumed_position() const noexcept { return consumed_; }
    std::size_t consumer_index() const noexcept { return consumer_index_; }

   private:
    void publish_consumed() noexcept {
      fast_ring_detail::atomic_at(storage_, Layout::cursor_offset(consumer_index_))
          .store(consumed_, std::memory_order_release);
      dirty_ = false;
    }

    std::byte* storage_;
    std::size_t consumer_index_;
    std::uint64_t consumed_ = 0;
    bool read_outstanding_ = false;
    bool dirty_ = false;
  };

  Producer producer() noexcept { return Producer(storage_); }

  std::expected<Consumer, FastRingError> consumer(std::size_t consumer_index) noexcept {
    if (consumer_index >= ConsumerCount) {
      return std::unexpected(FastRingError::InvalidArgument);
    }
    return Consumer(storage_, consumer_index);
  }

 private:
  explicit FastSpmcRing(std::byte* storage) noexcept : storage_(storage) {}

  std::byte* storage_;
};

}  // namespace salias::ring

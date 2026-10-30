/**
 * @file src/core/ring/fast_mpmc_ring.hpp
 * @brief 固定 payload 的跨进程 MPMC fanout 快速 ring。
 * @details 组合每 producer 独立 lane 与每 consumer 独立游标。producer 之间不共享
 *          RMW；consumer 在各 lane 间轮询，每条消息广播给全部 consumer。
 */
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>

#include "core/ring/fast_ring_error.hpp"
#include "core/ring/fast_ring_layout.hpp"
#include "core/ring/fast_spmc_ring.hpp"

namespace salias::ring {

namespace fast_mpmc_detail {

inline constexpr std::uint64_t kMagic = 0x5341464D504D4331ull;  // "SAFMPMC1"
inline constexpr std::uint32_t kVersion = 1;
inline constexpr std::size_t kLaneOffset = fast_ring_detail::kCacheLine;

template <std::size_t SpmcStorageSize, std::size_t ProducerCount>
struct Layout {
  static constexpr std::size_t kStorageSize = kLaneOffset + ProducerCount * SpmcStorageSize;

  static constexpr std::size_t lane_offset(std::size_t producer) noexcept {
    return kLaneOffset + producer * SpmcStorageSize;
  }
};

}  // namespace fast_mpmc_detail

/**
 * @brief 多生产者、多消费者广播 ring 容器。
 * @tparam PayloadSize 每条消息的固定 payload 字节数。
 * @tparam CapacitySlotCount 每个 producer lane 的 slot 数。
 * @tparam ProducerCount 固定 producer 数量。
 * @tparam ConsumerCount 固定 consumer 数量；每个 consumer 接收所有 producer 的消息。
 */
template <std::size_t PayloadSize, std::size_t CapacitySlotCount, std::size_t ProducerCount,
          std::size_t ConsumerCount>
class FastMpmcRing {
  static_assert(ProducerCount > 0, "FastMpmcRing requires at least one producer");
  static_assert(ConsumerCount > 0, "FastMpmcRing requires at least one consumer");

 public:
  using Lane = FastSpmcRing<PayloadSize, CapacitySlotCount, ConsumerCount>;
  using Producer = typename Lane::Producer;
  using Layout = fast_mpmc_detail::Layout<Lane::required_storage_size(), ProducerCount>;
  using CreateResult = std::expected<FastMpmcRing, FastRingError>;

  static constexpr std::size_t payload_size() noexcept { return PayloadSize; }
  static constexpr std::size_t capacity_slots_per_producer() noexcept { return CapacitySlotCount; }
  static constexpr std::size_t producer_count() noexcept { return ProducerCount; }
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

    std::memset(storage.data(), 0, fast_mpmc_detail::kLaneOffset);
    for (std::size_t producer = 0; producer < ProducerCount; ++producer) {
      std::memset(lane_pointer(storage.data(), producer), 0, Lane::Layout::kDataOffset);
    }
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset) =
        fast_mpmc_detail::kMagic;
    *reinterpret_cast<std::uint32_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 8) =
        fast_mpmc_detail::kVersion;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 16) =
        PayloadSize;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 24) =
        CapacitySlotCount;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 40) =
        ProducerCount;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 48) =
        ConsumerCount;

    for (std::size_t producer = 0; producer < ProducerCount; ++producer) {
      auto created = Lane::create(std::span<std::byte>(lane_pointer(storage.data(), producer),
                                                       Lane::required_storage_size()));
      if (!created) {
        return std::unexpected(created.error());
      }
    }
    ready.store(1, std::memory_order_release);
    return FastMpmcRing(storage.data());
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
            storage.data() + fast_ring_detail::kIdentityOffset) != fast_mpmc_detail::kMagic) {
      return std::unexpected(FastRingError::BadMagic);
    }
    if (*reinterpret_cast<const std::uint32_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                8) != fast_mpmc_detail::kVersion ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                16) != PayloadSize ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                24) != CapacitySlotCount ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                40) != ProducerCount ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                48) != ConsumerCount) {
      return std::unexpected(FastRingError::IncompatibleLayout);
    }
    for (std::size_t producer = 0; producer < ProducerCount; ++producer) {
      auto attached = Lane::attach(std::span<std::byte>(lane_pointer(storage.data(), producer),
                                                        Lane::required_storage_size()));
      if (!attached) {
        return std::unexpected(attached.error());
      }
    }
    return FastMpmcRing(storage.data());
  }

  std::expected<Producer, FastRingError> producer(std::size_t producer_index) noexcept {
    if (producer_index >= ProducerCount) {
      return std::unexpected(FastRingError::InvalidArgument);
    }
    return Producer(lane_pointer(storage_, producer_index));
  }

  class Consumer {
   public:
    using ReadResult = std::expected<std::span<const std::byte>, FastRingError>;

    Consumer(std::byte* storage, std::size_t consumer_index) noexcept
        : storage_(storage), consumer_index_(consumer_index) {}

    ReadResult try_read() noexcept {
      if (active_lane_) {
        return std::unexpected(FastRingError::ReadOutstanding);
      }
      for (std::size_t scan = 0; scan < ProducerCount; ++scan) {
        const std::size_t producer = (next_lane_ + scan) % ProducerCount;
        const auto lane = lane_pointer(storage_, producer);
        const auto sequence =
            fast_ring_detail::atomic_at(lane, Lane::Layout::slot_offset(consumed_[producer]) +
                                                  fast_spmc_detail::kSlotSequenceOffset)
                .load(std::memory_order_acquire);
        if (sequence != consumed_[producer] + 1) {
          continue;
        }
        active_lane_ = producer;
        if (remaining_lane_batch_ == 0) {
          remaining_lane_batch_ = kLaneBatch;
        }
        return std::span<const std::byte>(lane + Lane::Layout::slot_offset(consumed_[producer]) +
                                              fast_spmc_detail::kSlotPayloadOffset,
                                          PayloadSize);
      }
      return std::unexpected(FastRingError::Empty);
    }

    std::expected<void, FastRingError> release(bool publish = true) noexcept {
      if (!active_lane_) {
        return std::unexpected(FastRingError::NoOutstandingRead);
      }
      const std::size_t producer = *active_lane_;
      ++consumed_[producer];
      dirty_[producer] = true;
      if (publish) {
        publish_consumed(producer);
      }
      active_lane_.reset();
      --remaining_lane_batch_;
      next_lane_ = remaining_lane_batch_ == 0 ? (producer + 1) % ProducerCount : producer;
      return {};
    }

    std::expected<void, FastRingError> flush_release() noexcept {
      if (active_lane_) {
        return std::unexpected(FastRingError::ReadOutstanding);
      }
      for (std::size_t producer = 0; producer < ProducerCount; ++producer) {
        if (dirty_[producer]) {
          publish_consumed(producer);
        }
      }
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

   private:
    void publish_consumed(std::size_t producer) noexcept {
      fast_ring_detail::atomic_at(lane_pointer(storage_, producer),
                                  Lane::Layout::cursor_offset(consumer_index_))
          .store(consumed_[producer], std::memory_order_release);
      dirty_[producer] = false;
    }

    std::byte* storage_;
    std::size_t consumer_index_;
    std::array<std::uint64_t, ProducerCount> consumed_{};
    std::array<bool, ProducerCount> dirty_{};
    std::optional<std::size_t> active_lane_;
    std::size_t next_lane_ = 0;
    std::size_t remaining_lane_batch_ = 0;

    // 限制单次停留的消息数，避免高流量 producer lane 长时间独占 MPMC consumer。
    static constexpr std::size_t kLaneBatch = CapacitySlotCount < 64 ? CapacitySlotCount : 64;
  };

  std::expected<Consumer, FastRingError> consumer(std::size_t consumer_index) noexcept {
    if (consumer_index >= ConsumerCount) {
      return std::unexpected(FastRingError::InvalidArgument);
    }
    return Consumer(storage_, consumer_index);
  }

 private:
  explicit FastMpmcRing(std::byte* storage) noexcept : storage_(storage) {}

  static std::byte* lane_pointer(std::byte* storage, std::size_t producer) noexcept {
    return storage + Layout::lane_offset(producer);
  }

  std::byte* storage_;
};

}  // namespace salias::ring

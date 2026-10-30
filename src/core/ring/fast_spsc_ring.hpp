/**
 * @file src/core/ring/fast_spsc_ring.hpp
 * @brief 固定 payload、固定槽容量的跨进程 SPSC 快速 ring。
 * @details 每个 slot 在 payload 前带一个完成序号。producer 写完整 payload 后，
 *          以 release-store 在原 slot 发布 position+1；consumer 对同一序号执行
 *          acquire-load。这样不维护逐消息反弹的全局 published cache line，也不扫描
 *          变长帧头。consumer 通过独立 consumed 游标释放空间，producer 仅在全环时刷新。
 *
 *          协议约束：一个 Producer 句柄只能由一个 producer 线程独占，一个 Consumer
 *          句柄只能由一个 consumer 线程独占；producer 句柄应在开始发布前创建。
 *          尚未 commit 的 claim 对 consumer 不可见；尚未 release 的 read 会阻止覆盖。
 */
#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <span>

#include "core/ring/fast_ring_error.hpp"
#include "core/ring/fast_ring_layout.hpp"

namespace salias::ring {

namespace fast_spsc_detail {

inline constexpr std::uint64_t kMagic = 0x5341465350534332ull;  // "SAFSPSC2"
inline constexpr std::uint32_t kVersion = 2;
inline constexpr std::size_t kConsumedOffset = fast_ring_detail::kCacheLine;
inline constexpr std::size_t kDataOffset = 2 * fast_ring_detail::kCacheLine;
inline constexpr std::size_t kSlotSequenceOffset = 0;
inline constexpr std::size_t kSlotPayloadOffset = sizeof(std::uint64_t);

constexpr std::size_t slot_stride(std::size_t payload_size) noexcept {
  return fast_ring_detail::align_up(kSlotPayloadOffset + payload_size, 64);
}

template <std::size_t PayloadSize, std::size_t Capacity>
struct Layout {
  static constexpr std::size_t kSlotStride = slot_stride(PayloadSize);
  static constexpr std::size_t kDataBytes = kSlotStride * Capacity;
  static constexpr std::size_t kStorageSize = kDataOffset + kDataBytes;

  static constexpr std::size_t slot_offset(std::uint64_t position) noexcept {
    return kDataOffset + (position & (Capacity - 1)) * kSlotStride;
  }
};

}  // namespace fast_spsc_detail

/**
 * @brief 固定大小 payload 的跨进程单生产者单消费者 ring 视图。
 * @tparam PayloadSize 每条消息 payload 字节数，必须大于零。
 * @tparam CapacitySlotCount slot 数量，必须为大于等于 2 的 2 的幂。
 */
template <std::size_t PayloadSize, std::size_t CapacitySlotCount>
class FastSpscRing {
  static_assert(PayloadSize > 0, "FastSpscRing payload size must be greater than zero");
  static_assert(CapacitySlotCount >= 2, "FastSpscRing requires at least two slots");
  static_assert(std::has_single_bit(CapacitySlotCount),
                "FastSpscRing slot capacity must be a power of two");

 public:
  using Layout = fast_spsc_detail::Layout<PayloadSize, CapacitySlotCount>;
  using CreateResult = std::expected<FastSpscRing, FastRingError>;

  static constexpr std::size_t payload_size() noexcept { return PayloadSize; }
  static constexpr std::size_t capacity_slots() noexcept { return CapacitySlotCount; }
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
        fast_spsc_detail::kMagic;
    *reinterpret_cast<std::uint32_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 8) =
        fast_spsc_detail::kVersion;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 16) =
        PayloadSize;
    *reinterpret_cast<std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset + 24) =
        CapacitySlotCount;
    ready.store(1, std::memory_order_release);
    return FastSpscRing(storage.data());
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
            storage.data() + fast_ring_detail::kIdentityOffset) != fast_spsc_detail::kMagic) {
      return std::unexpected(FastRingError::BadMagic);
    }
    if (*reinterpret_cast<const std::uint32_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                8) != fast_spsc_detail::kVersion ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                16) != PayloadSize ||
        *reinterpret_cast<const std::uint64_t*>(storage.data() + fast_ring_detail::kIdentityOffset +
                                                24) != CapacitySlotCount) {
      return std::unexpected(FastRingError::IncompatibleLayout);
    }
    return FastSpscRing(storage.data());
  }

  class Producer {
   public:
    using ClaimResult = std::expected<std::span<std::byte>, FastRingError>;

    explicit Producer(std::byte* storage) noexcept : storage_(storage) {
      consumed_cache_ = fast_ring_detail::atomic_at(storage_, fast_spsc_detail::kConsumedOffset)
                            .load(std::memory_order_acquire);
    }

    ClaimResult try_claim() noexcept {
      if (claim_outstanding_) {
        return std::unexpected(FastRingError::ClaimOutstanding);
      }
      if (published_ - consumed_cache_ >= CapacitySlotCount) {
        consumed_cache_ = fast_ring_detail::atomic_at(storage_, fast_spsc_detail::kConsumedOffset)
                              .load(std::memory_order_acquire);
        if (published_ - consumed_cache_ >= CapacitySlotCount) {
          return std::unexpected(FastRingError::BackPressured);
        }
      }
      const auto next_index = (published_ + 1) & (CapacitySlotCount - 1);
      __builtin_prefetch(
          storage_ + fast_spsc_detail::kDataOffset + next_index * Layout::kSlotStride, 1, 3);
      claim_outstanding_ = true;
      return std::span<std::byte>(
          storage_ + Layout::slot_offset(published_) + fast_spsc_detail::kSlotPayloadOffset,
          PayloadSize);
    }

    std::expected<void, FastRingError> commit() noexcept {
      if (!claim_outstanding_) {
        return std::unexpected(FastRingError::NoOutstandingClaim);
      }
      claim_outstanding_ = false;
      auto sequence = fast_ring_detail::atomic_at(
          storage_, Layout::slot_offset(published_) + fast_spsc_detail::kSlotSequenceOffset);
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
    std::byte* storage_;
    std::uint64_t published_ = 0;
    std::uint64_t consumed_cache_ = 0;
    bool claim_outstanding_ = false;
  };

  class Consumer {
   public:
    using ReadResult = std::expected<std::span<const std::byte>, FastRingError>;

    explicit Consumer(std::byte* storage) noexcept : storage_(storage) {
      consumed_ = fast_ring_detail::atomic_at(storage_, fast_spsc_detail::kConsumedOffset)
                      .load(std::memory_order_acquire);
    }

    ReadResult try_read() noexcept {
      if (read_outstanding_) {
        return std::unexpected(FastRingError::ReadOutstanding);
      }
      const auto sequence =
          fast_ring_detail::atomic_at(
              storage_, Layout::slot_offset(consumed_) + fast_spsc_detail::kSlotSequenceOffset)
              .load(std::memory_order_acquire);
      if (sequence != consumed_ + 1) {
        return std::unexpected(FastRingError::Empty);
      }
      read_outstanding_ = true;
      return std::span<const std::byte>(
          storage_ + Layout::slot_offset(consumed_) + fast_spsc_detail::kSlotPayloadOffset,
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

    /** @brief 批量消费时发布延迟的消费位置；发布前 producer 不会复用已读 slot。 */
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

   private:
    void publish_consumed() noexcept {
      fast_ring_detail::atomic_at(storage_, fast_spsc_detail::kConsumedOffset)
          .store(consumed_, std::memory_order_release);
      dirty_ = false;
    }

    std::byte* storage_;
    std::uint64_t consumed_ = 0;
    bool read_outstanding_ = false;
    bool dirty_ = false;
  };

  Producer producer() noexcept { return Producer(storage_); }
  Consumer consumer() noexcept { return Consumer(storage_); }

 private:
  explicit FastSpscRing(std::byte* storage) noexcept : storage_(storage) {}

  std::byte* storage_;
};

}  // namespace salias::ring

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <utility>

#include "core/channel/channel_config.hpp"
#include "core/channel/error.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/platform/mapping.hpp"
#include "core/platform/result.hpp"
#include "core/ring/cache_aligned.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

namespace detail {

inline std::uint32_t frame_generation(std::uint64_t position, std::size_t cap) noexcept {
  return static_cast<std::uint32_t>((position / cap) & 0x00FF'FFFFu);
}

inline std::uint32_t mpsc_meta(std::uint64_t position, std::size_t cap,
                               bool committed) noexcept {
  std::uint32_t flags = frame::FLAG_BEGIN | frame::FLAG_END;
  if (committed) {
    flags |= frame::FLAG_COMMITTED;
  }
  return (frame_generation(position, cap) << 8) | flags;
}

inline bool mpsc_has_capacity(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                              std::size_t need) noexcept {
  const std::uint64_t used = tail - head;
  return used <= capacity && need <= capacity - used;
}

inline void store_meta_release(ring::MagicRing& ring, std::uint64_t position,
                               std::uint32_t meta) noexcept {
  auto meta_dst = ring.slice_mut(position + sizeof(std::uint32_t), sizeof(std::uint32_t));
  auto* meta_ptr = reinterpret_cast<std::uint32_t*>(meta_dst.data());
  // SAFETY: every frame starts at an 8-byte aligned position, making the meta member 4-byte
  // aligned. All concurrent accesses to this cell use compatible atomic operations.
  std::atomic_ref<std::uint32_t>(*meta_ptr).store(meta, std::memory_order_release);
}

inline void write_uncommitted_header(ring::MagicRing& ring, std::uint64_t position,
                                     std::uint32_t payload_len, std::uint32_t meta) noexcept {
  auto len_dst = ring.slice_mut(position, sizeof(payload_len));
  std::memcpy(len_dst.data(), &payload_len, sizeof(payload_len));
  store_meta_release(ring, position, meta);
}

inline std::uint32_t load_meta_acquire(const ring::MagicRing& ring,
                                       std::uint64_t position) noexcept {
  auto meta_src = ring.slice(position + sizeof(std::uint32_t), sizeof(std::uint32_t));
  auto* meta_ptr =
      const_cast<std::uint32_t*>(reinterpret_cast<const std::uint32_t*>(meta_src.data()));
  // SAFETY: producers publish the same 4-byte meta cell with release semantics in
  // store_meta_release(). This acquire load observes the preceding payload and len writes.
  return std::atomic_ref<std::uint32_t>(*meta_ptr).load(std::memory_order_acquire);
}

inline std::uint32_t load_len_after_commit(const ring::MagicRing& ring,
                                           std::uint64_t position) noexcept {
  std::uint32_t len = 0;
  auto len_src = ring.slice(position, sizeof(len));
  std::memcpy(&len, len_src.data(), sizeof(len));
  return len;
}

}  // namespace detail

template <wait::WaitStrategy Wait = wait::SpinPause>
// Salias-native shared-memory MPSC channel. Producers reserve disjoint regions in the mmap-backed
// MagicRing, so process-local heap queues such as concurrentqueue are not a drop-in substitute.
class MpscChannel {
 public:
  using CreateResult = platform::Result<MpscChannel, ChannelError>;
  using OfferResult = platform::Result<bool, flow::FlowError>;

  class Tx;
  class Rx;

  static CreateResult create(const ChannelConfig& config) noexcept {
    if (config.capacity == 0 || config.fixed_size) {
      return CreateResult::failure(ChannelError::BadConfig);
    }

    auto mapping = platform::Mapping::create(platform::MapOptions{
        .size = config.capacity,
        .huge = config.huge,
        .numa_node = config.numa_node,
    });
    if (!mapping) {
      return CreateResult::failure(ChannelError::PlatformFail);
    }

    auto ring = ring::MagicRing::create(std::move(mapping).value());
    if (!ring) {
      return CreateResult::failure(ChannelError::RingFail);
    }

    return CreateResult::success(MpscChannel(std::move(ring).value(), Wait{}));
  }

  MpscChannel(MpscChannel&&) noexcept = default;
  MpscChannel& operator=(MpscChannel&&) noexcept = default;
  MpscChannel(const MpscChannel&) = delete;
  MpscChannel& operator=(const MpscChannel&) = delete;

  std::size_t capacity() const noexcept { return ring_.capacity(); }

  Tx tx() noexcept { return Tx(*this); }
  Rx rx() noexcept { return Rx(*this); }

 private:
  explicit MpscChannel(ring::MagicRing ring, Wait wait) noexcept
      : ring_(std::move(ring)), wait_(std::move(wait)) {}

  ring::MagicRing ring_;
  ring::CacheAligned<std::uint64_t> reserved_tail_{};
  ring::CacheAligned<std::uint64_t> consumer_pos_{};
  std::uint32_t wait_word_ = 0;
  Wait wait_;

 public:
  // Multi-producer sending endpoint. Each Tx instance may be owned by one producer thread; separate
  // Tx instances coordinate through the parent channel's CAS-reserved tail.
  class Tx {
   public:
    explicit Tx(MpscChannel& channel) noexcept : channel_(&channel) {}

    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || channel_->capacity() == 0) {
        return flow::Producer::ClaimResult::failure(flow::FlowError::MessageTooLarge);
      }

      const std::size_t need = frame::frame_len(payload_len);
      if (need > channel_->capacity()) {
        return flow::Producer::ClaimResult::failure(flow::FlowError::MessageTooLarge);
      }

      auto reserved_tail = std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
                               .load(std::memory_order_acquire);
      for (;;) {
        if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_head_, need)) {
          // SAFETY: the single consumer release-stores consumer_pos_ in advance(). Loading it with
          // acquire prevents any producer from reusing bytes before the consumer has released them.
          cached_head_ = std::atomic_ref<std::uint64_t>(channel_->consumer_pos_.value)
                             .load(std::memory_order_acquire);
          if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_head_, need)) {
            return flow::Producer::ClaimResult::failure(flow::FlowError::BackPressured);
          }
        }

        const std::uint64_t next_tail = reserved_tail + need;
        // SAFETY: a successful CAS grants this Tx exclusive ownership of [reserved_tail,next_tail).
        // Other producers can only reserve disjoint ranges by advancing reserved_tail_ further.
        if (std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
                .compare_exchange_weak(reserved_tail, next_tail, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
          const std::uint32_t meta =
              detail::mpsc_meta(reserved_tail, channel_->capacity(), false);
          detail::write_uncommitted_header(channel_->ring_, reserved_tail, payload_len, meta);
          return flow::Producer::ClaimResult::success(flow::Claim{
              .payload = channel_->ring_.slice_mut(reserved_tail + frame::kHeaderSize, payload_len),
              .start_pos = reserved_tail,
              .payload_len = payload_len,
              .meta = meta,
          });
        }
      }
    }

    void commit(const flow::Claim& claim) noexcept {
      const std::uint32_t committed_meta = claim.meta | frame::FLAG_COMMITTED;
      detail::store_meta_release(channel_->ring_, claim.start_pos, committed_meta);
      std::atomic_ref<std::uint32_t>(channel_->wait_word_)
          .fetch_add(1, std::memory_order_release);
      channel_->wait_.wake(&channel_->wait_word_);
    }

    OfferResult offer(std::span<const std::byte> payload) noexcept {
      auto claim_result = claim(static_cast<std::uint32_t>(payload.size()));
      if (!claim_result) {
        return OfferResult::failure(claim_result.error());
      }
      std::memcpy(claim_result.value().payload.data(), payload.data(), payload.size());
      commit(claim_result.value());
      return OfferResult::success(true);
    }

   private:
    MpscChannel* channel_;
    std::uint64_t cached_head_ = 0;
  };

  // Single-consumer endpoint. It observes frame commit flags in position order and never advances
  // past a claimed-but-uncommitted gap.
  class Rx {
   public:
    explicit Rx(MpscChannel& channel) noexcept : channel_(&channel) {}

    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || channel_->capacity() == 0) {
        return std::nullopt;
      }

      const std::uint64_t head = channel_->consumer_pos_.value;
      if (head >= cached_tail_) {
        cached_tail_ = std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
                           .load(std::memory_order_acquire);
        if (head == cached_tail_) {
          return std::nullopt;
        }
      }

      const std::uint32_t meta = detail::load_meta_acquire(channel_->ring_, head);
      const auto flags = meta & 0xFFu;
      if ((flags & frame::FLAG_COMMITTED) == 0 ||
          (meta >> 8) != detail::frame_generation(head, channel_->capacity())) {
        return std::nullopt;
      }

      const std::uint32_t payload_len = detail::load_len_after_commit(channel_->ring_, head);
      const std::uint64_t next = head + frame::frame_len(payload_len);
      if (frame::frame_len(payload_len) > channel_->capacity() || next > cached_tail_) {
        return std::nullopt;
      }

      return flow::Message{
          .payload = channel_->ring_.slice(head + frame::kHeaderSize, payload_len),
          .position = head,
          .next_position = next,
          .meta = meta,
      };
    }

    flow::Message recv() noexcept {
      for (;;) {
        if (auto message = try_recv(); message.has_value()) {
          return *message;
        }
        const auto expected =
            std::atomic_ref<std::uint32_t>(channel_->wait_word_).load(std::memory_order_acquire);
        channel_->wait_.wait(&channel_->wait_word_, expected);
      }
    }

    void release(const flow::Message& message) noexcept { advance(message.next_position); }

   private:
    void advance(std::uint64_t new_head) noexcept {
      // SAFETY: release publishes consumer progress to all producer Tx instances. Producers
      // acquire-load this value before reusing the corresponding ring bytes.
      std::atomic_ref<std::uint64_t>(channel_->consumer_pos_.value)
          .store(new_head, std::memory_order_release);
    }

    MpscChannel* channel_;
    std::uint64_t cached_tail_ = 0;
  };
};

}  // namespace salias::channel

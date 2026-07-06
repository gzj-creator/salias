#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
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

inline std::uint32_t broadcast_meta(std::uint64_t start_pos, std::size_t cap) noexcept {
  const auto generation = static_cast<std::uint32_t>((start_pos / cap) & 0x00FF'FFFFu);
  return (generation << 8) | frame::FLAG_BEGIN | frame::FLAG_END | frame::FLAG_COMMITTED;
}

inline bool broadcast_has_capacity(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                                   std::size_t need) noexcept {
  const std::uint64_t used = tail - head;
  return used <= capacity && need <= capacity - used;
}

}  // namespace detail

template <wait::WaitStrategy Wait = wait::SpinPause, std::size_t MaxSubscribers = 8>
class BroadcastChannel {
 public:
  static constexpr std::uint32_t kInvalidSubscriber =
      static_cast<std::uint32_t>(MaxSubscribers);

  using CreateResult = platform::Result<BroadcastChannel, ChannelError>;
  using OfferResult = platform::Result<bool, flow::FlowError>;

  class Tx;
  class Rx;

  static CreateResult create(const ChannelConfig& config) noexcept {
    if (config.capacity == 0 || config.fixed_size || MaxSubscribers == 0) {
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

    return CreateResult::success(BroadcastChannel(std::move(ring).value(), Wait{}));
  }

  BroadcastChannel(BroadcastChannel&&) noexcept = default;
  BroadcastChannel& operator=(BroadcastChannel&&) noexcept = default;
  BroadcastChannel(const BroadcastChannel&) = delete;
  BroadcastChannel& operator=(const BroadcastChannel&) = delete;

  std::size_t capacity() const noexcept { return ring_.capacity(); }

  Tx tx() noexcept { return Tx(*this); }

  std::optional<std::uint32_t> subscribe_index() noexcept {
    const auto count =
        std::atomic_ref<std::uint32_t>(subscriber_count_).load(std::memory_order_relaxed);
    if (count >= MaxSubscribers) {
      return std::nullopt;
    }

    const auto tail =
        std::atomic_ref<std::uint64_t>(producer_pos_.value).load(std::memory_order_acquire);
    std::atomic_ref<std::uint64_t>(subscriber_heads_[count].value)
        .store(tail, std::memory_order_release);
    std::atomic_ref<std::uint32_t>(subscriber_count_).store(count + 1, std::memory_order_release);
    return count;
  }

  Rx subscribe() noexcept {
    auto index = subscribe_index();
    if (!index) {
      return Rx();
    }
    return Rx(*this, *index);
  }

  Rx rx(std::uint32_t index) noexcept { return Rx(*this, index); }

 private:
  explicit BroadcastChannel(ring::MagicRing ring, Wait wait) noexcept
      : ring_(std::move(ring)), wait_(std::move(wait)) {}

  std::uint64_t min_consumer_head(std::uint64_t tail) const noexcept {
    const auto count =
        std::atomic_ref<const std::uint32_t>(subscriber_count_).load(std::memory_order_acquire);
    if (count == 0) {
      return tail;
    }

    std::uint64_t min_head = std::numeric_limits<std::uint64_t>::max();
    for (std::uint32_t i = 0; i < count && i < MaxSubscribers; ++i) {
      // SAFETY: each subscriber head is advanced with release semantics by exactly one Rx.
      // Acquiring all active heads makes the producer's overwrite boundary the slowest subscriber.
      const auto head = std::atomic_ref<const std::uint64_t>(subscriber_heads_[i].value)
                            .load(std::memory_order_acquire);
      min_head = head < min_head ? head : min_head;
    }
    return min_head == std::numeric_limits<std::uint64_t>::max() ? tail : min_head;
  }

  ring::MagicRing ring_;
  ring::CacheAligned<std::uint64_t> producer_pos_{};
  std::array<ring::CacheAligned<std::uint64_t>, MaxSubscribers> subscriber_heads_{};
  std::uint32_t subscriber_count_ = 0;
  std::uint32_t wait_word_ = 0;
  Wait wait_;

 public:
  // Single broadcast producer. It writes one physical frame and relies on the slowest subscriber
  // head to decide when ring space can be reused.
  class Tx {
   public:
    explicit Tx(BroadcastChannel& channel) noexcept : channel_(&channel) {}

    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || channel_->capacity() == 0) {
        return flow::Producer::ClaimResult::failure(flow::FlowError::MessageTooLarge);
      }

      const std::size_t need = frame::frame_len(payload_len);
      if (need > channel_->capacity()) {
        return flow::Producer::ClaimResult::failure(flow::FlowError::MessageTooLarge);
      }

      const std::uint64_t tail = channel_->producer_pos_.value;
      if (!detail::broadcast_has_capacity(channel_->capacity(), tail, cached_min_head_, need)) {
        cached_min_head_ = channel_->min_consumer_head(tail);
        if (!detail::broadcast_has_capacity(channel_->capacity(), tail, cached_min_head_, need)) {
          return flow::Producer::ClaimResult::failure(flow::FlowError::BackPressured);
        }
      }

      return flow::Producer::ClaimResult::success(flow::Claim{
          .payload = channel_->ring_.slice_mut(tail + frame::kHeaderSize, payload_len),
          .start_pos = tail,
          .payload_len = payload_len,
          .meta = detail::broadcast_meta(tail, channel_->capacity()),
      });
    }

    void commit(const flow::Claim& claim) noexcept {
      std::array<std::byte, frame::kHeaderSize> header{};
      frame::encode_header(header, claim.payload_len, claim.meta);
      auto header_dst = channel_->ring_.slice_mut(claim.start_pos, frame::kHeaderSize);
      std::memcpy(header_dst.data(), header.data(), header.size());

      // SAFETY: one producer owns producer_pos_. Header and payload bytes are complete before this
      // release-store; every subscriber acquire-loads producer_pos_ before reading the frame.
      std::atomic_ref<std::uint64_t>(channel_->producer_pos_.value)
          .store(claim.start_pos + frame::frame_len(claim.payload_len), std::memory_order_release);

      std::atomic_ref<std::uint32_t>(channel_->wait_word_)
          .fetch_add(1, std::memory_order_release);
      const auto count = std::atomic_ref<std::uint32_t>(channel_->subscriber_count_)
                             .load(std::memory_order_acquire);
      for (std::uint32_t i = 0; i < count && i < MaxSubscribers; ++i) {
        channel_->wait_.wake(&channel_->wait_word_);
      }
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
    BroadcastChannel* channel_;
    std::uint64_t cached_min_head_ = 0;
  };

  // Broadcast subscriber. Each Rx has a distinct head cell and therefore consumes independently.
  class Rx {
   public:
    Rx() noexcept = default;
    Rx(BroadcastChannel& channel, std::uint32_t index) noexcept
        : channel_(&channel), index_(index) {}

    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || index_ >= MaxSubscribers) {
        return std::nullopt;
      }

      const std::uint64_t head = channel_->subscriber_heads_[index_].value;
      if (head >= cached_tail_) {
        // SAFETY: the producer release-stores producer_pos_ after writing the frame. This acquire
        // load makes the frame bytes visible before header decoding and payload reads.
        cached_tail_ = std::atomic_ref<std::uint64_t>(channel_->producer_pos_.value)
                           .load(std::memory_order_acquire);
        if (head == cached_tail_) {
          return std::nullopt;
        }
      }

      auto header_src = channel_->ring_.slice(head, frame::kHeaderSize);
      std::array<std::byte, frame::kHeaderSize> header_bytes{};
      std::memcpy(header_bytes.data(), header_src.data(), header_bytes.size());
      const frame::FrameHeader header = frame::decode_header(header_bytes);
      const std::uint64_t next = head + frame::frame_len(header.len);

      return flow::Message{
          .payload = channel_->ring_.slice(head + frame::kHeaderSize, header.len),
          .position = head,
          .next_position = next,
          .meta = header.meta,
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

    void release(const flow::Message& message) noexcept {
      if (channel_ == nullptr || index_ >= MaxSubscribers) {
        return;
      }
      // SAFETY: exactly one Rx owns this subscriber head. The producer acquire-loads all heads when
      // calculating the reliable broadcast overwrite boundary.
      std::atomic_ref<std::uint64_t>(channel_->subscriber_heads_[index_].value)
          .store(message.next_position, std::memory_order_release);
    }

   private:
    BroadcastChannel* channel_ = nullptr;
    std::uint32_t index_ = kInvalidSubscriber;
    std::uint64_t cached_tail_ = 0;
  };
};

}  // namespace salias::channel

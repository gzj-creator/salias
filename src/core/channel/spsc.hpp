#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>

#include "core/channel/channel_config.hpp"
#include "core/channel/error.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/platform/mapping.hpp"
#include "core/platform/result.hpp"
#include "core/ring/cache_aligned.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <wait::WaitStrategy Wait = wait::SpinPause>
class SpscChannel {
 public:
  using CreateResult = platform::Result<SpscChannel, ChannelError>;
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

    return CreateResult::success(SpscChannel(std::move(ring).value(), Wait{}));
  }

  SpscChannel(SpscChannel&&) noexcept = default;
  SpscChannel& operator=(SpscChannel&&) noexcept = default;
  SpscChannel(const SpscChannel&) = delete;
  SpscChannel& operator=(const SpscChannel&) = delete;

  std::size_t capacity() const noexcept { return ring_.capacity(); }

  Tx tx() noexcept { return Tx(*this); }
  Rx rx() noexcept { return Rx(*this); }

 private:
  explicit SpscChannel(ring::MagicRing ring, Wait wait) noexcept
      : ring_(std::move(ring)), wait_(std::move(wait)) {}

  flow::Positions positions() noexcept {
    return flow::Positions{
        .producer = &producer_pos_.value,
        .consumer = &consumer_pos_.value,
        .cap = ring_.capacity(),
    };
  }

  ring::MagicRing ring_;
  ring::CacheAligned<std::uint64_t> producer_pos_{};
  ring::CacheAligned<std::uint64_t> consumer_pos_{};
  std::uint32_t wait_word_ = 0;
  Wait wait_;

 public:
  // Sending endpoint. It borrows the parent channel; callers must ensure the channel outlives Tx.
  // Each Tx instance is single-producer state and must not be used concurrently by multiple writers.
  class Tx {
   public:
    explicit Tx(SpscChannel& channel) noexcept
        : channel_(&channel), producer_(channel.ring_, channel.positions()) {}

    flow::Producer::ClaimResult claim(std::uint32_t len) noexcept { return producer_.claim(len); }

    void commit(const flow::Claim& claim) noexcept {
      producer_.commit(claim);
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
    SpscChannel* channel_;
    flow::Producer producer_;
  };

  // Receiving endpoint. It borrows the parent channel; callers must ensure the channel outlives Rx.
  // Each Rx instance is single-consumer state and must not be used concurrently by multiple readers.
  class Rx {
   public:
    explicit Rx(SpscChannel& channel) noexcept
        : channel_(&channel), consumer_(channel.ring_, channel.positions()) {}

    std::optional<flow::Message> try_recv() noexcept { return consumer_.poll(); }

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
      consumer_.advance(message.next_position);
    }

   private:
    SpscChannel* channel_;
    flow::Consumer consumer_;
  };
};

}  // namespace salias::channel

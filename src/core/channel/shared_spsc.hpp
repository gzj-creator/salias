#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <utility>

#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/position.hpp"
#include "core/flow/producer.hpp"
#include "core/platform/result.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <wait::WaitStrategy Wait = wait::SpinPause>
class SharedSpscChannel {
 public:
  using OfferResult = platform::Result<bool, flow::FlowError>;

  class Tx;
  class Rx;

  SharedSpscChannel(ring::MagicRing ring, flow::Positions positions,
                    std::uint32_t* wait_word, Wait wait = Wait{}) noexcept
      : ring_(std::move(ring)),
        positions_(positions),
        wait_word_(wait_word),
        wait_(std::move(wait)) {}

  SharedSpscChannel(SharedSpscChannel&&) noexcept = default;
  SharedSpscChannel& operator=(SharedSpscChannel&&) noexcept = default;
  SharedSpscChannel(const SharedSpscChannel&) = delete;
  SharedSpscChannel& operator=(const SharedSpscChannel&) = delete;

  std::size_t capacity() const noexcept { return ring_.capacity(); }

  Tx tx() noexcept { return Tx(*this); }
  Rx rx() noexcept { return Rx(*this); }

 private:
  ring::MagicRing ring_;
  flow::Positions positions_;
  std::uint32_t* wait_word_ = nullptr;
  Wait wait_;

 public:
  // Sending endpoint over externally-owned shared position cells. The parent channel owns the ring
  // mapping; the control block containing positions and wait word must outlive Tx.
  class Tx {
   public:
    explicit Tx(SharedSpscChannel& channel) noexcept
        : channel_(&channel), producer_(channel.ring_, channel.positions_) {}

    flow::Producer::ClaimResult claim(std::uint32_t len) noexcept { return producer_.claim(len); }

    void commit(const flow::Claim& claim) noexcept {
      producer_.commit(claim);
      if (channel_->wait_word_ != nullptr) {
        std::atomic_ref<std::uint32_t>(*channel_->wait_word_)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(channel_->wait_word_);
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
    SharedSpscChannel* channel_;
    flow::Producer producer_;
  };

  // Receiving endpoint over externally-owned shared position cells. It has the same single-consumer
  // contract as SpscChannel::Rx, but the position cells can live in another process-visible mapping.
  class Rx {
   public:
    explicit Rx(SharedSpscChannel& channel) noexcept
        : channel_(&channel), consumer_(channel.ring_, channel.positions_) {}

    std::optional<flow::Message> try_recv() noexcept { return consumer_.poll(); }

    flow::Message recv() noexcept {
      for (;;) {
        if (auto message = try_recv(); message.has_value()) {
          return *message;
        }
        if (channel_->wait_word_ == nullptr) {
          continue;
        }
        const auto expected =
            std::atomic_ref<std::uint32_t>(*channel_->wait_word_).load(std::memory_order_acquire);
        channel_->wait_.wait(channel_->wait_word_, expected);
      }
    }

    void release(const flow::Message& message) noexcept {
      consumer_.advance(message.next_position);
    }

   private:
    SharedSpscChannel* channel_;
    flow::Consumer consumer_;
  };
};

}  // namespace salias::channel

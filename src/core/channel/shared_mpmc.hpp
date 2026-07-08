#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <limits>
#include <optional>
#include <span>
#include <utility>

#include "core/channel/mpsc.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/header.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <wait::WaitStrategy Wait = wait::SpinPause, std::size_t MaxSubscribers = 8>
class SharedMpmcChannel {
  static_assert(MaxSubscribers > 0);

 public:
  static constexpr std::uint32_t kInvalidSubscriber =
      static_cast<std::uint32_t>(MaxSubscribers);

  using SubscriberHeads = std::array<std::uint64_t*, MaxSubscribers>;
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // Wrap a shared fanout ring. The external control block owns reserved_tail,
  // subscriber_count, subscriber_heads, and wait_word for the lifetime of this object.
  SharedMpmcChannel(ring::MagicRing ring, std::uint64_t* reserved_tail,
                    std::uint32_t* subscriber_count, SubscriberHeads subscriber_heads,
                    std::uint32_t* wait_word, Wait wait = Wait{}) noexcept
      : ring_(std::move(ring)),
        reserved_tail_(reserved_tail),
        subscriber_count_(subscriber_count),
        subscriber_heads_(subscriber_heads),
        wait_word_(wait_word),
        wait_(std::move(wait)) {}

  SharedMpmcChannel(SharedMpmcChannel&&) noexcept = default;
  SharedMpmcChannel& operator=(SharedMpmcChannel&&) noexcept = default;
  SharedMpmcChannel(const SharedMpmcChannel&) = delete;
  SharedMpmcChannel& operator=(const SharedMpmcChannel&) = delete;

  std::size_t capacity() const noexcept { return ring_.capacity(); }

  Tx tx() noexcept { return Tx(*this); }

  std::optional<std::uint32_t> subscribe_index() noexcept {
    if (reserved_tail_ == nullptr || subscriber_count_ == nullptr) {
      return std::nullopt;
    }
    for (auto* head : subscriber_heads_) {
      if (head == nullptr) {
        return std::nullopt;
      }
    }

    auto count_ref = std::atomic_ref<std::uint32_t>(*subscriber_count_);
    auto count = count_ref.load(std::memory_order_acquire);
    for (;;) {
      if (count >= MaxSubscribers) {
        return std::nullopt;
      }
      if (count_ref.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
        break;
      }
    }

    const auto tail =
        std::atomic_ref<std::uint64_t>(*reserved_tail_).load(std::memory_order_acquire);
    std::atomic_ref<std::uint64_t>(*subscriber_heads_[count])
        .store(tail, std::memory_order_release);
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
  std::uint64_t min_subscriber_head(std::uint64_t tail) const noexcept {
    if (subscriber_count_ == nullptr) {
      return tail;
    }

    const auto count =
        std::atomic_ref<std::uint32_t>(*subscriber_count_).load(std::memory_order_acquire);
    if (count == 0) {
      return tail;
    }

    std::uint64_t min_head = std::numeric_limits<std::uint64_t>::max();
    const auto bounded_count = count < MaxSubscribers ? count : MaxSubscribers;
    for (std::uint32_t i = 0; i < bounded_count; ++i) {
      auto* head = subscriber_heads_[i];
      if (head == nullptr) {
        continue;
      }
      const auto value = std::atomic_ref<std::uint64_t>(*head).load(std::memory_order_acquire);
      min_head = value < min_head ? value : min_head;
    }
    return min_head == std::numeric_limits<std::uint64_t>::max() ? tail : min_head;
  }

  ring::MagicRing ring_;
  std::uint64_t* reserved_tail_ = nullptr;
  std::uint32_t* subscriber_count_ = nullptr;
  SubscriberHeads subscriber_heads_{};
  std::uint32_t* wait_word_ = nullptr;
  Wait wait_;

 public:
  // Multi-producer endpoint. Producers CAS a shared reservation tail, write an
  // uncommitted frame, then release-store the committed metadata word.
  class Tx {
   public:
    explicit Tx(SharedMpmcChannel& channel) noexcept : channel_(&channel) {}

    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || channel_->capacity() == 0 ||
          channel_->reserved_tail_ == nullptr) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::size_t need = frame::frame_len(payload_len);
      if (need > channel_->capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto reserved_tail =
          std::atomic_ref<std::uint64_t>(*channel_->reserved_tail_).load(std::memory_order_acquire);
      for (;;) {
        if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_min_head_,
                                       need)) {
          cached_min_head_ = channel_->min_subscriber_head(reserved_tail);
          if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_min_head_,
                                         need)) {
            return std::unexpected(flow::FlowError::BackPressured);
          }
        }

        const std::uint64_t next_tail = reserved_tail + need;
        if (std::atomic_ref<std::uint64_t>(*channel_->reserved_tail_)
                .compare_exchange_weak(reserved_tail, next_tail, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
          const std::uint32_t meta =
              detail::mpsc_meta(reserved_tail, channel_->capacity(), false);
          detail::write_uncommitted_header(channel_->ring_, reserved_tail, payload_len, meta);
          return flow::Claim{
              .payload = channel_->ring_.slice_mut(reserved_tail + frame::kHeaderSize, payload_len),
              .start_pos = reserved_tail,
              .payload_len = payload_len,
              .meta = meta,
          };
        }
      }
    }

    void commit(const flow::Claim& claim) noexcept {
      const std::uint32_t committed_meta = claim.meta | frame::FLAG_COMMITTED;
      detail::store_meta_release(channel_->ring_, claim.start_pos, committed_meta);
      if (channel_->wait_word_ != nullptr) {
        std::atomic_ref<std::uint32_t>(*channel_->wait_word_)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(channel_->wait_word_);
      }
    }

    OfferResult offer(std::span<const std::byte> payload) noexcept {
      auto claim_result = claim(static_cast<std::uint32_t>(payload.size()));
      if (!claim_result) {
        return std::unexpected(claim_result.error());
      }
      std::memcpy(claim_result.value().payload.data(), payload.data(), payload.size());
      commit(claim_result.value());
      return true;
    }

   private:
    SharedMpmcChannel* channel_;
    std::uint64_t cached_min_head_ = 0;
  };

  // Fanout subscriber endpoint. Each subscriber owns one shared head slot and
  // reads the same committed physical frames independently.
  class Rx {
   public:
    Rx() noexcept = default;
    Rx(SharedMpmcChannel& channel, std::uint32_t index) noexcept
        : channel_(&channel), index_(index) {}

    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || index_ >= MaxSubscribers ||
          channel_->reserved_tail_ == nullptr || channel_->subscriber_heads_[index_] == nullptr) {
        return std::nullopt;
      }

      auto* head_ptr = channel_->subscriber_heads_[index_];
      const std::uint64_t head = *head_ptr;
      if (head >= cached_tail_) {
        cached_tail_ =
            std::atomic_ref<std::uint64_t>(*channel_->reserved_tail_).load(std::memory_order_acquire);
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
        if (channel_ == nullptr || channel_->wait_word_ == nullptr) {
          continue;
        }
        const auto expected =
            std::atomic_ref<std::uint32_t>(*channel_->wait_word_).load(std::memory_order_acquire);
        channel_->wait_.wait(channel_->wait_word_, expected);
      }
    }

    void release(const flow::Message& message) noexcept {
      if (channel_ == nullptr || index_ >= MaxSubscribers ||
          channel_->subscriber_heads_[index_] == nullptr) {
        return;
      }
      std::atomic_ref<std::uint64_t>(*channel_->subscriber_heads_[index_])
          .store(message.next_position, std::memory_order_release);
    }

   private:
    SharedMpmcChannel* channel_ = nullptr;
    std::uint32_t index_ = kInvalidSubscriber;
    std::uint64_t cached_tail_ = 0;
  };
};

}  // namespace salias::channel

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <utility>

#include "core/channel/mpsc.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/codec.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <wait::WaitStrategy Wait = wait::SpinPause>
class SharedMpscChannel {
 public:
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // 封装共享 MPSC ring 和外部控制块中的 reserved tail / consumer head。
  SharedMpscChannel(ring::MagicRing ring, std::uint64_t* reserved_tail,
                    std::uint64_t* consumer_pos, std::uint32_t* wait_word,
                    Wait wait = Wait{}) noexcept
      : ring_(std::move(ring)),
        reserved_tail_(reserved_tail),
        consumer_pos_(consumer_pos),
        wait_word_(wait_word),
        wait_(std::move(wait)) {}

  SharedMpscChannel(SharedMpscChannel&&) noexcept = default;
  SharedMpscChannel& operator=(SharedMpscChannel&&) noexcept = default;
  SharedMpscChannel(const SharedMpscChannel&) = delete;
  SharedMpscChannel& operator=(const SharedMpscChannel&) = delete;

  std::size_t capacity() const noexcept { return ring_.capacity(); }

  Tx tx() noexcept { return Tx(*this); }
  Rx rx() noexcept { return Rx(*this); }

 private:
  ring::MagicRing ring_;
  std::uint64_t* reserved_tail_ = nullptr;
  std::uint64_t* consumer_pos_ = nullptr;
  std::uint32_t* wait_word_ = nullptr;
  Wait wait_;

 public:
  // 多生产者端点；不同进程通过共享 reserved_tail_ CAS 预留互不重叠区间。
  class Tx {
   public:
    explicit Tx(SharedMpscChannel& channel) noexcept : channel_(&channel) {}

    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || channel_->capacity() == 0 ||
          channel_->reserved_tail_ == nullptr || channel_->consumer_pos_ == nullptr) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::size_t need = frame::frame_len(payload_len);
      if (need > channel_->capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto reserved_tail =
          std::atomic_ref<std::uint64_t>(*channel_->reserved_tail_).load(std::memory_order_acquire);
      for (;;) {
        if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_head_, need)) {
          cached_head_ =
              std::atomic_ref<std::uint64_t>(*channel_->consumer_pos_).load(std::memory_order_acquire);
          if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_head_, need)) {
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
    SharedMpscChannel* channel_;
    std::uint64_t cached_head_ = 0;
  };

  // 单消费者端点；按位置顺序消费已提交帧，不跳过未提交 gap。
  class Rx {
   public:
    explicit Rx(SharedMpscChannel& channel) noexcept : channel_(&channel) {}

    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || channel_->consumer_pos_ == nullptr ||
          channel_->reserved_tail_ == nullptr) {
        return std::nullopt;
      }

      const std::uint64_t head = *channel_->consumer_pos_;
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
        if (channel_->wait_word_ == nullptr) {
          continue;
        }
        const auto expected =
            std::atomic_ref<std::uint32_t>(*channel_->wait_word_).load(std::memory_order_acquire);
        channel_->wait_.wait(channel_->wait_word_, expected);
      }
    }

    void release(const flow::Message& message) noexcept {
      std::atomic_ref<std::uint64_t>(*channel_->consumer_pos_)
          .store(message.next_position, std::memory_order_release);
    }

   private:
    SharedMpscChannel* channel_;
    std::uint64_t cached_tail_ = 0;
  };
};

}  // namespace salias::channel

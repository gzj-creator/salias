#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "core/channel/hybrid_mpsc.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/frame/sequence.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <Order Ordering = Order::Ordered, wait::WaitStrategy Wait = wait::SpinPause>
class SharedHybridMpscChannel {
 public:
  struct SharedControl {
    std::uint64_t* global_seq = nullptr;
    std::vector<std::uint64_t*> consumer_sequences;
    std::uint32_t* wait_word = nullptr;
    std::uint32_t* num_producers = nullptr;
    std::uint32_t* num_consumers = nullptr;
    // 发布流控窗口（字节，0 表示满环）。常量配置，创建/连接时从命名控制块复制得到。
    std::uint64_t publication_window = 0;
  };

  struct ConsumerSharedState {
    std::uint64_t* position = nullptr;
    std::uint64_t* sequence = nullptr;
  };

  struct ProducerSharedState {
    std::uint64_t* visible_producer_pos = nullptr;
    std::uint64_t* local_sequence = nullptr;
    std::vector<ConsumerSharedState> consumers;
  };

  using CreateResult = std::expected<SharedHybridMpscChannel, ChannelError>;
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // Wrap externally owned MAP_SHARED control words and one magic ring per producer.
  SharedHybridMpscChannel(std::vector<ring::MagicRing> rings,
                          std::vector<ProducerSharedState> producer_states, SharedControl control,
                          Wait wait = Wait{}) noexcept
      : producer_rings_(), control_(control), wait_(std::move(wait)) {
    producer_rings_.reserve(rings.size());
    for (std::size_t i = 0; i < rings.size(); ++i) {
      producer_rings_.push_back(ProducerRing{
          .ring = std::move(rings[i]),
          .shared = producer_states[i],
          .local_sequence = producer_states[i].local_sequence == nullptr
                                ? 0
                                : std::atomic_ref<std::uint64_t>(*producer_states[i].local_sequence)
                                      .load(std::memory_order_acquire),
      });
    }
  }

  SharedHybridMpscChannel(SharedHybridMpscChannel&&) noexcept = default;
  SharedHybridMpscChannel& operator=(SharedHybridMpscChannel&&) noexcept = default;
  SharedHybridMpscChannel(const SharedHybridMpscChannel&) = delete;
  SharedHybridMpscChannel& operator=(const SharedHybridMpscChannel&) = delete;

  std::uint32_t producer_count() const noexcept {
    if (control_.num_producers == nullptr) {
      return 0;
    }
    return std::atomic_ref<std::uint32_t>(*control_.num_producers).load(std::memory_order_acquire);
  }

  std::uint32_t consumer_count() const noexcept {
    if (control_.num_consumers == nullptr) {
      return 0;
    }
    return std::atomic_ref<std::uint32_t>(*control_.num_consumers).load(std::memory_order_acquire);
  }

  Tx tx(std::uint32_t producer_id) noexcept { return Tx(*this, producer_id); }
  Rx rx(std::uint32_t consumer_id = 0) noexcept { return Rx(*this, consumer_id); }

 private:
  struct ProducerRing {
    ring::MagicRing ring;
    ProducerSharedState shared;
    std::uint64_t local_sequence = 0;
  };

  bool has_producer(std::uint32_t producer_id) const noexcept {
    return producer_id < producer_rings_.size();
  }

  bool has_consumer(std::uint32_t consumer_id) const noexcept {
    return consumer_id < consumer_count();
  }

  // 返回流控闸门用的有效容量：窗口为 0 时即环容量，否则取 min(环容量, 窗口)。
  // 只限制生产者领先消费者的在途字节数，不改变环的寻址与回绕（那仍用真实 capacity）。
  std::size_t effective_capacity(std::size_t ring_capacity) const noexcept {
    if (control_.publication_window == 0) {
      return ring_capacity;
    }
    const auto window = static_cast<std::size_t>(control_.publication_window);
    return window < ring_capacity ? window : ring_capacity;
  }

  static std::uint64_t minimum_consumer_position(const ProducerRing& producer) noexcept {
    if (producer.shared.consumers.empty() ||
        producer.shared.consumers.front().position == nullptr) {
      return 0;
    }
    std::uint64_t minimum =
        std::atomic_ref<std::uint64_t>(*producer.shared.consumers.front().position)
            .load(std::memory_order_acquire);
    for (std::size_t index = 1; index < producer.shared.consumers.size(); ++index) {
      if (producer.shared.consumers[index].position == nullptr) {
        return 0;
      }
      const std::uint64_t position =
          std::atomic_ref<std::uint64_t>(*producer.shared.consumers[index].position)
              .load(std::memory_order_acquire);
      if (position < minimum) {
        minimum = position;
      }
    }
    return minimum;
  }

  void write_uncommitted_header(ProducerRing& producer, std::uint64_t position,
                                std::uint32_t payload_len, std::uint64_t sequence) noexcept {
    auto len_dst = producer.ring.slice_mut(position, sizeof(payload_len));
    std::memcpy(len_dst.data(), &payload_len, sizeof(payload_len));

    const std::uint32_t meta = hybrid_detail::meta_for_sequence(sequence, false);
    hybrid_detail::store_meta_release(producer.ring, position, meta);
  }

  std::optional<flow::Message> try_read_from_ring(std::uint32_t producer_id, std::uint64_t position,
                                                  std::uint64_t target_sequence,
                                                  std::uint64_t visible_bound) noexcept {
    auto& producer = producer_rings_[producer_id];
    if (position >= visible_bound) {
      return std::nullopt;
    }

    const std::uint32_t meta = hybrid_detail::load_meta_acquire(producer.ring, position);
    const std::uint32_t flags = meta & 0xFFu;
    if ((flags & frame::FLAG_COMMITTED) == 0) {
      return std::nullopt;
    }
    if (frame::sequence_low_from_meta(meta) != frame::sequence_low(target_sequence)) {
      return std::nullopt;
    }

    const std::uint32_t payload_len = hybrid_detail::load_len_after_commit(producer.ring, position);
    const std::uint64_t next_position = position + frame::frame_len(payload_len);
    if (frame::frame_len(payload_len) > producer.ring.capacity() ||
        next_position > visible_bound) {
      return std::nullopt;
    }

    return flow::Message{
        .payload = producer.ring.slice(position + frame::kHeaderSize, payload_len),
        .position = position,
        .next_position = next_position,
        .meta = meta,
        .sequence = target_sequence,
        .producer_id = producer_id,
    };
  }

  std::vector<ProducerRing> producer_rings_;
  SharedControl control_;
  Wait wait_;

 public:
  class Tx {
   public:
    using BatchClaimResult = std::expected<flow::BatchClaim, flow::FlowError>;

    explicit Tx(SharedHybridMpscChannel& channel, std::uint32_t producer_id) noexcept
        : channel_(&channel), producer_id_(producer_id) {}

    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_) ||
          (Ordering == Order::Ordered && channel_->control_.global_seq == nullptr)) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      if (producer.shared.visible_producer_pos == nullptr || producer.shared.consumers.empty() ||
          (Ordering == Order::Fifo && producer.shared.local_sequence == nullptr)) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::size_t need = frame::frame_len(payload_len);
      if (need > producer.ring.capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::uint64_t tail =
          std::atomic_ref<std::uint64_t>(*producer.shared.visible_producer_pos)
              .load(std::memory_order_acquire);
      const std::size_t effective_cap = channel_->effective_capacity(producer.ring.capacity());
      if (!hybrid_detail::has_capacity(effective_cap, tail, cached_consumer_pos_, need)) {
        cached_consumer_pos_ = channel_->minimum_consumer_position(producer);
        if (!hybrid_detail::has_capacity(effective_cap, tail, cached_consumer_pos_, need)) {
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      const std::uint64_t sequence = [&producer, this] {
        if constexpr (Ordering == Order::Ordered) {
          return std::atomic_ref<std::uint64_t>(*channel_->control_.global_seq)
              .fetch_add(1, std::memory_order_relaxed);
        }
        const std::uint64_t sequence = producer.local_sequence++;
        std::atomic_ref<std::uint64_t>(*producer.shared.local_sequence)
            .store(producer.local_sequence, std::memory_order_relaxed);
        return sequence;
      }();
      channel_->write_uncommitted_header(producer, tail, payload_len, sequence);
      std::atomic_ref<std::uint64_t>(*producer.shared.visible_producer_pos)
          .store(tail + need, std::memory_order_release);

      return flow::Claim{
          .payload = producer.ring.slice_mut(tail + frame::kHeaderSize, payload_len),
          .start_pos = tail,
          .payload_len = payload_len,
          .meta = hybrid_detail::meta_for_sequence(sequence, false),
          .sequence = sequence,
          .producer_id = producer_id_,
      };
    }

    BatchClaimResult claim_batch(std::uint32_t payload_len, std::uint32_t max_frames) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_) ||
          (Ordering == Order::Ordered && channel_->control_.global_seq == nullptr) ||
          max_frames == 0) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      if (producer.shared.visible_producer_pos == nullptr || producer.shared.consumers.empty() ||
          (Ordering == Order::Fifo && producer.shared.local_sequence == nullptr)) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::size_t per = frame::frame_len(payload_len);
      if (per > producer.ring.capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::uint64_t tail =
          std::atomic_ref<std::uint64_t>(*producer.shared.visible_producer_pos)
              .load(std::memory_order_acquire);
      const std::size_t effective_cap = channel_->effective_capacity(producer.ring.capacity());
      std::uint32_t fit =
          hybrid_detail::batch_fit(effective_cap, tail, cached_consumer_pos_, per, max_frames);
      if (fit == 0) {
        cached_consumer_pos_ = channel_->minimum_consumer_position(producer);
        fit = hybrid_detail::batch_fit(effective_cap, tail, cached_consumer_pos_, per, max_frames);
        if (fit == 0) {
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      const std::uint64_t base_sequence = [&producer, fit, this] {
        if constexpr (Ordering == Order::Ordered) {
          return std::atomic_ref<std::uint64_t>(*channel_->control_.global_seq)
              .fetch_add(fit, std::memory_order_relaxed);
        }
        const std::uint64_t sequence = producer.local_sequence;
        producer.local_sequence += fit;
        std::atomic_ref<std::uint64_t>(*producer.shared.local_sequence)
            .store(producer.local_sequence, std::memory_order_relaxed);
        return sequence;
      }();
      const std::uint64_t span_bytes = static_cast<std::uint64_t>(per) * fit;
      for (std::uint32_t i = 0; i < fit; ++i) {
        const std::uint64_t frame_position = tail + static_cast<std::uint64_t>(per) * i;
        channel_->write_uncommitted_header(producer, frame_position, payload_len,
                                           base_sequence + i);
      }
      std::atomic_ref<std::uint64_t>(*producer.shared.visible_producer_pos)
          .store(tail + span_bytes, std::memory_order_release);

      return flow::BatchClaim{
          .region = producer.ring.slice_mut(tail, static_cast<std::size_t>(span_bytes)),
          .start_pos = tail,
          .frame_len = static_cast<std::uint32_t>(per),
          .frame_count = fit,
          .base_sequence = base_sequence,
          .producer_id = producer_id_,
      };
    }

    void commit(const flow::Claim& claim) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_)) {
        return;
      }
      auto& producer = channel_->producer_rings_[producer_id_];
      hybrid_detail::store_meta_release(producer.ring, claim.start_pos,
                                        claim.meta | frame::FLAG_COMMITTED);
      if (channel_->control_.wait_word != nullptr && channel_->wait_.needs_wake()) {
        std::atomic_ref<std::uint32_t>(*channel_->control_.wait_word)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(channel_->control_.wait_word);
      }
    }

    void commit_batch(const flow::BatchClaim& batch) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_)) {
        return;
      }
      auto& producer = channel_->producer_rings_[producer_id_];
      for (std::uint32_t i = 0; i < batch.frame_count; ++i) {
        const std::uint64_t position =
            batch.start_pos + static_cast<std::uint64_t>(batch.frame_len) * i;
        const std::uint32_t committed_meta =
            hybrid_detail::meta_for_sequence(batch.base_sequence + i, true);
        hybrid_detail::store_meta_release(producer.ring, position, committed_meta);
      }
      if (channel_->control_.wait_word != nullptr && channel_->wait_.needs_wake()) {
        std::atomic_ref<std::uint32_t>(*channel_->control_.wait_word)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(channel_->control_.wait_word);
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
    SharedHybridMpscChannel* channel_;
    std::uint32_t producer_id_;
    std::uint64_t cached_consumer_pos_ = 0;
  };

  class Rx {
   public:
    explicit Rx(SharedHybridMpscChannel& channel, std::uint32_t consumer_id) noexcept
        : channel_(&channel),
          consumer_id_(consumer_id),
          read_positions_(channel.producer_rings_.size(), 0),
          next_sequences_(channel.producer_rings_.size(), 0),
          cached_visible_pos_(channel.producer_rings_.size(), 0),
          flushed_positions_(channel.producer_rings_.size(), 0) {
      if (!channel.has_consumer(consumer_id)) {
        return;
      }
      if constexpr (Ordering == Order::Ordered) {
        if (consumer_id < channel.control_.consumer_sequences.size() &&
            channel.control_.consumer_sequences[consumer_id] != nullptr) {
          expected_sequence_ =
              std::atomic_ref<std::uint64_t>(*channel.control_.consumer_sequences[consumer_id])
                  .load(std::memory_order_acquire);
        }
      }
      for (std::size_t producer_id = 0; producer_id < channel.producer_rings_.size();
           ++producer_id) {
        const auto& consumers = channel.producer_rings_[producer_id].shared.consumers;
        if (consumer_id >= consumers.size()) {
          continue;
        }
        if (consumers[consumer_id].position != nullptr) {
          read_positions_[producer_id] =
              std::atomic_ref<std::uint64_t>(*consumers[consumer_id].position)
                  .load(std::memory_order_acquire);
        }
        if (consumers[consumer_id].sequence != nullptr) {
          next_sequences_[producer_id] =
              std::atomic_ref<std::uint64_t>(*consumers[consumer_id].sequence)
                  .load(std::memory_order_acquire);
        }
      }
      flushed_positions_ = read_positions_;
    }

    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          channel_->producer_rings_.empty() ||
          (Ordering == Order::Ordered &&
           (consumer_id_ >= channel_->control_.consumer_sequences.size() ||
            channel_->control_.consumer_sequences[consumer_id_] == nullptr))) {
        return std::nullopt;
      }

      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t scanned = 0; scanned < count; ++scanned) {
        const std::uint32_t producer_id =
            static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);

        // 只有当本地读位置追平缓存的可见位时，才跨核 acquire-load 生产者位置刷新缓存。
        // 稳态批量消费时，一次刷新可覆盖一整批，跨核 load 从 “每消息” 降到 “每批次每环一次”。
        if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
          auto* visible_pos = channel_->producer_rings_[producer_id].shared.visible_producer_pos;
          if (visible_pos == nullptr) {
            continue;
          }
          cached_visible_pos_[producer_id] =
              std::atomic_ref<std::uint64_t>(*visible_pos).load(std::memory_order_acquire);
          if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
            continue;
          }
        }

        const std::uint64_t target_sequence =
            Ordering == Order::Ordered ? expected_sequence_ : next_sequences_[producer_id];
        auto message =
            channel_->try_read_from_ring(producer_id, read_positions_[producer_id], target_sequence,
                                         cached_visible_pos_[producer_id]);
        if (message.has_value()) {
          last_hit_ring_ = static_cast<std::uint32_t>((producer_id + 1) % count);
          return message;
        }
      }
      return std::nullopt;
    }

    // 批量接收：选中一个可读环后在同环内紧循环连续排空，最多填 cap 条到 out[]。
    // 每条消息只更新本地读位置（consume 语义），不写共享槽——进度由调用方在批末统一 flush_progress()。
    // 返回本次收集的条数（0 表示当前无可读消息）。
    std::uint32_t try_recv_run(flow::Message* out, std::uint32_t cap) noexcept {
      if (out == nullptr || cap == 0 || channel_ == nullptr ||
          !channel_->has_consumer(consumer_id_) || channel_->producer_rings_.empty() ||
          (Ordering == Order::Ordered &&
           (consumer_id_ >= channel_->control_.consumer_sequences.size() ||
            channel_->control_.consumer_sequences[consumer_id_] == nullptr))) {
        return 0;
      }

      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t scanned = 0; scanned < count; ++scanned) {
        const std::uint32_t producer_id =
            static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);

        if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
          auto* visible_pos = channel_->producer_rings_[producer_id].shared.visible_producer_pos;
          if (visible_pos == nullptr) {
            continue;
          }
          cached_visible_pos_[producer_id] =
              std::atomic_ref<std::uint64_t>(*visible_pos).load(std::memory_order_acquire);
          if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
            continue;
          }
        }

        std::uint32_t produced = 0;
        while (produced < cap) {
          const std::uint64_t target_sequence =
              Ordering == Order::Ordered ? expected_sequence_ : next_sequences_[producer_id];
          auto message = channel_->try_read_from_ring(producer_id, read_positions_[producer_id],
                                                      target_sequence,
                                                      cached_visible_pos_[producer_id]);
          if (!message.has_value()) {
            break;
          }
          out[produced++] = *message;
          consume(*message);
        }

        if (produced > 0) {
          last_hit_ring_ = static_cast<std::uint32_t>((producer_id + 1) % count);
          return produced;
        }
      }
      return 0;
    }

    flow::Message recv() noexcept {
      for (;;) {
        if (auto message = try_recv(); message.has_value()) {
          return *message;
        }
        if (channel_ == nullptr || channel_->control_.wait_word == nullptr) {
          continue;
        }
        const auto expected = std::atomic_ref<std::uint32_t>(*channel_->control_.wait_word)
                                  .load(std::memory_order_acquire);
        channel_->wait_.wait(channel_->control_.wait_word, expected);
      }
    }

    void release(const flow::Message& message) noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          message.producer_id >= channel_->producer_rings_.size()) {
        return;
      }

      auto& producer = channel_->producer_rings_[message.producer_id];
      if (consumer_id_ >= producer.shared.consumers.size()) {
        return;
      }
      auto& consumer = producer.shared.consumers[consumer_id_];
      if (consumer.position == nullptr || consumer.sequence == nullptr) {
        return;
      }

      read_positions_[message.producer_id] = message.next_position;
      std::atomic_ref<std::uint64_t>(*consumer.position)
          .store(message.next_position, std::memory_order_release);
      flushed_positions_[message.producer_id] = message.next_position;
      if constexpr (Ordering == Order::Ordered) {
        expected_sequence_ = message.sequence + 1;
        std::atomic_ref<std::uint64_t>(*channel_->control_.consumer_sequences[consumer_id_])
            .store(expected_sequence_, std::memory_order_release);
        std::atomic_ref<std::uint64_t>(*consumer.sequence)
            .store(expected_sequence_, std::memory_order_release);
      } else {
        next_sequences_[message.producer_id] = message.sequence + 1;
        std::atomic_ref<std::uint64_t>(*consumer.sequence)
            .store(next_sequences_[message.producer_id], std::memory_order_release);
      }
    }

    // 只更新本地消费进度，不写共享槽；热路径批量消费时配合 flush_progress() 使用。
    // 把 “每消息 2 次共享 release-store” 收敛为 “每批次每环 2 次”。
    void consume(const flow::Message& message) noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          message.producer_id >= channel_->producer_rings_.size()) {
        return;
      }
      read_positions_[message.producer_id] = message.next_position;
      if constexpr (Ordering == Order::Ordered) {
        expected_sequence_ = message.sequence + 1;
      } else {
        next_sequences_[message.producer_id] = message.sequence + 1;
      }
    }

    // 把本地消费进度 release-store 到共享内存，让生产者回收环空间。
    // 仅对自上次 flush 后有推进的环写入，避免重复脏化生产者会读的缓存行。
    void flush_progress() noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_)) {
        return;
      }
      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t producer_id = 0; producer_id < count; ++producer_id) {
        if (read_positions_[producer_id] == flushed_positions_[producer_id]) {
          continue;
        }
        auto& producer = channel_->producer_rings_[producer_id];
        if (consumer_id_ >= producer.shared.consumers.size()) {
          continue;
        }
        auto& consumer = producer.shared.consumers[consumer_id_];
        if (consumer.position == nullptr || consumer.sequence == nullptr) {
          continue;
        }
        std::atomic_ref<std::uint64_t>(*consumer.position)
            .store(read_positions_[producer_id], std::memory_order_release);
        if constexpr (Ordering == Order::Ordered) {
          if (consumer_id_ < channel_->control_.consumer_sequences.size() &&
              channel_->control_.consumer_sequences[consumer_id_] != nullptr) {
            std::atomic_ref<std::uint64_t>(*channel_->control_.consumer_sequences[consumer_id_])
                .store(expected_sequence_, std::memory_order_release);
          }
          std::atomic_ref<std::uint64_t>(*consumer.sequence)
              .store(expected_sequence_, std::memory_order_release);
        } else {
          std::atomic_ref<std::uint64_t>(*consumer.sequence)
              .store(next_sequences_[producer_id], std::memory_order_release);
        }
        flushed_positions_[producer_id] = read_positions_[producer_id];
      }
    }

   private:
    SharedHybridMpscChannel* channel_;
    std::uint32_t consumer_id_ = 0;
    std::uint32_t last_hit_ring_ = 0;
    std::vector<std::uint64_t> read_positions_;
    std::vector<std::uint64_t> next_sequences_;
    std::vector<std::uint64_t> cached_visible_pos_;
    std::vector<std::uint64_t> flushed_positions_;
    std::uint64_t expected_sequence_ = 0;
  };
};

}  // namespace salias::channel

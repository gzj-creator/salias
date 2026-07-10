#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/channel/error.hpp"
#include "core/channel/hybrid_control.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/frame/sequence.hpp"
#include "core/platform/map_options.hpp"
#include "core/platform/mapping.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

namespace hybrid_detail {

inline bool has_capacity(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                         std::size_t need) noexcept {
  const std::uint64_t used = tail - head;
  return used <= capacity && need <= capacity - used;
}

inline std::uint32_t batch_fit(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                               std::size_t frame_len, std::uint32_t max_frames) noexcept {
  if (frame_len == 0 || max_frames == 0) {
    return 0;
  }
  const std::uint64_t used = tail - head;
  if (used > capacity) {
    return 0;
  }
  const std::uint64_t free_bytes = capacity - used;
  const std::uint64_t fit = free_bytes / frame_len;
  if (fit == 0) {
    return 0;
  }
  return fit < max_frames ? static_cast<std::uint32_t>(fit) : max_frames;
}

inline bool sequence_low_window_fits(std::uint32_t num_producers,
                                     std::size_t capacity_per_producer) noexcept {
  if (num_producers == 0 || capacity_per_producer == 0) {
    return false;
  }
  if (num_producers == 1) {
    return true;
  }

  const std::uint64_t frames_per_ring =
      static_cast<std::uint64_t>(capacity_per_producer / frame::kFrameAlign);
  if (frames_per_ring == 0) {
    return false;
  }

  // Hybrid stores only the low 24 sequence bits in the 8-byte frame header. Until a
  // shared side metadata store exists for the high bits, reject configurations where
  // other producer rings can hold a full low-bit period ahead of the expected sequence.
  constexpr std::uint64_t kSequenceLowPeriod = std::uint64_t{1} << frame::kSequenceLowBits;
  return static_cast<std::uint64_t>(num_producers - 1) <
         ((kSequenceLowPeriod + frames_per_ring - 1) / frames_per_ring);
}

inline std::uint32_t meta_for_sequence(std::uint64_t sequence, bool committed) noexcept {
  std::uint32_t meta = frame::FLAG_BEGIN | frame::FLAG_END;
  if (committed) {
    meta |= frame::FLAG_COMMITTED;
  }
  std::uint64_t sequence_high = 0;
  frame::encode_sequence(sequence, meta, sequence_high);
  return meta;
}

inline std::size_t sequence_slot(std::uint64_t position, std::size_t capacity) noexcept {
  return static_cast<std::size_t>(position & (capacity - 1u)) / frame::kFrameAlign;
}

inline void store_meta_release(ring::MagicRing& ring, std::uint64_t position,
                               std::uint32_t meta) noexcept {
  auto meta_dst = ring.slice_mut(position + sizeof(std::uint32_t), sizeof(std::uint32_t));
  auto* meta_ptr = reinterpret_cast<std::uint32_t*>(meta_dst.data());
  std::atomic_ref<std::uint32_t>(*meta_ptr).store(meta, std::memory_order_release);
}

inline std::uint32_t load_meta_acquire(const ring::MagicRing& ring,
                                       std::uint64_t position) noexcept {
  auto meta_src = ring.slice(position + sizeof(std::uint32_t), sizeof(std::uint32_t));
  auto* meta_ptr =
      const_cast<std::uint32_t*>(reinterpret_cast<const std::uint32_t*>(meta_src.data()));
  return std::atomic_ref<std::uint32_t>(*meta_ptr).load(std::memory_order_acquire);
}

inline std::uint32_t load_len_after_commit(const ring::MagicRing& ring,
                                           std::uint64_t position) noexcept {
  std::uint32_t len = 0;
  auto len_src = ring.slice(position, sizeof(len));
  std::memcpy(&len, len_src.data(), sizeof(len));
  return len;
}

}  // namespace hybrid_detail

template <Order Ordering = Order::Ordered, wait::WaitStrategy Wait = wait::SpinPause>
class HybridMpscChannel {
 public:
  struct Config {
    std::uint32_t num_producers = 1;
    std::uint32_t num_consumers = 1;
    std::size_t ring_capacity_per_producer = 4u * 1024u * 1024u;
    platform::HugePage huge = platform::HugePage::None;
    int numa_node = -1;
    std::size_t publication_window = 0;
  };

  using CreateResult = std::expected<HybridMpscChannel, ChannelError>;
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // Create an in-process hybrid MPSC channel with one private magic ring per producer.
  static CreateResult create(const Config& config) noexcept {
    const std::uint32_t sequence_domains = Ordering == Order::Ordered ? config.num_producers : 1;
    if (config.num_producers == 0 || config.num_consumers == 0 ||
        config.ring_capacity_per_producer == 0 ||
        !hybrid_detail::sequence_low_window_fits(sequence_domains,
                                                 config.ring_capacity_per_producer)) {
      return std::unexpected(ChannelError::BadConfig);
    }

    auto control = std::make_unique<HybridSharedControl>();
    control->num_producers = config.num_producers;
    control->publication_window = config.publication_window;

    std::vector<ProducerRing> producer_rings;
    producer_rings.reserve(config.num_producers);
    for (std::uint32_t i = 0; i < config.num_producers; ++i) {
      auto mapping = platform::Mapping::create(platform::MapOptions{
          .size = config.ring_capacity_per_producer,
          .huge = config.huge,
          .numa_node = config.numa_node,
      });
      if (!mapping) {
        return std::unexpected(ChannelError::PlatformFail);
      }

      auto ring = ring::MagicRing::create(std::move(mapping).value());
      if (!ring) {
        return std::unexpected(ChannelError::RingFail);
      }
      producer_rings.emplace_back(std::move(ring).value(), config.num_consumers);
    }

    auto consumer_sequences = std::make_unique<std::atomic<std::uint64_t>[]>(config.num_consumers);
    for (std::uint32_t consumer_id = 0; consumer_id < config.num_consumers; ++consumer_id) {
      consumer_sequences[consumer_id].store(0, std::memory_order_relaxed);
    }

    return HybridMpscChannel(std::move(control), std::move(producer_rings),
                             std::move(consumer_sequences), Wait{});
  }

  HybridMpscChannel(HybridMpscChannel&&) noexcept = default;
  HybridMpscChannel& operator=(HybridMpscChannel&&) noexcept = default;
  HybridMpscChannel(const HybridMpscChannel&) = delete;
  HybridMpscChannel& operator=(const HybridMpscChannel&) = delete;

  std::uint32_t producer_count() const noexcept {
    return control_ == nullptr ? 0u : control_->num_producers;
  }

  std::size_t ring_capacity_per_producer() const noexcept {
    return producer_rings_.empty() ? 0u : producer_rings_.front().ring.capacity();
  }

  Tx tx(std::uint32_t producer_id) noexcept { return Tx(*this, producer_id); }
  Rx rx(std::uint32_t consumer_id = 0) noexcept { return Rx(*this, consumer_id); }

 private:
  struct alignas(128) ProducerRing {
    ProducerRing(ring::MagicRing created_ring, std::uint32_t created_consumer_count)
        : ring(std::move(created_ring)),
          consumer_positions(
              std::make_unique<std::atomic<std::uint64_t>[]>(created_consumer_count)),
          consumer_count(created_consumer_count) {
      for (std::uint32_t consumer_id = 0; consumer_id < consumer_count; ++consumer_id) {
        consumer_positions[consumer_id].store(0, std::memory_order_relaxed);
      }
    }

    ProducerRing(ProducerRing&& other) noexcept
        : ring(std::move(other.ring)),
          producer_pos(other.producer_pos),
          cached_consumer_pos(other.cached_consumer_pos),
          visible_producer_pos(other.visible_producer_pos.load(std::memory_order_relaxed)),
          consumer_positions(std::move(other.consumer_positions)),
          consumer_count(other.consumer_count),
          total_written(other.total_written),
          backpressure_count(other.backpressure_count) {}

    ProducerRing& operator=(ProducerRing&& other) noexcept {
      if (this != &other) {
        ring = std::move(other.ring);
        producer_pos = other.producer_pos;
        cached_consumer_pos = other.cached_consumer_pos;
        visible_producer_pos.store(other.visible_producer_pos.load(std::memory_order_relaxed),
                                   std::memory_order_relaxed);
        consumer_positions = std::move(other.consumer_positions);
        consumer_count = other.consumer_count;
        total_written = other.total_written;
        backpressure_count = other.backpressure_count;
      }
      return *this;
    }

    ProducerRing(const ProducerRing&) = delete;
    ProducerRing& operator=(const ProducerRing&) = delete;

    ring::MagicRing ring;
    alignas(128) std::uint64_t producer_pos = 0;
    std::uint64_t cached_consumer_pos = 0;
    alignas(128) std::atomic<std::uint64_t> visible_producer_pos{0};
    std::unique_ptr<std::atomic<std::uint64_t>[]> consumer_positions;
    std::uint32_t consumer_count = 0;
    std::uint64_t total_written = 0;
    std::uint64_t backpressure_count = 0;
  };

  explicit HybridMpscChannel(std::unique_ptr<HybridSharedControl> control,
                             std::vector<ProducerRing> producer_rings,
                             std::unique_ptr<std::atomic<std::uint64_t>[]> consumer_sequences,
                             Wait wait) noexcept
      : control_(std::move(control)),
        producer_rings_(std::move(producer_rings)),
        consumer_sequences_(std::move(consumer_sequences)),
        wait_(std::move(wait)) {}

  bool has_producer(std::uint32_t producer_id) const noexcept {
    return producer_id < producer_rings_.size();
  }

  bool has_consumer(std::uint32_t consumer_id) const noexcept {
    return !producer_rings_.empty() && consumer_id < producer_rings_.front().consumer_count;
  }

  static std::uint64_t minimum_consumer_position(const ProducerRing& producer) noexcept {
    std::uint64_t minimum = producer.consumer_positions[0].load(std::memory_order_acquire);
    for (std::uint32_t consumer_id = 1; consumer_id < producer.consumer_count; ++consumer_id) {
      const std::uint64_t position =
          producer.consumer_positions[consumer_id].load(std::memory_order_acquire);
      if (position < minimum) {
        minimum = position;
      }
    }
    return minimum;
  }

  // 有效流控容量：窗口为 0 时即环容量，否则取 min(环容量, 窗口)。不改变环的寻址与回绕。
  std::size_t effective_capacity(std::size_t ring_capacity) const noexcept {
    if (control_ == nullptr || control_->publication_window == 0) {
      return ring_capacity;
    }
    const auto window = static_cast<std::size_t>(control_->publication_window);
    return window < ring_capacity ? window : ring_capacity;
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

  std::unique_ptr<HybridSharedControl> control_;
  std::vector<ProducerRing> producer_rings_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> consumer_sequences_;
  Wait wait_;

 public:
  class Tx {
   public:
    using BatchClaimResult = std::expected<flow::BatchClaim, flow::FlowError>;

    explicit Tx(HybridMpscChannel& channel, std::uint32_t producer_id) noexcept
        : channel_(&channel), producer_id_(producer_id) {}

    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_)) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      const std::size_t need = frame::frame_len(payload_len);
      if (need > producer.ring.capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      if (!hybrid_detail::has_capacity(channel_->effective_capacity(producer.ring.capacity()),
                                       producer.producer_pos, producer.cached_consumer_pos, need)) {
        producer.cached_consumer_pos = channel_->minimum_consumer_position(producer);
        if (!hybrid_detail::has_capacity(channel_->effective_capacity(producer.ring.capacity()),
                                         producer.producer_pos, producer.cached_consumer_pos,
                                         need)) {
          ++producer.backpressure_count;
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      const std::uint64_t sequence = [&producer, this] {
        if constexpr (Ordering == Order::Ordered) {
          return channel_->control_->global_seq.fetch_add(1, std::memory_order_relaxed);
        }
        return producer.total_written;
      }();
      const std::uint64_t position = producer.producer_pos;
      producer.producer_pos += need;
      ++producer.total_written;
      channel_->write_uncommitted_header(producer, position, payload_len, sequence);
      producer.visible_producer_pos.store(producer.producer_pos, std::memory_order_release);

      return flow::Claim{
          .payload = producer.ring.slice_mut(position + frame::kHeaderSize, payload_len),
          .start_pos = position,
          .payload_len = payload_len,
          .meta = hybrid_detail::meta_for_sequence(sequence, false),
          .sequence = sequence,
          .producer_id = producer_id_,
      };
    }

    BatchClaimResult claim_batch(std::uint32_t payload_len, std::uint32_t max_frames) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_) || max_frames == 0) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      const std::size_t per = frame::frame_len(payload_len);
      if (per > producer.ring.capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      std::uint32_t fit = hybrid_detail::batch_fit(
          channel_->effective_capacity(producer.ring.capacity()), producer.producer_pos,
          producer.cached_consumer_pos, per, max_frames);
      if (fit == 0) {
        producer.cached_consumer_pos = channel_->minimum_consumer_position(producer);
        fit = hybrid_detail::batch_fit(channel_->effective_capacity(producer.ring.capacity()),
                                       producer.producer_pos, producer.cached_consumer_pos, per,
                                       max_frames);
        if (fit == 0) {
          ++producer.backpressure_count;
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      const std::uint64_t base_sequence = [&producer, fit, this] {
        if constexpr (Ordering == Order::Ordered) {
          return channel_->control_->global_seq.fetch_add(fit, std::memory_order_relaxed);
        }
        return producer.total_written;
      }();
      const std::uint64_t position = producer.producer_pos;
      const std::uint64_t span_bytes = static_cast<std::uint64_t>(per) * fit;
      producer.producer_pos += span_bytes;
      producer.total_written += fit;

      for (std::uint32_t i = 0; i < fit; ++i) {
        const std::uint64_t frame_position = position + static_cast<std::uint64_t>(per) * i;
        channel_->write_uncommitted_header(producer, frame_position, payload_len,
                                           base_sequence + i);
      }
      producer.visible_producer_pos.store(producer.producer_pos, std::memory_order_release);

      return flow::BatchClaim{
          .region = producer.ring.slice_mut(position, static_cast<std::size_t>(span_bytes)),
          .start_pos = position,
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
      const std::uint32_t committed_meta = claim.meta | frame::FLAG_COMMITTED;
      hybrid_detail::store_meta_release(producer.ring, claim.start_pos, committed_meta);
      if (channel_->wait_.needs_wake()) {
        std::atomic_ref<std::uint32_t>(channel_->control_->wait_word)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(&channel_->control_->wait_word);
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
      if (channel_->wait_.needs_wake()) {
        std::atomic_ref<std::uint32_t>(channel_->control_->wait_word)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(&channel_->control_->wait_word);
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
    HybridMpscChannel* channel_;
    std::uint32_t producer_id_;
  };

  class Rx {
   public:
    explicit Rx(HybridMpscChannel& channel, std::uint32_t consumer_id) noexcept
        : channel_(&channel),
          consumer_id_(consumer_id),
          read_positions_(channel.producer_rings_.size(), 0),
          next_sequences_(channel.producer_rings_.size(), 0),
          cached_visible_pos_(channel.producer_rings_.size(), 0),
          flushed_positions_(channel.producer_rings_.size(), 0),
          expected_sequence_(
              channel.has_consumer(consumer_id)
                  ? channel.consumer_sequences_[consumer_id].load(std::memory_order_acquire)
                  : 0) {}

    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          channel_->producer_rings_.empty()) {
        return std::nullopt;
      }

      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t scanned = 0; scanned < count; ++scanned) {
        const std::uint32_t producer_id =
            static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);

        // 只有本地读位置追平缓存可见位时才跨核 acquire-load 刷新，一次刷新覆盖一整批。
        if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
          cached_visible_pos_[producer_id] =
              channel_->producer_rings_[producer_id].visible_producer_pos.load(
                  std::memory_order_acquire);
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
    // 每条只更新本地读位置（consume 语义），不写共享槽——进度由调用方批末统一 flush_progress()。
    std::uint32_t try_recv_run(flow::Message* out, std::uint32_t cap) noexcept {
      if (out == nullptr || cap == 0 || channel_ == nullptr ||
          !channel_->has_consumer(consumer_id_) || channel_->producer_rings_.empty()) {
        return 0;
      }

      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t scanned = 0; scanned < count; ++scanned) {
        const std::uint32_t producer_id =
            static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);

        if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
          cached_visible_pos_[producer_id] =
              channel_->producer_rings_[producer_id].visible_producer_pos.load(
                  std::memory_order_acquire);
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
        const auto expected = std::atomic_ref<std::uint32_t>(channel_->control_->wait_word)
                                  .load(std::memory_order_acquire);
        channel_->wait_.wait(&channel_->control_->wait_word, expected);
      }
    }

    void release(const flow::Message& message) noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          message.producer_id >= channel_->producer_rings_.size() ||
          (Ordering == Order::Ordered && message.sequence != expected_sequence_)) {
        return;
      }

      auto& producer = channel_->producer_rings_[message.producer_id];
      read_positions_[message.producer_id] = message.next_position;
      producer.consumer_positions[consumer_id_].store(message.next_position,
                                                      std::memory_order_release);
      flushed_positions_[message.producer_id] = message.next_position;
      if constexpr (Ordering == Order::Ordered) {
        ++expected_sequence_;
        channel_->consumer_sequences_[consumer_id_].store(expected_sequence_,
                                                          std::memory_order_release);
      } else {
        next_sequences_[message.producer_id] = message.sequence + 1;
      }
    }

    // 只更新本地消费进度，不写共享槽；配合 flush_progress() 用于热路径批量消费。
    void consume(const flow::Message& message) noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          message.producer_id >= channel_->producer_rings_.size() ||
          (Ordering == Order::Ordered && message.sequence != expected_sequence_)) {
        return;
      }
      read_positions_[message.producer_id] = message.next_position;
      if constexpr (Ordering == Order::Ordered) {
        ++expected_sequence_;
      } else {
        next_sequences_[message.producer_id] = message.sequence + 1;
      }
    }

    // 把本地消费进度发布到生产者可见的原子槽，仅对有推进的环写入。
    void flush_progress() noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_)) {
        return;
      }
      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t producer_id = 0; producer_id < count; ++producer_id) {
        if (read_positions_[producer_id] == flushed_positions_[producer_id]) {
          continue;
        }
        channel_->producer_rings_[producer_id].consumer_positions[consumer_id_].store(
            read_positions_[producer_id], std::memory_order_release);
        flushed_positions_[producer_id] = read_positions_[producer_id];
      }
      if constexpr (Ordering == Order::Ordered) {
        channel_->consumer_sequences_[consumer_id_].store(expected_sequence_,
                                                          std::memory_order_release);
      }
    }

    template <class Handler>
    std::uint32_t poll(Handler&& handler, std::uint32_t limit) noexcept {
      constexpr std::uint32_t kChunk = 32;
      std::array<flow::Message, kChunk> buffer;
      std::uint32_t processed = 0;
      while (processed < limit) {
        const std::uint32_t want = std::min(kChunk, limit - processed);
        const std::uint32_t got = try_recv_run(buffer.data(), want);
        if (got == 0) {
          break;
        }
        for (std::uint32_t i = 0; i < got; ++i) {
          if constexpr (std::is_invocable_v<Handler&, std::span<const std::byte>>) {
            std::invoke(handler, buffer[i].payload);
          } else {
            std::invoke(handler, buffer[i]);
          }
        }
        flush_progress();
        processed += got;
      }
      return processed;
    }

   private:
    HybridMpscChannel* channel_;
    std::uint32_t consumer_id_ = 0;
    std::vector<std::uint64_t> read_positions_;
    std::vector<std::uint64_t> next_sequences_;
    std::vector<std::uint64_t> cached_visible_pos_;
    std::vector<std::uint64_t> flushed_positions_;
    std::uint64_t expected_sequence_ = 0;
    std::uint32_t last_hit_ring_ = 0;
  };
};

}  // namespace salias::channel

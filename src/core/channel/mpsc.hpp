#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
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
#include "core/ring/cache_aligned.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

namespace detail {

// 计算写入帧 metadata 的 generation 部分。
inline std::uint32_t frame_generation(std::uint64_t position, std::size_t cap) noexcept {
  return static_cast<std::uint32_t>((position / cap) & 0x00FF'FFFFu);
}

// 构造 MPSC 帧 metadata，并按需标记 committed。
inline std::uint32_t mpsc_meta(std::uint64_t position, std::size_t cap,
                               bool committed) noexcept {
  std::uint32_t flags = frame::FLAG_BEGIN | frame::FLAG_END;
  if (committed) {
    flags |= frame::FLAG_COMMITTED;
  }
  return (frame_generation(position, cap) << 8) | flags;
}

// 判断 tail/head 窗口是否还能容纳 need 字节。
inline bool mpsc_has_capacity(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                              std::size_t need) noexcept {
  const std::uint64_t used = tail - head;
  return used <= capacity && need <= capacity - used;
}

// 以 release 语义发布帧 metadata 字。
inline void store_meta_release(ring::MagicRing& ring, std::uint64_t position,
                               std::uint32_t meta) noexcept {
  auto meta_dst = ring.slice_mut(position + sizeof(std::uint32_t), sizeof(std::uint32_t));
  auto* meta_ptr = reinterpret_cast<std::uint32_t*>(meta_dst.data());
  // 安全性：每帧都从 8 字节对齐位置开始，因此 meta 成员满足 4 字节对齐。
  // 对该单元的所有并发访问都使用兼容的原子操作。
  std::atomic_ref<std::uint32_t>(*meta_ptr).store(meta, std::memory_order_release);
}

// 向 ring 写入帧长度和未提交 metadata 标记。
inline void write_uncommitted_header(ring::MagicRing& ring, std::uint64_t position,
                                     std::uint32_t payload_len, std::uint32_t meta) noexcept {
  auto len_dst = ring.slice_mut(position, sizeof(payload_len));
  std::memcpy(len_dst.data(), &payload_len, sizeof(payload_len));
  store_meta_release(ring, position, meta);
}

// 在读取帧字节前以 acquire 语义读取 metadata 字。
inline std::uint32_t load_meta_acquire(const ring::MagicRing& ring,
                                       std::uint64_t position) noexcept {
  auto meta_src = ring.slice(position + sizeof(std::uint32_t), sizeof(std::uint32_t));
  auto* meta_ptr =
      const_cast<std::uint32_t*>(reinterpret_cast<const std::uint32_t*>(meta_src.data()));
  // 安全性：生产者通过 store_meta_release() 以 release 语义发布同一个 4 字节 meta 单元。
  // 这里的 acquire load 可观察到此前写入的 payload 和 len。
  return std::atomic_ref<std::uint32_t>(*meta_ptr).load(std::memory_order_acquire);
}

// 在 metadata 提交标记可见后读取 payload 长度。
inline std::uint32_t load_len_after_commit(const ring::MagicRing& ring,
                                           std::uint64_t position) noexcept {
  std::uint32_t len = 0;
  auto len_src = ring.slice(position, sizeof(len));
  std::memcpy(&len, len_src.data(), sizeof(len));
  return len;
}

}  // namespace detail

template <wait::WaitStrategy Wait = wait::SpinPause>
// Salias 原生共享内存 MPSC 通道；多个生产者在 mmap 支持的 MagicRing 中预留互斥区间。
// 进程本地堆队列不能替代这一路径。
class MpscChannel {
 public:
  using CreateResult = std::expected<MpscChannel, ChannelError>;
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // 创建进程内 MPSC 通道，所有生产者共享一个 reservation tail。
  static CreateResult create(const ChannelConfig& config) noexcept {
    if (config.capacity == 0 || config.fixed_size) {
      return std::unexpected(ChannelError::BadConfig);
    }

    auto mapping = platform::Mapping::create(platform::MapOptions{
        .size = config.capacity,
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

    return MpscChannel(std::move(ring).value(), Wait{});
  }

  // 移动通道及其持有的 ring 映射。
  MpscChannel(MpscChannel&&) noexcept = default;
  // 移动赋值通道及其持有的 ring 映射。
  MpscChannel& operator=(MpscChannel&&) noexcept = default;
  // 禁止拷贝；通道独占 ring 存储。
  MpscChannel(const MpscChannel&) = delete;
  // 禁止拷贝赋值；通道独占 ring 存储。
  MpscChannel& operator=(const MpscChannel&) = delete;

  // 返回逻辑 ring 容量，单位为字节。
  std::size_t capacity() const noexcept { return ring_.capacity(); }

  // 创建通过共享 reservation tail 协调的生产者端点。
  Tx tx() noexcept { return Tx(*this); }
  // 创建唯一消费者端点。
  Rx rx() noexcept { return Rx(*this); }

 private:
  // 保存已创建的 ring 和等待策略。
  explicit MpscChannel(ring::MagicRing ring, Wait wait) noexcept
      : ring_(std::move(ring)), wait_(std::move(wait)) {}

  ring::MagicRing ring_;
  ring::CacheAligned<std::uint64_t> reserved_tail_{};
  ring::CacheAligned<std::uint64_t> consumer_pos_{};
  std::uint32_t wait_word_ = 0;
  Wait wait_;

 public:
  // 多生产者发送端点；每个 Tx 应由一个生产者线程持有。
  // 不同 Tx 通过父通道的 CAS reservation tail 协调。
  class Tx {
   public:
    // 将生产者端点绑定到父 MPSC 通道。
    explicit Tx(MpscChannel& channel) noexcept : channel_(&channel) {}

    // 为当前生产者原子预留互不重叠的帧区间。
    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || channel_->capacity() == 0) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::size_t need = frame::frame_len(payload_len);
      if (need > channel_->capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto reserved_tail = std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
                               .load(std::memory_order_acquire);
      for (;;) {
        if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_head_, need)) {
          // 安全性：唯一消费者在 advance() 中以 release 语义写入 consumer_pos_。
          // 生产者用 acquire 读取它，避免在消费者释放前复用旧字节。
          cached_head_ = std::atomic_ref<std::uint64_t>(channel_->consumer_pos_.value)
                             .load(std::memory_order_acquire);
          if (!detail::mpsc_has_capacity(channel_->capacity(), reserved_tail, cached_head_, need)) {
            return std::unexpected(flow::FlowError::BackPressured);
          }
        }

        const std::uint64_t next_tail = reserved_tail + need;
        // 安全性：CAS 成功后，当前 Tx 独占 [reserved_tail,next_tail) 区间。
        // 其他生产者只能通过继续推进 reserved_tail_ 预留互不重叠的区间。
        if (std::atomic_ref<std::uint64_t>(channel_->reserved_tail_.value)
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

    // 将已预留帧标记为 committed 并唤醒消费者。
    void commit(const flow::Claim& claim) noexcept {
      const std::uint32_t committed_meta = claim.meta | frame::FLAG_COMMITTED;
      detail::store_meta_release(channel_->ring_, claim.start_pos, committed_meta);
      std::atomic_ref<std::uint32_t>(channel_->wait_word_)
          .fetch_add(1, std::memory_order_release);
      channel_->wait_.wake(&channel_->wait_word_);
    }

    // 将 payload 复制到预留帧并提交。
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
    MpscChannel* channel_;
    std::uint64_t cached_head_ = 0;
  };

  // 单消费者端点；按位置顺序观察帧提交标记。
  // 不会越过已声明但未提交的空洞。
  class Rx {
   public:
    // 将消费者端点绑定到父 MPSC 通道。
    explicit Rx(MpscChannel& channel) noexcept : channel_(&channel) {}

    // 非阻塞尝试读取下一条已按序提交的帧。
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

    // 等待直到下一条已按序提交的帧可用。
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

    // 释放已消费消息并推进生产者可见的 head。
    void release(const flow::Message& message) noexcept { advance(message.next_position); }

   private:
    // 以 release 语义发布新的消费者 head。
    void advance(std::uint64_t new_head) noexcept {
      // 安全性：release 发布消费者进度给所有生产者 Tx。
      // 生产者复用对应 ring 字节前会 acquire-load 该值。
      std::atomic_ref<std::uint64_t>(channel_->consumer_pos_.value)
          .store(new_head, std::memory_order_release);
    }

    MpscChannel* channel_;
    std::uint64_t cached_tail_ = 0;
  };
};

}  // namespace salias::channel

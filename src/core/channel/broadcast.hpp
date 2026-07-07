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

// 根据 start_pos 构造带 generation 的已提交 broadcast metadata。
inline std::uint32_t broadcast_meta(std::uint64_t start_pos, std::size_t cap) noexcept {
  const auto generation = static_cast<std::uint32_t>((start_pos / cap) & 0x00FF'FFFFu);
  return (generation << 8) | frame::FLAG_BEGIN | frame::FLAG_END | frame::FLAG_COMMITTED;
}

// 判断所有订阅者释放进度后是否仍有 need 字节可复用空间。
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

  using CreateResult = std::expected<BroadcastChannel, ChannelError>;
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // 创建进程内可靠 broadcast 通道。
  static CreateResult create(const ChannelConfig& config) noexcept {
    if (config.capacity == 0 || config.fixed_size || MaxSubscribers == 0) {
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

    return BroadcastChannel(std::move(ring).value(), Wait{});
  }

  // 移动 broadcast 通道及其持有的 ring 映射。
  BroadcastChannel(BroadcastChannel&&) noexcept = default;
  // 移动赋值 broadcast 通道及其持有的 ring 映射。
  BroadcastChannel& operator=(BroadcastChannel&&) noexcept = default;
  // 禁止拷贝；通道独占 ring 存储。
  BroadcastChannel(const BroadcastChannel&) = delete;
  // 禁止拷贝赋值；通道独占 ring 存储。
  BroadcastChannel& operator=(const BroadcastChannel&) = delete;

  // 返回逻辑 ring 容量，单位为字节。
  std::size_t capacity() const noexcept { return ring_.capacity(); }

  // 创建唯一 broadcast 生产者端点。
  Tx tx() noexcept { return Tx(*this); }

  // 分配订阅者索引，并将其 head 初始化到当前生产者 tail。
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

  // 创建订阅端点；达到订阅者上限时返回无效端点。
  Rx subscribe() noexcept {
    auto index = subscribe_index();
    if (!index) {
      return Rx();
    }
    return Rx(*this, *index);
  }

  // 为已有订阅者索引创建订阅端点。
  Rx rx(std::uint32_t index) noexcept { return Rx(*this, index); }

 private:
  // 保存已创建的 ring 和等待策略。
  explicit BroadcastChannel(ring::MagicRing ring, Wait wait) noexcept
      : ring_(std::move(ring)), wait_(std::move(wait)) {}

  // 返回最慢活跃订阅者的 head；无订阅者时返回 tail。
  std::uint64_t min_consumer_head(std::uint64_t tail) const noexcept {
    auto& mutable_count = const_cast<std::uint32_t&>(subscriber_count_);
    const auto count =
        std::atomic_ref<std::uint32_t>(mutable_count).load(std::memory_order_acquire);
    if (count == 0) {
      return tail;
    }

    std::uint64_t min_head = std::numeric_limits<std::uint64_t>::max();
    for (std::uint32_t i = 0; i < count && i < MaxSubscribers; ++i) {
      // 安全性：每个订阅者 head 只由对应 Rx 以 release 语义推进。
      // 对所有活跃 head 做 acquire 读取后，生产者覆盖边界就是最慢订阅者位置。
      auto& mutable_head = const_cast<std::uint64_t&>(subscriber_heads_[i].value);
      const auto head =
          std::atomic_ref<std::uint64_t>(mutable_head).load(std::memory_order_acquire);
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
  // 单 broadcast 生产者；写入一份物理帧。
  // ring 空间复用由最慢订阅者 head 决定。
  class Tx {
   public:
    // 将生产者端点绑定到父 broadcast 通道。
    explicit Tx(BroadcastChannel& channel) noexcept : channel_(&channel) {}

    // 在所有订阅者仍有足够剩余空间时预留一条 broadcast 帧。
    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || channel_->capacity() == 0) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::size_t need = frame::frame_len(payload_len);
      if (need > channel_->capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::uint64_t tail = channel_->producer_pos_.value;
      if (!detail::broadcast_has_capacity(channel_->capacity(), tail, cached_min_head_, need)) {
        cached_min_head_ = channel_->min_consumer_head(tail);
        if (!detail::broadcast_has_capacity(channel_->capacity(), tail, cached_min_head_, need)) {
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      return flow::Claim{
          .payload = channel_->ring_.slice_mut(tail + frame::kHeaderSize, payload_len),
          .start_pos = tail,
          .payload_len = payload_len,
          .meta = detail::broadcast_meta(tail, channel_->capacity()),
      };
    }

    // 发布 broadcast 帧并唤醒活跃订阅者。
    void commit(const flow::Claim& claim) noexcept {
      std::array<std::byte, frame::kHeaderSize> header{};
      frame::encode_header(header, claim.payload_len, claim.meta);
      auto header_dst = channel_->ring_.slice_mut(claim.start_pos, frame::kHeaderSize);
      std::memcpy(header_dst.data(), header.data(), header.size());

      // 安全性：唯一生产者独占 producer_pos_；帧头和 payload 在 release-store 前写完。
      // 每个订阅者读取帧前都会 acquire-load producer_pos_。
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

    // 将 payload 复制到 broadcast 帧并提交。
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
    BroadcastChannel* channel_;
    std::uint64_t cached_min_head_ = 0;
  };

  // broadcast 订阅者；每个 Rx 拥有独立 head 单元，可独立消费。
  class Rx {
   public:
    // 创建无效订阅端点。
    Rx() noexcept = default;
    // 将订阅端点绑定到父通道和订阅者索引。
    Rx(BroadcastChannel& channel, std::uint32_t index) noexcept
        : channel_(&channel), index_(index) {}

    // 非阻塞尝试接收当前订阅者的下一条帧。
    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || index_ >= MaxSubscribers) {
        return std::nullopt;
      }

      const std::uint64_t head = channel_->subscriber_heads_[index_].value;
      if (head >= cached_tail_) {
        // 安全性：生产者写完帧后以 release 语义发布 producer_pos_。
        // 这里的 acquire load 保证解码帧头和读取 payload 前可见完整帧字节。
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

    // 等待直到当前订阅者的下一条帧可用。
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

    // 释放当前订阅者索引收到的帧。
    void release(const flow::Message& message) noexcept {
      if (channel_ == nullptr || index_ >= MaxSubscribers) {
        return;
      }
      // 安全性：每个订阅者 head 只归一个 Rx 所有。
      // 生产者计算可靠 broadcast 覆盖边界时会 acquire-load 所有 head。
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

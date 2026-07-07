#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>

#include "core/channel/channel_config.hpp"
#include "core/channel/error.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/platform/mapping.hpp"
#include "core/ring/cache_aligned.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <wait::WaitStrategy Wait = wait::SpinPause>
class SpscChannel {
 public:
  using CreateResult = std::expected<SpscChannel, ChannelError>;
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // 创建进程内 SPSC 通道，内部包含双映射 ring 和本地位置单元。
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

    return SpscChannel(std::move(ring).value(), Wait{});
  }

  // 移动通道及其持有的 ring 映射。
  SpscChannel(SpscChannel&&) noexcept = default;
  // 移动赋值通道及其持有的 ring 映射。
  SpscChannel& operator=(SpscChannel&&) noexcept = default;
  // 禁止拷贝；通道独占 ring 存储。
  SpscChannel(const SpscChannel&) = delete;
  // 禁止拷贝赋值；通道独占 ring 存储。
  SpscChannel& operator=(const SpscChannel&) = delete;

  // 返回逻辑 ring 容量，单位为字节。
  std::size_t capacity() const noexcept { return ring_.capacity(); }

  // 创建借用当前通道的单生产者端点。
  Tx tx() noexcept { return Tx(*this); }
  // 创建借用当前通道的单消费者端点。
  Rx rx() noexcept { return Rx(*this); }

 private:
  // 保存已创建的 ring 和等待策略。
  explicit SpscChannel(ring::MagicRing ring, Wait wait) noexcept
      : ring_(std::move(ring)), wait_(std::move(wait)) {}

  // 为 flow 层构造非持有的位置指针。
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
  // 发送端点借用父通道；调用者必须保证父通道生命周期覆盖 Tx。
  // 每个 Tx 只允许单生产者使用，不能被多个写入者并发调用。
  class Tx {
   public:
    // 将生产者端点绑定到父 SPSC 通道。
    explicit Tx(SpscChannel& channel) noexcept
        : channel_(&channel), producer_(channel.ring_, channel.positions()) {}

    // 预留 payload 字节但不发布帧。
    flow::Producer::ClaimResult claim(std::uint32_t len) noexcept { return producer_.claim(len); }

    // 发布此前预留的 claim，并唤醒接收端。
    void commit(const flow::Claim& claim) noexcept {
      producer_.commit(claim);
      std::atomic_ref<std::uint32_t>(channel_->wait_word_)
          .fetch_add(1, std::memory_order_release);
      channel_->wait_.wake(&channel_->wait_word_);
    }

    // 将 payload 复制到预留帧并发布。
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
    SpscChannel* channel_;
    flow::Producer producer_;
  };

  // 接收端点借用父通道；调用者必须保证父通道生命周期覆盖 Rx。
  // 每个 Rx 只允许单消费者使用，不能被多个读者并发调用。
  class Rx {
   public:
    // 将消费者端点绑定到父 SPSC 通道。
    explicit Rx(SpscChannel& channel) noexcept
        : channel_(&channel), consumer_(channel.ring_, channel.positions()) {}

    // 非阻塞尝试接收一条消息。
    std::optional<flow::Message> try_recv() noexcept { return consumer_.poll(); }

    // 等待直到消息可用并返回该消息。
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

    // 释放已消费消息，使生产者可复用空间。
    void release(const flow::Message& message) noexcept {
      consumer_.advance(message.next_position);
    }

   private:
    SpscChannel* channel_;
    flow::Consumer consumer_;
  };
};

}  // namespace salias::channel

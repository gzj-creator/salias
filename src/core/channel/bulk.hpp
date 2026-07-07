#pragma once

#include <expected>
#include <optional>
#include <span>
#include <utility>

#include "core/channel/channel_config.hpp"
#include "core/channel/error.hpp"
#include "core/channel/spsc.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <wait::WaitStrategy Wait = wait::SpinPause>
class BulkChannel {
 public:
  using CreateResult = std::expected<BulkChannel, ChannelError>;
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // 创建基于 SPSC 实现的 bulk 通道。
  static CreateResult create(const ChannelConfig& config) noexcept {
    if (config.fixed_size) {
      return std::unexpected(ChannelError::BadConfig);
    }

    auto channel = SpscChannel<Wait>::create(config);
    if (!channel) {
      return std::unexpected(channel.error());
    }

    return BulkChannel(std::move(channel).value());
  }

  // 移动 bulk 通道及其底层 SPSC 通道。
  BulkChannel(BulkChannel&&) noexcept = default;
  // 移动赋值 bulk 通道及其底层 SPSC 通道。
  BulkChannel& operator=(BulkChannel&&) noexcept = default;
  // 禁止拷贝；底层通道独占存储。
  BulkChannel(const BulkChannel&) = delete;
  // 禁止拷贝赋值；底层通道独占存储。
  BulkChannel& operator=(const BulkChannel&) = delete;

  // 返回逻辑 ring 容量，单位为字节。
  std::size_t capacity() const noexcept { return channel_.capacity(); }

  // 创建 bulk 生产者端点。
  Tx tx() noexcept { return Tx(channel_.tx()); }
  // 创建 bulk 消费者端点。
  Rx rx() noexcept { return Rx(channel_.rx()); }

 private:
  // 封装已创建的 SPSC 通道。
  explicit BulkChannel(SpscChannel<Wait> channel) noexcept : channel_(std::move(channel)) {}

  SpscChannel<Wait> channel_;

 public:
  // bulk 生产者端点保留 SPSC claim/commit 约束。
  // 消息必须放入单帧，过大时返回 MessageTooLarge，不提供分片路径。
  class Tx {
   public:
    // 封装 SPSC 生产者端点。
    explicit Tx(typename SpscChannel<Wait>::Tx tx) noexcept : tx_(std::move(tx)) {}

    // 预留一个连续 bulk payload 帧。
    flow::Producer::ClaimResult claim(std::uint32_t len) noexcept { return tx_.claim(len); }
    // 发布此前预留的 bulk 帧。
    void commit(const flow::Claim& claim) noexcept { tx_.commit(claim); }
    // 将 payload 作为一个 bulk 帧发布。
    OfferResult offer(std::span<const std::byte> payload) noexcept { return tx_.offer(payload); }

   private:
    typename SpscChannel<Wait>::Tx tx_;
  };

  // bulk 消费者端点；L0 双映射保证返回的完整消息 payload 连续。
  class Rx {
   public:
    // 封装 SPSC 消费者端点。
    explicit Rx(typename SpscChannel<Wait>::Rx rx) noexcept : rx_(std::move(rx)) {}

    // 非阻塞尝试接收一条 bulk 消息。
    std::optional<flow::Message> try_recv() noexcept { return rx_.try_recv(); }
    // 等待直到一条 bulk 消息可用。
    flow::Message recv() noexcept { return rx_.recv(); }
    // 释放已消费的 bulk 消息。
    void release(const flow::Message& message) noexcept { rx_.release(message); }

   private:
    typename SpscChannel<Wait>::Rx rx_;
  };
};

}  // namespace salias::channel

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <utility>

#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/position.hpp"
#include "core/flow/producer.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

namespace salias::channel {

template <wait::WaitStrategy Wait = wait::SpinPause>
class SharedSpscChannel {
 public:
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  // 封装共享 SPSC ring、外部持有的位置单元和可选 wait word。
  SharedSpscChannel(ring::MagicRing ring, flow::Positions positions,
                    std::uint32_t* wait_word, Wait wait = Wait{}) noexcept
      : ring_(std::move(ring)),
        positions_(positions),
        wait_word_(wait_word),
        wait_(std::move(wait)) {}

  // 移动共享通道包装器及其持有的 ring 映射。
  SharedSpscChannel(SharedSpscChannel&&) noexcept = default;
  // 移动赋值共享通道包装器及其持有的 ring 映射。
  SharedSpscChannel& operator=(SharedSpscChannel&&) noexcept = default;
  // 禁止拷贝；包装器持有 ring 映射。
  SharedSpscChannel(const SharedSpscChannel&) = delete;
  // 禁止拷贝赋值；包装器持有 ring 映射。
  SharedSpscChannel& operator=(const SharedSpscChannel&) = delete;

  // 返回逻辑 ring 容量，单位为字节。
  std::size_t capacity() const noexcept { return ring_.capacity(); }

  // 创建借用当前共享通道的发送端点。
  Tx tx() noexcept { return Tx(*this); }
  // 创建借用当前共享通道的接收端点。
  Rx rx() noexcept { return Rx(*this); }

 private:
  ring::MagicRing ring_;
  flow::Positions positions_;
  std::uint32_t* wait_word_ = nullptr;
  Wait wait_;

 public:
  // 基于外部共享位置单元的发送端点；父通道持有 ring 映射。
  // 包含位置和 wait word 的控制块生命周期必须覆盖 Tx。
  class Tx {
   public:
    // 将发送端点绑定到父 shared SPSC 通道。
    explicit Tx(SharedSpscChannel& channel) noexcept
        : channel_(&channel), producer_(channel.ring_, channel.positions_) {}

    // 预留 payload 字节但不发布帧。
    flow::Producer::ClaimResult claim(std::uint32_t len) noexcept { return producer_.claim(len); }

    // 发布此前预留的 claim，并唤醒 futex/spin 等待方。
    void commit(const flow::Claim& claim) noexcept {
      producer_.commit(claim);
      if (channel_->wait_word_ != nullptr) {
        std::atomic_ref<std::uint32_t>(*channel_->wait_word_)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(channel_->wait_word_);
      }
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
    SharedSpscChannel* channel_;
    flow::Producer producer_;
  };

  // 基于外部共享位置单元的接收端点；单消费者约束与 SpscChannel::Rx 相同。
  // 位置单元可以位于另一个进程可见的映射中。
  class Rx {
   public:
    // 将接收端点绑定到父 shared SPSC 通道。
    explicit Rx(SharedSpscChannel& channel) noexcept
        : channel_(&channel), consumer_(channel.ring_, channel.positions_) {}

    // 非阻塞尝试接收一条消息。
    std::optional<flow::Message> try_recv() noexcept { return consumer_.poll(); }

    // 等待直到消息可用；配置 wait_word_ 时使用它等待。
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

    // 释放已消费消息，使生产者可复用空间。
    void release(const flow::Message& message) noexcept {
      consumer_.advance(message.next_position);
    }

   private:
    SharedSpscChannel* channel_;
    flow::Consumer consumer_;
  };
};

}  // namespace salias::channel

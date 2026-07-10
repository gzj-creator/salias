#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "core/flow/producer.hpp"
#include "salias/config.hpp"
#include "salias/error.hpp"

namespace salias {

template <Mode M>
class Channel;
template <Mode M>
struct ChannelState;
template <Mode M>
struct PublisherEndpoint;
template <Mode M>
class Publisher;

template <Mode M>
class PublishClaim {
 public:
  /// 转移一个尚未提交的发布 claim；来源对象变为无效，不能再提交。
  PublishClaim(PublishClaim&& other) noexcept;
  /// 释放当前未提交 claim 的句柄后，接管另一个 claim。
  PublishClaim& operator=(PublishClaim&& other) noexcept;

  PublishClaim(const PublishClaim&) = delete;
  PublishClaim& operator=(const PublishClaim&) = delete;

  /// 返回已预留的 ring 内 payload 写入区。
  /// 调用方必须在 commit() 前完成写入，且不能在 claim 对象销毁后继续持有该 span。
  std::span<std::byte> payload() noexcept;

  /// 发布当前 claim；重复调用、moved-from 对象或默认无效对象会被忽略。
  void commit() noexcept;

 private:
  /// 创建绑定到共享通道状态的已预留 claim。
  PublishClaim(std::shared_ptr<PublisherEndpoint<M>> endpoint, flow::Claim claim) noexcept;

  std::shared_ptr<PublisherEndpoint<M>> endpoint_;
  flow::Claim claim_{};
  bool committed_ = true;

  friend class Publisher<M>;
};

template <Mode M>
class Publisher {
 public:
  /// 预留一条消息的 ring 内 payload 写入区。
  /// 成功后调用方必须写入 payload 并调用 PublishClaim::commit() 发布消息。
  Result<PublishClaim<M>> try_claim(std::size_t payload_len) noexcept;

  /// 尝试把 payload 作为一条消息发布。
  /// 返回 true 表示发布成功；背压或消息过大时返回对应 Error。
  Result<bool> offer(std::span<const std::byte> payload) noexcept;

  /// 尝试按顺序发布一批消息。
  /// 返回已发布消息数；如果第一条都无法发布，返回背压或消息过大错误。
  Result<std::size_t> offer_batch(std::span<const std::span<const std::byte>> payloads) noexcept;

 private:
  /// 创建绑定到共享通道状态的发布端。
  explicit Publisher(std::shared_ptr<PublisherEndpoint<M>> endpoint) noexcept;

  std::shared_ptr<PublisherEndpoint<M>> endpoint_;

  friend class Channel<M>;
};

using FifoMpscPublisher = Publisher<Mode::FifoMpsc>;
using FifoFanoutPublisher = Publisher<Mode::FifoFanout>;
using OrderedMpscPublisher = Publisher<Mode::OrderedMpsc>;
using OrderedFanoutPublisher = Publisher<Mode::OrderedFanout>;

}  // namespace salias

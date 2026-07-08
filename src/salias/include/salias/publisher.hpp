#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "salias/error.hpp"

namespace salias {

class Channel;
struct ChannelState;

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
  PublishClaim(std::shared_ptr<ChannelState> state, std::span<std::byte> payload,
               std::uint64_t start_pos, std::uint32_t payload_len, std::uint32_t meta) noexcept;

  std::shared_ptr<ChannelState> state_;
  std::span<std::byte> payload_;
  std::uint64_t start_pos_ = 0;
  std::uint32_t payload_len_ = 0;
  std::uint32_t meta_ = 0;
  bool committed_ = true;

  friend class Publisher;
};

class Publisher {
 public:
  /// 预留一条消息的 ring 内 payload 写入区。
  /// 成功后调用方必须写入 payload 并调用 PublishClaim::commit() 发布消息。
  Result<PublishClaim> try_claim(std::size_t payload_len) noexcept;

  /// 尝试把 payload 作为一条消息发布。
  /// 返回 true 表示发布成功；背压或消息过大时返回对应 Error。
  Result<bool> offer(std::span<const std::byte> payload) noexcept;

 private:
  /// 创建绑定到共享通道状态的发布端。
  explicit Publisher(std::shared_ptr<ChannelState> state) noexcept;

  std::shared_ptr<ChannelState> state_;

  friend class Channel;
};

}  // namespace salias

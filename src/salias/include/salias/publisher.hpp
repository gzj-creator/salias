#pragma once

#include <cstddef>
#include <memory>
#include <span>

#include "salias/error.hpp"

namespace salias {

class Channel;
struct ChannelState;

class Publisher {
 public:
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

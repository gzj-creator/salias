#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "salias/message.hpp"

namespace salias {

class Channel;
struct ChannelState;

class Subscriber {
 public:
  /// 非阻塞尝试接收一条已提交消息。
  /// 当前无可读消息时返回 std::nullopt。
  std::optional<Message> try_recv() noexcept;

  /// 等待直到有一条已提交消息可读，并返回借用的 payload 视图。
  /// 调用者处理完后必须通过 release() 释放消息占用的环形空间。
  Message recv() noexcept;

  /// 释放此前收到的消息，使通道可以复用对应环形空间。
  /// 传入其他通道的消息不属于受支持用法。
  void release(const Message& message) noexcept;

 private:
  /// 创建绑定到共享通道状态的订阅端，可携带 broadcast 订阅索引。
  explicit Subscriber(std::shared_ptr<ChannelState> state,
                      std::uint32_t subscription_index = 0) noexcept;

  std::shared_ptr<ChannelState> state_;
  std::uint32_t subscription_index_ = 0;

  friend class Channel;
};

}  // namespace salias

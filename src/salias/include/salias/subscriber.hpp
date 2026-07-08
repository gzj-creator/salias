#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>

#include "salias/message.hpp"

namespace salias {

class Channel;
struct ChannelState;

class Subscriber {
 public:
  using PollCallback = void (*)(const Message& message, void* user) noexcept;

  /// 非阻塞尝试接收一条已提交消息。
  /// 当前无可读消息时返回 std::nullopt。
  std::optional<Message> try_recv() noexcept;

  /// 等待直到有一条已提交消息可读，并返回借用的 payload 视图。
  /// 调用者处理完后必须通过 release() 释放消息占用的环形空间。
  Message recv() noexcept;

  /// 释放此前收到的消息，使通道可以复用对应环形空间。
  /// 传入其他通道的消息不属于受支持用法。
  void release(const Message& message) noexcept;

  /// 批量轮询最多 max_messages 条消息，并在回调返回后自动释放每条消息。
  /// callback 收到的 payload 只在当前回调期间有效，调用者不能在回调后继续持有该视图。
  std::size_t poll(std::uint32_t max_messages, PollCallback callback, void* user) noexcept;

  /// 使用不抛异常的 C++ callable 批量轮询消息。
  template <class Handler>
  std::size_t poll(std::uint32_t max_messages, Handler&& handler) noexcept {
    using HandlerType = std::remove_reference_t<Handler>;
    static_assert(std::is_nothrow_invocable_v<HandlerType&, const Message&>,
                  "Subscriber::poll handler must be noexcept and accept const Message&");
    return poll(max_messages,
                [](const Message& message, void* user) noexcept {
                  (*static_cast<HandlerType*>(user))(message);
                },
                static_cast<void*>(std::addressof(handler)));
  }

 private:
  /// 创建绑定到共享通道状态的订阅端，可携带 broadcast 订阅索引。
  explicit Subscriber(std::shared_ptr<ChannelState> state,
                      std::uint32_t subscription_index = 0) noexcept;

  std::shared_ptr<ChannelState> state_;
  std::uint32_t subscription_index_ = 0;

  friend class Channel;
};

}  // namespace salias

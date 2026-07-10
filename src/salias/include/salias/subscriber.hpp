#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>

#include "salias/config.hpp"
#include "salias/message.hpp"

namespace salias {

template <Mode M>
class Channel;
template <Mode M>
struct ChannelState;
template <Mode M>
struct SubscriberEndpoint;

template <Mode M>
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
  /// handler 在头文件循环内内联执行，避免跨 .so 边界的每消息间接调用；进度按批 flush。
  template <class Handler>
  std::size_t poll(std::uint32_t max_messages, Handler&& handler) noexcept {
    using HandlerType = std::remove_reference_t<Handler>;
    static_assert(std::is_nothrow_invocable_v<HandlerType&, const Message&>,
                  "Subscriber::poll handler must be noexcept and accept const Message&");
    if (max_messages == 0) {
      return 0;
    }
    constexpr std::uint32_t kChunk = 32;
    Message buffer[kChunk];
    std::size_t consumed = 0;
    while (consumed < max_messages) {
      const std::uint32_t remaining = static_cast<std::uint32_t>(
          std::min<std::size_t>(kChunk, max_messages - consumed));
      const std::size_t got = fetch_batch(buffer, remaining);
      if (got == 0) {
        break;
      }
      for (std::size_t i = 0; i < got; ++i) {
        handler(buffer[i]);
      }
      flush_batch();
      consumed += got;
    }
    return consumed;
  }

 private:
  /// 批量取回最多 cap 条已提交消息到 out[]，只推进消费者本地进度，不发布给生产者。
  /// 返回取回条数；返回 0 表示当前无可读消息。配合 flush_batch() 使用。
  std::size_t fetch_batch(Message* out, std::uint32_t cap) noexcept;

  /// 把本批消费进度发布给生产者，使其可回收环空间。
  void flush_batch() noexcept;

  /// 创建绑定到共享通道状态的订阅端，可携带 MPMC fanout 订阅索引。
  explicit Subscriber(std::shared_ptr<SubscriberEndpoint<M>> endpoint) noexcept;

  std::shared_ptr<SubscriberEndpoint<M>> endpoint_;

  friend class Channel<M>;
};

using FifoMpscSubscriber = Subscriber<Mode::FifoMpsc>;
using FifoFanoutSubscriber = Subscriber<Mode::FifoFanout>;
using OrderedMpscSubscriber = Subscriber<Mode::OrderedMpsc>;
using OrderedFanoutSubscriber = Subscriber<Mode::OrderedFanout>;

}  // namespace salias

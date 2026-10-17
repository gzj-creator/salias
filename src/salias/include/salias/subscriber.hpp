/**
 * @file src/salias/include/salias/subscriber.hpp
 * @brief salias 公共 API 的订阅端：单条/批量接收与轮询接口。
 * @details 位于 L7 公共外观层，Subscriber 是消费者侧的用户入口，封装 L3 flow 层的
 *          consumer 引擎。提供三种接收路径：
 *          - try_recv()：非阻塞单条接收，返回借用视图，需配合 release() 释放环空间。
 *          - recv()：阻塞等待一条可读消息（经 L4 wait strategy 自旋/退避）。
 *          - poll()：批量轮询，分函数指针回调和模板 callable 两种重载。
 *          模板 callable 重载内联于头文件，避免跨 .so 边界的每消息间接调用，
 *          且按批（kChunk=32）累积进度后统一 flush，减少对生产者的可见进度更新频率，
 *          降低缓存行争用。fanout 模式下每个 Subscriber 携带独立订阅索引，实现多消费者
 *          各自独立消费进度。通过 std::shared_ptr<SubscriberEndpoint> 共享底层通道状态。
 */
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/message.hpp"

/// salias 公共 API 命名空间，聚合 L7 用户可见的通道、发布者、订阅者与消息类型。
namespace salias {

// 前向声明：避免循环包含，实现细节定义于 L5 channel / L3 flow 实现层。
template <Mode M, class Schema>
class Channel;
template <Mode M>
struct ChannelState;
template <Mode M>
struct SubscriberEndpoint;
template <Mode M, class Schema = RawPayloadSchema>
class Subscriber;

/**
 * @brief 订阅端，从通道读取消息的消费者接口。
 * @details 封装 L3 flow 层 consumer 引擎，通过 shared_ptr<SubscriberEndpoint> 共享
 *          通道状态。每个端点持有带 generation 的订阅注册；Subscriber 拷贝共享同一注册，
 *          最后一个副本析构时注销并释放槽位。接收到的 Message 为借用视图，
 *          需通过 release() 释放对应环空间以供生产者回收复用。
 */
template <Mode M>
class Subscriber<M, RawPayloadSchema> {
 public:
  Subscriber() noexcept = default;

  /// 批量轮询使用的 C 风格函数指针回调类型；user 为透传的用户上下文指针。
  using PollCallback = void (*)(const Message& message, void* user) noexcept;

  /**
   * @brief 非阻塞尝试接收一条已提交消息。
   * @return 已提交消息的借用视图，或当前无可读消息时返回 std::nullopt。
   * @note 返回的 Message 需后续通过 release() 释放环空间。
   *       LatestMpmc 在同一 producer ring 有未释放借用时跳过该 ring，仍可读取其他 ring。
   */
  std::optional<Message> try_recv() noexcept;

  /**
   * @brief 显式报告 DropOldest 订阅端丢旧的非阻塞接收。
   * @return Message、成功但为空（暂无消息），或 Error::Lagged / Error::BadConfig。
   * @note Lagged 会推进到保留窗口起点，但不返回或借用消息；调用方重试即可接收。
   *       Reliable 订阅端保持背压语义，不会生成 Lagged。无效端点返回 BadConfig。
   */
  Result<std::optional<Message>> try_recv_checked() noexcept;

  /**
   * @brief 按固定类型复制接收一条消息，并自动释放共享 ring 槽位。
   * @tparam T 可按对象表示复制的固定大小消息类型。
   * @return 本地拥有的 T；无消息时返回空 optional。
   * @retval Error::PayloadSizeMismatch payload 大小与 sizeof(T) 不一致；该消息已被消费并释放。
   * @retval Error::Lagged LatestMpmc 丢旧订阅端检测到游标落后于保留窗口。
   * @note 该接口不为消息分配堆内存，但会产生一次 payload memcpy。T 及其成员不得包含进程内指针。
   */
  template <detail::WireMessage T>
  Result<std::optional<T>> try_recv_copy() noexcept {
    auto received = try_recv_checked();
    if (!received) {
      return std::unexpected(received.error());
    }
    if (!*received) {
      return std::optional<T>{};
    }

    return copy_received<T>(**received);
  }

  /// @return DropOldest 端点已观测到的淘汰条数；Reliable 端点始终为 0。
  std::uint64_t lagged_messages() const noexcept;
  /// @return DropOldest 端点主动跳过的保留消息条数；Reliable 端点始终为 0。
  std::uint64_t skipped_messages() const noexcept;
  /// @return 此端点在当前进程中跳过的过期消息数。
  std::uint64_t expired_messages() const noexcept;

  /**
   * @brief 将 DropOldest 订阅端的指定 producer ring 推进到最新已提交帧，保留该帧未读。
   * @return 推进的消息条数（淘汰 + 主动跳过），或 BackPressured / BadConfig。
   * @note 有未释放借用或淘汰竞争时返回 BackPressured，不改变进度或计数。
   *       仅支持 LatestMpmc 的 DropOldest 订阅端；Reliable 端点、非法索引、无效端点和
   *       其他模式返回 BadConfig。
   *       仅对该 ring 的已发布快照生效，后续写入仍可能淘汰目标帧。
   */
  Result<std::uint64_t> skip_to_latest(std::uint32_t producer_id = 0) noexcept;

  /**
   * @brief 阻塞等待直到有一条已提交消息可读，并返回借用的 payload 视图。
   * @return 可读消息的借用视图（始终有效，因会等待至有消息）。
   * @note 调用者处理完后必须通过 release() 释放消息占用的环形空间。
   *       内部经 L4 wait strategy（自旋 + 退避）等待生产者发布。
   */
  Message recv() noexcept;

  /**
   * @brief 阻塞等待并按固定类型复制接收一条消息。
   * @tparam T 可按对象表示复制的固定大小消息类型。
   * @return 本地拥有的 T；不可恢复错误直接返回。
   * @retval Error::PayloadSizeMismatch 首条收到的消息长度不匹配；该消息已释放。
   * @retval Error::BadConfig 当前端点无效。
   * @note 与 try_recv_copy() 一样不分配堆内存，返回值脱离共享 ring 生命周期有效。
   */
  template <detail::WireMessage T>
  Result<T> recv_copy() noexcept {
    if (!valid()) {
      return std::unexpected(Error::BadConfig);
    }

    const Message message = recv();
    return copy_received<T>(message);
  }

  /**
   * @brief 释放此前收到的消息，使通道可以复用对应环形空间。
   * @param message 此前从本 Subscriber 收到的消息引用。
   * @note 传入其他通道的消息不属于受支持用法，行为未定义。
   *       release 推进消费者进度，使生产者可回收对应 ring 槽位。
   */
  void release(const Message& message) noexcept;

  /**
   * @brief 批量轮询最多 max_messages 条消息，并在回调返回后自动释放每条消息。
   * @param max_messages 单次轮询的最大消息数上限。
   * @param callback 每条消息的回调函数指针。
   * @param user 透传给回调的用户上下文指针。
   * @return 实际处理的消息数。
   * @note callback 收到的 payload 只在当前回调期间有效，调用者不能在回调后继续持有。
   *       LatestMpmc 按最多 32 条的内部批次 pin，整批回调完成后统一释放。
   */
  std::size_t poll(std::uint32_t max_messages, PollCallback callback, void* user) noexcept;

  /**
   * @brief 使用不抛异常的 C++ callable 批量轮询消息。
   * @tparam Handler 可调用对象类型，须满足 noexcept 且接受 const Message&。
   * @param max_messages 单次轮询的最大消息数上限。
   * @param handler 每条消息的处理器，以转发引用传入。
   * @return 实际处理的消息数。
   * @note handler 在头文件循环内内联执行，避免跨 .so 边界的每消息间接调用；
   *       进度按批 flush（fetch_batch + flush_batch），减少对生产者的可见进度更新。
   *       LatestMpmc 按最多 32 条的内部批次 pin，整批回调完成后统一释放。
   */
  template <class Handler>
  std::size_t poll(std::uint32_t max_messages, Handler&& handler) noexcept {
    using HandlerType = std::remove_reference_t<Handler>;
    // 编译期校验 handler 签名：必须 noexcept 且接受 const Message&，
    // 保证热路径中不会抛出异常（核心引擎全程 noexcept 约束）。
    static_assert(std::is_nothrow_invocable_v<HandlerType&, const Message&>,
                  "Subscriber::poll handler must be noexcept and accept const Message&");
    if (max_messages == 0) {
      return 0;
    }
    // 按批处理，每批最多 kChunk 条；用栈上缓冲区暂存，统一 fetch 后逐条回调再 flush。
    constexpr std::uint32_t kChunk = 32;
    Message buffer[kChunk];
    std::size_t consumed = 0;
    while (consumed < max_messages) {
      // 计算本批应取条数：剩余需求与 kChunk 取较小值。
      const std::uint32_t remaining =
          static_cast<std::uint32_t>(std::min<std::size_t>(kChunk, max_messages - consumed));
      const std::size_t got = fetch_batch(buffer, remaining);
      if (got == 0) {
        break;  // 本批无可读消息，提前结束轮询。
      }
      for (std::size_t i = 0; i < got; ++i) {
        handler(buffer[i]);
      }
      // 整批处理完毕后再 flush 消费进度，减少对生产者进度字段的写次数（缓存行争用）。
      flush_batch();
      consumed += got;
    }
    return consumed;
  }

 private:
  bool valid() const noexcept;

  template <detail::WireMessage T>
  Result<T> copy_received(const Message& message) noexcept {
    if (message.payload.size() != sizeof(T)) {
      release(message);
      return std::unexpected(Error::PayloadSizeMismatch);
    }

    const T value = detail::copy_from_bytes<T>(message.payload);
    release(message);
    return Result<T>{std::in_place, value};
  }

  /// 批量取回最多 cap 条已提交消息到 out[]，只推进消费者本地进度，不发布给生产者。
  /// 返回取回条数；返回 0 表示当前无可读消息。配合 flush_batch() 使用。
  std::size_t fetch_batch(Message* out, std::uint32_t cap) noexcept;

  /// 把本批消费进度发布给生产者，使其可回收环空间。
  void flush_batch() noexcept;

  /// 创建绑定到共享通道状态的订阅端，可携带 MPMC fanout 订阅索引。
  explicit Subscriber(std::shared_ptr<SubscriberEndpoint<M>> endpoint) noexcept;

  Result<std::optional<Message>> try_recv_checked_fixed(std::uint32_t payload_size) noexcept;
  Message recv_fixed(std::uint32_t payload_size) noexcept;
  Result<std::uint64_t> skip_to_latest_fixed(std::uint32_t producer_id,
                                             std::uint32_t payload_size) noexcept;

  std::shared_ptr<SubscriberEndpoint<M>> endpoint_;  ///< 共享通道状态的订阅端句柄。

  template <Mode, class>
  friend class Channel;  ///< Channel 通过私有构造创建 Subscriber。
  template <Mode, class>
  friend class Subscriber;  ///< Schema-bound facade 使用固定 schema 的解析路径。
};

/** @brief Schema-bound subscriber that returns owned values and exposes no borrowed payload API. */
template <Mode M, detail::WireSchema Schema>
class Subscriber<M, Schema> {
 public:
  using T = typename Schema::message_type;

  Subscriber() noexcept = default;

  Result<std::optional<T>> try_recv_copy() noexcept {
    auto received = subscriber_.try_recv_checked_fixed(sizeof(T));
    if (!received) {
      return std::unexpected(received.error());
    }
    if (!*received) {
      return std::optional<T>{};
    }

    const T value = detail::copy_from_bytes<T>((**received).payload);
    subscriber_.release(**received);
    return std::optional<T>{value};
  }

  Result<T> recv_copy() noexcept {
    if (!subscriber_.valid()) {
      return std::unexpected(Error::BadConfig);
    }
    const Message message = subscriber_.recv_fixed(sizeof(T));
    const T value = detail::copy_from_bytes<T>(message.payload);
    subscriber_.release(message);
    return Result<T>{std::in_place, value};
  }

  std::uint64_t lagged_messages() const noexcept { return subscriber_.lagged_messages(); }
  std::uint64_t skipped_messages() const noexcept { return subscriber_.skipped_messages(); }
  std::uint64_t expired_messages() const noexcept { return subscriber_.expired_messages(); }

  Result<std::uint64_t> skip_to_latest(std::uint32_t producer_id = 0) noexcept {
    return subscriber_.skip_to_latest_fixed(producer_id, sizeof(T));
  }

 private:
  explicit Subscriber(Subscriber<M, RawPayloadSchema> subscriber) noexcept
      : subscriber_(std::move(subscriber)) {}

  Subscriber<M, RawPayloadSchema> subscriber_;

  template <Mode, class>
  friend class Channel;
};

using FifoMpscSubscriber = Subscriber<Mode::FifoMpsc>;        ///< FIFO MPSC 订阅端别名。
using FifoMpmcSubscriber = Subscriber<Mode::FifoMpmc>;  ///< FIFO 多生产者多消费者广播订阅端别名。
using OrderedMpscSubscriber = Subscriber<Mode::OrderedMpsc>;  ///< 保序 MPSC 订阅端别名。
using OrderedMpmcSubscriber =
    Subscriber<Mode::OrderedMpmc>;  ///< 保序多生产者多消费者广播订阅端别名。
using LatestMpmcSubscriber = Subscriber<Mode::LatestMpmc>;  ///< 丢旧 fanout 订阅端别名。

}  // namespace salias

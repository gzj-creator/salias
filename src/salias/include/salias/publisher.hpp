/**
 * @file src/salias/include/salias/publisher.hpp
 * @brief salias 公共 API 的发布端：零拷贝 claim/commit 与 offer/offer_batch 发布接口。
 * @details 位于 L7 公共外观层，Publisher 是生产者侧的用户入口，封装 L3 flow 层的
 *          producer 引擎。提供两种发布路径：
 *          - try_claim() + Publisher::commit()：零拷贝，先预留 ring 内 payload 写入区，
 *            用户直接写入共享内存，再由持有端点的 Publisher 提交，避免一次内存拷贝，
 *            适合低延迟热路径。
 *          - offer() / offer_batch()：便利接口，内部拷贝 payload 后提交，适合简单场景。
 *          Publisher 通过 std::shared_ptr<PublisherEndpoint> 共享底层通道状态，
 *          多个 Publisher 实例可绑定同一通道。线程安全由具体协议（MPSC/MPMC）保证。
 *          字节与消息数流控窗口在 claim/offer 时施加背压检查。
 */
#pragma once

#include <cassert>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>

#include "core/flow/producer.hpp"
#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/message.hpp"

/// salias 公共 API 命名空间，聚合 L7 用户可见的通道、发布者、订阅者与消息类型。
namespace salias {

// 前向声明：避免循环包含，实现细节定义于 L5 channel / L3 flow 实现层。
template <Mode M, class Schema = RawPayloadSchema>
class Channel;
template <Mode M>
struct ChannelState;
template <Mode M>
struct PublisherEndpoint;
template <Mode M, class Schema = RawPayloadSchema>
class Publisher;
template <Mode M, class Schema = RawPayloadSchema>
class PublishClaim;

/**
 * @brief 一次零拷贝发布的预留句柄（值对象，不拥有发布端）。
 * @details 由 Publisher::try_claim() 返回，仅保存 ring 内预留位置信息和发布端身份。
 *          用户通过 payload() 写入后必须调用原 Publisher 的 commit(std::move(claim))。
 *          该设计避免每条消息复制 shared_ptr 的原子引用计数。claim 不得比申请它的
 *          Publisher 活得更久；只有携带匹配身份的 Publisher 才能提交。
 *          乱序提交不会越过未提交的空洞，补齐后才连续发布；放弃未提交 claim 不受支持，
 *          会阻塞后续发布。仅可移动、不可拷贝。
 *          committed_ 默认为 true 表示"无需提交"的无效/moved-from 状态；真实 claim
 *          构造时置为 false，提交后置 true。调试构建中未提交即析构会触发断言。
 */
template <Mode M>
class PublishClaim<M, RawPayloadSchema> {
 public:
  /// @brief 转移一个尚未提交的发布 claim；来源对象变为无效，不能再提交。
  /// @param other 被转移的 claim，转移后处于 moved-from 状态。
  PublishClaim(PublishClaim&& other) noexcept;
  /// @brief 释放当前未提交 claim 的句柄后，接管另一个 claim。
  /// @param other 被转移的 claim。
  /// @return *this。
  PublishClaim& operator=(PublishClaim&& other) noexcept;

  PublishClaim(const PublishClaim&) = delete;  ///< 禁止拷贝：claim 独占预留区。
  PublishClaim& operator=(const PublishClaim&) = delete;  ///< 禁止拷贝赋值。

  /// @brief 销毁未提交 claim 时在调试构建中断言；发布端生命周期由调用方保证。
  ~PublishClaim() noexcept;

  /**
   * @brief 返回已预留的 ring 内 payload 写入区。
   * @return 指向共享内存 ring 内写入区的可变 span。
   * @note 调用方必须在 Publisher::commit() 前完成写入，且不能在 claim 对象销毁后继续持有该 span。
   *       span 指向的内存位于共享内存段，映射在所有进程地址空间。
   */
  std::span<std::byte> payload() noexcept;

 private:
  /// 创建仅携带发布端身份与预留位置的 claim；发布端由调用方保证存活。
  PublishClaim(const PublisherEndpoint<M>* owner, flow::Claim claim) noexcept;

  /// @brief 返回预留的底层 flow::Claim；仅供 Publisher::commit() 使用。
  flow::Claim& claim_handle() noexcept { return claim_; }

  /// @brief 返回申请该 claim 的发布端身份；仅用于归属校验，不解引用。
  const PublisherEndpoint<M>* owner() const noexcept { return owner_; }

  /// @brief 标记 claim 已提交并清空预留描述，避免重复提交。
  void mark_committed() noexcept {
    owner_ = nullptr;
    claim_ = {};
    committed_ = true;
  }

  const PublisherEndpoint<M>* owner_ = nullptr;  ///< 非拥有发布端身份令牌，不参与生命周期管理。
  flow::Claim claim_{};                          ///< L3 flow 层的底层预留描述符。
  bool committed_ = true;  ///< 是否已提交；默认 true 表示无效默认态，真实 claim 为 false。

  friend class Publisher<M, RawPayloadSchema>;  ///< Publisher 通过私有构造创建 PublishClaim。
};

/**
 * @brief 发布端，向通道写入消息的生产者接口。
 * @details 封装 L3 flow 层 producer 引擎，通过 shared_ptr<PublisherEndpoint> 共享
 *          通道状态。提供零拷贝（try_claim）与拷贝（offer/offer_batch）两种发布路径。
 *          由 Channel::publisher() 创建，对同一通道可创建多个 Publisher 实例。
 */
template <Mode M>
class Publisher<M, RawPayloadSchema> {
 public:
  /**
   * @brief 零拷贝预留一条消息的 ring 内 payload 写入区。
   * @param payload_len 期望的负载字节数。
   * @retval PublishClaim 预留成功，用户写入后需调用 Publisher::commit()。
   * @retval Error::BackPressured 字节/消息数流控窗口达上限，或环形缓冲已满。
   * @retval Error::MessageTooLarge payload_len 超过环容量，或 LatestMpmc 单帧超过保留水位。
   * @note 成功后调用方必须写入 payload 并调用 Publisher::commit() 发布消息。
   */
  Result<PublishClaim<M>> try_claim(std::size_t payload_len) noexcept;

  /**
   * @brief 预留一条带 TTL 的消息；截止时间在调用时采样，之后填充 payload 的时间也计入 TTL。
   * @param payload_len payload 字节数。
   * @param ttl 非负时长；0 表示截止时间立即到达。
   * @retval PublishClaim 成功后写入 payload 并调用 Publisher::commit()。
   * @retval Error::BadConfig ttl 为负数。
   * @note 截止时间存为同主机 CLOCK_MONOTONIC 纳秒值，发布端和订阅端必须运行于同一系统启动周期。
   */
  Result<PublishClaim<M>> try_claim_for(std::size_t payload_len,
                                        std::chrono::nanoseconds ttl) noexcept;

  /**
   * @brief 提交由当前 Publisher 申请的 claim，使帧对外可见。
   * @param claim 已填充的 claim；通过移动语义转移，提交后失效。
   * @pre claim 必须由当前 Publisher（或其副本）申请，且 Publisher 必须存活。
   * @note 归属不匹配时调试构建触发断言，发布构建拒绝提交且不访问其他发布端。
   *       提交本身使用当前 Publisher 的端点，claim 不再持有 shared_ptr。
   */
  void commit(PublishClaim<M, RawPayloadSchema>&& claim) noexcept;

  /**
   * @brief 尝试把 payload 作为一条消息发布（内部拷贝后提交）。
   * @param payload 待发布的负载数据视图。
   * @retval true 发布成功。
   * @retval Error::BackPressured 字节/消息数流控窗口达上限，或当前无法复用环空间。
   * @retval Error::MessageTooLarge payload 超过环容量，或 LatestMpmc 单帧超过保留水位。
   */
  Result<bool> offer(std::span<const std::byte> payload) noexcept;

  /** @brief 发布带 TTL 的消息；截止时间在调用时采样，0 表示立即到期。 */
  Result<bool> offer_for(std::span<const std::byte> payload, std::chrono::nanoseconds ttl) noexcept;

  /**
   * @brief 按固定类型发布一条消息。
   * @tparam T 可按对象表示复制的固定大小消息类型。
   * @param value 待复制到共享 ring 的本地值。
   * @retval true 发布成功。
   * @retval Error::BackPressured 环形缓冲或流控窗口暂时不可用。
   * @retval Error::MessageTooLarge sizeof(T) 超过通道可容纳的单帧大小。
   * @note 该接口不分配堆内存；内部只将 T 的对象表示复制一次到共享 ring。
   *       T 及其成员不得包含进程内指针，发布端和订阅端必须使用兼容的布局。
   */
  template <detail::WireMessage T>
  Result<bool> send(const T& value) noexcept {
    return offer(detail::as_bytes(value));
  }

  /**
   * @brief 按固定类型发布一条带 TTL 的消息。
   * @tparam T 可按对象表示复制的固定大小消息类型。
   * @param value 待复制到共享 ring 的本地值。
   * @param ttl 从本次调用开始计算的消息有效时长。
   * @retval true 发布成功。
   * @retval Error::BadConfig ttl 为负数。
   * @note T 的复制和 TTL 语义与 send() / offer_for() 一致。
   */
  template <detail::WireMessage T>
  Result<bool> send_for(const T& value, std::chrono::nanoseconds ttl) noexcept {
    return offer_for(detail::as_bytes(value), ttl);
  }

  /**
   * @brief 尝试按顺序发布一批消息。
   * @param payloads 待发布的负载数组视图，按顺序提交。
   * @retval std::size_t 已成功发布的消息数；等长批次受窗口限制时可能只发布前缀。
   * @retval Error::BackPressured 第一条消息即遭遇背压。
   * @retval Error::MessageTooLarge 第一条消息过大。
   * @note 批量发布减少每条消息的发布开销；返回值为实际发布条数，调用方据此重试剩余。
   */
  Result<std::size_t> offer_batch(std::span<const std::span<const std::byte>> payloads) noexcept;

  /**
   * @brief 按顺序发布同一 TTL 的消息批次；背压时返回已发布前缀长度。
   * @note 整个批次共用从本次调用开始计算的截止时间；变长批次逐帧提交。
   */
  Result<std::size_t> offer_batch_for(std::span<const std::span<const std::byte>> payloads,
                                      std::chrono::nanoseconds ttl) noexcept;

 private:
  /// 创建绑定到共享通道状态的发布端。
  explicit Publisher(std::shared_ptr<PublisherEndpoint<M>> endpoint) noexcept;

  std::shared_ptr<PublisherEndpoint<M>> endpoint_;  ///< 共享通道状态的发布端句柄。

  friend class Channel<M, RawPayloadSchema>;  ///< Channel 通过私有构造创建 Publisher。
};

/**
 * @brief Schema-bound zero-copy claim; its payload size is fixed by the channel schema.
 * @note The payload span is writable only before Publisher::commit() and while this claim
 *       owns the reservation. The claim does not keep the publisher alive.
 */
template <Mode M, detail::WireSchema Schema>
class PublishClaim<M, Schema> {
 public:
  using T = typename Schema::message_type;

  PublishClaim(PublishClaim&&) noexcept = default;
  PublishClaim& operator=(PublishClaim&&) noexcept = default;
  PublishClaim(const PublishClaim&) = delete;
  PublishClaim& operator=(const PublishClaim&) = delete;

  std::span<std::byte> payload() noexcept { return claim_.payload(); }

 private:
  explicit PublishClaim(PublishClaim<M, RawPayloadSchema> claim) noexcept
      : claim_(std::move(claim)) {}

  PublishClaim<M, RawPayloadSchema> claim_;

  template <Mode, class>
  friend class Publisher;
};

/** @brief Publisher facade restricted to one registered wire message type. */
template <Mode M, detail::WireSchema Schema>
class Publisher<M, Schema> {
 public:
  using T = typename Schema::message_type;

  Publisher() noexcept = default;

  template <class U>
    requires std::same_as<std::remove_cvref_t<U>, T>
  Result<bool> send(U&& value) noexcept {
    return publisher_.send(value);
  }

  template <class U>
    requires std::same_as<std::remove_cvref_t<U>, T>
  Result<bool> send_for(U&& value, std::chrono::nanoseconds ttl) noexcept {
    return publisher_.send_for(value, ttl);
  }

  Result<PublishClaim<M, Schema>> try_claim() noexcept {
    auto claim = publisher_.try_claim(sizeof(T));
    if (!claim) {
      return std::unexpected(claim.error());
    }
    return PublishClaim<M, Schema>(std::move(*claim));
  }

  Result<PublishClaim<M, Schema>> try_claim_for(std::chrono::nanoseconds ttl) noexcept {
    auto claim = publisher_.try_claim_for(sizeof(T), ttl);
    if (!claim) {
      return std::unexpected(claim.error());
    }
    return PublishClaim<M, Schema>(std::move(*claim));
  }

  /// @brief 提交 schema-bound claim；委托给底层 raw Publisher 完成归属校验与发布。
  void commit(PublishClaim<M, Schema>&& claim) noexcept {
    publisher_.commit(std::move(claim.claim_));
  }

 private:
  explicit Publisher(Publisher<M, RawPayloadSchema> publisher) noexcept
      : publisher_(std::move(publisher)) {}

  Publisher<M, RawPayloadSchema> publisher_;

  template <Mode, class>
  friend class Channel;
};

using FifoMpscPublisher = Publisher<Mode::FifoMpsc>;            ///< FIFO MPSC 发布端别名。
using FifoMpmcPublisher = Publisher<Mode::FifoMpmc>;  ///< FIFO 多生产者多消费者广播发布端别名。
using OrderedMpscPublisher = Publisher<Mode::OrderedMpsc>;      ///< 保序 MPSC 发布端别名。
using OrderedMpmcPublisher =
    Publisher<Mode::OrderedMpmc>;  ///< 保序多生产者多消费者广播发布端别名。
using LatestMpmcPublisher = Publisher<Mode::LatestMpmc>;  ///< 丢旧 fanout 发布端别名。

}  // namespace salias

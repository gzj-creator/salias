/**
 * @file src/salias/include/salias/publisher.hpp
 * @brief salias 公共 API 的发布端：零拷贝 claim/commit 与 offer/offer_batch 发布接口。
 * @details 位于 L7 公共外观层，Publisher 是生产者侧的用户入口，封装 L3 flow 层的
 *          producer 引擎。提供两种发布路径：
 *          - try_claim() + PublishClaim：零拷贝，先预留 ring 内 payload 写入区，
 *            用户直接写入共享内存，再 commit()，避免一次内存拷贝，适合低延迟热路径。
 *          - offer() / offer_batch()：便利接口，内部拷贝 payload 后提交，适合简单场景。
 *          Publisher 通过 std::shared_ptr<PublisherEndpoint> 共享底层通道状态，
 *          多个 Publisher 实例可绑定同一通道。线程安全由具体协议（MPSC/MPMC）保证。
 *          flow 控制窗口（publication_window）在 claim/offer 时施加背压检查。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "core/flow/producer.hpp"
#include "salias/config.hpp"
#include "salias/error.hpp"

/// salias 公共 API 命名空间，聚合 L7 用户可见的通道、发布者、订阅者与消息类型。
namespace salias {

// 前向声明：避免循环包含，实现细节定义于 L5 channel / L3 flow 实现层。
template <Mode M>
class Channel;
template <Mode M>
struct ChannelState;
template <Mode M>
struct PublisherEndpoint;
template <Mode M>
class Publisher;

/**
 * @brief 一次零拷贝发布的预留句柄（RAII 风格）。
 * @details 由 Publisher::try_claim() 返回，持有 ring 内已预留的 payload 写入区。
 *          生命周期模型：try_claim() 成功后用户通过 payload() 获取写入区直接写入，
 *          随后必须调用 commit() 将消息发布给消费者；若未 commit 而析构，预留空间
 *          被自动放弃回收。仅可移动、不可拷贝，保证同一时刻只有一个 claim 持有该区。
 *          committed_ 默认为 true 表示"无需提交"的无效默认态；真实 claim 构造时
 *          置为 false，commit() 后置 true。
 */
template <Mode M>
class PublishClaim {
 public:
  /// @brief 转移一个尚未提交的发布 claim；来源对象变为无效，不能再提交。
  /// @param other 被转移的 claim，转移后处于 moved-from 状态。
  PublishClaim(PublishClaim&& other) noexcept;
  /// @brief 释放当前未提交 claim 的句柄后，接管另一个 claim。
  /// @param other 被转移的 claim。
  /// @return *this。
  PublishClaim& operator=(PublishClaim&& other) noexcept;

  PublishClaim(const PublishClaim&) = delete;             ///< 禁止拷贝：claim 独占预留区。
  PublishClaim& operator=(const PublishClaim&) = delete;  ///< 禁止拷贝赋值。

  /**
   * @brief 返回已预留的 ring 内 payload 写入区。
   * @return 指向共享内存 ring 内写入区的可变 span。
   * @note 调用方必须在 commit() 前完成写入，且不能在 claim 对象销毁后继续持有该 span。
   *       span 指向的内存位于共享内存段，映射在所有进程地址空间。
   */
  std::span<std::byte> payload() noexcept;

  /**
   * @brief 发布当前 claim 对应的消息，使其对消费者可见。
   * @note 重复调用、moved-from 对象或默认无效对象会被忽略（幂等）。
   *       commit 通过发布序列号（acquire/release 语义）使消费者可见该消息。
   */
  void commit() noexcept;

 private:
  /// 创建绑定到共享通道状态的已预留 claim。
  PublishClaim(std::shared_ptr<PublisherEndpoint<M>> endpoint, flow::Claim claim) noexcept;

  std::shared_ptr<PublisherEndpoint<M>> endpoint_;  ///< 共享通道状态的发布端句柄。
  flow::Claim claim_{};                             ///< L3 flow 层的底层预留描述符。
  bool committed_ = true;  ///< 是否已提交；默认 true 表示无效默认态，真实 claim 为 false。

  friend class Publisher<M>;  ///< Publisher 通过私有构造创建 PublishClaim。
};

/**
 * @brief 发布端，向通道写入消息的生产者接口。
 * @details 封装 L3 flow 层 producer 引擎，通过 shared_ptr<PublisherEndpoint> 共享
 *          通道状态。提供零拷贝（try_claim）与拷贝（offer/offer_batch）两种发布路径。
 *          由 Channel::publisher() 创建，对同一通道可创建多个 Publisher 实例。
 */
template <Mode M>
class Publisher {
 public:
  /**
   * @brief 零拷贝预留一条消息的 ring 内 payload 写入区。
   * @param payload_len 期望的负载字节数。
   * @retval PublishClaim 预留成功，用户写入后需 commit()。
   * @retval Error::BackPressured 流控窗口在途字节数达上限或环形缓冲已满。
   * @retval Error::MessageTooLarge payload_len 超过环容量限制。
   * @note 成功后调用方必须写入 payload 并调用 PublishClaim::commit() 发布消息。
   */
  Result<PublishClaim<M>> try_claim(std::size_t payload_len) noexcept;

  /**
   * @brief 尝试把 payload 作为一条消息发布（内部拷贝后提交）。
   * @param payload 待发布的负载数据视图。
   * @retval true 发布成功。
   * @retval Error::BackPressured 背压，当前无法发布。
   * @retval Error::MessageTooLarge payload 超过环容量。
   */
  Result<bool> offer(std::span<const std::byte> payload) noexcept;

  /**
   * @brief 尝试按顺序发布一批消息。
   * @param payloads 待发布的负载数组视图，按顺序提交。
   * @retval std::size_t 已成功发布的消息数。
   * @retval Error::BackPressured 第一条消息即遭遇背压。
   * @retval Error::MessageTooLarge 第一条消息过大。
   * @note 批量发布减少每条消息的发布开销；返回值为实际发布条数，调用方据此重试剩余。
   */
  Result<std::size_t> offer_batch(std::span<const std::span<const std::byte>> payloads) noexcept;

 private:
  /// 创建绑定到共享通道状态的发布端。
  explicit Publisher(std::shared_ptr<PublisherEndpoint<M>> endpoint) noexcept;

  std::shared_ptr<PublisherEndpoint<M>> endpoint_;  ///< 共享通道状态的发布端句柄。

  friend class Channel<M>;  ///< Channel 通过私有构造创建 Publisher。
};

using FifoMpscPublisher = Publisher<Mode::FifoMpsc>;      ///< FIFO MPSC 发布端别名。
using FifoFanoutPublisher = Publisher<Mode::FifoFanout>;  ///< FIFO fanout 发布端别名。
using OrderedMpscPublisher = Publisher<Mode::OrderedMpsc>;      ///< 保序 MPSC 发布端别名。
using OrderedFanoutPublisher = Publisher<Mode::OrderedFanout>;  ///< 保序 fanout 发布端别名。

}  // namespace salias

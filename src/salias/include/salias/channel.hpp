/**
 * @file src/salias/include/salias/channel.hpp
 * @brief salias 公共 API 的具名 IPC 通道句柄。
 * @details 位于 L7 公共外观层，Channel 是用户访问整个 IPC 栈的入口。
 *          create() 负责创建并拥有共享内存段（经 L0 平台层 mmap/memfd_create/huge page），
 *          初始化 L1 ring 与 L3 flow 状态；connect() 作为对端连接已有通道，
 *          在映射环形区前校验共享元数据版本一致性。Channel 持有共享状态的
 *          std::shared_ptr<ChannelState>，多个 Channel 句柄可共享同一底层通道；
 *          引用计数归零时释放共享资源。模板参数 M 选择通道协议（FIFO/Ordered、
 *          MPSC/Fanout）。线程安全：Channel 句柄本身可被多线程持有与拷贝，
 *          但 publisher()/subscriber() 返回的端点各自有独立的线程安全契约。
 */
#pragma once

#include <memory>
#include <string_view>

#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/publisher.hpp"
#include "salias/subscriber.hpp"

/// salias 公共 API 命名空间，聚合 L7 用户可见的通道、发布者、订阅者与消息类型。
namespace salias {

// 前向声明：通道的共享状态实现细节，定义于 L5 channel 实现层，公共头仅暴露指针。
template <Mode M>
struct ChannelState;
template <Mode M>
class Channel;

/**
 * @brief 具名 IPC 通道句柄，连接生产者与消费者的核心抽象。
 * @details 通过共享内存实现进程间通信。生命周期由 std::shared_ptr<ChannelState> 管理：
 *          create/connect 持有强引用，所有句柄析构后释放共享段。支持移动与拷贝，
 *          拷贝共享底层状态。模板参数 M 编译期固定协议，消除运行时分发开销。
 */
template <Mode M>
class Channel {
 public:
  /**
   * @brief 创建并拥有一条具名 IPC 通道。
   * @param config 通道配置；name 必须非空，capacity 需为 2 的幂。
   * @retval Channel 创建成功的通道句柄。
   * @retval Error::PlatformFail 平台层（mmap/memfd_create）失败。
   * @retval Error::BadConfig 配置非法（name 为空或容量不合法）。
   * @note 模板参数 M 决定通道协议，Config::mode 会被外观层覆盖为 M。
   *       创建成功后该进程成为通道 owner，负责初始化共享元数据。
   */
  static Result<Channel> create(const Config& config);

  /**
   * @brief 连接已有或即将创建的具名 IPC 通道。
   * @param name 通道名称，需与 create() 时一致。
   * @retval Channel 连接成功的通道句柄。
   * @retval Error::NotFound 通道不存在。
   * @retval Error::VersionMismatch 共享元数据版本与当前编译版本不一致。
   * @note 会有限等待 owner 发布 ready 标记，并在映射环形区前校验共享元数据。
   *       连接者不拥有共享段，仅映射并附加为生产者/消费者。
   */
  static Result<Channel> connect(std::string_view name);

  /// @brief 移动构造通道句柄；底层共享状态保持有效。
  Channel(Channel&&) noexcept = default;
  /// @brief 移动赋值通道句柄；底层共享状态保持有效。
  Channel& operator=(Channel&&) noexcept = default;
  /// @brief 拷贝构造通道句柄并共享底层通道状态。
  Channel(const Channel&) = default;
  /// @brief 拷贝赋值通道句柄并共享底层通道状态。
  Channel& operator=(const Channel&) = default;

  /**
   * @brief 创建绑定到当前通道的发布端。
   * @return 绑定该通道共享状态的 Publisher<M>。
   * @note noexcept；返回值按值传递，持有 endpoint 的 shared_ptr 引用。
   */
  Publisher<M> publisher() noexcept;
  /**
   * @brief 创建绑定到当前通道的订阅端。
   * @return 绑定该通道共享状态的 Subscriber<M>。
   * @note noexcept；fanout 模式下分配独立的订阅索引。
   */
  Subscriber<M> subscriber() noexcept;

 private:
  /// 封装 create/connect 成功后得到的共享通道状态。
  explicit Channel(std::shared_ptr<ChannelState<M>> state) noexcept;

  std::shared_ptr<ChannelState<M>> state_;
};

using FifoMpscChannel = Channel<Mode::FifoMpsc>;        ///< FIFO 多生产者单消费者通道别名。
using FifoFanoutChannel = Channel<Mode::FifoFanout>;    ///< FIFO 多消费者扇出通道别名。
using OrderedMpscChannel = Channel<Mode::OrderedMpsc>;  ///< 保序多生产者单消费者通道别名。
using OrderedFanoutChannel = Channel<Mode::OrderedFanout>;  ///< 保序多消费者扇出通道别名。

}  // namespace salias

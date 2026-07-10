#pragma once

#include <memory>
#include <string_view>

#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/publisher.hpp"
#include "salias/subscriber.hpp"

namespace salias {

template <Mode M>
struct ChannelState;
template <Mode M>
class Channel;

template <Mode M>
class Channel {
 public:
  /// 创建具名 IPC 通道 owner。
  /// name 必须非空；模板参数 M 决定通道协议，Config::mode 会被外观层覆盖为 M。
  static Result<Channel> create(const Config& config);

  /// 按模板参数 M 连接已有或即将创建的具名 IPC 通道。
  /// 会有限等待拥有者发布 ready 标记，并在映射环形区前校验共享元数据。
  static Result<Channel> connect(std::string_view name);

  /// 移动通道句柄；底层共享状态保持有效。
  Channel(Channel&&) noexcept = default;
  /// 移动赋值通道句柄；底层共享状态保持有效。
  Channel& operator=(Channel&&) noexcept = default;
  /// 拷贝通道句柄并共享底层通道状态。
  Channel(const Channel&) = default;
  /// 拷贝赋值通道句柄并共享底层通道状态。
  Channel& operator=(const Channel&) = default;

  /// 创建绑定到当前通道的发布端。
  Publisher<M> publisher() noexcept;
  /// 创建绑定到当前通道的订阅端。
  Subscriber<M> subscriber() noexcept;

 private:
  /// 封装 create/connect 成功后得到的共享通道状态。
  explicit Channel(std::shared_ptr<ChannelState<M>> state) noexcept;

  std::shared_ptr<ChannelState<M>> state_;
};

using FifoMpscChannel = Channel<Mode::FifoMpsc>;
using FifoFanoutChannel = Channel<Mode::FifoFanout>;
using OrderedMpscChannel = Channel<Mode::OrderedMpsc>;
using OrderedFanoutChannel = Channel<Mode::OrderedFanout>;

}  // namespace salias

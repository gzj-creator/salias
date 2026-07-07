#pragma once

#include <memory>
#include <string_view>

#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/publisher.hpp"
#include "salias/subscriber.hpp"

namespace salias {

struct ChannelState;

class Channel {
 public:
  /// 创建通道；name 为空时创建进程内通道，命名 SPSC 通道会创建控制块和环形区。
  /// 返回 Channel；配置无效、平台映射失败或共享元数据异常时返回对应 Error。
  static Result<Channel> create(const Config& config);

  /// 连接已有或即将创建的命名 SPSC 通道。
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
  Publisher publisher() noexcept;
  /// 创建绑定到当前通道的订阅端。
  Subscriber subscriber() noexcept;

 private:
  /// 封装 create/connect 成功后得到的共享通道状态。
  explicit Channel(std::shared_ptr<ChannelState> state) noexcept;

  std::shared_ptr<ChannelState> state_;
};

}  // namespace salias

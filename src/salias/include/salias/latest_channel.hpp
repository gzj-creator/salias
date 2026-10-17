/**
 * @file src/salias/include/salias/latest_channel.hpp
 * @brief 静态配置容量与丢旧水位的 IPC fanout 通道。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <new>
#include <string_view>
#include <utility>

#include "salias/channel.hpp"

namespace salias {

/**
 * @brief 保留每个 producer 最新窗口的 IPC fanout 通道。
 * @tparam Capacity 每个 producer ring 的字节容量，须符合 Channel 容量约束。
 * @tparam HighWatermarkPercent 环内保留窗口占容量的百分比，默认 90%。
 *
 * 默认订阅者可跳过已移出保留窗口的旧消息；也可为指定订阅者选择 Reliable，
 * 此时其未读消息不会被覆盖，慢订阅者会对生产者施加背压。消费者正在借用的 Message
 * 会被 pin，在 release 前生产者不会覆盖其 payload；pin 暂时阻止写入时仍返回 BackPressured。
 * 保留水位按完整帧计算（含 8 字节帧头与对齐），单帧超出水位返回 MessageTooLarge。
 * 同一订阅端每个 producer ring 同时最多持有一个单条借用或一批 poll 借用。
 * Config::publication_window 必须为 0，因为 LatestMpmc 使用保留水位而非消费者游标背压；
 * 可用 Config::publication_window_messages 进一步限制保留帧数。
 */
template <std::size_t Capacity, std::uint32_t HighWatermarkPercent = 90>
class LatestMpmcChannel {
  static_assert(Capacity > 0, "LatestMpmcChannel capacity must be greater than zero");
  static_assert(HighWatermarkPercent > 0 && HighWatermarkPercent <= 100,
                "LatestMpmcChannel watermark must be in [1, 100]");

 public:
  /// @brief 创建结果类型。
  using CreateResult = Result<LatestMpmcChannel>;

  /**
   * @brief 创建通道；容量与丢旧水位由模板参数固定。
   * @param config 设置名称、producer/consumer 数量及平台选项；capacity 与
   *        high_watermark_percent 字段由模板参数覆盖，publication_window 须为 0。
   */
  static CreateResult create(const Config& config) {
    try {
      Config typed_config = config;
      typed_config.mode = Mode::LatestMpmc;
      typed_config.capacity = Capacity;
      typed_config.high_watermark_percent = HighWatermarkPercent;
      auto channel = Channel<Mode::LatestMpmc>::create(typed_config);
      if (!channel) {
        return std::unexpected(channel.error());
      }
      return LatestMpmcChannel(std::move(*channel));
    } catch (const std::bad_alloc&) {
      return std::unexpected(Error::OutOfMemory);
    }
  }

  /**
   * @brief 连接通道，并校验共享容量与本类型模板参数一致。
   */
  static CreateResult connect(std::string_view name) {
    auto channel = Channel<Mode::LatestMpmc>::connect(name);
    if (!channel) {
      return std::unexpected(channel.error());
    }
    if (channel->capacity() != Capacity ||
        channel->high_watermark_percent() != HighWatermarkPercent) {
      return std::unexpected(Error::BadConfig);
    }
    return LatestMpmcChannel(std::move(*channel));
  }

  LatestMpmcChannel(LatestMpmcChannel&&) noexcept = default;
  LatestMpmcChannel& operator=(LatestMpmcChannel&&) noexcept = default;
  LatestMpmcChannel(const LatestMpmcChannel&) = default;
  LatestMpmcChannel& operator=(const LatestMpmcChannel&) = default;

  /// @return 当前存活的已注册订阅槽数量。
  std::uint32_t connected_consumer_count() const noexcept {
    return channel_.connected_consumer_count();
  }

  /// @return 创建绑定到该通道的丢旧发布端。
  LatestMpmcPublisher publisher() { return channel_.publisher(); }

  /// @return 创建绑定到该通道的丢旧订阅端。
  LatestMpmcSubscriber subscriber() { return channel_.subscriber(); }

  /// @return 创建丢旧订阅端；槽位耗尽或端点分配失败时返回明确错误。
  Result<LatestMpmcSubscriber> try_subscriber() { return channel_.try_subscriber(); }

  /// @return 按指定交付策略创建订阅端。
  LatestMpmcSubscriber subscriber(SubscriptionPolicy policy) { return channel_.subscriber(policy); }

  /// @return 按指定交付策略创建订阅端；槽位耗尽或端点分配失败时返回明确错误。
  Result<LatestMpmcSubscriber> try_subscriber(SubscriptionPolicy policy) {
    return channel_.try_subscriber(policy);
  }

  /// @return 编译期固定的 ring 容量。
  static constexpr std::size_t capacity() noexcept { return Capacity; }

  /// @return 编译期固定的丢旧水位比例。
  static constexpr std::uint32_t high_watermark_percent() noexcept { return HighWatermarkPercent; }

 private:
  explicit LatestMpmcChannel(Channel<Mode::LatestMpmc> channel) noexcept
      : channel_(std::move(channel)) {}

  Channel<Mode::LatestMpmc> channel_;
};

}  // namespace salias

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
 * 慢消费者可跳过已移出保留窗口的旧消息。消费者正在借用的 Message 会被 pin，
 * 在 release 前生产者不会覆盖其 payload；pin 暂时阻止写入时仍返回 BackPressured。
 * 保留水位按完整帧计算（含 8 字节帧头与对齐），单帧超出水位返回 MessageTooLarge。
 * 同一订阅端每个 producer ring 同时最多持有一个单条借用或一批 poll 借用。
 * Config::publication_window 必须为 0，因为 LatestFanout 使用保留水位而非消费者游标背压。
 */
template <std::size_t Capacity, std::uint32_t HighWatermarkPercent = 90>
class LatestFanoutChannel {
  static_assert(Capacity > 0, "LatestFanoutChannel capacity must be greater than zero");
  static_assert(HighWatermarkPercent > 0 && HighWatermarkPercent <= 100,
                "LatestFanoutChannel watermark must be in [1, 100]");

 public:
  /// @brief 创建结果类型。
  using CreateResult = Result<LatestFanoutChannel>;

  /**
   * @brief 创建通道；容量与丢旧水位由模板参数固定。
   * @param config 设置名称、producer/consumer 数量及平台选项；capacity 与
   *        high_watermark_percent 字段由模板参数覆盖，publication_window 须为 0。
   */
  static CreateResult create(const Config& config) {
    try {
      Config typed_config = config;
      typed_config.mode = Mode::LatestFanout;
      typed_config.capacity = Capacity;
      typed_config.high_watermark_percent = HighWatermarkPercent;
      auto channel = Channel<Mode::LatestFanout>::create(typed_config);
      if (!channel) {
        return std::unexpected(channel.error());
      }
      return LatestFanoutChannel(std::move(*channel));
    } catch (const std::bad_alloc&) {
      return std::unexpected(Error::OutOfMemory);
    }
  }

  /**
   * @brief 连接通道，并校验共享容量与本类型模板参数一致。
   */
  static CreateResult connect(std::string_view name) {
    auto channel = Channel<Mode::LatestFanout>::connect(name);
    if (!channel) {
      return std::unexpected(channel.error());
    }
    if (channel->capacity() != Capacity ||
        channel->high_watermark_percent() != HighWatermarkPercent) {
      return std::unexpected(Error::BadConfig);
    }
    return LatestFanoutChannel(std::move(*channel));
  }

  LatestFanoutChannel(LatestFanoutChannel&&) noexcept = default;
  LatestFanoutChannel& operator=(LatestFanoutChannel&&) noexcept = default;
  LatestFanoutChannel(const LatestFanoutChannel&) = default;
  LatestFanoutChannel& operator=(const LatestFanoutChannel&) = default;

  /// @return 当前存活的已注册订阅槽数量。
  std::uint32_t connected_consumer_count() const noexcept {
    return channel_.connected_consumer_count();
  }

  /// @return 创建绑定到该通道的丢旧发布端。
  LatestFanoutPublisher publisher() { return channel_.publisher(); }

  /// @return 创建绑定到该通道的丢旧订阅端。
  LatestFanoutSubscriber subscriber() { return channel_.subscriber(); }

  /// @return 编译期固定的 ring 容量。
  static constexpr std::size_t capacity() noexcept { return Capacity; }

  /// @return 编译期固定的丢旧水位比例。
  static constexpr std::uint32_t high_watermark_percent() noexcept { return HighWatermarkPercent; }

 private:
  explicit LatestFanoutChannel(Channel<Mode::LatestFanout> channel) noexcept
      : channel_(std::move(channel)) {}

  Channel<Mode::LatestFanout> channel_;
};

}  // namespace salias

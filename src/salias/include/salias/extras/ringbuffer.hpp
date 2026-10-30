/**
 * @file src/salias/include/salias/extras/ringbuffer.hpp
 * @brief 编译期定容量、有界丢旧队列。
 * @details Ringbuffer 是进程内的轻量循环队列，不依赖共享内存映射；容量与高水位比例
 *          均在编译期确定。队列达到高水位后，新值入队并淘汰最旧值，使队列始终保留
 *          最新数据。该类型不提供并发同步，调用者须保证单线程访问或自行加锁。
 */
#pragma once

#include <array>
#include <cstddef>
#include <optional>
#include <type_traits>
#include <utility>

/// salias extras 命名空间，聚合不进入核心快速入口的可选能力。
namespace salias::extras {

/**
 * @brief 编译期定容量的丢旧 Ringbuffer。
 * @details 高水位默认是容量的 90%（向下取整，最少保留一项）。入队导致占用超过
 *          高水位时，淘汰最旧项而保留新项。该容器仅适用于单线程访问；它不是
 *          MagicRing 的替代品，也不改变 IPC Channel 的背压策略。
 * @tparam T 队列元素类型，须满足无异常移动构造与析构。
 * @tparam Capacity 队列槽位数，须大于零。
 * @tparam HighWatermarkPercent 入队后允许的最大占用比例，范围为 1 到 100。
 */
template <class T, std::size_t Capacity, std::size_t HighWatermarkPercent = 90>
class Ringbuffer {
  static_assert(Capacity > 0, "Ringbuffer capacity must be greater than zero");
  static_assert(HighWatermarkPercent > 0 && HighWatermarkPercent <= 100,
                "Ringbuffer high watermark must be in [1, 100]");
  static_assert(std::is_nothrow_move_constructible_v<T>,
                "Ringbuffer values must be nothrow move constructible");
  static_assert(std::is_nothrow_destructible_v<T>,
                "Ringbuffer values must be nothrow destructible");

 public:
  /// 入队值的移动构造不抛异常，因此该操作可用于 salias 的显式错误处理路径。
  bool push(T value) noexcept {
    bool dropped_oldest = false;
    if (size_ == Capacity) {
      discard_oldest();
      dropped_oldest = true;
    }

    slots_[tail_].emplace(std::move(value));
    tail_ = next_index(tail_);
    ++size_;

    if (size_ > max_size()) {
      discard_oldest();
      dropped_oldest = true;
    }
    dropped_ += dropped_oldest ? 1u : 0u;
    return dropped_oldest;
  }

  /**
   * @brief 移出最旧元素。
   * @return 队列非空时返回元素；队列为空时返回 std::nullopt。
   */
  std::optional<T> try_pop() noexcept {
    if (size_ == 0) {
      return std::nullopt;
    }
    std::optional<T> value(std::move(slots_[head_]));
    slots_[head_].reset();
    head_ = next_index(head_);
    --size_;
    return value;
  }

  /// @return 当前队列元素数。
  std::size_t size() const noexcept { return size_; }
  /// @return 编译期固定的槽位数。
  static constexpr std::size_t capacity() noexcept { return Capacity; }
  /// @return 高水位对应的最大保留元素数。
  static constexpr std::size_t max_size() noexcept {
    const std::size_t whole = (Capacity / 100) * HighWatermarkPercent;
    const std::size_t fraction = ((Capacity % 100) * HighWatermarkPercent) / 100;
    const std::size_t limit = whole + fraction;
    return limit == 0 ? 1 : limit;
  }
  /// @return 队列存活期间累计淘汰的最旧元素数。
  std::size_t dropped_count() const noexcept { return dropped_; }

 private:
  /// @brief 返回环内下一个槽位，避免在热路径使用运行期取模。
  static constexpr std::size_t next_index(std::size_t index) noexcept {
    return index + 1 == Capacity ? 0 : index + 1;
  }

  /// @brief 销毁并移过最旧元素；调用前须保证队列非空。
  void discard_oldest() noexcept {
    slots_[head_].reset();
    head_ = next_index(head_);
    --size_;
  }

  std::array<std::optional<T>, Capacity> slots_{};  ///< 固定容量的元素槽位。
  std::size_t head_ = 0;  ///< 最旧元素所在位置。
  std::size_t tail_ = 0;  ///< 下一次写入位置。
  std::size_t size_ = 0;  ///< 当前有效元素数，用于区分空队列与回绕位置。
  std::size_t dropped_ = 0;  ///< 累计丢弃的最旧元素数。
};

}  // namespace salias::extras

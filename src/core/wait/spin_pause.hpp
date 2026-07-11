/**
 * @file src/core/wait/spin_pause.hpp
 * @brief 纯自旋等待策略 SpinPause，满足 WaitStrategy 约束。
 * @details 属于 L4 等待策略层：消费端在 wait() 中以 cpu_relax() 忙等，
 *  直到目标 word 的值与 expected 不同即返回，延迟最低但 CPU 占用满载。
 *  无内部状态、无需 futex/eventfd 唤醒，适合延迟极敏感且等待窗口极短的场景。
 *  适用于单进程或跨进程（word 位于 MAP_SHARED 共享内存即可）。
 */
#pragma once

#include <atomic>
#include <cstdint>

#include "core/wait/cpu_relax.hpp"

namespace salias::wait {

/// 等待策略原语层命名空间，集中提供自旋、阻塞等消费端等待手段。

/**
 * @brief 纯自旋等待策略，满足 WaitStrategy 概念。
 * @details 通过 atomic_ref 以 acquire 语义轮询外部 word，配合 cpu_relax() 忙等。
 *  无任何内部状态，wake()/reset() 均为空操作。线程安全；各方法无前置/后置条件。
 */
struct SpinPause {
  /**
   * @brief 是否需要显式唤醒。
   * @retval false 自旋策略持续轮询 word，生产端无需调用 wake()。
   */
  constexpr bool needs_wake() const noexcept { return false; }

  /**
   * @brief 自旋等待，直到 *word 的值与 expected 不同。
   * @param word 指向被轮询的 32 位原子字，非持有指针，须在返回前保持有效；
   *             跨进程使用时须位于 MAP_SHARED 共享内存。
   * @param expected 期望的旧值；当 *word != expected 时立即返回。
   * @details 以 std::memory_order_acquire 加载，保证此后读到 producer 发布的
   *          payload 与 frame header 数据（获取语义，与 producer 端 release 配对）。
   *          cpu_relax() 仅作 CPU 提示，不陷入内核调度。
   */
  void wait(std::uint32_t* word, std::uint32_t expected) noexcept {
    while (std::atomic_ref<std::uint32_t>(*word).load(std::memory_order_acquire) == expected) {
      cpu_relax();
    }
  }

  /// 空唤醒；自旋等待方直接观察 word，无需生产端显式唤醒。
  void wake(std::uint32_t*) noexcept {}
  /// 空重置；SpinPause 没有内部状态。
  void reset() noexcept {}
};

}  // namespace salias::wait

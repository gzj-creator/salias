/**
 * @file src/core/wait/cpu_relax.hpp
 * @brief 提供架构无关的 CPU 自旋提示原语 cpu_relax()。
 * @details 属于 L4 等待策略层的基础设施：所有自旋类等待策略（spin）
 *  在忙等循环中调用 cpu_relax() 以降低功耗并改善超线程并发效率。
 *  x86/x86-64 下展开为 _mm_pause（PAUSE 指令），ARM 下展开为 yield 指令。
 *  本头文件为内联自由函数，无状态、线程安全，可在任意线程/进程上下文调用。
 */
#pragma once

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace salias::wait {

/// 等待策略原语层命名空间，集中提供自旋、阻塞等消费端等待手段。

/**
 * @brief 在自旋循环中发出当前架构最轻量的 pause/yield 提示。
 * @details CPU 提示指令不会改变可观察的内存语义，仅用于：
 *   - 向超线程 sibling 线程让出执行资源；
 *   - 避免忙等造成的流水线功率惩罚；
 *   - 在 ARM 上作为轻量内存屏障提示。
 *  该函数不调用操作系统的线程调度（不陷入内核），延迟最低但 CPU 占用满载。
 * @note 线程安全；无内存序语义，调用方需自行保证必要的 acquire/release 屏障。
 */
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#else
  __asm__ __volatile__("yield" ::: "memory");
#endif
}

}  // namespace salias::wait

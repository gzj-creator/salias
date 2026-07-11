/**
 * @file src/core/wait/wait_strategy.hpp
 * @brief 定义等待策略统一接口概念 WaitStrategy。
 * @details 属于 L4 等待策略层：所有消费端等待手段（自旋、futex、eventfd 等）须满足
 *  此概念，以便 channel 层以模板参数静态派发，避免虚函数开销。
 *  接口围绕一个 32 位原子字 word：消费端在 wait() 中阻塞/忙等，生产端在发布后
 *  调用 wake() 唤醒。跨进程策略要求 word 位于 mmap 的 MAP_SHARED 共享内存。
 */
#pragma once

#include <concepts>
#include <cstdint>

namespace salias::wait {

/// 等待策略原语层命名空间，集中提供自旋、阻塞等消费端等待手段。

/**
 * @brief 等待策略的静态接口约束。
 * @details 满足该概念的类型可被 channel 层以模板实参注入，实现零开销抽象的等待策略
 *  静态派发。word 为非持有指针，调用方须保证其在 wait()/wake() 期间保持有效；
 *  跨进程策略额外要求 word 位于 MAP_SHARED 内存。
 * @tparam Strategy 被约束的等待策略类型。
 * @note 纯编译期约束，不产生运行时开销。
 */
template <class Strategy>
concept WaitStrategy = requires(Strategy strategy, std::uint32_t* word, std::uint32_t expected) {
  { strategy.needs_wake() } -> std::same_as<bool>;
  { strategy.wait(word, expected) } -> std::same_as<void>;
  { strategy.wake(word) } -> std::same_as<void>;
  { strategy.reset() } -> std::same_as<void>;
};

}  // namespace salias::wait

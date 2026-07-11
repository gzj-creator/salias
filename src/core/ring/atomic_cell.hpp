/** @file src/core/ring/atomic_cell.hpp
 * @brief 提供 64 位共享内存单元的 acquire/release 原子访问原语。
 * @details 本文件位于 L1 ring 环形缓冲层，为上层（L3 flow 的生产/消费 position
 *  与 flow window 流控）提供跨进程可见的 64 位序列号（sequence）读写原语。底层
 *  通过 std::atomic_ref 对“已存在于共享内存中的普通 std::uint64_t”做原子访问，
 *  避免在每个单元上内嵌 std::atomic（后者在共享内存布局上不可移植）。
 *  关键不变式：所有参与进程/线程必须使用本文件提供的原语访问同一单元，且单元
 *  必须 8 字节对齐；acquire 读取与 release 写入成对配合，建立跨进程的
 *  happens-before 关系，使 reader 看到 sequence 之前写入的 payload。
 */
#pragma once

#include <atomic>
#include <cstdint>

namespace salias::ring {
/// L1 ring 层原子单元访问命名空间；提供跨进程共享内存中 64 位序列号的安全读写。

/**
 * @brief 对共享内存中的 64 位单元执行 acquire 读取。
 * @details acquire 语义保证：本读操作之后的访问不会被重排到此读之前，从而在
 *  读到 release 写入的新 sequence 值时，也观察到该 release 写之前的所有
 *  payload 写入——这是跨进程位置交接（producer→consumer）正确性的核心。
 * @param cell 要读取的 64 位单元引用；可为 const，内部会做 const_cast。
 * @retval std::uint64_t 读取到的当前值。
 * @note cell 必须 8 字节对齐并在调用期间存活；所有参与进程/线程都必须用兼容的
 *  原子操作访问该单元，否则为未定义行为。noexcept，不抛异常。
 */
// 对已有 64 位单元执行 acquire 读取，支持位于共享内存中的单元。
// 安全性：cell 必须 8 字节对齐，并在调用期间存活；所有参与进程/线程都必须用兼容原子操作访问。
inline std::uint64_t load_acquire(const std::uint64_t& cell) noexcept {
  // const_cast 将 const 引用还原为可变引用，以满足 atomic_ref 非常量模板形参要求；
  // 实际语义由 acquire 内存序约束，调用者仍以只读方式使用。
  auto& mutable_cell = const_cast<std::uint64_t&>(cell);
  return std::atomic_ref<std::uint64_t>(mutable_cell).load(std::memory_order_acquire);
}

/**
 * @brief 以 release 语义发布 64 位单元，供上层交接位置使用。
 * @details release 语义保证：本写操作之前的访问（如 payload 写入）不会被重排到此
 *  写之后，从而当 consumer 以 acquire 读到新 sequence 时，必定观察到此前写入的
 *  payload。这构成了 producer 发布 (publish) 与 consumer 订阅 (subscribe) 之间
 *  的 happens-before 边界。
 * @param cell 要写入的 64 位单元引用。
 * @param value 要发布的值（通常为新的 sequence 或 position）。
 * @note cell 必须 8 字节对齐，且不能被并发的非原子访问触碰；noexcept，不抛异常。
 */
// 以 release 语义发布 64 位单元，供上层交接位置使用。
// 安全性：cell 必须 8 字节对齐，且不能被并发的非原子访问触碰。
inline void store_release(std::uint64_t& cell, std::uint64_t value) noexcept {
  std::atomic_ref<std::uint64_t>(cell).store(value, std::memory_order_release);
}

}  // namespace salias::ring

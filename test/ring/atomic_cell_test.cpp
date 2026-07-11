/**
 * @file test/ring/atomic_cell_test.cpp
 * @brief L1 ring 层 64 位共享内存单元 acquire/release 原子访问原语的单元测试。
 * @details 本测试位于 L1 ring 环形缓冲层之上，验证 atomic_cell.hpp 提供的
 *  store_release / load_acquire 原语在普通 std::uint64_t 上的正确性。这对原语是
 *  L3 flow 层生产/消费 position 交接与 flow window 流控的基础：producer 以 release
 *  发布新的 sequence，consumer 以 acquire 读取，建立跨线程/跨进程的 happens-before
 *  关系，确保 consumer 看到 sequence 之前写入的 payload。此处仅验证最基本的可见性
 *  语义——release 写入的值必须能被随后的 acquire 读取观察到。
 */
#include "core/ring/atomic_cell.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace {
/// 匿名命名空间，隔离 atomic_cell 测试用例的链接符号到本编译单元。

/**
 * @brief 验证 release 写入的值能被 acquire 读取观察到。
 * @details 对一个普通 std::uint64_t 单元先执行 store_release（release 语义，
 *  保证此写之前的访问不被重排到此写之后），再执行 load_acquire（acquire 语义，
 *  保证此读之后的访问不被重排到此读之前）。期望读取到刚写入的值，验证
 *  std::atomic_ref 在 64 位单元上的 acquire/release 配对正确性。
 */
// 验证 release store 能被 acquire load 观察到。
TEST(AtomicCellTest, StoreReleaseIsObservedByLoadAcquire) {
  // 普通整数单元，模拟位于共享内存中的 sequence 字段（无内嵌 std::atomic）。
  std::uint64_t cell = 0;

  // 以 release 语义发布值 42，模拟 producer 发布新 sequence。
  salias::ring::store_release(cell, 42);

  // 以 acquire 语义读取，模拟 consumer 订阅；release/acquire 配对保证可见性。
  EXPECT_EQ(salias::ring::load_acquire(cell), 42u);
}

}  // namespace

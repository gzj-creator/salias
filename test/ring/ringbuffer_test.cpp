/**
 * @file test/ring/ringbuffer_test.cpp
 * @brief 验证编译期定容量 Ringbuffer 的水位与丢旧语义。
 * @details 测试覆盖默认 90% 高水位、自定义高水位、回绕后的 FIFO 顺序、空队列及
 *          单槽容量边界。容器是单线程类型，本测试不并发访问。
 */
#include <gtest/gtest.h>

#include <cstddef>
#include "salias/ringbuffer.hpp"

namespace {

TEST(RingbufferTest, DefaultWatermarkDropsOldestAndKeepsNewest) {
  salias::Ringbuffer<int, 10> queue;
  EXPECT_EQ(queue.max_size(), 9u);

  for (int value = 1; value <= 9; ++value) {
    EXPECT_FALSE(queue.push(value));
  }
  EXPECT_TRUE(queue.push(10));
  EXPECT_EQ(queue.size(), 9u);
  EXPECT_EQ(queue.dropped_count(), 1u);

  for (int value = 2; value <= 10; ++value) {
    auto item = queue.try_pop();
    ASSERT_TRUE(item);
    EXPECT_EQ(*item, value);
  }
}

TEST(RingbufferTest, ConfiguredWatermarkIsCompileTimeAndWraps) {
  salias::Ringbuffer<int, 8, 50> queue;
  static_assert(decltype(queue)::capacity() == 8);
  static_assert(decltype(queue)::max_size() == 4);

  for (int value = 1; value <= 4; ++value) {
    EXPECT_FALSE(queue.push(value));
  }
  EXPECT_TRUE(queue.push(5));
  ASSERT_EQ(queue.try_pop(), 2);
  ASSERT_EQ(queue.try_pop(), 3);
  EXPECT_FALSE(queue.push(6));
  EXPECT_FALSE(queue.push(7));
  ASSERT_EQ(queue.try_pop(), 4);
  ASSERT_EQ(queue.try_pop(), 5);
  ASSERT_EQ(queue.try_pop(), 6);
  ASSERT_EQ(queue.try_pop(), 7);
  EXPECT_FALSE(queue.try_pop());
  EXPECT_EQ(queue.dropped_count(), 1u);
}

TEST(RingbufferTest, SingleSlotAlwaysRetainsLatestValue) {
  salias::Ringbuffer<int, 1> queue;
  EXPECT_EQ(queue.max_size(), 1u);
  EXPECT_FALSE(queue.push(1));
  EXPECT_TRUE(queue.push(2));
  ASSERT_EQ(queue.try_pop(), 2);
  EXPECT_EQ(queue.dropped_count(), 1u);
}

}  // namespace

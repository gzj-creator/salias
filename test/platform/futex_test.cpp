#include "core/platform/futex.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace {

using salias::platform::Futex;
using salias::platform::PlatformError;

// 验证受保护 word 变化并发送 wake 后 futex wait 会返回。
TEST(FutexTest, WaitReturnsAfterWakeOrObservedValueChange) {
  alignas(std::uint32_t) std::uint32_t word = 0;
  std::atomic<bool> waiter_started = false;
  PlatformError wait_result = PlatformError::FutexFailed;

  std::thread waiter([&] {
    waiter_started.store(true, std::memory_order_release);
    wait_result = Futex(&word).wait(0, std::chrono::seconds(5));
  });

  while (!waiter_started.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  std::atomic_ref<std::uint32_t>(word).store(1, std::memory_order_release);
  EXPECT_GE(Futex(&word).wake_one(), 0);

  waiter.join();

  EXPECT_EQ(wait_result, PlatformError::Ok);
  EXPECT_EQ(std::atomic_ref<std::uint32_t>(word).load(std::memory_order_acquire), 1u);
}

}  // namespace

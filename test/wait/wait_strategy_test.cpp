#include "core/wait/busy_spin.hpp"
#include "core/wait/futex_wait.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"
#include "core/wait/yielding.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace {

static_assert(salias::wait::WaitStrategy<salias::wait::BusySpin>);
static_assert(salias::wait::WaitStrategy<salias::wait::SpinPause>);
static_assert(salias::wait::WaitStrategy<salias::wait::Yielding>);
static_assert(salias::wait::WaitStrategy<salias::wait::FutexWait>);

// 验证 FutexWait 会在被观察 word 改变后醒来。
TEST(FutexWaitTest, WakesWhenWordChanges) {
  std::uint32_t word = 0;
  std::atomic<bool> waiter_started = false;

  std::thread waiter([&] {
    salias::wait::FutexWait wait;
    waiter_started.store(true, std::memory_order_release);
    wait.wait(&word, 0);
  });

  while (!waiter_started.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  std::atomic_ref<std::uint32_t>(word).store(1, std::memory_order_release);
  salias::wait::FutexWait{}.wake(&word);

  waiter.join();
  EXPECT_EQ(std::atomic_ref<std::uint32_t>(word).load(std::memory_order_acquire), 1u);
}

}  // namespace

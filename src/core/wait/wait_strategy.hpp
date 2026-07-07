#pragma once

#include <concepts>
#include <cstdint>

namespace salias::wait {

// 静态等待策略约束；word 为非持有指针，必须在 wait()/wake() 期间保持有效。
// 跨进程策略要求 word 位于 MAP_SHARED 内存。
template <class Strategy>
concept WaitStrategy = requires(Strategy strategy, std::uint32_t* word, std::uint32_t expected) {
  { strategy.wait(word, expected) } -> std::same_as<void>;
  { strategy.wake(word) } -> std::same_as<void>;
  { strategy.reset() } -> std::same_as<void>;
};

}  // namespace salias::wait

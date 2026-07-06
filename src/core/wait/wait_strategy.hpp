#pragma once

#include <concepts>
#include <cstdint>

namespace salias::wait {

// Static wait strategy contract. The word is non-owning and must remain alive for the duration of
// wait()/wake(); cross-process strategies require it to live in MAP_SHARED memory.
template <class Strategy>
concept WaitStrategy = requires(Strategy strategy, std::uint32_t* word, std::uint32_t expected) {
  { strategy.wait(word, expected) } -> std::same_as<void>;
  { strategy.wake(word) } -> std::same_as<void>;
  { strategy.reset() } -> std::same_as<void>;
};

}  // namespace salias::wait

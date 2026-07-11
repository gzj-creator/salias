#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "Aeron.h"
#include "tools/final_bench/worker_common.hpp"

namespace salias::final_bench {

inline std::string aeron_channel(std::size_t term_length) {
  return "aeron:ipc?term-length=" + std::to_string(term_length);
}

inline std::shared_ptr<aeron::Subscription> find_subscription(aeron::Aeron& aeron,
                                                               std::int64_t id) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (std::chrono::steady_clock::now() < deadline) {
    if (auto subscription = aeron.findSubscription(id)) return subscription;
    std::this_thread::yield();
  }
  return nullptr;
}

inline std::shared_ptr<aeron::ExclusivePublication> find_publication(aeron::Aeron& aeron,
                                                                     std::int64_t id) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (std::chrono::steady_clock::now() < deadline) {
    if (auto publication = aeron.findExclusivePublication(id)) return publication;
    std::this_thread::yield();
  }
  return nullptr;
}

}  // namespace salias::final_bench

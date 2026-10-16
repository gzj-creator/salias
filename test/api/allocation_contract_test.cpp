#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <salias/channel.hpp>
#include <salias/latest_channel.hpp>
#include <string>

namespace {

std::atomic<bool> fail_next_allocation = false;
std::atomic<std::int64_t> allocations_before_failure = -1;

class FailureInjection {
 public:
  explicit FailureInjection(std::int64_t budget) noexcept {
    allocations_before_failure.store(budget, std::memory_order_relaxed);
  }
  ~FailureInjection() { allocations_before_failure.store(-1, std::memory_order_relaxed); }
};

// Fail each allocation in turn, including vector construction inside the shared engine.
template <class Channel>
bool check_create_allocations(const salias::Config& config) {
  for (std::int64_t budget = 0; budget < 128; ++budget) {
    bool succeeded = false;
    try {
      FailureInjection injection(budget);
      auto channel = Channel::create(config);
      succeeded = channel.has_value();
      if (!channel && channel.error() != salias::Error::OutOfMemory) {
        return false;
      }
    } catch (const std::bad_alloc&) {
      return false;
    }
    {
      auto recreated = Channel::create(config);
      if (!recreated) {
        return false;
      }
    }
    if (succeeded) {
      return true;
    }
  }
  return false;
}

template <class Channel>
bool check_connect_allocations(const salias::Config& config) {
  auto owner = Channel::create(config);
  if (!owner) {
    return false;
  }
  for (std::int64_t budget = 0; budget < 128; ++budget) {
    try {
      FailureInjection injection(budget);
      auto channel = Channel::connect(config.name);
      if (channel) {
        return true;
      }
      if (channel.error() != salias::Error::OutOfMemory) {
        return false;
      }
    } catch (const std::bad_alloc&) {
      return false;
    }
  }
  return false;
}

template <class Channel>
bool check_subscriber_allocations(const salias::Config& config) {
  for (std::int64_t budget = 0; budget < 32; ++budget) {
    auto channel = Channel::create(config);
    if (!channel) {
      return false;
    }
    try {
      FailureInjection injection(budget);
      auto subscriber = channel->subscriber();
      return !subscriber.try_recv();
    } catch (const std::bad_alloc&) {
      auto retry = channel->try_subscriber();
      if (!retry || channel->connected_consumer_count() != 1) {
        return false;
      }
      *retry = {};
      if (channel->connected_consumer_count() != 0) {
        return false;
      }
    }
  }
  return false;
}

}  // namespace

void* operator new(std::size_t size) {
  if (fail_next_allocation.exchange(false, std::memory_order_relaxed)) {
    throw std::bad_alloc();
  }
  if (allocations_before_failure.load(std::memory_order_relaxed) >= 0 &&
      allocations_before_failure.fetch_sub(1, std::memory_order_relaxed) == 0) {
    throw std::bad_alloc();
  }
  if (void* memory = std::malloc(size)) {
    return memory;
  }
  throw std::bad_alloc();
}

void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

int main() {
  salias::Config create_failure_config;
  create_failure_config.name = std::string(128, 'a');

  fail_next_allocation.store(true, std::memory_order_relaxed);
  auto failed_create = salias::FifoMpscChannel::create(create_failure_config);
  fail_next_allocation.store(false, std::memory_order_relaxed);

  if (failed_create || failed_create.error() != salias::Error::OutOfMemory) {
    return 1;
  }

  try {
    fail_next_allocation.store(true, std::memory_order_relaxed);
    auto failed_latest = salias::LatestFanoutChannel<4096>::create(create_failure_config);
    fail_next_allocation.store(false, std::memory_order_relaxed);
    if (failed_latest || failed_latest.error() != salias::Error::OutOfMemory) {
      return 4;
    }
  } catch (const std::bad_alloc&) {
    fail_next_allocation.store(false, std::memory_order_relaxed);
    return 5;
  }

  const std::string missing_channel_name(64, 'b');

  fail_next_allocation.store(true, std::memory_order_relaxed);
  auto failed_connect = salias::FifoMpscChannel::connect(missing_channel_name);
  fail_next_allocation.store(false, std::memory_order_relaxed);

  if (failed_connect || failed_connect.error() != salias::Error::OutOfMemory) {
    return 3;
  }

  fail_next_allocation.store(true, std::memory_order_relaxed);
  auto failed_latest_connect = salias::LatestFanoutChannel<4096>::connect(missing_channel_name);
  fail_next_allocation.store(false, std::memory_order_relaxed);
  if (failed_latest_connect || failed_latest_connect.error() != salias::Error::OutOfMemory) {
    return 6;
  }

  create_failure_config.capacity = 4096;
  create_failure_config.num_producers = 2;
  create_failure_config.name = "allocations-" + std::to_string(::getpid()) + std::string(64, 'c');
  if (!check_create_allocations<salias::FifoMpscChannel>(create_failure_config) ||
      !check_create_allocations<salias::FifoFanoutChannel>(create_failure_config) ||
      !check_create_allocations<salias::OrderedMpscChannel>(create_failure_config) ||
      !check_create_allocations<salias::OrderedFanoutChannel>(create_failure_config) ||
      !check_create_allocations<salias::Channel<salias::Mode::LatestFanout>>(
          create_failure_config) ||
      !check_create_allocations<salias::LatestFanoutChannel<4096>>(create_failure_config)) {
    return 7;
  }
  if (!check_connect_allocations<salias::FifoMpscChannel>(create_failure_config) ||
      !check_connect_allocations<salias::FifoFanoutChannel>(create_failure_config) ||
      !check_connect_allocations<salias::OrderedMpscChannel>(create_failure_config) ||
      !check_connect_allocations<salias::OrderedFanoutChannel>(create_failure_config) ||
      !check_connect_allocations<salias::LatestFanoutChannel<4096>>(create_failure_config)) {
    return 9;
  }
  if (!check_subscriber_allocations<salias::FifoMpscChannel>(create_failure_config) ||
      !check_subscriber_allocations<salias::FifoFanoutChannel>(create_failure_config) ||
      !check_subscriber_allocations<salias::OrderedMpscChannel>(create_failure_config) ||
      !check_subscriber_allocations<salias::OrderedFanoutChannel>(create_failure_config) ||
      !check_subscriber_allocations<salias::LatestFanoutChannel<4096>>(create_failure_config)) {
    return 8;
  }
}

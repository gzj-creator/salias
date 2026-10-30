#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <salias/channel.hpp>
#include <salias/latest_by_key_channel.hpp>
#include <salias/latest_channel.hpp>
#include <string>

namespace {

std::atomic<bool> fail_next_allocation = false;
std::atomic<std::int64_t> allocations_before_failure = -1;
std::atomic<std::uint64_t> allocation_count = 0;

struct TypedPayload {
  std::uint64_t id = 0;
  std::uint32_t sequence = 0;
};

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
  allocation_count.fetch_add(1, std::memory_order_relaxed);
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
  auto failed_create = salias::Channel<salias::Mode::FifoMpsc>::create(create_failure_config);
  fail_next_allocation.store(false, std::memory_order_relaxed);

  if (failed_create || failed_create.error() != salias::Error::OutOfMemory) {
    return 1;
  }

  try {
    fail_next_allocation.store(true, std::memory_order_relaxed);
    auto failed_latest = salias::LatestMpmcChannel<4096>::create(create_failure_config);
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
  auto failed_connect = salias::Channel<salias::Mode::FifoMpsc>::connect(missing_channel_name);
  fail_next_allocation.store(false, std::memory_order_relaxed);

  if (failed_connect || failed_connect.error() != salias::Error::OutOfMemory) {
    return 3;
  }

  fail_next_allocation.store(true, std::memory_order_relaxed);
  auto failed_latest_connect = salias::LatestMpmcChannel<4096>::connect(missing_channel_name);
  fail_next_allocation.store(false, std::memory_order_relaxed);
  if (failed_latest_connect || failed_latest_connect.error() != salias::Error::OutOfMemory) {
    return 6;
  }

  create_failure_config.capacity = 4096;
  create_failure_config.num_producers = 2;
  create_failure_config.name = "allocations-" + std::to_string(::getpid()) + std::string(64, 'c');
  if (!check_create_allocations<salias::Channel<salias::Mode::FifoMpsc>>(create_failure_config) ||
      !check_create_allocations<salias::Channel<salias::Mode::FifoMpmc>>(create_failure_config) ||
      !check_create_allocations<salias::Channel<salias::Mode::OrderedMpsc>>(
          create_failure_config) ||
      !check_create_allocations<salias::Channel<salias::Mode::OrderedMpmc>>(
          create_failure_config) ||
      !check_create_allocations<salias::LatestMpmcChannel<4096>>(create_failure_config)) {
    return 7;
  }
  if (!check_connect_allocations<salias::Channel<salias::Mode::FifoMpsc>>(create_failure_config) ||
      !check_connect_allocations<salias::Channel<salias::Mode::FifoMpmc>>(create_failure_config) ||
      !check_connect_allocations<salias::Channel<salias::Mode::OrderedMpsc>>(
          create_failure_config) ||
      !check_connect_allocations<salias::Channel<salias::Mode::OrderedMpmc>>(
          create_failure_config) ||
      !check_connect_allocations<salias::LatestMpmcChannel<4096>>(create_failure_config)) {
    return 9;
  }
  if (!check_subscriber_allocations<salias::Channel<salias::Mode::FifoMpsc>>(
          create_failure_config) ||
      !check_subscriber_allocations<salias::Channel<salias::Mode::FifoMpmc>>(
          create_failure_config) ||
      !check_subscriber_allocations<salias::Channel<salias::Mode::OrderedMpsc>>(
          create_failure_config) ||
      !check_subscriber_allocations<salias::Channel<salias::Mode::OrderedMpmc>>(
          create_failure_config) ||
      !check_subscriber_allocations<salias::LatestMpmcChannel<4096>>(create_failure_config)) {
    return 8;
  }

  salias::Config typed_config;
  typed_config.name = "allocations-typed-" + std::to_string(::getpid());
  typed_config.capacity = 4096;
  auto typed_channel = salias::Channel<salias::Mode::FifoMpsc>::create(typed_config);
  if (!typed_channel) {
    return 14;
  }
  auto typed_publisher = typed_channel->publisher();
  auto typed_subscriber = typed_channel->subscriber();
  allocation_count.store(0, std::memory_order_relaxed);
  for (std::uint32_t sequence = 0; sequence < 1000; ++sequence) {
    const TypedPayload typed_payload{.id = 3, .sequence = sequence};
    if (!typed_publisher.send(typed_payload)) {
      return 15;
    }
    auto typed_received = typed_subscriber.try_recv_copy<TypedPayload>();
    if (!typed_received || !*typed_received || (**typed_received).id != typed_payload.id ||
        (**typed_received).sequence != typed_payload.sequence) {
      return 16;
    }
  }
  if (allocation_count.load(std::memory_order_relaxed) != 0) {
    return 17;
  }

  using LatestByKey = salias::LatestByKeyChannel<std::uint64_t, std::uint64_t, 1>;
  const std::string latest_by_key_name = "allocations-latest-by-key-" + std::to_string(::getpid());
  auto latest_by_key = LatestByKey::create(latest_by_key_name);
  if (!latest_by_key) {
    return 10;
  }
  auto publisher = latest_by_key->publisher();
  auto subscriber = latest_by_key->subscriber();
  allocation_count.store(0, std::memory_order_relaxed);
  for (std::uint64_t sequence = 0; sequence < 1000; ++sequence) {
    if (!publisher.offer(1, sequence)) {
      return 11;
    }
    auto received = subscriber.try_recv_checked();
    if (!received || !*received || (**received).value != sequence) {
      return 12;
    }
  }
  if (allocation_count.load(std::memory_order_relaxed) != 0) {
    return 13;
  }
}

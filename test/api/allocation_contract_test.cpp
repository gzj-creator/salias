#include <salias/channel.hpp>

#include <atomic>
#include <cstdlib>
#include <new>
#include <string>

namespace {

std::atomic<bool> fail_next_allocation = false;

}  // namespace

void* operator new(std::size_t size) {
  if (fail_next_allocation.exchange(false, std::memory_order_relaxed)) {
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

  const std::string missing_channel_name(64, 'b');

  fail_next_allocation.store(true, std::memory_order_relaxed);
  auto failed_connect = salias::FifoMpscChannel::connect(missing_channel_name);
  fail_next_allocation.store(false, std::memory_order_relaxed);

  if (failed_connect || failed_connect.error() != salias::Error::OutOfMemory) {
    return 3;
  }
}

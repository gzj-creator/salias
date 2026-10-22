/**
 * @file registration_cost.cpp
 * @brief Time the actual acquire_consumer implementation without changing src/.
 * @details Including the implementation exposes the internal registration API in
 *          this separate probe only. Build against either source revision, never
 *          link this probe into a product or use it for throughput measurements.
 */
#include "salias/src/channel.cpp"

#include <sched.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/wait.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>

namespace {

struct Flags {
  std::atomic<unsigned> ready{0};
  std::atomic<bool> stop{false};
  bool light_path = false;
};

void pin(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
    throw std::runtime_error("pin failed");
  }
}

std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());
}

int publisher(const std::string& name, Flags& flags) {
  ::prctl(PR_SET_PDEATHSIG, SIGKILL);
  pin(2);
  auto state = salias::ChannelTraits<salias::Mode::FifoMpmc>::connect(
      name, salias::detail::kRawPayloadSchema);
  if (!state) return 2;
  const auto id = state->acquire_producer_id();
  if (!id) return 3;
  auto tx = state->tx(*id);
  static_cast<void>(tx);
#if __has_include("core/platform/asymmetric_fence.hpp")
  flags.light_path = salias::platform::fence::publisher_light_path_ready();
#endif
  flags.ready.store(1, std::memory_order_release);
  while (!flags.stop.load(std::memory_order_acquire)) {
    std::atomic_signal_fence(std::memory_order_seq_cst);
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const unsigned samples = argc == 2 ? static_cast<unsigned>(std::stoul(argv[1])) : 5000;
    if (argc > 2 || samples == 0) return 2;
    pin(1);
    auto* memory = ::mmap(nullptr, sizeof(Flags), PROT_READ | PROT_WRITE,
                          MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (memory == MAP_FAILED) return 3;
    auto* flags = std::construct_at(static_cast<Flags*>(memory));
    salias::Config config;
    config.name = "p31-registration-" + std::to_string(::getpid());
    config.mode = salias::Mode::FifoMpmc;
    config.num_consumers = 2;
    auto state = salias::ChannelTraits<salias::Mode::FifoMpmc>::create(
        config, salias::detail::kRawPayloadSchema);
    if (!state) throw std::runtime_error("create failed");
    const auto pid = ::fork();
    if (pid < 0) throw std::runtime_error("fork failed");
    if (pid == 0) {
      const int result = publisher(config.name, *flags);
      if (result) flags->ready.store(2, std::memory_order_release);
      ::_exit(result);
    }
    const auto deadline = now_ns() + 5'000'000'000ULL;
    while (flags->ready.load(std::memory_order_acquire) == 0 && now_ns() < deadline) {
      std::this_thread::yield();
    }
    if (flags->ready.load(std::memory_order_acquire) != 1) {
      ::kill(pid, SIGKILL);
      ::waitpid(pid, nullptr, 0);
      throw std::runtime_error("publisher startup failed");
    }
    std::vector<std::uint64_t> times;
    times.reserve(samples);
    for (unsigned i = 0; i < samples + 1000; ++i) {
      const auto start = now_ns();
      auto registration = state->acquire_consumer(salias::SubscriptionPolicy::Reliable);
      const auto elapsed = now_ns() - start;
      if (!registration) {
        flags->stop.store(true, std::memory_order_release);
        ::waitpid(pid, nullptr, 0);
        throw std::runtime_error("acquire_consumer failed");
      }
      if (i >= 1000) times.push_back(elapsed);
      // Release outside the timed interval, before the next registration.
    }
    flags->stop.store(true, std::memory_order_release);
    int status = 0;
    if (::waitpid(pid, &status, 0) != pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      throw std::runtime_error("publisher failed");
    }
    const bool light_path = flags->light_path;
    ::munmap(flags, sizeof(Flags));
    std::cout << "{\"samples\":" << times.size()
              << ",\"warmup\":1000,\"registrar_cpu\":1,\"publisher_cpu\":2,"
                 "\"publisher_busy_but_not_claiming\":true,\"publisher_light_path\":"
              << (light_path ? "true" : "false") << ",\"elapsed_ns\":[";
    for (std::size_t i = 0; i < times.size(); ++i) {
      if (i) std::cout << ',';
      std::cout << times[i];
    }
    std::cout << "]}\n";
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}

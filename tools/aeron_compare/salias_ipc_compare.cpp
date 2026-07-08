#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "salias/salias.hpp"

namespace {

inline constexpr std::uint32_t kMaxWorkers = 64;

struct Options {
  std::string scenario = "mpsc";
  std::uint64_t messages = 200'000;
  std::uint32_t producers = 4;
  std::uint32_t consumers = 1;
  std::size_t payload = 64;
  std::size_t capacity = 1u << 22;
  std::uint32_t poll_limit = 64;
  std::string name;
};

struct alignas(64) SharedRunState {
  std::uint32_t ready = 0;
  std::byte ready_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::uint32_t start = 0;
  std::byte start_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::uint32_t failed = 0;
  std::byte failed_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> consumed{};
};

static_assert(alignof(SharedRunState) == 64);

// 解析 salias IPC 对比工具的命令行参数。
Options parse_options(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto require_value = [&](const char* name) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << name << '\n';
        std::exit(2);
      }
      return argv[++i];
    };

    if (arg == "--scenario") {
      options.scenario = require_value("--scenario");
    } else if (arg == "--messages") {
      options.messages = std::stoull(require_value("--messages"));
    } else if (arg == "--producers") {
      options.producers = static_cast<std::uint32_t>(std::stoul(require_value("--producers")));
    } else if (arg == "--consumers") {
      options.consumers = static_cast<std::uint32_t>(std::stoul(require_value("--consumers")));
    } else if (arg == "--payload") {
      options.payload = static_cast<std::size_t>(std::stoull(require_value("--payload")));
    } else if (arg == "--capacity") {
      options.capacity = static_cast<std::size_t>(std::stoull(require_value("--capacity")));
    } else if (arg == "--poll-limit") {
      options.poll_limit = static_cast<std::uint32_t>(std::stoul(require_value("--poll-limit")));
    } else if (arg == "--name") {
      options.name = require_value("--name");
    } else if (arg == "--help") {
      std::cout << "usage: salias_ipc_compare --scenario mpsc|mpmc "
                   "[--messages N] [--producers N] [--consumers N] [--payload N] "
                   "[--capacity N] [--poll-limit N] [--name NAME]\n";
      std::exit(0);
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(2);
    }
  }

  if (options.scenario == "mpsc") {
    if (options.producers == 1) {
      options.producers = 4;
    }
    options.consumers = 1;
  } else if (options.scenario == "mpmc") {
    if (options.producers == 1) {
      options.producers = 4;
    }
    if (options.consumers == 1) {
      options.consumers = 2;
    }
  } else {
    std::cerr << "unknown scenario: " << options.scenario << '\n';
    std::exit(2);
  }

  if (options.producers == 0 || options.consumers == 0 ||
      options.producers + options.consumers > kMaxWorkers || options.payload == 0 ||
      options.poll_limit == 0) {
    std::cerr << "invalid producers/consumers/payload/poll-limit\n";
    std::exit(2);
  }

  if (options.name.empty()) {
    options.name = "salias-ipc-" + std::to_string(::getpid()) + "-" + options.scenario;
  }
  return options;
}

salias::Config make_named_config(const Options& options) {
  salias::Config config;
  config.name = options.name;
  config.capacity = options.capacity;
  config.mode = options.scenario == "mpsc" ? salias::Mode::Mpsc : salias::Mode::Mpmc;
  return config;
}

SharedRunState& create_shared_state() {
  void* mapping = ::mmap(nullptr, sizeof(SharedRunState), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    throw std::runtime_error("failed to mmap shared run state");
  }
  auto* state = new (mapping) SharedRunState();
  return *state;
}

void destroy_shared_state(SharedRunState& state) noexcept {
  state.~SharedRunState();
  static_cast<void>(::munmap(&state, sizeof(SharedRunState)));
}

void mark_failed(SharedRunState& state) noexcept {
  std::atomic_ref<std::uint32_t>(state.failed).store(1, std::memory_order_release);
}

bool has_failed(SharedRunState& state) noexcept {
  return std::atomic_ref<std::uint32_t>(state.failed).load(std::memory_order_acquire) != 0;
}

void ready_and_wait_for_start(SharedRunState& state) noexcept {
  std::atomic_ref<std::uint32_t>(state.ready).fetch_add(1, std::memory_order_release);
  while (std::atomic_ref<std::uint32_t>(state.start).load(std::memory_order_acquire) == 0) {
    if (has_failed(state)) {
      return;
    }
    std::this_thread::yield();
  }
}

bool claim_until_committed(salias::Publisher& publisher, std::size_t payload_len,
                           std::uint64_t marker, SharedRunState& state) noexcept {
  for (;;) {
    if (has_failed(state)) {
      return false;
    }
    auto claim_result = publisher.try_claim(payload_len);
    if (claim_result) {
      auto claim = std::move(claim_result).value();
      if (claim.payload().size() >= sizeof(marker)) {
        std::memcpy(claim.payload().data(), &marker, sizeof(marker));
      }
      claim.commit();
      return true;
    }
    if (claim_result.error() != salias::Error::BackPressured) {
      mark_failed(state);
      return false;
    }
    std::this_thread::yield();
  }
}

[[noreturn]] void run_consumer_child(const Options& options, std::uint32_t consumer_index,
                                     SharedRunState& state) noexcept {
  try {
    auto connected = salias::Channel::connect(options.name);
    if (!connected) {
      mark_failed(state);
      _exit(20);
    }
    auto channel = std::move(connected).value();
    auto subscriber = channel.subscriber();
    ready_and_wait_for_start(state);
    if (has_failed(state)) {
      _exit(21);
    }

    const std::uint64_t expected =
        static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t consumed = 0;
    auto handler = [&consumed](const salias::Message&) noexcept { ++consumed; };
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (consumed < expected) {
      if (has_failed(state) || std::chrono::steady_clock::now() > deadline) {
        mark_failed(state);
        _exit(22);
      }
      const auto polled = subscriber.poll(options.poll_limit, handler);
      if (polled == 0) {
        std::this_thread::yield();
      }
    }
    std::atomic_ref<std::uint64_t>(state.consumed[consumer_index])
        .store(consumed, std::memory_order_release);
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(23);
  }
}

[[noreturn]] void run_producer_child(const Options& options, std::uint32_t producer_index,
                                     SharedRunState& state) noexcept {
  try {
    auto connected = salias::Channel::connect(options.name);
    if (!connected) {
      mark_failed(state);
      _exit(30);
    }
    auto channel = std::move(connected).value();
    auto publisher = channel.publisher();
    ready_and_wait_for_start(state);
    if (has_failed(state)) {
      _exit(31);
    }

    for (std::uint64_t i = 0; i < options.messages; ++i) {
      const std::uint64_t marker = (static_cast<std::uint64_t>(producer_index) << 48) | i;
      if (!claim_until_committed(publisher, options.payload, marker, state)) {
        _exit(32);
      }
    }
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(33);
  }
}

pid_t fork_consumer(const Options& options, std::uint32_t consumer_index,
                    SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork consumer failed");
  }
  if (pid == 0) {
    run_consumer_child(options, consumer_index, state);
  }
  return pid;
}

pid_t fork_producer(const Options& options, std::uint32_t producer_index,
                    SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork producer failed");
  }
  if (pid == 0) {
    run_producer_child(options, producer_index, state);
  }
  return pid;
}

struct Result {
  std::uint32_t producers = 0;
  std::uint32_t consumers = 0;
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::size_t payload = 0;
  std::uint32_t poll_limit = 0;
  double seconds = 0.0;
};

Result run_case(const Options& options) {
  auto owner_result = salias::Channel::create(make_named_config(options));
  if (!owner_result) {
    throw std::runtime_error("failed to create named salias IPC channel");
  }
  auto owner = std::move(owner_result).value();
  SharedRunState& state = create_shared_state();

  std::vector<pid_t> children;
  children.reserve(options.producers + options.consumers);
  try {
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      children.push_back(fork_consumer(options, consumer, state));
    }
    for (std::uint32_t producer = 0; producer < options.producers; ++producer) {
      children.push_back(fork_producer(options, producer, state));
    }

    const std::uint32_t expected_ready = options.producers + options.consumers;
    while (std::atomic_ref<std::uint32_t>(state.ready).load(std::memory_order_acquire) !=
           expected_ready) {
      if (has_failed(state)) {
        throw std::runtime_error("child failed before benchmark start");
      }
      std::this_thread::yield();
    }

    const auto begin = std::chrono::steady_clock::now();
    std::atomic_ref<std::uint32_t>(state.start).store(1, std::memory_order_release);

    for (const pid_t child : children) {
      int status = 0;
      if (::waitpid(child, &status, 0) != child) {
        mark_failed(state);
        throw std::runtime_error("waitpid failed");
      }
      if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        mark_failed(state);
      }
    }
    const auto end = std::chrono::steady_clock::now();

    if (has_failed(state)) {
      throw std::runtime_error("salias IPC child failed");
    }

    const std::uint64_t expected_per_consumer =
        static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t delivered = 0;
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      const auto value =
          std::atomic_ref<std::uint64_t>(state.consumed[consumer]).load(std::memory_order_acquire);
      if (value != expected_per_consumer) {
        throw std::runtime_error("salias IPC delivery mismatch");
      }
      delivered += value;
    }

    destroy_shared_state(state);
    return Result{.producers = options.producers,
                  .consumers = options.consumers,
                  .published = static_cast<std::uint64_t>(options.producers) * options.messages,
                  .delivered = delivered,
                  .payload = options.payload,
                  .poll_limit = options.poll_limit,
                  .seconds = std::chrono::duration<double>(end - begin).count()};
  } catch (...) {
    mark_failed(state);
    for (const pid_t child : children) {
      static_cast<void>(::kill(child, SIGTERM));
      int status = 0;
      static_cast<void>(::waitpid(child, &status, 0));
    }
    destroy_shared_state(state);
    throw;
  }
}

void print_result(const std::string& scenario, const Result& result) {
  const double publish_rate = static_cast<double>(result.published) / result.seconds;
  const double delivery_rate = static_cast<double>(result.delivered) / result.seconds;
  const double mib_per_second =
      delivery_rate * static_cast<double>(result.payload) / (1024.0 * 1024.0);
  std::cout << "RESULT library=salias-ipc"
            << " scenario=" << scenario
            << " producers=" << result.producers
            << " consumers=" << result.consumers
            << " payload=" << result.payload
            << " poll_limit=" << result.poll_limit
            << " published=" << result.published
            << " delivered=" << result.delivered
            << " seconds=" << result.seconds
            << " publish_msg_per_sec=" << publish_rate
            << " delivery_msg_per_sec=" << delivery_rate
            << " delivery_mib_per_sec=" << mib_per_second << '\n';
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = parse_options(argc, argv);
    print_result(options.scenario, run_case(options));
  } catch (const std::exception& error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 1;
  }
  return 0;
}

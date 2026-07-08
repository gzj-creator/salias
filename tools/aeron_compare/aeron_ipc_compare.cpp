#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "Aeron.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "concurrent/logbuffer/BufferClaim.h"

namespace {

inline constexpr std::uint32_t kMaxWorkers = 64;

struct Options {
  std::string scenario = "spsc";
  std::string dir;
  std::uint64_t messages = 1'000'000;
  std::uint32_t producers = 1;
  std::uint32_t consumers = 1;
  std::int32_t stream_id = 1001;
  std::int32_t payload = 64;
  std::int32_t fragment_limit = 64;
};

struct SharedRunState {
  std::atomic<std::uint32_t> ready{0};
  std::atomic<std::uint32_t> start{0};
  std::atomic<std::uint32_t> failed{0};
  std::array<std::atomic<std::uint64_t>, kMaxWorkers> consumed{};
};

// 解析 Aeron IPC 对比工具的命令行参数。
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
    } else if (arg == "--dir") {
      options.dir = require_value("--dir");
    } else if (arg == "--messages") {
      options.messages = std::stoull(require_value("--messages"));
    } else if (arg == "--producers") {
      options.producers = static_cast<std::uint32_t>(std::stoul(require_value("--producers")));
    } else if (arg == "--consumers") {
      options.consumers = static_cast<std::uint32_t>(std::stoul(require_value("--consumers")));
    } else if (arg == "--payload") {
      options.payload = static_cast<std::int32_t>(std::stol(require_value("--payload")));
    } else if (arg == "--stream") {
      options.stream_id = static_cast<std::int32_t>(std::stol(require_value("--stream")));
    } else if (arg == "--fragment-limit") {
      options.fragment_limit =
          static_cast<std::int32_t>(std::stol(require_value("--fragment-limit")));
    } else if (arg == "--help") {
      std::cout << "usage: aeron_ipc_compare --dir DIR --scenario spsc|mpsc|spmc|mpmc "
                   "[--messages N] [--producers N] [--consumers N] [--payload N]\n";
      std::exit(0);
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(2);
    }
  }

  if (options.dir.empty()) {
    std::cerr << "--dir is required\n";
    std::exit(2);
  }
  return options;
}

SharedRunState& create_shared_state() {
  void* mapping = ::mmap(nullptr, sizeof(SharedRunState), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    throw std::runtime_error("failed to mmap shared run state");
  }
  return *new (mapping) SharedRunState();
}

void destroy_shared_state(SharedRunState& state) noexcept {
  state.~SharedRunState();
  static_cast<void>(::munmap(&state, sizeof(SharedRunState)));
}

void mark_failed(SharedRunState& state) noexcept {
  state.failed.store(1, std::memory_order_release);
}

bool has_failed(SharedRunState& state) noexcept {
  return state.failed.load(std::memory_order_acquire) != 0;
}

bool ready_and_wait_for_start(SharedRunState& state) noexcept {
  state.ready.fetch_add(1, std::memory_order_release);
  while (state.start.load(std::memory_order_acquire) == 0) {
    if (has_failed(state)) {
      return false;
    }
    std::this_thread::yield();
  }
  return true;
}

std::shared_ptr<aeron::Subscription> find_subscription(aeron::Aeron& aeron,
                                                       std::int64_t id,
                                                       SharedRunState& state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (has_failed(state)) {
      return nullptr;
    }
    auto subscription = aeron.findSubscription(id);
    if (subscription) {
      return subscription;
    }
    std::this_thread::yield();
  }
  return nullptr;
}

std::shared_ptr<aeron::ExclusivePublication> find_publication(aeron::Aeron& aeron,
                                                              std::int64_t id,
                                                              SharedRunState& state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (has_failed(state)) {
      return nullptr;
    }
    auto publication = aeron.findExclusivePublication(id);
    if (publication) {
      return publication;
    }
    std::this_thread::yield();
  }
  return nullptr;
}

bool wait_subscription_connected(const std::shared_ptr<aeron::Subscription>& subscription,
                                 std::uint32_t expected_images, SharedRunState& state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (has_failed(state)) {
      return false;
    }
    if (subscription && subscription->isConnected() &&
        subscription->imageCount() >= static_cast<int>(expected_images)) {
      return true;
    }
    std::this_thread::yield();
  }
  return false;
}

bool wait_publication_connected(const std::shared_ptr<aeron::ExclusivePublication>& publication,
                                SharedRunState& state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (has_failed(state)) {
      return false;
    }
    if (publication && publication->isConnected()) {
      return true;
    }
    std::this_thread::yield();
  }
  return false;
}

struct Result {
  std::uint32_t producers = 0;
  std::uint32_t consumers = 0;
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::int32_t payload = 0;
  double seconds = 0.0;
};

[[noreturn]] void run_consumer_child(const Options& options, std::uint32_t consumer_index,
                                     SharedRunState& state) noexcept {
  try {
    aeron::Context context;
    context.aeronDir(options.dir);
    aeron::Aeron aeron(context);

    const std::string channel = "aeron:ipc";
    const auto id = aeron.addSubscription(channel, options.stream_id);
    auto subscription = find_subscription(aeron, id, state);
    if (!subscription ||
        !wait_subscription_connected(subscription, options.producers, state)) {
      mark_failed(state);
      _exit(20);
    }
    if (!ready_and_wait_for_start(state)) {
      _exit(21);
    }

    const std::uint64_t expected =
        static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t consumed = 0;
    aeron::concurrent::BusySpinIdleStrategy idle;
    auto handler = [&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                       aeron::util::index_t, aeron::Header&) {
      ++consumed;
    };
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (consumed < expected) {
      if (has_failed(state) || std::chrono::steady_clock::now() > deadline) {
        mark_failed(state);
        _exit(22);
      }
      const int fragments = subscription->poll(handler, options.fragment_limit);
      idle.idle(fragments);
    }
    state.consumed[consumer_index].store(consumed, std::memory_order_release);
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(23);
  }
}

[[noreturn]] void run_producer_child(const Options& options, std::uint32_t producer_index,
                                     SharedRunState& state) noexcept {
  try {
    aeron::Context context;
    context.aeronDir(options.dir);
    aeron::Aeron aeron(context);

    const std::string channel = "aeron:ipc";
    const auto id = aeron.addExclusivePublication(channel, options.stream_id);
    auto publication = find_publication(aeron, id, state);
    if (!publication || !wait_publication_connected(publication, state)) {
      mark_failed(state);
      _exit(30);
    }
    if (!ready_and_wait_for_start(state)) {
      _exit(31);
    }

    aeron::concurrent::BusySpinIdleStrategy idle;
    aeron::concurrent::logbuffer::BufferClaim claim;
    for (std::uint64_t i = 0; i < options.messages; ++i) {
      idle.reset();
      for (;;) {
        if (has_failed(state)) {
          _exit(32);
        }
        const auto position = publication->tryClaim(options.payload, claim);
        if (position > 0) {
          const std::int64_t marker =
              static_cast<std::int64_t>((static_cast<std::uint64_t>(producer_index) << 48) | i);
          claim.buffer().putInt64(claim.offset(), marker);
          claim.commit();
          break;
        }
        if (position == aeron::PUBLICATION_CLOSED ||
            position == aeron::MAX_POSITION_EXCEEDED) {
          mark_failed(state);
          _exit(33);
        }
        idle.idle();
      }
    }
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(34);
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

// 按请求的生产者和消费者进程数量运行一个 Aeron IPC benchmark 场景。
Result run_case(const Options& options) {
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
    while (state.ready.load(std::memory_order_acquire) != expected_ready) {
      if (has_failed(state)) {
        throw std::runtime_error("Aeron child failed before benchmark start");
      }
      std::this_thread::yield();
    }

    const auto begin = std::chrono::steady_clock::now();
    state.start.store(1, std::memory_order_release);

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
      throw std::runtime_error("Aeron IPC child failed");
    }

    const std::uint64_t expected_per_consumer =
        static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t delivered = 0;
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      const auto value = state.consumed[consumer].load(std::memory_order_acquire);
      if (value != expected_per_consumer) {
        throw std::runtime_error("Aeron delivery mismatch");
      }
      delivered += value;
    }

    destroy_shared_state(state);
    return Result{.producers = options.producers,
                  .consumers = options.consumers,
                  .published = static_cast<std::uint64_t>(options.producers) * options.messages,
                  .delivered = delivered,
                  .payload = options.payload,
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

// 打印一行机器可读的 Aeron benchmark 结果。
void print_result(const std::string& scenario, const Result& result) {
  const double publish_rate = static_cast<double>(result.published) / result.seconds;
  const double delivery_rate = static_cast<double>(result.delivered) / result.seconds;
  const double mib_per_second =
      delivery_rate * static_cast<double>(result.payload) / (1024.0 * 1024.0);
  std::cout << "RESULT library=aeron-cpp"
            << " scenario=" << scenario
            << " producers=" << result.producers
            << " consumers=" << result.consumers
            << " payload=" << result.payload
            << " published=" << result.published
            << " delivered=" << result.delivered
            << " seconds=" << result.seconds
            << " publish_msg_per_sec=" << publish_rate
            << " delivery_msg_per_sec=" << delivery_rate
            << " delivery_mib_per_sec=" << mib_per_second << '\n';
}

}  // namespace

// 规整请求的场景，运行 benchmark，并报告失败原因。
int main(int argc, char** argv) {
  try {
    Options options = parse_options(argc, argv);
    if (options.scenario == "spsc") {
      options.producers = 1;
      options.consumers = 1;
    } else if (options.scenario == "mpsc") {
      if (options.producers == 1) {
        options.producers = 4;
      }
      options.consumers = 1;
    } else if (options.scenario == "spmc") {
      options.producers = 1;
      if (options.consumers == 1) {
        options.consumers = 2;
      }
    } else if (options.scenario == "mpmc") {
      if (options.producers == 1) {
        options.producers = 4;
      }
      if (options.consumers == 1) {
        options.consumers = 2;
      }
    } else {
      std::cerr << "unknown scenario: " << options.scenario << '\n';
      return 2;
    }
    if (options.producers == 0 || options.consumers == 0 ||
        options.producers + options.consumers > kMaxWorkers || options.payload <= 0 ||
        options.fragment_limit <= 0) {
      std::cerr << "invalid producers/consumers/payload/fragment-limit\n";
      return 2;
    }
    print_result(options.scenario, run_case(options));
  } catch (const std::exception& error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 1;
  }
  return 0;
}

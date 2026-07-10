#include <sched.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
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
#include "latency_histogram.hpp"

namespace {

inline constexpr std::uint32_t kMaxWorkers = 64;
namespace latency = salias::tools::latency;

struct Options {
  std::string scenario = "spsc";
  std::string dir;
  std::uint64_t messages = 1'000'000;
  std::uint32_t producers = 1;
  std::uint32_t consumers = 1;
  std::int32_t stream_id = 1001;
  std::int32_t payload = 64;
  std::int32_t fragment_limit = 64;
  std::int32_t term_length = 0;
  int cpu_base = -1;
  std::uint32_t cpu_stride = 1;
  std::uint64_t latency_sample_rate = 0;
  bool producers_set = false;
  bool consumers_set = false;
};

struct SharedRunState {
  std::atomic<std::uint32_t> ready{0};
  std::atomic<std::uint32_t> start{0};
  std::atomic<std::uint32_t> failed{0};
  std::array<std::atomic<std::uint64_t>, kMaxWorkers> consumed{};
  std::array<std::uint64_t, kMaxWorkers> latency_samples{};
  std::array<std::uint64_t, kMaxWorkers> latency_max_ns{};
  alignas(64)
      std::array<std::array<std::uint64_t, latency::kBucketCount>, kMaxWorkers> latency_buckets{};
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
      options.producers_set = true;
    } else if (arg == "--consumers") {
      options.consumers = static_cast<std::uint32_t>(std::stoul(require_value("--consumers")));
      options.consumers_set = true;
    } else if (arg == "--payload") {
      options.payload = static_cast<std::int32_t>(std::stol(require_value("--payload")));
    } else if (arg == "--stream") {
      options.stream_id = static_cast<std::int32_t>(std::stol(require_value("--stream")));
    } else if (arg == "--fragment-limit") {
      options.fragment_limit =
          static_cast<std::int32_t>(std::stol(require_value("--fragment-limit")));
    } else if (arg == "--term-length") {
      options.term_length = static_cast<std::int32_t>(std::stol(require_value("--term-length")));
    } else if (arg == "--cpu-base") {
      options.cpu_base = std::stoi(require_value("--cpu-base"));
    } else if (arg == "--cpu-stride") {
      options.cpu_stride = static_cast<std::uint32_t>(std::stoul(require_value("--cpu-stride")));
    } else if (arg == "--latency-sample-rate") {
      options.latency_sample_rate = std::stoull(require_value("--latency-sample-rate"));
    } else if (arg == "--help") {
      std::cout << "usage: aeron_ipc_compare --dir DIR --scenario spsc|mpsc|spmc|mpmc "
                   "[--messages N] [--producers N] [--consumers N] [--payload N] "
                   "[--fragment-limit N] [--term-length N] [--cpu-base N] [--cpu-stride N] "
                   "[--latency-sample-rate N]\n";
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

bool pinning_enabled(const Options& options) noexcept { return options.cpu_base >= 0; }

int worker_cpu(const Options& options, std::uint32_t worker_index) noexcept {
  return options.cpu_base + static_cast<int>(worker_index * options.cpu_stride);
}

void pin_worker_if_requested(const Options& options, std::uint32_t worker_index) {
  if (!pinning_enabled(options)) {
    return;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(worker_cpu(options, worker_index), &set);
  if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
    throw std::runtime_error("failed to pin Aeron IPC worker");
  }
}

std::string ipc_channel(const Options& options) {
  if (options.term_length <= 0) {
    return "aeron:ipc";
  }
  return "aeron:ipc?term-length=" + std::to_string(options.term_length);
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

std::shared_ptr<aeron::Subscription> find_subscription(aeron::Aeron& aeron, std::int64_t id,
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

std::shared_ptr<aeron::ExclusivePublication> find_publication(aeron::Aeron& aeron, std::int64_t id,
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
  struct LatencySummary {
    std::uint64_t samples = 0;
    std::uint64_t p50_ns = 0;
    std::uint64_t p99_ns = 0;
    std::uint64_t p999_ns = 0;
    std::uint64_t max_ns = 0;
  };

  std::uint32_t producers = 0;
  std::uint32_t consumers = 0;
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::int32_t payload = 0;
  double seconds = 0.0;
  std::vector<LatencySummary> consumer_latency;
  LatencySummary aggregate_latency;
};

Result::LatencySummary summarize_latency(const std::uint64_t* buckets, std::uint64_t samples,
                                         std::uint64_t max_ns) noexcept {
  return Result::LatencySummary{
      samples,
      latency::percentile(buckets, samples, 50, 100),
      latency::percentile(buckets, samples, 99, 100),
      latency::percentile(buckets, samples, 999, 1000),
      max_ns,
  };
}

[[noreturn]] void run_consumer_child(const Options& options, std::uint32_t consumer_index,
                                     SharedRunState& state) noexcept {
  try {
    pin_worker_if_requested(options, consumer_index);
    aeron::Context context;
    context.aeronDir(options.dir);
    aeron::Aeron aeron(context);

    const std::string channel = ipc_channel(options);
    const auto id = aeron.addSubscription(channel, options.stream_id);
    auto subscription = find_subscription(aeron, id, state);
    if (!subscription || !wait_subscription_connected(subscription, options.producers, state)) {
      mark_failed(state);
      _exit(20);
    }
    if (!ready_and_wait_for_start(state)) {
      _exit(21);
    }

    const std::uint64_t expected = static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t consumed = 0;
    std::uint64_t sample_count = 0;
    std::uint64_t max_latency_ns = 0;
    auto* const latency_buckets = state.latency_buckets[consumer_index].data();
    aeron::concurrent::BusySpinIdleStrategy idle;
    auto handler = [&](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                       aeron::util::index_t length, aeron::Header&) {
      ++consumed;
      if (options.latency_sample_rate == 0 || length < static_cast<aeron::util::index_t>(8)) {
        return;
      }
      const std::uint64_t send_time_ns = static_cast<std::uint64_t>(buffer.getInt64(offset));
      if (send_time_ns == 0) {
        return;
      }
      const std::uint64_t receive_time_ns = latency::monotonic_now_ns();
      if (receive_time_ns < send_time_ns) {
        return;
      }
      const std::uint64_t elapsed_ns = receive_time_ns - send_time_ns;
      latency::record(latency_buckets, elapsed_ns);
      ++sample_count;
      max_latency_ns = std::max(max_latency_ns, elapsed_ns);
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
    state.latency_samples[consumer_index] = sample_count;
    state.latency_max_ns[consumer_index] = max_latency_ns;
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(23);
  }
}

[[noreturn]] void run_producer_child(const Options& options, std::uint32_t producer_index,
                                     SharedRunState& state) noexcept {
  try {
    pin_worker_if_requested(options, options.consumers + producer_index);
    aeron::Context context;
    context.aeronDir(options.dir);
    aeron::Aeron aeron(context);

    const std::string channel = ipc_channel(options);
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
          const bool sampled =
              options.latency_sample_rate != 0 && i % options.latency_sample_rate == 0;
          if (options.latency_sample_rate != 0) {
            const std::uint64_t send_time_ns = sampled ? latency::monotonic_now_ns() : 0;
            claim.buffer().putInt64(claim.offset(), static_cast<std::int64_t>(send_time_ns));
            claim.buffer().putInt64(claim.offset() + 8, marker);
          } else {
            claim.buffer().putInt64(claim.offset(), marker);
          }
          claim.commit();
          break;
        }
        if (position == aeron::PUBLICATION_CLOSED || position == aeron::MAX_POSITION_EXCEEDED) {
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

pid_t fork_consumer(const Options& options, std::uint32_t consumer_index, SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork consumer failed");
  }
  if (pid == 0) {
    run_consumer_child(options, consumer_index, state);
  }
  return pid;
}

pid_t fork_producer(const Options& options, std::uint32_t producer_index, SharedRunState& state) {
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
    std::vector<Result::LatencySummary> consumer_latency;
    consumer_latency.reserve(options.consumers);
    std::array<std::uint64_t, latency::kBucketCount> aggregate_buckets{};
    std::uint64_t aggregate_samples = 0;
    std::uint64_t aggregate_max_ns = 0;
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      const auto value = state.consumed[consumer].load(std::memory_order_acquire);
      if (value != expected_per_consumer) {
        throw std::runtime_error("Aeron delivery mismatch");
      }
      delivered += value;
      const std::uint64_t samples = state.latency_samples[consumer];
      const std::uint64_t max_ns = state.latency_max_ns[consumer];
      consumer_latency.push_back(
          summarize_latency(state.latency_buckets[consumer].data(), samples, max_ns));
      latency::merge(aggregate_buckets.data(), state.latency_buckets[consumer].data());
      aggregate_samples += samples;
      aggregate_max_ns = std::max(aggregate_max_ns, max_ns);
    }

    const Result::LatencySummary aggregate_latency =
        summarize_latency(aggregate_buckets.data(), aggregate_samples, aggregate_max_ns);

    destroy_shared_state(state);
    return Result{.producers = options.producers,
                  .consumers = options.consumers,
                  .published = static_cast<std::uint64_t>(options.producers) * options.messages,
                  .delivered = delivered,
                  .payload = options.payload,
                  .seconds = std::chrono::duration<double>(end - begin).count(),
                  .consumer_latency = std::move(consumer_latency),
                  .aggregate_latency = aggregate_latency};
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
void print_result(const Options& options, const std::string& scenario, const Result& result) {
  const double publish_rate = static_cast<double>(result.published) / result.seconds;
  const double delivery_rate = static_cast<double>(result.delivered) / result.seconds;
  const double mib_per_second =
      delivery_rate * static_cast<double>(result.payload) / (1024.0 * 1024.0);
  for (std::size_t consumer = 0; consumer < result.consumer_latency.size(); ++consumer) {
    const auto& summary = result.consumer_latency[consumer];
    std::cout << "LATENCY library=aeron-cpp" << " scenario=" << scenario << " consumer=" << consumer
              << " samples=" << summary.samples << " p50_ns=" << summary.p50_ns
              << " p99_ns=" << summary.p99_ns << " p999_ns=" << summary.p999_ns
              << " max_ns=" << summary.max_ns << '\n';
  }
  std::cout << "RESULT library=aeron-cpp" << " scenario=" << scenario
            << " pinning=" << (pinning_enabled(options) ? "on" : "off")
            << " cpu_base=" << options.cpu_base << " cpu_stride=" << options.cpu_stride
            << " term_length=" << options.term_length << " producers=" << result.producers
            << " consumers=" << result.consumers << " payload=" << result.payload
            << " published=" << result.published << " delivered=" << result.delivered
            << " seconds=" << result.seconds << " publish_msg_per_sec=" << publish_rate
            << " delivery_msg_per_sec=" << delivery_rate
            << " delivery_mib_per_sec=" << mib_per_second
            << " latency_sample_rate=" << options.latency_sample_rate
            << " latency_samples=" << result.aggregate_latency.samples
            << " latency_p50_ns=" << result.aggregate_latency.p50_ns
            << " latency_p99_ns=" << result.aggregate_latency.p99_ns
            << " latency_p999_ns=" << result.aggregate_latency.p999_ns
            << " latency_max_ns=" << result.aggregate_latency.max_ns << '\n';
}

}  // namespace

// 规整请求的场景，运行 benchmark，并报告失败原因。
int main(int argc, char** argv) {
  try {
    Options options = parse_options(argc, argv);
    if (options.scenario == "spsc") {
      if (!options.producers_set) {
        options.producers = 1;
      }
      if (!options.consumers_set) {
        options.consumers = 1;
      }
    } else if (options.scenario == "mpsc") {
      if (!options.producers_set) {
        options.producers = 4;
      }
      if (!options.consumers_set) {
        options.consumers = 1;
      }
    } else if (options.scenario == "spmc") {
      if (!options.producers_set) {
        options.producers = 1;
      }
      if (!options.consumers_set) {
        options.consumers = 2;
      }
    } else if (options.scenario == "mpmc") {
      if (!options.producers_set) {
        options.producers = 4;
      }
      if (!options.consumers_set) {
        options.consumers = 2;
      }
    } else {
      std::cerr << "unknown scenario: " << options.scenario << '\n';
      return 2;
    }
    if (options.producers == 0 || options.consumers == 0 ||
        options.producers + options.consumers > kMaxWorkers || options.payload <= 0 ||
        options.fragment_limit <= 0 || options.term_length < 0 || options.cpu_stride == 0) {
      std::cerr << "invalid producers/consumers/payload/fragment-limit/term-length/cpu-stride\n";
      return 2;
    }
    if (options.latency_sample_rate != 0 && options.payload < 16) {
      std::cerr << "latency sampling requires payload >= 16 bytes\n";
      return 2;
    }
    print_result(options, options.scenario, run_case(options));
  } catch (const std::exception& error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 1;
  }
  return 0;
}

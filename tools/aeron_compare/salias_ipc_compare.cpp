#include <sched.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
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

#include "latency_histogram.hpp"
#include "salias/salias.hpp"

namespace {

inline constexpr std::uint32_t kMaxWorkers = 64;
inline constexpr std::size_t kHuge1GiB = std::size_t{1024} * 1024 * 1024;
namespace latency = salias::tools::latency;

struct Options {
  std::string scenario = "fifo";
  std::string page = "normal";
  std::uint64_t messages = 200'000;
  std::uint32_t producers = 4;
  std::uint32_t consumers = 1;
  std::size_t payload = 64;
  std::size_t capacity = 1u << 22;
  std::size_t batch_size = 1;
  bool capacity_set = false;
  std::uint32_t poll_limit = 64;
  std::string name;
  int cpu_base = -1;
  std::uint32_t cpu_stride = 1;
  std::uint64_t latency_sample_rate = 0;
  bool producers_set = false;
  bool consumers_set = false;
};

struct alignas(64) SharedRunState {
  std::uint32_t ready = 0;
  std::byte ready_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::uint32_t start = 0;
  std::byte start_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::uint32_t failed = 0;
  std::byte failed_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> consumed{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> latency_samples{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> latency_max_ns{};
  alignas(64)
      std::array<std::array<std::uint64_t, latency::kBucketCount>, kMaxWorkers> latency_buckets{};
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
      options.producers_set = true;
    } else if (arg == "--consumers") {
      options.consumers = static_cast<std::uint32_t>(std::stoul(require_value("--consumers")));
      options.consumers_set = true;
    } else if (arg == "--payload") {
      options.payload = static_cast<std::size_t>(std::stoull(require_value("--payload")));
    } else if (arg == "--capacity") {
      options.capacity = static_cast<std::size_t>(std::stoull(require_value("--capacity")));
      options.capacity_set = true;
    } else if (arg == "--batch-size") {
      options.batch_size = static_cast<std::size_t>(std::stoull(require_value("--batch-size")));
    } else if (arg == "--poll-limit") {
      options.poll_limit = static_cast<std::uint32_t>(std::stoul(require_value("--poll-limit")));
    } else if (arg == "--page") {
      options.page = require_value("--page");
    } else if (arg == "--name") {
      options.name = require_value("--name");
    } else if (arg == "--cpu-base") {
      options.cpu_base = std::stoi(require_value("--cpu-base"));
    } else if (arg == "--cpu-stride") {
      options.cpu_stride = static_cast<std::uint32_t>(std::stoul(require_value("--cpu-stride")));
    } else if (arg == "--latency-sample-rate") {
      options.latency_sample_rate = std::stoull(require_value("--latency-sample-rate"));
    } else if (arg == "--help") {
      std::cout << "usage: salias_ipc_compare --scenario fifo|ordered "
                   "[--messages N] [--producers N] [--consumers N] [--payload N] "
                   "[--capacity N] [--batch-size N] [--poll-limit N] "
                   "[--page normal|huge2m|huge1g] "
                   "[--name NAME] [--cpu-base N] [--cpu-stride N] "
                   "[--latency-sample-rate N]\n";
      std::exit(0);
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(2);
    }
  }

  if (options.scenario == "fifo" || options.scenario == "ordered") {
    if (!options.producers_set) {
      options.producers = 4;
    }
    if (!options.consumers_set) {
      options.consumers = 1;
    }
  } else {
    std::cerr << "unknown scenario: " << options.scenario << '\n';
    std::exit(2);
  }

  if (options.page != "normal" && options.page != "huge2m" && options.page != "huge1g") {
    std::cerr << "unknown page: " << options.page << '\n';
    std::exit(2);
  }
  if (!options.capacity_set && options.page == "huge1g") {
    options.capacity = kHuge1GiB;
  }

  if (options.producers == 0 || options.consumers == 0 ||
      options.producers + options.consumers > kMaxWorkers || options.payload == 0 ||
      options.batch_size == 0 || options.poll_limit == 0 || options.cpu_stride == 0) {
    std::cerr << "invalid producers/consumers/payload/batch-size/poll-limit/cpu-stride\n";
    std::exit(2);
  }
  if (options.latency_sample_rate != 0 && options.payload < 2 * sizeof(std::uint64_t)) {
    std::cerr << "latency sampling requires payload >= 16 bytes\n";
    std::exit(2);
  }
  if (options.name.empty()) {
    options.name = "salias-ipc-" + std::to_string(::getpid()) + "-" + options.scenario;
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
    throw std::runtime_error("failed to pin salias IPC worker");
  }
}

salias::HugePage huge_page_for(const std::string& page) noexcept {
  if (page == "huge2m") {
    return salias::HugePage::Size2MB;
  }
  if (page == "huge1g") {
    return salias::HugePage::Size1GB;
  }
  return salias::HugePage::None;
}

salias::Config make_named_config(const Options& options) {
  salias::Config config;
  config.name = options.name;
  config.capacity = options.capacity;
  config.huge = huge_page_for(options.page);
  config.num_producers = options.producers;
  config.num_consumers = options.consumers;
  if (options.scenario == "fifo") {
    config.mode = options.consumers == 1 ? salias::Mode::FifoMpsc : salias::Mode::FifoFanout;
  } else {
    config.mode = options.consumers == 1 ? salias::Mode::OrderedMpsc : salias::Mode::OrderedFanout;
  }
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

bool should_sample(const Options& options, std::uint64_t sequence) noexcept {
  return options.latency_sample_rate != 0 && sequence % options.latency_sample_rate == 0;
}

void write_payload(std::span<std::byte> payload, std::uint64_t marker, std::uint64_t send_time_ns,
                   bool latency_enabled) noexcept {
  const std::size_t marker_offset = latency_enabled ? sizeof(send_time_ns) : 0;
  if (latency_enabled && payload.size() >= sizeof(send_time_ns)) {
    std::memcpy(payload.data(), &send_time_ns, sizeof(send_time_ns));
  }
  if (payload.size() >= marker_offset + sizeof(marker)) {
    std::memcpy(payload.data() + marker_offset, &marker, sizeof(marker));
  }
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

template <salias::Mode M>
bool claim_until_committed(salias::Publisher<M>& publisher, std::size_t payload_len,
                           std::uint64_t marker, std::uint64_t sequence, const Options& options,
                           SharedRunState& state) noexcept {
  for (;;) {
    if (has_failed(state)) {
      return false;
    }
    auto claim_result = publisher.try_claim(payload_len);
    if (claim_result) {
      auto claim = std::move(claim_result).value();
      const std::uint64_t send_time_ns =
          should_sample(options, sequence) ? latency::monotonic_now_ns() : 0;
      write_payload(claim.payload(), marker, send_time_ns, options.latency_sample_rate != 0);
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

template <salias::Mode M>
bool offer_batch_until_committed(salias::Publisher<M>& publisher, const Options& options,
                                 std::uint32_t producer_index, SharedRunState& state) noexcept {
  std::vector<std::byte> payload_storage(options.batch_size * options.payload);
  std::vector<std::span<const std::byte>> payloads(options.batch_size);
  for (std::size_t i = 0; i < options.batch_size; ++i) {
    payloads[i] =
        std::span<const std::byte>(payload_storage.data() + i * options.payload, options.payload);
  }

  std::uint64_t published = 0;
  while (published < options.messages) {
    if (has_failed(state)) {
      return false;
    }
    const std::size_t requested = static_cast<std::size_t>(std::min<std::uint64_t>(
        static_cast<std::uint64_t>(options.batch_size), options.messages - published));
    for (std::size_t i = 0; i < requested; ++i) {
      const std::uint64_t sequence = published + i;
      const std::uint64_t marker = (static_cast<std::uint64_t>(producer_index) << 48) | sequence;
      const std::uint64_t send_time_ns =
          should_sample(options, sequence) ? latency::monotonic_now_ns() : 0;
      write_payload(
          std::span<std::byte>(payload_storage.data() + i * options.payload, options.payload),
          marker, send_time_ns, options.latency_sample_rate != 0);
    }

    auto offered = publisher.offer_batch(
        std::span<const std::span<const std::byte>>(payloads.data(), requested));
    if (offered) {
      const std::size_t count = offered.value();
      if (count > 0) {
        published += count;
        continue;
      }
    } else if (offered.error() != salias::Error::BackPressured) {
      mark_failed(state);
      return false;
    }
    std::this_thread::yield();
  }
  return true;
}

template <salias::Mode M>
[[noreturn]] void run_consumer_child(const Options& options, std::uint32_t consumer_index,
                                     SharedRunState& state) noexcept {
  try {
    pin_worker_if_requested(options, consumer_index);
    auto connected = salias::Channel<M>::connect(options.name);
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

    const std::uint64_t expected = static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t consumed = 0;
    std::uint64_t sample_count = 0;
    std::uint64_t max_latency_ns = 0;
    auto* const latency_buckets = state.latency_buckets[consumer_index].data();
    auto handler = [&](const salias::Message& message) noexcept {
      ++consumed;
      if (options.latency_sample_rate == 0 || message.payload.size() < sizeof(std::uint64_t)) {
        return;
      }
      std::uint64_t send_time_ns = 0;
      std::memcpy(&send_time_ns, message.payload.data(), sizeof(send_time_ns));
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
      static_cast<void>(subscriber.poll(options.poll_limit, handler));
    }
    std::atomic_ref<std::uint64_t>(state.consumed[consumer_index])
        .store(consumed, std::memory_order_release);
    state.latency_samples[consumer_index] = sample_count;
    state.latency_max_ns[consumer_index] = max_latency_ns;
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(23);
  }
}

template <salias::Mode M>
[[noreturn]] void run_producer_child(const Options& options, std::uint32_t producer_index,
                                     SharedRunState& state) noexcept {
  try {
    pin_worker_if_requested(options, options.consumers + producer_index);
    auto connected = salias::Channel<M>::connect(options.name);
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

    if (options.batch_size == 1) {
      for (std::uint64_t i = 0; i < options.messages; ++i) {
        const std::uint64_t marker = (static_cast<std::uint64_t>(producer_index) << 48) | i;
        if (!claim_until_committed<M>(publisher, options.payload, marker, i, options, state)) {
          _exit(32);
        }
      }
    } else {
      if (!offer_batch_until_committed<M>(publisher, options, producer_index, state)) {
        _exit(32);
      }
    }
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(33);
  }
}

template <salias::Mode M>
pid_t fork_consumer(const Options& options, std::uint32_t consumer_index, SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork consumer failed");
  }
  if (pid == 0) {
    run_consumer_child<M>(options, consumer_index, state);
  }
  return pid;
}

template <salias::Mode M>
pid_t fork_producer(const Options& options, std::uint32_t producer_index, SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork producer failed");
  }
  if (pid == 0) {
    run_producer_child<M>(options, producer_index, state);
  }
  return pid;
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
  std::size_t payload = 0;
  std::size_t capacity = 0;
  std::uint32_t poll_limit = 0;
  double seconds = 0.0;
  std::vector<LatencySummary> consumer_latency;
  LatencySummary aggregate_latency;
};

Result::LatencySummary summarize_latency(const std::uint64_t* buckets, std::uint64_t samples,
                                         std::uint64_t max_ns) noexcept {
  return Result::LatencySummary{
      .samples = samples,
      .p50_ns = latency::percentile(buckets, samples, 50, 100),
      .p99_ns = latency::percentile(buckets, samples, 99, 100),
      .p999_ns = latency::percentile(buckets, samples, 999, 1000),
      .max_ns = max_ns,
  };
}

template <salias::Mode M>
Result run_case_typed(const Options& options) {
  auto owner_result = salias::Channel<M>::create(make_named_config(options));
  if (!owner_result) {
    throw std::runtime_error("failed to create named salias IPC channel");
  }
  auto owner = std::move(owner_result).value();
  SharedRunState& state = create_shared_state();

  std::vector<pid_t> children;
  children.reserve(options.producers + options.consumers);
  try {
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      children.push_back(fork_consumer<M>(options, consumer, state));
    }
    for (std::uint32_t producer = 0; producer < options.producers; ++producer) {
      children.push_back(fork_producer<M>(options, producer, state));
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
    std::vector<Result::LatencySummary> consumer_latency;
    consumer_latency.reserve(options.consumers);
    std::array<std::uint64_t, latency::kBucketCount> aggregate_buckets{};
    std::uint64_t aggregate_samples = 0;
    std::uint64_t aggregate_max_ns = 0;
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      const auto value =
          std::atomic_ref<std::uint64_t>(state.consumed[consumer]).load(std::memory_order_acquire);
      if (value != expected_per_consumer) {
        throw std::runtime_error("salias IPC delivery mismatch");
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
                  .capacity = options.capacity,
                  .poll_limit = options.poll_limit,
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

Result run_case(const Options& options) {
  if (options.scenario == "fifo") {
    return options.consumers == 1 ? run_case_typed<salias::Mode::FifoMpsc>(options)
                                  : run_case_typed<salias::Mode::FifoFanout>(options);
  }
  return options.consumers == 1 ? run_case_typed<salias::Mode::OrderedMpsc>(options)
                                : run_case_typed<salias::Mode::OrderedFanout>(options);
}

void print_result(const Options& options, const Result& result) {
  const double publish_rate = static_cast<double>(result.published) / result.seconds;
  const double delivery_rate = static_cast<double>(result.delivered) / result.seconds;
  const double mib_per_second =
      delivery_rate * static_cast<double>(result.payload) / (1024.0 * 1024.0);
  for (std::size_t consumer = 0; consumer < result.consumer_latency.size(); ++consumer) {
    const auto& summary = result.consumer_latency[consumer];
    std::cout << "LATENCY library=salias-ipc" << " scenario=" << options.scenario
              << " consumer=" << consumer << " samples=" << summary.samples
              << " p50_ns=" << summary.p50_ns << " p99_ns=" << summary.p99_ns
              << " p999_ns=" << summary.p999_ns << " max_ns=" << summary.max_ns << '\n';
  }
  std::cout << "RESULT library=salias-ipc" << " scenario=" << options.scenario
            << " page=" << options.page << " pinning=" << (pinning_enabled(options) ? "on" : "off")
            << " cpu_base=" << options.cpu_base << " cpu_stride=" << options.cpu_stride
            << " producers=" << result.producers << " consumers=" << result.consumers
            << " payload=" << result.payload << " capacity=" << result.capacity
            << " batch_size=" << options.batch_size << " poll_limit=" << result.poll_limit
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

int main(int argc, char** argv) {
  try {
    const Options options = parse_options(argc, argv);
    print_result(options, run_case(options));
  } catch (const std::exception& error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 1;
  }
  return 0;
}

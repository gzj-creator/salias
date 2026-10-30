/**
 * @file bench/fast_ring_latency.cpp
 * @brief Fast Ring 四拓扑跨进程 open-loop 延迟基准。
 * @details 总速率固定，producer 按 planned schedule 发送并采样 attempt 或 planned
 *          origin；consumer 记录 end-to-end ns 直方图。每个 consumer 接收全部消息。
 */
#include <sched.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <iostream>
#include <new>
#include <optional>
#include <span>
#include <string_view>

#include "../tools/aeron_compare/latency_histogram.hpp"
#include "../tools/aeron_compare/latency_pacing.hpp"
#include "core/platform/monotonic_clock.hpp"
#include "core/ring/fast_mpmc_ring.hpp"
#include "core/ring/fast_mpsc_ring.hpp"
#include "core/ring/fast_spmc_ring.hpp"
#include "core/ring/fast_spsc_ring.hpp"
#include "core/wait/cpu_relax.hpp"

namespace {

inline constexpr std::size_t kMiB = 1u << 20;
inline constexpr std::size_t kStandardRingPayloadBytes = kMiB;
inline constexpr std::size_t kSmallRingPayloadBytes = kMiB / 4;
inline constexpr std::size_t kMaxConsumers = 2;
namespace latency = salias::tools::latency;

std::uint64_t sample_now_ns() noexcept {
  timespec value{};
  if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
    return 0;
  }
  return static_cast<std::uint64_t>(value.tv_sec) * 1'000'000'000u +
         static_cast<std::uint64_t>(value.tv_nsec);
}

enum class Topology { Spsc, Mpsc, Spmc, Mpmc };
enum class Origin { Attempt, Planned };
enum class VerifyMode { None, Marker, Full };

constexpr std::string_view verify_name(VerifyMode mode) noexcept {
  return mode == VerifyMode::Full ? "full" : mode == VerifyMode::Marker ? "marker" : "none";
}

struct Options {
  Topology topology = Topology::Spsc;
  Origin origin = Origin::Attempt;
  std::uint64_t rate = 1'000'000;
  std::uint32_t seconds = 2;
  std::uint64_t sample_rate = 16;
  std::size_t payload = 64;
  VerifyMode verify = VerifyMode::Marker;
  int cpu_base = -1;
  std::uint32_t cpu_stride = 1;
};

struct alignas(128) SharedState {
  std::uint32_t ready = 0;
  std::uint32_t failed = 0;
  std::uint32_t start = 0;
  std::uint32_t reserved = 0;
  std::uint64_t start_ns = 0;
  std::array<std::uint64_t, kMaxConsumers> delivered{};
  std::array<std::uint64_t, kMaxConsumers> end_ns{};
  std::array<std::uint64_t, kMaxConsumers> max_latency_ns{};
  std::array<std::array<std::uint64_t, latency::kBucketCount>, kMaxConsumers> histogram{};
};

void usage() {
  std::cerr << "usage: salias_fast_ring_latency --topology spsc|mpsc|spmc|mpmc "
               "[--origin attempt|planned] [--rate N] [--seconds N] "
               "[--sample-rate N] [--payload 64|256] [--verify none|marker|full] "
               "[--cpu-base N] [--cpu-stride N]\n";
}

bool parse_topology(std::string_view value, Topology& topology) noexcept {
  if (value == "spsc") {
    topology = Topology::Spsc;
  } else if (value == "mpsc") {
    topology = Topology::Mpsc;
  } else if (value == "spmc") {
    topology = Topology::Spmc;
  } else if (value == "mpmc") {
    topology = Topology::Mpmc;
  } else {
    return false;
  }
  return true;
}

bool parse_options(int argc, char** argv, Options& options) {
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    auto value = [&]() -> const char* {
      if (index + 1 >= argc) {
        return nullptr;
      }
      return argv[++index];
    };
    if (argument == "--topology") {
      const char* text = value();
      if (text == nullptr || !parse_topology(text, options.topology)) {
        return false;
      }
    } else if (argument == "--origin") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      const std::string_view origin(text);
      if (origin == "attempt") {
        options.origin = Origin::Attempt;
      } else if (origin == "planned") {
        options.origin = Origin::Planned;
      } else {
        return false;
      }
    } else if (argument == "--rate") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      options.rate = std::strtoull(text, nullptr, 10);
    } else if (argument == "--seconds") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      options.seconds = static_cast<std::uint32_t>(std::strtoul(text, nullptr, 10));
    } else if (argument == "--sample-rate") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      options.sample_rate = std::strtoull(text, nullptr, 10);
    } else if (argument == "--payload") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      options.payload = std::strtoull(text, nullptr, 10);
    } else if (argument == "--verify") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      const std::string_view verify(text);
      if (verify == "full") {
        options.verify = VerifyMode::Full;
      } else if (verify == "marker") {
        options.verify = VerifyMode::Marker;
      } else if (verify == "none") {
        options.verify = VerifyMode::None;
      } else {
        return false;
      }
    } else if (argument == "--cpu-base") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      options.cpu_base = static_cast<int>(std::strtol(text, nullptr, 10));
    } else if (argument == "--cpu-stride") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      options.cpu_stride = static_cast<std::uint32_t>(std::strtoul(text, nullptr, 10));
    } else if (argument == "--help" || argument == "-h") {
      usage();
      std::exit(0);
    } else {
      return false;
    }
  }
  return options.rate > 0 && options.seconds > 0 && options.sample_rate > 0 &&
         (options.payload == 64 || options.payload == 256) && options.cpu_stride > 0;
}

bool pin_cpu(int cpu) noexcept {
  if (cpu < 0) {
    return true;
  }
  if (cpu >= CPU_SETSIZE) {
    return false;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  return ::sched_setaffinity(0, sizeof(set), &set) == 0;
}

bool pin_worker(const Options& options, std::size_t worker_index) noexcept {
  return options.cpu_base < 0 ||
         pin_cpu(options.cpu_base + static_cast<int>(worker_index * options.cpu_stride));
}

void mark_failed(SharedState& state) noexcept {
  std::atomic_ref<std::uint32_t>(state.failed).store(1, std::memory_order_release);
}

bool has_failed(SharedState& state) noexcept {
  return std::atomic_ref<std::uint32_t>(state.failed).load(std::memory_order_acquire) != 0;
}

bool ready_and_wait(SharedState& state) noexcept {
  std::atomic_ref<std::uint32_t>(state.ready).fetch_add(1, std::memory_order_acq_rel);
  while (std::atomic_ref<std::uint32_t>(state.start).load(std::memory_order_acquire) == 0) {
    if (has_failed(state)) {
      return false;
    }
    salias::wait::cpu_relax();
  }
  return true;
}

[[noreturn]] void fail_child(SharedState& state, int code) noexcept {
  mark_failed(state);
  _exit(code);
}

template <std::size_t PayloadSize>
void fill_payload(std::span<std::byte> payload, std::uint64_t origin_ns, std::uint32_t producer,
                  std::uint64_t sequence, bool fill_full_payload) noexcept {
  static_assert(PayloadSize % sizeof(std::uint64_t) == 0);
  auto* words = reinterpret_cast<std::uint64_t*>(payload.data());
  words[0] = origin_ns;
  words[1] = static_cast<std::uint64_t>(producer) | (sequence << 32);
  if (!fill_full_payload) {
    return;
  }
  const auto pattern = sequence ^ (static_cast<std::uint64_t>(producer) * 0x9E3779B97F4A7C15ull);
  for (std::size_t index = 2; index < PayloadSize / sizeof(std::uint64_t); ++index) {
    words[index] = pattern + index;
  }
}

template <std::size_t PayloadSize>
bool validate_payload(std::span<const std::byte> payload, std::uint32_t expected_producer,
                      std::uint64_t expected_sequence, bool verify_full_payload) noexcept {
  if (payload.size() != PayloadSize) {
    return false;
  }
  const auto* words = reinterpret_cast<const std::uint64_t*>(payload.data());
  const auto expected_marker =
      static_cast<std::uint64_t>(expected_producer) | (expected_sequence << 32);
  if (words[1] != expected_marker) {
    return false;
  }
  if (!verify_full_payload) {
    return true;
  }
  const auto pattern =
      expected_sequence ^ (static_cast<std::uint64_t>(expected_producer) * 0x9E3779B97F4A7C15ull);
  for (std::size_t index = 2; index < PayloadSize / sizeof(std::uint64_t); ++index) {
    if (words[index] != pattern + index) {
      return false;
    }
  }
  return true;
}

template <class Ring>
auto make_producer(Ring& ring, std::size_t producer_index) noexcept {
  if constexpr (requires { ring.producer(producer_index); }) {
    return ring.producer(producer_index);
  } else {
    return std::expected<typename Ring::Producer, salias::ring::FastRingError>(ring.producer());
  }
}

template <class Ring>
auto make_consumer(Ring& ring, std::size_t consumer_index) noexcept {
  if constexpr (requires { ring.consumer(consumer_index); }) {
    return ring.consumer(consumer_index);
  } else {
    return std::expected<typename Ring::Consumer, salias::ring::FastRingError>(ring.consumer());
  }
}

template <class Ring, std::size_t ProducerCount, std::size_t ConsumerCount>
int run_case(const Options& options, std::string_view topology_name) {
  using Error = salias::ring::FastRingError;
  const std::uint64_t per_producer_rate = options.rate / ProducerCount;
  const std::uint64_t messages_per_producer = per_producer_rate * options.seconds;
  const std::uint64_t total_messages = messages_per_producer * ProducerCount;
  const std::uint64_t expected_samples_per_consumer =
      ((messages_per_producer + options.sample_rate - 1) / options.sample_rate) * ProducerCount;

  void* ring_mapping = ::mmap(nullptr, Ring::required_storage_size(), PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (ring_mapping == MAP_FAILED) {
    std::cerr << "failed to mmap ring\n";
    return 1;
  }
  const auto ring_bytes =
      std::span<std::byte>(static_cast<std::byte*>(ring_mapping), Ring::required_storage_size());
  auto owner = Ring::create(ring_bytes);
  if (!owner) {
    std::cerr << "failed to initialize ring\n";
    return 1;
  }
  void* state_mapping = ::mmap(nullptr, sizeof(SharedState), PROT_READ | PROT_WRITE,
                               MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (state_mapping == MAP_FAILED) {
    std::cerr << "failed to mmap state\n";
    return 1;
  }
  auto* state = new (state_mapping) SharedState();

  std::array<pid_t, ConsumerCount> consumers{};
  std::array<pid_t, ProducerCount> producers{};
  consumers.fill(-1);
  producers.fill(-1);
  const auto stop_children = [&]() {
    for (pid_t pid : consumers) {
      if (pid > 0) {
        ::kill(pid, SIGTERM);
      }
    }
    for (pid_t pid : producers) {
      if (pid > 0) {
        ::kill(pid, SIGTERM);
      }
    }
  };

  for (std::size_t consumer_index = 0; consumer_index < ConsumerCount; ++consumer_index) {
    consumers[consumer_index] = ::fork();
    if (consumers[consumer_index] < 0) {
      stop_children();
      return 1;
    }
    if (consumers[consumer_index] == 0) {
      if (!pin_worker(options, consumer_index)) {
        fail_child(*state, 20);
      }
      auto attached = Ring::attach(ring_bytes);
      if (!attached) {
        fail_child(*state, 21);
      }
      auto consumer = make_consumer(*attached, consumer_index);
      if (!consumer) {
        fail_child(*state, 22);
      }
      if (!ready_and_wait(*state)) {
        _exit(23);
      }

      std::uint64_t delivered = 0;
      std::uint64_t max_latency_ns = 0;
      if (options.verify == VerifyMode::None) {
        while (delivered < total_messages) {
          auto read = consumer->try_read();
          if (!read) {
            if (read.error() != Error::Empty) {
              fail_child(*state, 24);
            }
            continue;
          }
          const auto* words = reinterpret_cast<const std::uint64_t*>(read->data());
          if (words[0] != 0) {
            const auto receive_ns = sample_now_ns();
            if (receive_ns == 0 || receive_ns < words[0]) {
              fail_child(*state, 26);
            }
            latency::record(state->histogram[consumer_index].data(), receive_ns - words[0]);
            max_latency_ns = std::max(max_latency_ns, receive_ns - words[0]);
          }
          if (!consumer->release(false)) {
            fail_child(*state, 27);
          }
          ++delivered;
          if (delivered % 64 == 0 && !consumer->flush_release()) {
            fail_child(*state, 28);
          }
        }
      } else {
        std::array<std::uint64_t, ProducerCount> expected{};
        while (delivered < total_messages) {
          auto read = consumer->try_read();
          if (!read) {
            if (read.error() != Error::Empty) {
              fail_child(*state, 24);
            }
            continue;
          }
          const auto* words = reinterpret_cast<const std::uint64_t*>(read->data());
          const auto producer = static_cast<std::uint32_t>(words[1]);
          if (producer >= ProducerCount ||
              !validate_payload<Ring::payload_size()>(*read, producer, expected[producer],
                                                      options.verify == VerifyMode::Full)) {
            fail_child(*state, 25);
          }
          if (expected[producer] % options.sample_rate == 0) {
            const auto receive_ns = sample_now_ns();
            if (receive_ns == 0 || words[0] == 0 || receive_ns < words[0]) {
              fail_child(*state, 26);
            }
            latency::record(state->histogram[consumer_index].data(), receive_ns - words[0]);
            max_latency_ns = std::max(max_latency_ns, receive_ns - words[0]);
          }
          if (!consumer->release(false)) {
            fail_child(*state, 27);
          }
          ++expected[producer];
          ++delivered;
          if (delivered % 64 == 0 && !consumer->flush_release()) {
            fail_child(*state, 28);
          }
        }
      }
      if (!consumer->flush_release()) {
        fail_child(*state, 29);
      }
      std::atomic_ref<std::uint64_t>(state->delivered[consumer_index])
          .store(delivered, std::memory_order_release);
      state->max_latency_ns[consumer_index] = max_latency_ns;
      const auto end_ns = salias::platform::monotonic_now_ns();
      if (!end_ns) {
        fail_child(*state, 30);
      }
      std::atomic_ref<std::uint64_t>(state->end_ns[consumer_index])
          .store(*end_ns, std::memory_order_release);
      _exit(0);
    }
  }

  for (std::size_t producer_index = 0; producer_index < ProducerCount; ++producer_index) {
    producers[producer_index] = ::fork();
    if (producers[producer_index] < 0) {
      stop_children();
      return 1;
    }
    if (producers[producer_index] == 0) {
      if (!pin_worker(options, ConsumerCount + producer_index)) {
        fail_child(*state, 31);
      }
      auto attached = Ring::attach(ring_bytes);
      if (!attached) {
        fail_child(*state, 32);
      }
      auto producer = make_producer(*attached, producer_index);
      if (!producer) {
        fail_child(*state, 33);
      }
      if (!ready_and_wait(*state)) {
        _exit(34);
      }
      const auto start_ns =
          std::atomic_ref<std::uint64_t>(state->start_ns).load(std::memory_order_acquire);
      for (std::uint64_t sequence = 0; sequence < messages_per_producer; ++sequence) {
        const auto planned_ns =
            latency::planned_arrival_ns(start_ns, sequence, per_producer_rate,
                                        static_cast<std::uint32_t>(producer_index), ProducerCount);
        latency::wait_for_arrival(planned_ns);
        const bool sampled = sequence % options.sample_rate == 0;
        const auto origin_ns =
            sampled ? (options.origin == Origin::Planned ? planned_ns : sample_now_ns()) : 0;
        for (;;) {
          auto claim = producer->try_claim();
          if (claim) {
            fill_payload<Ring::payload_size()>(*claim, origin_ns,
                                               static_cast<std::uint32_t>(producer_index), sequence,
                                               options.verify == VerifyMode::Full);
            if (producer->commit()) {
              break;
            }
            fail_child(*state, 35);
          }
          if (claim.error() != Error::BackPressured) {
            fail_child(*state, 36);
          }
          salias::wait::cpu_relax();
        }
      }
      _exit(0);
    }
  }

  while (std::atomic_ref<std::uint32_t>(state->ready).load(std::memory_order_acquire) !=
         ProducerCount + ConsumerCount) {
    if (has_failed(*state)) {
      stop_children();
      break;
    }
    salias::wait::cpu_relax();
  }
  std::optional<std::uint64_t> now_ns;
  if (!has_failed(*state)) {
    now_ns = salias::platform::monotonic_now_ns();
    if (!now_ns) {
      mark_failed(*state);
    }
  }
  if (now_ns) {
    std::atomic_ref<std::uint64_t>(state->start_ns)
        .store(*now_ns + 10'000'000, std::memory_order_release);
  }
  std::atomic_ref<std::uint32_t>(state->start).store(1, std::memory_order_release);

  bool children_ok = true;
  for (pid_t pid : consumers) {
    int status = 0;
    if (pid > 0 && ::waitpid(pid, &status, 0) == pid) {
      children_ok = children_ok && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }
  }
  for (pid_t pid : producers) {
    int status = 0;
    if (pid > 0 && ::waitpid(pid, &status, 0) == pid) {
      children_ok = children_ok && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }
  }

  std::array<std::uint64_t, ConsumerCount> sample_counts{};
  bool delivery_ok = true;
  std::uint64_t latest_end_ns = 0;
  for (std::size_t consumer_index = 0; consumer_index < ConsumerCount; ++consumer_index) {
    const auto delivered = std::atomic_ref<std::uint64_t>(state->delivered[consumer_index])
                               .load(std::memory_order_acquire);
    delivery_ok = delivery_ok && delivered == total_messages;
    for (std::uint64_t count : state->histogram[consumer_index]) {
      sample_counts[consumer_index] += count;
    }
    delivery_ok = delivery_ok && sample_counts[consumer_index] == expected_samples_per_consumer;
    latest_end_ns =
        std::max(latest_end_ns, std::atomic_ref<std::uint64_t>(state->end_ns[consumer_index])
                                    .load(std::memory_order_acquire));
  }
  if (!children_ok || !delivery_ok || has_failed(*state) || !now_ns || latest_end_ns < *now_ns) {
    std::cerr << "Fast Ring latency case failed\n";
    return 1;
  }

  const auto worst_percentile = [&](std::uint32_t numerator, std::uint32_t denominator) {
    std::uint64_t worst = 0;
    for (std::size_t consumer_index = 0; consumer_index < ConsumerCount; ++consumer_index) {
      worst = std::max(worst,
                       latency::percentile(state->histogram[consumer_index].data(),
                                           sample_counts[consumer_index], numerator, denominator));
    }
    return worst;
  };
  const double seconds = static_cast<double>(latest_end_ns - *now_ns) / 1'000'000'000.0;
  const double publish_rate = static_cast<double>(total_messages) / seconds;
  const double delivery_rate = publish_rate * static_cast<double>(ConsumerCount);
  const double mib_per_second =
      delivery_rate * static_cast<double>(Ring::payload_size()) / (1024.0 * 1024.0);
  for (std::size_t consumer_index = 0; consumer_index < ConsumerCount; ++consumer_index) {
    std::cout << "LATENCY topology=" << topology_name << " consumer=" << consumer_index
              << " origin=" << (options.origin == Origin::Attempt ? "attempt" : "planned")
              << " samples=" << sample_counts[consumer_index] << " p50_ns="
              << latency::percentile(state->histogram[consumer_index].data(),
                                     sample_counts[consumer_index], 50, 100)
              << " p99_ns="
              << latency::percentile(state->histogram[consumer_index].data(),
                                     sample_counts[consumer_index], 99, 100)
              << " p999_ns="
              << latency::percentile(state->histogram[consumer_index].data(),
                                     sample_counts[consumer_index], 999, 1000)
              << " max_ns=" << state->max_latency_ns[consumer_index] << '\n';
  }
  std::cout << "RESULT library=fast-ring topology=" << topology_name
            << " producers=" << ProducerCount << " consumers=" << ConsumerCount
            << " payload=" << options.payload
            << " origin=" << (options.origin == Origin::Attempt ? "attempt" : "planned")
            << " published=" << total_messages << " delivered_per_consumer=" << total_messages
            << " delivered=" << total_messages * ConsumerCount
            << " verified=" << total_messages * ConsumerCount
            << " samples_per_consumer=" << expected_samples_per_consumer << " seconds=" << seconds
            << " publish_msg_per_sec=" << publish_rate << " delivery_msg_per_sec=" << delivery_rate
            << " delivery_mib_per_sec=" << mib_per_second
            << " rate_per_producer=" << per_producer_rate
            << " latency_sample_rate=" << options.sample_rate
            << " verification=" << verify_name(options.verify)
            << " latency_samples=" << expected_samples_per_consumer * ConsumerCount
            << " latency_origin=" << (options.origin == Origin::Attempt ? "attempt" : "planned")
            << " p50_ns=" << worst_percentile(50, 100) << " p99_ns=" << worst_percentile(99, 100)
            << " p999_ns=" << worst_percentile(999, 1000)
            << " latency_p50_ns=" << worst_percentile(50, 100)
            << " latency_p99_ns=" << worst_percentile(99, 100)
            << " latency_p999_ns=" << worst_percentile(999, 1000) << " max_ns="
            << *std::max_element(state->max_latency_ns.begin(),
                                 state->max_latency_ns.begin() + ConsumerCount)
            << " latency_max_ns="
            << *std::max_element(state->max_latency_ns.begin(),
                                 state->max_latency_ns.begin() + ConsumerCount)
            << '\n';

  ::munmap(state_mapping, sizeof(SharedState));
  ::munmap(ring_mapping, Ring::required_storage_size());
  return 0;
}

template <std::size_t PayloadSize>
int dispatch(const Options& options) {
  constexpr std::size_t kCapacity = std::bit_ceil(kStandardRingPayloadBytes / PayloadSize);
  constexpr std::size_t kSmallCapacity = std::bit_ceil(kSmallRingPayloadBytes / PayloadSize);
  switch (options.topology) {
    case Topology::Spsc:
      return run_case<salias::ring::FastSpscRing<PayloadSize, kCapacity>, 1, 1>(options, "Spsc");
    case Topology::Mpsc:
      return run_case<salias::ring::FastMpscRing<PayloadSize, kSmallCapacity, 2>, 2, 1>(options,
                                                                                        "Mpsc");
    case Topology::Spmc:
      return run_case<salias::ring::FastSpmcRing<PayloadSize, kCapacity, 2>, 1, 2>(options, "Spmc");
    case Topology::Mpmc:
      return run_case<salias::ring::FastMpmcRing<PayloadSize, kSmallCapacity, 2, 2>, 2, 2>(options,
                                                                                           "Mpmc");
  }
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse_options(argc, argv, options)) {
    usage();
    return 2;
  }
  if (options.cpu_base >= 0) {
    const int coordinator_cpu = options.cpu_base <= 0 ? 0 : options.cpu_base - 1;
    if (!pin_cpu(coordinator_cpu)) {
      return 1;
    }
  }
  return options.payload == 64 ? dispatch<64>(options) : dispatch<256>(options);
}

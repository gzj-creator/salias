/**
 * @file bench/fast_ring_compare.cpp
 * @brief Fast Ring 四拓扑跨进程吞吐基准。
 * @details 每个 producer 和 consumer 都是独立进程。producer 写入完整固定 payload，
 *          consumer 对每个 payload 做全字节、producer 身份和每 producer 序号校验。
 *          支持 SPSC、2P1S、1P2S 和 2P2S。
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
inline constexpr std::size_t kProducerIdBytes = sizeof(std::uint32_t);
inline constexpr std::size_t kSequenceOffset = 8;

enum class Topology { Spsc, Mpsc, Spmc, Mpmc };
enum class VerifyMode { None, Marker, Full };

constexpr std::string_view verify_name(VerifyMode mode) noexcept {
  return mode == VerifyMode::Full ? "full" : mode == VerifyMode::Marker ? "marker" : "none";
}

struct Options {
  Topology topology = Topology::Spsc;
  std::uint64_t messages = 10'000'000;
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
  std::array<std::uint64_t, kMaxConsumers> delivered{};
  std::array<std::uint64_t, kMaxConsumers> end_ns{};
  std::array<std::uint64_t, 2> producer_end_ns{};
};

void usage() {
  std::cerr << "usage: salias_fast_ring_compare --topology spsc|mpsc|spmc|mpmc "
               "[--messages N] [--payload 64|256] [--verify none|marker|full] "
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
    } else if (argument == "--messages") {
      const char* text = value();
      if (text == nullptr) {
        return false;
      }
      options.messages = std::strtoull(text, nullptr, 10);
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
  return options.messages > 0 && (options.payload == 64 || options.payload == 256) &&
         options.cpu_stride > 0;
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
  if (options.cpu_base < 0) {
    return true;
  }
  return pin_cpu(options.cpu_base + static_cast<int>(worker_index * options.cpu_stride));
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

template <std::size_t PayloadSize>
void fill_payload(std::span<std::byte> payload, std::uint32_t producer, std::uint64_t sequence,
                  bool fill_full_payload) noexcept {
  static_assert(PayloadSize % sizeof(std::uint64_t) == 0);
  auto* words = reinterpret_cast<std::uint64_t*>(payload.data());
  words[0] = producer;
  words[1] = sequence;
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
  std::uint32_t producer = 0;
  std::uint64_t sequence = 0;
  std::memcpy(&producer, payload.data(), sizeof(producer));
  std::memcpy(&sequence, payload.data() + kSequenceOffset, sizeof(sequence));
  if (producer != expected_producer || sequence != expected_sequence) {
    return false;
  }
  if (!verify_full_payload) {
    return true;
  }
  const auto* words = reinterpret_cast<const std::uint64_t*>(payload.data());
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

[[noreturn]] void fail_child(SharedState& state, int code) noexcept {
  mark_failed(state);
  _exit(code);
}

template <class Ring, std::size_t ProducerCount, std::size_t ConsumerCount>
int run_case(const Options& options, std::string_view topology_name) {
  using Error = salias::ring::FastRingError;

  void* ring_mapping = ::mmap(nullptr, Ring::required_storage_size(), PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (ring_mapping == MAP_FAILED) {
    std::cerr << "failed to mmap ring\n";
    return 1;
  }
  auto ring_bytes =
      std::span<std::byte>(static_cast<std::byte*>(ring_mapping), Ring::required_storage_size());
  auto owner = Ring::create(ring_bytes);
  if (!owner) {
    std::cerr << "failed to initialize ring: " << static_cast<int>(owner.error()) << '\n';
    ::munmap(ring_mapping, Ring::required_storage_size());
    return 1;
  }

  void* state_mapping = ::mmap(nullptr, sizeof(SharedState), PROT_READ | PROT_WRITE,
                               MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (state_mapping == MAP_FAILED) {
    std::cerr << "failed to mmap state\n";
    ::munmap(ring_mapping, Ring::required_storage_size());
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
      std::cerr << "fork consumer failed\n";
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
      const std::uint64_t kTotal = ProducerCount * options.messages;
      if (options.verify == VerifyMode::None) {
        while (delivered < kTotal) {
          auto read = consumer->try_read();
          if (!read) {
            if (read.error() != Error::Empty) {
              fail_child(*state, 24);
            }
            salias::wait::cpu_relax();
            continue;
          }
          if (!consumer->release(false)) {
            fail_child(*state, 26);
          }
          ++delivered;
          if (delivered % 64 == 0 && !consumer->flush_release()) {
            fail_child(*state, 28);
          }
        }
      } else {
        std::array<std::uint64_t, ProducerCount> expected{};
        while (delivered < kTotal) {
          auto read = consumer->try_read();
          if (!read) {
            if (read.error() != Error::Empty) {
              fail_child(*state, 24);
            }
            salias::wait::cpu_relax();
            continue;
          }
          std::uint32_t producer = 0;
          std::memcpy(&producer, read->data(), sizeof(producer));
          if (producer >= ProducerCount ||
              !validate_payload<Ring::payload_size()>(*read, producer, expected[producer],
                                                      options.verify == VerifyMode::Full)) {
            fail_child(*state, 25);
          }
          if (!consumer->release(false)) {
            fail_child(*state, 26);
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
      const auto end_ns = salias::platform::monotonic_now_ns();
      if (!end_ns) {
        fail_child(*state, 27);
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
      std::cerr << "fork producer failed\n";
      return 1;
    }
    if (producers[producer_index] == 0) {
      if (!pin_worker(options, ConsumerCount + producer_index)) {
        fail_child(*state, 30);
      }
      auto attached = Ring::attach(ring_bytes);
      if (!attached) {
        fail_child(*state, 31);
      }
      auto producer = make_producer(*attached, producer_index);
      if (!producer) {
        fail_child(*state, 32);
      }
      if (!ready_and_wait(*state)) {
        _exit(33);
      }
      for (std::uint64_t sequence = 0; sequence < options.messages; ++sequence) {
        const auto marker = (static_cast<std::uint64_t>(producer_index) << 48) | sequence;
        for (;;) {
          auto claim = producer->try_claim();
          if (claim) {
            if (options.verify == VerifyMode::None) {
              std::memcpy(claim->data(), &marker, sizeof(marker));
            } else {
              fill_payload<Ring::payload_size()>(*claim, static_cast<std::uint32_t>(producer_index),
                                                 sequence, options.verify == VerifyMode::Full);
            }
            if (producer->commit()) {
              break;
            }
            fail_child(*state, 34);
          }
          if (claim.error() != Error::BackPressured) {
            fail_child(*state, 35);
          }
          salias::wait::cpu_relax();
        }
      }
      const auto end_ns = salias::platform::monotonic_now_ns();
      if (!end_ns) {
        fail_child(*state, 36);
      }
      std::atomic_ref<std::uint64_t>(state->producer_end_ns[producer_index])
          .store(*end_ns, std::memory_order_release);
      _exit(0);
    }
  }

  constexpr std::uint32_t kExpectedReady = ProducerCount + ConsumerCount;
  while (std::atomic_ref<std::uint32_t>(state->ready).load(std::memory_order_acquire) !=
         kExpectedReady) {
    if (has_failed(*state)) {
      stop_children();
      break;
    }
    salias::wait::cpu_relax();
  }

  std::optional<std::uint64_t> start_time;
  if (!has_failed(*state)) {
    start_time = salias::platform::monotonic_now_ns();
    if (!start_time) {
      mark_failed(*state);
    }
  }
  std::atomic_ref<std::uint32_t>(state->start).store(1, std::memory_order_release);

  bool children_ok = true;
  int failed_status = 0;
  for (pid_t pid : consumers) {
    int status = 0;
    if (pid > 0 && ::waitpid(pid, &status, 0) == pid) {
      if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        children_ok = false;
        failed_status = status;
      }
    }
  }
  for (pid_t pid : producers) {
    int status = 0;
    if (pid > 0 && ::waitpid(pid, &status, 0) == pid) {
      if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        children_ok = false;
        failed_status = status;
      }
    }
  }

  const std::uint64_t kTotalPublished = ProducerCount * options.messages;
  std::uint64_t latest_end_ns = 0;
  bool delivery_ok = true;
  for (std::size_t consumer_index = 0; consumer_index < ConsumerCount; ++consumer_index) {
    const auto delivered = std::atomic_ref<std::uint64_t>(state->delivered[consumer_index])
                               .load(std::memory_order_acquire);
    const auto end_ns = std::atomic_ref<std::uint64_t>(state->end_ns[consumer_index])
                            .load(std::memory_order_acquire);
    delivery_ok = delivery_ok && delivered == kTotalPublished && end_ns != 0;
    latest_end_ns = std::max(latest_end_ns, end_ns);
  }
  const bool success = children_ok && delivery_ok && !has_failed(*state) && start_time &&
                       latest_end_ns >= *start_time;
  if (!success) {
    std::cerr << "Fast Ring case failed: status=" << failed_status << " topology=" << topology_name
              << " payload=" << options.payload << " failed_flag=" << has_failed(*state) << '\n';
    ::munmap(state_mapping, sizeof(SharedState));
    ::munmap(ring_mapping, Ring::required_storage_size());
    return 1;
  }

  const double seconds = static_cast<double>(latest_end_ns - *start_time) / 1'000'000'000.0;
  const double publish_rate = static_cast<double>(kTotalPublished) / seconds;
  const double delivery_rate = publish_rate * static_cast<double>(ConsumerCount);
  const double mib_per_second =
      delivery_rate * static_cast<double>(options.payload) / (1024.0 * 1024.0);
  std::cout << "RESULT library=fast-ring topology=" << topology_name
            << " producers=" << ProducerCount << " consumers=" << ConsumerCount
            << " payload=" << options.payload << " capacity=" << Ring::required_storage_size()
            << " published=" << kTotalPublished << " delivered_per_consumer=" << kTotalPublished
            << " delivered=" << kTotalPublished * ConsumerCount
            << " verified=" << kTotalPublished * ConsumerCount << " seconds=" << seconds
            << " publish_msg_per_sec=" << publish_rate << " delivery_msg_per_sec=" << delivery_rate
            << " delivery_mib_per_sec=" << mib_per_second << " rate_per_producer=0"
            << " verification=" << verify_name(options.verify)
            << " latency_origin=claim" << " latency_sample_rate=0" << " latency_samples=0"
            << " latency_p50_ns=0" << " latency_p99_ns=0" << " latency_p999_ns=0"
            << " latency_max_ns=0" << " producer0_seconds="
            << static_cast<double>(std::atomic_ref<std::uint64_t>(state->producer_end_ns[0])
                                       .load(std::memory_order_acquire) -
                                   *start_time) /
                   1'000'000'000.0;
  if constexpr (ProducerCount == 2) {
    std::cout << " producer1_seconds="
              << static_cast<double>(std::atomic_ref<std::uint64_t>(state->producer_end_ns[1])
                                         .load(std::memory_order_acquire) -
                                     *start_time) /
                     1'000'000'000.0;
  }
  std::cout << '\n';

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
      std::cerr << "failed to pin coordinator\n";
      return 1;
    }
  }
  return options.payload == 64 ? dispatch<64>(options) : dispatch<256>(options);
}

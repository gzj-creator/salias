/**
 * @file bench/fixed_channel_poll_compare.cpp
 * @brief 对比 FixedChannel 逐条接收与批量 poll 的吞吐和开环延迟。
 * @details 两个线程共享一个具名 SPSC FixedChannel，producer 写满固定 64 B
 *          payload，consumer 使用 copy 或 poll 模式完整校验每条消息。copy
 *          模式走 try_recv_copy() 的逐条 cursor 发布路径；poll 模式在每个批次
 *          末尾统一 flush consumer cursor。延迟模式按目标速率发送并记录
 *          attempt 或 planned p50/p99/p99.9。
 */
#include <sched.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include "../tools/aeron_compare/latency_histogram.hpp"
#include "../tools/aeron_compare/latency_pacing.hpp"
#include "salias/fixed_channel.hpp"

namespace {

constexpr std::size_t kCapacity = 4096;
constexpr std::uint32_t kPollBatch = 64;

struct alignas(64) Message {
  std::uint64_t sequence = 0;
  std::uint32_t producer = 0;
  std::uint32_t reserved = 0;
  std::uint64_t origin_ns = 0;
  std::array<std::uint8_t, 40> body{};

  bool operator==(const Message&) const = default;
};
static_assert(sizeof(Message) == 64);

struct MessageSchema {
  using message_type = Message;
  static constexpr std::uint64_t schema_id = 0x4649584544504F4Cull;
  static constexpr std::uint32_t version = 1;
};

using Channel = salias::FixedChannel<salias::FixedTopology::Spsc, MessageSchema, kCapacity>;

enum class Mode { Copy, Poll };
enum class Workload { Throughput, Latency };
enum class Origin { Attempt, Planned };

struct Options {
  Mode mode = Mode::Poll;
  Workload workload = Workload::Throughput;
  Origin origin = Origin::Attempt;
  std::uint64_t messages = 20'000'000;
  std::uint64_t rate = 1'000'000;
  std::uint32_t seconds = 2;
  std::uint32_t sample_rate = 16;
  int producer_cpu = 1;
  int consumer_cpu = 2;
  std::uint32_t poll_batch = kPollBatch;
};

Message make_message(std::uint64_t sequence, std::uint64_t origin_ns = 0) {
  Message message;
  message.sequence = sequence;
  message.producer = 1;
  message.origin_ns = origin_ns;
  for (std::size_t index = 0; index < message.body.size(); ++index) {
    message.body[index] = static_cast<std::uint8_t>((sequence + index * 17u) & 0xFFu);
  }
  return message;
}

bool validate_message(const Message& message, std::uint64_t sequence) noexcept {
  if (message.sequence != sequence || message.producer != 1 || message.reserved != 0) {
    return false;
  }
  for (std::size_t index = 0; index < message.body.size(); ++index) {
    if (message.body[index] != static_cast<std::uint8_t>((sequence + index * 17u) & 0xFFu)) {
      return false;
    }
  }
  return true;
}

void pin_thread(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
    throw std::runtime_error("failed to pin benchmark thread");
  }
}

void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#else
  std::this_thread::yield();
#endif
}

std::string unique_name(Mode mode, Workload workload) {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  return "fixed-poll-" + std::to_string(::getpid()) + "-" + std::to_string(now) + "-" +
         (mode == Mode::Poll ? "poll" : "copy") + "-" +
         (workload == Workload::Latency ? "latency" : "throughput");
}

bool parse_u64(std::string_view text, std::uint64_t& value) {
  try {
    std::size_t consumed = 0;
    const auto parsed = std::stoull(std::string(text), &consumed, 10);
    if (consumed != text.size()) {
      return false;
    }
    value = parsed;
    return true;
  } catch (...) {
    return false;
  }
}

bool parse_i32(std::string_view text, int& value) {
  try {
    std::size_t consumed = 0;
    const auto parsed = std::stoll(std::string(text), &consumed, 10);
    if (consumed != text.size() || parsed < 0 || parsed > 4096) {
      return false;
    }
    value = static_cast<int>(parsed);
    return true;
  } catch (...) {
    return false;
  }
}

bool parse_options(int argc, char** argv, Options& options) {
  for (int index = 1; index < argc; ++index) {
    const std::string_view arg = argv[index];
    const auto value = [&]() -> std::string_view {
      if (index + 1 >= argc) {
        throw std::invalid_argument("missing option value");
      }
      return argv[++index];
    };
    try {
      if (arg == "--mode") {
        const auto mode = value();
        if (mode == "copy") {
          options.mode = Mode::Copy;
        } else if (mode == "poll") {
          options.mode = Mode::Poll;
        } else {
          return false;
        }
      } else if (arg == "--workload") {
        const auto workload = value();
        if (workload == "throughput") {
          options.workload = Workload::Throughput;
        } else if (workload == "latency") {
          options.workload = Workload::Latency;
        } else {
          return false;
        }
      } else if (arg == "--origin") {
        const auto origin = value();
        if (origin == "attempt") {
          options.origin = Origin::Attempt;
        } else if (origin == "planned") {
          options.origin = Origin::Planned;
        } else {
          return false;
        }
      } else if (arg == "--messages") {
        if (!parse_u64(value(), options.messages) || options.messages == 0) {
          return false;
        }
      } else if (arg == "--rate") {
        if (!parse_u64(value(), options.rate) || options.rate == 0) {
          return false;
        }
      } else if (arg == "--seconds") {
        std::uint64_t seconds = 0;
        if (!parse_u64(value(), seconds) || seconds == 0 || seconds > UINT32_MAX) {
          return false;
        }
        options.seconds = static_cast<std::uint32_t>(seconds);
      } else if (arg == "--sample-rate") {
        std::uint64_t sample_rate = 0;
        if (!parse_u64(value(), sample_rate) || sample_rate == 0 || sample_rate > UINT32_MAX) {
          return false;
        }
        options.sample_rate = static_cast<std::uint32_t>(sample_rate);
      } else if (arg == "--producer-cpu") {
        if (!parse_i32(value(), options.producer_cpu)) {
          return false;
        }
      } else if (arg == "--consumer-cpu") {
        if (!parse_i32(value(), options.consumer_cpu)) {
          return false;
        }
      } else if (arg == "--poll-batch") {
        std::uint64_t batch = 0;
        if (!parse_u64(value(), batch) || batch == 0 || batch > UINT32_MAX) {
          return false;
        }
        options.poll_batch = static_cast<std::uint32_t>(batch);
      } else {
        return false;
      }
    } catch (...) {
      return false;
    }
  }
  return options.producer_cpu != options.consumer_cpu;
}

void print_usage() {
  std::cerr << "usage: fixed_channel_poll_compare --mode copy|poll "
               "[--workload throughput|latency] [--origin attempt|planned] "
               "[--messages N] [--rate N] [--seconds N] [--sample-rate N] "
               "[--producer-cpu N] [--consumer-cpu N] [--poll-batch N]\n";
}

int run(const Options& options) {
  if (options.workload == Workload::Latency &&
      options.rate > std::numeric_limits<std::uint64_t>::max() / options.seconds) {
    std::cerr << "latency message count overflow\n";
    return 1;
  }
  const std::uint64_t total_messages =
      options.workload == Workload::Latency ? options.rate * options.seconds : options.messages;
  const std::string name = unique_name(options.mode, options.workload);
  auto channel = Channel::create(name);
  if (!channel) {
    std::cerr << "failed to create FixedChannel\n";
    return 2;
  }
  auto publisher_result = channel->publisher(0);
  auto subscriber_result = channel->subscriber(0);
  if (!publisher_result || !subscriber_result) {
    std::cerr << "failed to create endpoints\n";
    return 3;
  }

  std::atomic<std::uint32_t> ready{0};
  std::atomic<bool> start{false};
  std::atomic<std::uint64_t> start_ns{0};
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> consumed{0};
  std::atomic<std::uint64_t> elapsed_ns{0};
  std::array<std::uint64_t, salias::tools::latency::kBucketCount> histogram{};
  std::uint64_t latency_samples = 0;
  std::uint64_t max_latency_ns = 0;

  auto publisher = std::move(*publisher_result);
  std::thread producer([&, publisher = std::move(publisher)]() mutable noexcept {
    try {
      pin_thread(options.producer_cpu);
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        cpu_relax();
      }
      const std::uint64_t schedule_start_ns = start_ns.load(std::memory_order_acquire);
      for (std::uint64_t sequence = 0; sequence < total_messages; ++sequence) {
        std::uint64_t origin_ns = 0;
        if (options.workload == Workload::Latency) {
          const auto planned_ns = salias::tools::latency::planned_arrival_ns(
              schedule_start_ns, sequence, options.rate, 0, 1);
          salias::tools::latency::wait_for_arrival(planned_ns);
          if (sequence % options.sample_rate == 0) {
            origin_ns = options.origin == Origin::Planned
                            ? planned_ns
                            : salias::tools::latency::monotonic_now_ns();
          }
        }
        const Message message = make_message(sequence, origin_ns);
        for (;;) {
          const auto sent = publisher.send(message);
          if (sent) {
            break;
          }
          if (sent.error() != salias::Error::BackPressured) {
            failed.store(true, std::memory_order_release);
            return;
          }
          cpu_relax();
        }
      }
    } catch (...) {
      failed.store(true, std::memory_order_release);
    }
  });

  auto subscriber = std::move(*subscriber_result);
  std::thread consumer([&, subscriber = std::move(subscriber)]() mutable noexcept {
    try {
      pin_thread(options.consumer_cpu);
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        cpu_relax();
      }
      const auto begin = std::chrono::steady_clock::now();
      std::uint64_t received = 0;
      const auto record_latency = [&](const Message& message) noexcept {
        if (message.origin_ns == 0) {
          return true;
        }
        const auto receive_ns = salias::tools::latency::monotonic_now_ns();
        if (receive_ns == 0 || receive_ns < message.origin_ns) {
          return false;
        }
        const auto latency_ns = receive_ns - message.origin_ns;
        salias::tools::latency::record(histogram.data(), latency_ns);
        ++latency_samples;
        max_latency_ns = std::max(max_latency_ns, latency_ns);
        return true;
      };
      if (options.mode == Mode::Copy) {
        while (received < total_messages) {
          const auto message = subscriber.try_recv_copy();
          if (!message) {
            failed.store(true, std::memory_order_release);
            break;
          }
          if (!*message) {
            cpu_relax();
            continue;
          }
          if (!record_latency(**message) || !validate_message(**message, received)) {
            failed.store(true, std::memory_order_release);
            break;
          }
          ++received;
        }
      } else {
        bool valid = true;
        while (received < total_messages && valid) {
          const auto wanted = static_cast<std::uint32_t>(
              std::min<std::uint64_t>(options.poll_batch, total_messages - received));
          const std::uint64_t batch_base = received;
          const std::size_t got = subscriber.poll(wanted, [&](const Message& message) noexcept {
            if (!valid || !record_latency(message) || !validate_message(message, received)) {
              valid = false;
              return;
            }
            ++received;
          });
          if (!valid) {
            failed.store(true, std::memory_order_release);
            break;
          }
          if (got == 0) {
            cpu_relax();
            continue;
          }
          if (received != batch_base + got) {
            failed.store(true, std::memory_order_release);
            break;
          }
        }
      }
      const auto end = std::chrono::steady_clock::now();
      elapsed_ns.store(
          static_cast<std::uint64_t>(
              std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count()),
          std::memory_order_release);
      consumed.store(received, std::memory_order_release);
    } catch (...) {
      failed.store(true, std::memory_order_release);
    }
  });

  while (ready.load(std::memory_order_acquire) != 2) {
    cpu_relax();
  }
  start_ns.store(salias::tools::latency::monotonic_now_ns() +
                     (options.workload == Workload::Latency ? 10'000'000ull : 0ull),
                 std::memory_order_release);
  start.store(true, std::memory_order_release);
  producer.join();
  consumer.join();

  const std::uint64_t verified = consumed.load(std::memory_order_acquire);
  const std::uint64_t duration_ns = elapsed_ns.load(std::memory_order_acquire);
  const std::uint64_t expected_latency_samples =
      options.workload == Workload::Latency ? (total_messages - 1) / options.sample_rate + 1 : 0;
  if (failed.load(std::memory_order_acquire) || verified != total_messages || duration_ns == 0 ||
      (options.workload == Workload::Latency && latency_samples != expected_latency_samples)) {
    std::cerr << "fixed channel benchmark failed: verified=" << verified
              << " expected=" << total_messages << '\n';
    return 4;
  }

  const double seconds = static_cast<double>(duration_ns) / 1'000'000'000.0;
  const double mmsg_s = static_cast<double>(verified) / seconds / 1'000'000.0;
  std::cout << "RESULT mode=" << (options.mode == Mode::Poll ? "poll" : "copy")
            << " workload=" << (options.workload == Workload::Latency ? "latency" : "throughput")
            << " origin=" << (options.origin == Origin::Attempt ? "attempt" : "planned")
            << " messages=" << verified << " poll_batch=" << options.poll_batch
            << " seconds=" << seconds << " mmsg_s=" << mmsg_s << " verified=" << verified;
  if (options.workload == Workload::Latency) {
    std::cout << " samples=" << latency_samples << " p50_ns="
              << salias::tools::latency::percentile(histogram.data(), latency_samples, 50, 100)
              << " p99_ns="
              << salias::tools::latency::percentile(histogram.data(), latency_samples, 99, 100)
              << " p999_ns="
              << salias::tools::latency::percentile(histogram.data(), latency_samples, 999, 1000)
              << " max_ns=" << max_latency_ns;
  }
  std::cout << '\n';
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse_options(argc, argv, options)) {
    print_usage();
    return 1;
  }
  return run(options);
}

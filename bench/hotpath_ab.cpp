/**
 * @file bench/hotpath_ab.cpp
 * @brief 热路径前后对照：64B 消息的同线程往返与跨线程吞吐。
 * @details 不依赖 Google Benchmark。消费者空转时不在测试里额外 yield，
 *          这样测到的是通道自己的轮询行为。每条消息带序号，收齐后核对。
 */
#include <salias/salias.hpp>

#include <pthread.h>
#include <sched.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/flow/consumer.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/codec.hpp"
#include "core/platform/mapping.hpp"
#include "core/ring/magic_ring.hpp"

namespace {

constexpr std::size_t kPayloadSize = 64;
constexpr std::size_t kRingCapacity = 4u << 20;
constexpr std::size_t kLargeRingCapacity = 64u << 20;

struct Payload {
  std::uint64_t seq = 0;
  std::array<std::byte, kPayloadSize - sizeof(std::uint64_t)> padding{};
};
static_assert(sizeof(Payload) == kPayloadSize);

struct Sample {
  double ns_per_msg = 0;
  double mmsgs = 0;
  std::uint64_t backpressure = 0;
};

std::uint64_t monotonic_ns() noexcept {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<std::uint64_t>(ts.tv_sec) * 1000000000ull +
         static_cast<std::uint64_t>(ts.tv_nsec);
}

void pin_cpu(int cpu) noexcept {
  cpu_set_t set;
  CPU_ZERO(&set);
  const int ncpu = static_cast<int>(std::thread::hardware_concurrency());
  if (ncpu <= 0) {
    return;
  }
  CPU_SET(cpu % ncpu, &set);
  pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

double median_of(std::vector<double> values) {
  std::sort(values.begin(), values.end());
  const std::size_t n = values.size();
  if (n == 0) {
    return 0;
  }
  if (n % 2 == 1) {
    return values[n / 2];
  }
  return (values[n / 2 - 1] + values[n / 2]) / 2.0;
}

std::uint64_t median_u64(std::vector<std::uint64_t> values) {
  std::sort(values.begin(), values.end());
  if (values.empty()) {
    return 0;
  }
  return values[values.size() / 2];
}

salias::Config make_config(std::string name, salias::Mode mode, std::uint32_t producers,
                           std::size_t capacity = kRingCapacity) {
  salias::Config config;
  config.name = std::move(name);
  config.mode = mode;
  config.capacity = capacity;
  config.num_producers = producers;
  config.num_consumers = 1;
  return config;
}

template <salias::Mode M>
bool offer_one(salias::Publisher<M>& publisher, const Payload& payload, std::uint64_t& backpressure) {
  const auto bytes = std::as_bytes(std::span{&payload, 1});
  for (;;) {
    auto offered = publisher.offer(bytes);
    if (offered && offered.value()) {
      return true;
    }
    if (!offered && offered.error() == salias::Error::BackPressured) {
      ++backpressure;
      continue;
    }
    return false;
  }
}

template <salias::Mode M>
Sample run_spsc_xthread(std::uint64_t messages, int rep) {
  auto created = salias::Channel<M>::create(
      make_config("hotpath-spsc-" + std::to_string(::getpid()) + "-" + std::to_string(rep),
                  M == salias::Mode::OrderedMpsc ? salias::Mode::OrderedMpsc : salias::Mode::FifoMpsc,
                  1));
  if (!created) {
    std::cerr << "create failed\n";
    std::exit(2);
  }
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> backpressure{0};

  std::thread producer([&] {
    pin_cpu(1);
    while (!start.load(std::memory_order_acquire)) {
    }
    Payload payload{};
    std::uint64_t local_bp = 0;
    for (std::uint64_t seq = 0; seq < messages; ++seq) {
      payload.seq = seq;
      if (!offer_one(publisher, payload, local_bp)) {
        failed.store(true, std::memory_order_release);
        break;
      }
    }
    backpressure.store(local_bp, std::memory_order_relaxed);
  });

  pin_cpu(0);
  start.store(true, std::memory_order_release);
  const std::uint64_t t0 = monotonic_ns();
  for (std::uint64_t expect = 0; expect < messages; ++expect) {
    std::optional<salias::Message> message;
    while (!(message = subscriber.try_recv())) {
    }
    Payload received{};
    std::memcpy(&received, message->payload.data(), sizeof(received));
    if (received.seq != expect) {
      failed.store(true, std::memory_order_release);
      break;
    }
    subscriber.release(*message);
  }
  const std::uint64_t t1 = monotonic_ns();
  producer.join();
  if (failed.load(std::memory_order_acquire)) {
    std::cerr << "spsc checksum failed\n";
    std::exit(3);
  }
  const double ns = static_cast<double>(t1 - t0) / static_cast<double>(messages);
  Sample sample;
  sample.ns_per_msg = ns;
  sample.mmsgs = 1000.0 / ns;
  sample.backpressure = backpressure.load(std::memory_order_relaxed);
  return sample;
}

template <salias::Mode M>
Sample run_spsc_poll(std::uint64_t messages, int rep) {
  auto created = salias::Channel<M>::create(
      make_config("hotpath-poll-" + std::to_string(::getpid()) + "-" + std::to_string(rep),
                  M == salias::Mode::OrderedMpsc ? salias::Mode::OrderedMpsc : salias::Mode::FifoMpsc,
                  1));
  if (!created) {
    std::cerr << "create failed\n";
    std::exit(2);
  }
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> backpressure{0};

  std::thread producer([&] {
    pin_cpu(1);
    while (!start.load(std::memory_order_acquire)) {
    }
    Payload payload{};
    std::uint64_t local_bp = 0;
    for (std::uint64_t seq = 0; seq < messages; ++seq) {
      payload.seq = seq;
      if (!offer_one(publisher, payload, local_bp)) {
        failed.store(true, std::memory_order_release);
        break;
      }
    }
    backpressure.store(local_bp, std::memory_order_relaxed);
  });

  pin_cpu(0);
  std::uint64_t expect = 0;
  start.store(true, std::memory_order_release);
  const std::uint64_t t0 = monotonic_ns();
  while (expect < messages) {
    const std::size_t got = subscriber.poll(256, [&](const salias::Message& message) noexcept {
      Payload received{};
      std::memcpy(&received, message.payload.data(), sizeof(received));
      if (received.seq != expect) {
        failed.store(true, std::memory_order_relaxed);
      }
      ++expect;
    });
    if (got == 0 && failed.load(std::memory_order_relaxed)) {
      break;
    }
  }
  const std::uint64_t t1 = monotonic_ns();
  producer.join();
  if (failed.load(std::memory_order_acquire) || expect != messages) {
    std::cerr << "poll checksum failed\n";
    std::exit(13);
  }
  const double ns = static_cast<double>(t1 - t0) / static_cast<double>(messages);
  Sample sample;
  sample.ns_per_msg = ns;
  sample.mmsgs = 1000.0 / ns;
  sample.backpressure = backpressure.load(std::memory_order_relaxed);
  return sample;
}

// 环足够大，生产者发完前不会背压。计时只覆盖生产者循环，消费者同时在读。
template <salias::Mode M>
Sample run_producer_side(std::uint64_t messages, int rep) {
  auto created = salias::Channel<M>::create(make_config(
      "hotpath-prod-" + std::to_string(::getpid()) + "-" + std::to_string(rep),
      M == salias::Mode::OrderedMpsc ? salias::Mode::OrderedMpsc : salias::Mode::FifoMpsc, 1,
      kLargeRingCapacity));
  if (!created) {
    std::cerr << "create failed\n";
    std::exit(2);
  }
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> consumed{0};

  std::thread consumer([&] {
    pin_cpu(0);
    std::uint64_t expect = 0;
    while (!start.load(std::memory_order_acquire)) {
    }
    while (expect < messages) {
      const std::size_t got = subscriber.poll(256, [&](const salias::Message& message) noexcept {
        Payload received{};
        std::memcpy(&received, message.payload.data(), sizeof(received));
        if (received.seq != expect) {
          failed.store(true, std::memory_order_relaxed);
        }
        ++expect;
      });
      if (got == 0 && failed.load(std::memory_order_relaxed)) {
        break;
      }
    }
    consumed.store(expect, std::memory_order_release);
  });

  pin_cpu(1);
  Payload payload{};
  std::uint64_t backpressure = 0;
  start.store(true, std::memory_order_release);
  const std::uint64_t t0 = monotonic_ns();
  for (std::uint64_t seq = 0; seq < messages; ++seq) {
    payload.seq = seq;
    if (!offer_one(publisher, payload, backpressure)) {
      failed.store(true, std::memory_order_release);
      break;
    }
  }
  const std::uint64_t t1 = monotonic_ns();
  consumer.join();
  if (failed.load(std::memory_order_acquire) || consumed.load(std::memory_order_acquire) != messages) {
    std::cerr << "producer-side checksum failed\n";
    std::exit(14);
  }
  const double ns = static_cast<double>(t1 - t0) / static_cast<double>(messages);
  Sample sample;
  sample.ns_per_msg = ns;
  sample.mmsgs = 1000.0 / ns;
  sample.backpressure = backpressure;
  return sample;
}

template <salias::Mode M>
Sample run_offer_only(std::uint64_t messages, int rep) {
  auto created = salias::Channel<M>::create(make_config(
      "hotpath-offer-" + std::to_string(::getpid()) + "-" + std::to_string(rep),
      M == salias::Mode::OrderedMpsc ? salias::Mode::OrderedMpsc : salias::Mode::FifoMpsc, 1,
      kLargeRingCapacity));
  if (!created) {
    std::cerr << "create failed\n";
    std::exit(2);
  }
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  pin_cpu(0);
  Payload payload{};
  std::uint64_t backpressure = 0;
  const std::uint64_t t0 = monotonic_ns();
  for (std::uint64_t seq = 0; seq < messages; ++seq) {
    payload.seq = seq;
    if (!offer_one(publisher, payload, backpressure)) {
      std::cerr << "offer-only failed\n";
      std::exit(4);
    }
  }
  const std::uint64_t t1 = monotonic_ns();
  const double ns = static_cast<double>(t1 - t0) / static_cast<double>(messages);
  Sample sample;
  sample.ns_per_msg = ns;
  sample.mmsgs = 1000.0 / ns;
  sample.backpressure = backpressure;
  return sample;
}

template <salias::Mode M>
Sample run_spsc_roundtrip(std::uint64_t messages, int rep) {
  auto created = salias::Channel<M>::create(
      make_config("hotpath-rt-" + std::to_string(::getpid()) + "-" + std::to_string(rep),
                  M == salias::Mode::OrderedMpsc ? salias::Mode::OrderedMpsc : salias::Mode::FifoMpsc,
                  1));
  if (!created) {
    std::cerr << "create failed\n";
    std::exit(2);
  }
  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  pin_cpu(0);
  Payload payload{};
  std::uint64_t backpressure = 0;
  const std::uint64_t t0 = monotonic_ns();
  for (std::uint64_t seq = 0; seq < messages; ++seq) {
    payload.seq = seq;
    if (!offer_one(publisher, payload, backpressure)) {
      std::cerr << "roundtrip offer failed\n";
      std::exit(4);
    }
    auto message = subscriber.try_recv();
    if (!message) {
      std::cerr << "roundtrip missed message\n";
      std::exit(5);
    }
    Payload received{};
    std::memcpy(&received, message->payload.data(), sizeof(received));
    if (received.seq != seq) {
      std::cerr << "roundtrip checksum failed\n";
      std::exit(6);
    }
    subscriber.release(*message);
  }
  const std::uint64_t t1 = monotonic_ns();
  const double ns = static_cast<double>(t1 - t0) / static_cast<double>(messages);
  Sample sample;
  sample.ns_per_msg = ns;
  sample.mmsgs = 1000.0 / ns;
  sample.backpressure = backpressure;
  return sample;
}

template <salias::Mode M, bool Batch = false>
Sample run_4p(std::uint64_t messages_per_producer, int rep) {
  constexpr std::uint32_t kProducers = 4;
  auto created = salias::Channel<M>::create(make_config(
      "hotpath-4p-" + std::to_string(::getpid()) + "-" + std::to_string(rep), M,
      kProducers));
  if (!created) {
    std::cerr << "create failed\n";
    std::exit(2);
  }
  auto channel = std::move(created).value();
  std::vector<salias::Publisher<M>> publishers;
  publishers.reserve(kProducers);
  for (std::uint32_t i = 0; i < kProducers; ++i) {
    publishers.push_back(channel.publisher());
  }
  auto subscriber = channel.subscriber();
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> backpressure{0};
  std::vector<std::thread> threads;
  threads.reserve(kProducers);
  for (std::uint32_t id = 0; id < kProducers; ++id) {
    threads.emplace_back([&, id] {
      pin_cpu(static_cast<int>(id + 1));
      auto publisher = std::move(publishers[id]);
      while (!start.load(std::memory_order_acquire)) {
      }
      Payload payload{};
      std::uint64_t local_bp = 0;
      for (std::uint64_t seq = 0; seq < messages_per_producer; ++seq) {
        payload.seq = (static_cast<std::uint64_t>(id) << 48) | seq;
        if (!offer_one(publisher, payload, local_bp)) {
          failed.store(true, std::memory_order_release);
          break;
        }
      }
      backpressure.fetch_add(local_bp, std::memory_order_relaxed);
    });
  }

  pin_cpu(0);
  std::array<std::uint64_t, kProducers> seen{};
  const std::uint64_t total = messages_per_producer * kProducers;
  start.store(true, std::memory_order_release);
  const std::uint64_t t0 = monotonic_ns();
  if constexpr (Batch) {
    std::uint64_t got = 0;
    while (got < total && !failed.load(std::memory_order_relaxed)) {
      subscriber.poll(256, [&](const salias::Message& message) noexcept {
        Payload received{};
        std::memcpy(&received, message.payload.data(), sizeof(received));
        const auto id = static_cast<std::uint32_t>(received.seq >> 48);
        const auto seq = received.seq & 0x0000'FFFF'FFFF'FFFFull;
        if (id >= kProducers || message.producer_id != id || seq != seen[id]++) {
          failed.store(true, std::memory_order_relaxed);
        }
        if constexpr (M == salias::Mode::OrderedMpsc) {
          if (message.sequence != got) {
            failed.store(true, std::memory_order_relaxed);
          }
        }
        ++got;
      });
    }
  } else {
    for (std::uint64_t got = 0; got < total; ++got) {
      std::optional<salias::Message> message;
      while (!(message = subscriber.try_recv())) {
      }
      Payload received{};
      std::memcpy(&received, message->payload.data(), sizeof(received));
      const auto id = static_cast<std::uint32_t>(received.seq >> 48);
      const std::uint64_t seq = received.seq & 0x0000'FFFF'FFFF'FFFFull;
      if (id >= kProducers || seq != seen[id]) {
        failed.store(true, std::memory_order_release);
        break;
      }
      ++seen[id];
      subscriber.release(*message);
    }
  }
  const std::uint64_t t1 = monotonic_ns();
  for (auto& thread : threads) {
    thread.join();
  }
  if (failed.load(std::memory_order_acquire)) {
    std::cerr << "4p checksum failed\n";
    std::exit(7);
  }
  const double ns = static_cast<double>(t1 - t0) / static_cast<double>(total);
  Sample sample;
  sample.ns_per_msg = ns;
  sample.mmsgs = 1000.0 / ns;
  sample.backpressure = backpressure.load(std::memory_order_relaxed);
  return sample;
}

struct L3Ring {
  salias::platform::Mapping mapping;
  salias::ring::MagicRing ring;
  alignas(128) std::uint64_t producer_pos = 0;
  alignas(128) std::uint64_t consumer_pos = 0;

  static L3Ring create() {
    auto mapping = salias::platform::Mapping::create(
        salias::platform::MapOptions{.size = kRingCapacity});
    if (!mapping) {
      std::cerr << "mapping failed\n";
      std::exit(8);
    }
    auto ring = salias::ring::MagicRing::create(std::move(mapping).value());
    if (!ring) {
      std::cerr << "ring failed\n";
      std::exit(9);
    }
    L3Ring fixture;
    fixture.ring = std::move(ring).value();
    return fixture;
  }

  salias::flow::Positions positions() noexcept {
    return {.producer = &producer_pos, .consumer = &consumer_pos, .cap = ring.capacity()};
  }
};

Sample run_l3_xthread(std::uint64_t messages) {
  auto fixture = L3Ring::create();
  salias::flow::Producer producer(fixture.ring, fixture.positions());
  salias::flow::Consumer consumer(fixture.ring, fixture.positions());
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> backpressure{0};

  std::thread thread([&] {
    pin_cpu(1);
    while (!start.load(std::memory_order_acquire)) {
    }
    std::uint64_t local_bp = 0;
    for (std::uint64_t seq = 0; seq < messages; ++seq) {
      salias::flow::Producer::ClaimResult claim;
      for (;;) {
        claim = producer.claim(kPayloadSize);
        if (claim) {
          break;
        }
        if (claim.error() == salias::flow::FlowError::BackPressured) {
          ++local_bp;
          continue;
        }
        failed.store(true, std::memory_order_release);
        backpressure.store(local_bp, std::memory_order_relaxed);
        return;
      }
      Payload payload{};
      payload.seq = seq;
      std::memcpy(claim->payload.data(), &payload, sizeof(payload));
      producer.commit(*claim);
    }
    backpressure.store(local_bp, std::memory_order_relaxed);
  });

  pin_cpu(0);
  start.store(true, std::memory_order_release);
  const std::uint64_t t0 = monotonic_ns();
  for (std::uint64_t expect = 0; expect < messages; ++expect) {
    std::optional<salias::flow::Message> message;
    while (!(message = consumer.poll())) {
    }
    Payload received{};
    std::memcpy(&received, message->payload.data(), sizeof(received));
    if (received.seq != expect) {
      failed.store(true, std::memory_order_release);
      break;
    }
    consumer.advance(message->next_position);
  }
  const std::uint64_t t1 = monotonic_ns();
  thread.join();
  if (failed.load(std::memory_order_acquire)) {
    std::cerr << "l3 checksum failed\n";
    std::exit(10);
  }
  const double ns = static_cast<double>(t1 - t0) / static_cast<double>(messages);
  Sample sample;
  sample.ns_per_msg = ns;
  sample.mmsgs = 1000.0 / ns;
  sample.backpressure = backpressure.load(std::memory_order_relaxed);
  return sample;
}

Sample run_l3_roundtrip(std::uint64_t messages) {
  auto fixture = L3Ring::create();
  salias::flow::Producer producer(fixture.ring, fixture.positions());
  salias::flow::Consumer consumer(fixture.ring, fixture.positions());
  pin_cpu(0);
  const std::uint64_t t0 = monotonic_ns();
  for (std::uint64_t seq = 0; seq < messages; ++seq) {
    auto claim = producer.claim(kPayloadSize);
    if (!claim) {
      std::cerr << "l3 roundtrip claim failed\n";
      std::exit(11);
    }
    Payload payload{};
    payload.seq = seq;
    std::memcpy(claim->payload.data(), &payload, sizeof(payload));
    producer.commit(*claim);
    auto message = consumer.poll();
    if (!message) {
      std::cerr << "l3 roundtrip missed\n";
      std::exit(12);
    }
    consumer.advance(message->next_position);
  }
  const std::uint64_t t1 = monotonic_ns();
  const double ns = static_cast<double>(t1 - t0) / static_cast<double>(messages);
  Sample sample;
  sample.ns_per_msg = ns;
  sample.mmsgs = 1000.0 / ns;
  return sample;
}

void report(const char* name, const std::vector<Sample>& samples) {
  std::vector<double> ns;
  std::vector<double> rate;
  std::vector<std::uint64_t> bp;
  ns.reserve(samples.size());
  rate.reserve(samples.size());
  bp.reserve(samples.size());
  for (const Sample& sample : samples) {
    ns.push_back(sample.ns_per_msg);
    rate.push_back(sample.mmsgs);
    bp.push_back(sample.backpressure);
  }
  std::cout << "RESULT case=" << name << " reps=" << samples.size()
            << " median_ns=" << median_of(ns) << " median_mmsgs=" << median_of(rate)
            << " min_ns=" << *std::min_element(ns.begin(), ns.end())
            << " max_ns=" << *std::max_element(ns.begin(), ns.end())
            << " median_backpressure=" << median_u64(bp) << '\n';
}

template <class Fn>
void repeat(const char* name, int reps, Fn&& fn) {
  fn(0);
  std::vector<Sample> samples;
  samples.reserve(static_cast<std::size_t>(reps));
  for (int rep = 1; rep <= reps; ++rep) {
    samples.push_back(fn(rep));
  }
  report(name, samples);
}

}  // namespace

int main(int argc, char** argv) {
  int reps = 5;
  std::uint64_t messages = 2000000;
  std::string only;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--reps" && i + 1 < argc) {
      reps = std::stoi(argv[++i]);
    } else if (arg == "--messages" && i + 1 < argc) {
      messages = std::stoull(argv[++i]);
    } else if (arg == "--case" && i + 1 < argc) {
      only = argv[++i];
    }
  }
  std::cout << "INFO threads=" << std::thread::hardware_concurrency()
            << " messages=" << messages << " payload=" << kPayloadSize
            << " ring=" << kRingCapacity << " reps=" << reps << " warmup=1\n";

  const auto selected = [&](const char* name) { return only.empty() || only == name; };
  if (selected("fifo_producer_side")) {
    repeat("fifo_producer_side", reps, [&](int rep) {
      return run_producer_side<salias::Mode::FifoMpsc>(std::min<std::uint64_t>(messages, 400000),
                                                      rep);
    });
  }
  if (selected("ordered_producer_side")) {
    repeat("ordered_producer_side", reps, [&](int rep) {
      return run_producer_side<salias::Mode::OrderedMpsc>(std::min<std::uint64_t>(messages, 400000),
                                                          rep);
    });
  }
  if (selected("fifo_spsc_poll")) {
    repeat("fifo_spsc_poll", reps, [&](int rep) {
      return run_spsc_poll<salias::Mode::FifoMpsc>(messages, rep);
    });
  }
  if (selected("ordered_spsc_poll")) {
    repeat("ordered_spsc_poll", reps, [&](int rep) {
      return run_spsc_poll<salias::Mode::OrderedMpsc>(messages, rep);
    });
  }
  if (selected("fifo_spsc_try_recv")) {
    repeat("fifo_spsc_try_recv", reps, [&](int rep) {
      return run_spsc_xthread<salias::Mode::FifoMpsc>(messages, rep);
    });
  }
  if (selected("fifo_offer_only")) {
    repeat("fifo_offer_only", reps, [&](int rep) {
      return run_offer_only<salias::Mode::FifoMpsc>(std::min<std::uint64_t>(messages, 800000), rep);
    });
  }
  if (selected("fifo_spsc_roundtrip")) {
    repeat("fifo_spsc_roundtrip", reps, [&](int rep) {
      return run_spsc_roundtrip<salias::Mode::FifoMpsc>(messages, rep);
    });
  }
  if (selected("fifo_4p1c")) {
    repeat("fifo_4p1c", reps, [&](int rep) {
      return run_4p<salias::Mode::FifoMpsc>(messages / 4, rep);
    });
  }
  if (selected("fifo_4p1c_poll")) {
    repeat("fifo_4p1c_poll", reps, [&](int rep) {
      return run_4p<salias::Mode::FifoMpsc, true>(messages / 4, rep);
    });
  }
  if (selected("ordered_4p1c_poll")) {
    repeat("ordered_4p1c_poll", reps, [&](int rep) {
      return run_4p<salias::Mode::OrderedMpsc, true>(messages / 4, rep);
    });
  }
  if (selected("l3_spsc_xthread")) {
    repeat("l3_spsc_xthread", reps, [&](int) { return run_l3_xthread(messages); });
  }
  if (selected("l3_spsc_roundtrip")) {
    repeat("l3_spsc_roundtrip", reps, [&](int) { return run_l3_roundtrip(messages); });
  }
  return 0;
}

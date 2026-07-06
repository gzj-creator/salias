#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "salias/salias.hpp"

namespace {

struct Options {
  std::string scenario = "spsc";
  std::uint64_t messages = 1'000'000;
  std::uint32_t producers = 1;
  std::uint32_t consumers = 1;
  std::size_t payload = 64;
  std::size_t capacity = 1u << 22;
};

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
    } else if (arg == "--help") {
      std::cout << "usage: salias_bench_compare --scenario spsc|spmc|mpmc "
                   "[--messages N] [--producers N] [--consumers N] [--payload N]\n";
      std::exit(0);
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(2);
    }
  }
  return options;
}

salias::Config make_config(salias::Mode mode, const Options& options) {
  salias::Config config;
  config.mode = mode;
  config.capacity = options.capacity;
  return config;
}

void wait_for_start(std::atomic<std::uint32_t>& ready, std::atomic<bool>& start) {
  ready.fetch_add(1, std::memory_order_release);
  while (!start.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
}

bool offer_until_accepted(salias::Publisher& publisher, std::span<const std::byte> payload) {
  for (;;) {
    auto offered = publisher.offer(payload);
    if (offered && offered.value()) {
      return true;
    }
    if (!offered && offered.error() != salias::Error::BackPressured) {
      return false;
    }
    std::this_thread::yield();
  }
}

std::vector<std::byte> payload_for(std::size_t size) {
  std::vector<std::byte> payload(size, std::byte{0x5A});
  if (payload.size() >= sizeof(std::uint64_t)) {
    std::uint64_t marker = 0x53414C494153ull;
    std::memcpy(payload.data(), &marker, sizeof(marker));
  }
  return payload;
}

struct Result {
  std::uint32_t producers = 0;
  std::uint32_t consumers = 0;
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::size_t payload = 0;
  double seconds = 0.0;
};

Result run_spsc(const Options& options) {
  auto channel_result = salias::Channel::create(make_config(salias::Mode::Spsc, options));
  if (!channel_result) {
    throw std::runtime_error("failed to create salias SPSC channel");
  }
  auto channel = std::move(channel_result).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  const auto payload = payload_for(options.payload);

  std::atomic<std::uint32_t> ready{0};
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::uint64_t consumed = 0;

  std::thread consumer([&] {
    wait_for_start(ready, start);
    while (consumed < options.messages) {
      auto message = subscriber.try_recv();
      if (!message) {
        std::this_thread::yield();
        continue;
      }
      ++consumed;
      subscriber.release(*message);
    }
  });

  std::thread producer([&] {
    wait_for_start(ready, start);
    for (std::uint64_t i = 0; i < options.messages; ++i) {
      if (!offer_until_accepted(publisher, payload)) {
        failed.store(true, std::memory_order_release);
        return;
      }
    }
  });

  while (ready.load(std::memory_order_acquire) != 2) {
    std::this_thread::yield();
  }
  const auto begin = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  producer.join();
  consumer.join();
  const auto end = std::chrono::steady_clock::now();

  if (failed.load(std::memory_order_acquire) || consumed != options.messages) {
    throw std::runtime_error("salias SPSC delivery mismatch");
  }

  return Result{.producers = 1,
                .consumers = 1,
                .published = options.messages,
                .delivered = consumed,
                .payload = options.payload,
                .seconds = std::chrono::duration<double>(end - begin).count()};
}

Result run_spmc(const Options& options) {
  auto channel_result = salias::Channel::create(make_config(salias::Mode::Broadcast, options));
  if (!channel_result) {
    throw std::runtime_error("failed to create salias Broadcast channel");
  }
  auto channel = std::move(channel_result).value();
  auto publisher = channel.publisher();
  std::vector<salias::Subscriber> subscribers;
  subscribers.reserve(options.consumers);
  for (std::uint32_t i = 0; i < options.consumers; ++i) {
    subscribers.push_back(channel.subscriber());
  }
  const auto payload = payload_for(options.payload);

  std::atomic<std::uint32_t> ready{0};
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::vector<std::uint64_t> consumed(options.consumers, 0);
  std::vector<std::thread> threads;
  threads.reserve(options.consumers + 1);

  for (std::uint32_t i = 0; i < options.consumers; ++i) {
    threads.emplace_back([&, i] {
      auto& subscriber = subscribers[i];
      wait_for_start(ready, start);
      while (consumed[i] < options.messages) {
        auto message = subscriber.try_recv();
        if (!message) {
          std::this_thread::yield();
          continue;
        }
        ++consumed[i];
        subscriber.release(*message);
      }
    });
  }

  threads.emplace_back([&] {
    wait_for_start(ready, start);
    for (std::uint64_t i = 0; i < options.messages; ++i) {
      if (!offer_until_accepted(publisher, payload)) {
        failed.store(true, std::memory_order_release);
        return;
      }
    }
  });

  while (ready.load(std::memory_order_acquire) != options.consumers + 1) {
    std::this_thread::yield();
  }
  const auto begin = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }
  const auto end = std::chrono::steady_clock::now();

  std::uint64_t delivered = 0;
  for (const auto value : consumed) {
    if (value != options.messages) {
      throw std::runtime_error("salias SPMC delivery mismatch");
    }
    delivered += value;
  }
  if (failed.load(std::memory_order_acquire)) {
    throw std::runtime_error("salias SPMC offer failed");
  }

  return Result{.producers = 1,
                .consumers = options.consumers,
                .published = options.messages,
                .delivered = delivered,
                .payload = options.payload,
                .seconds = std::chrono::duration<double>(end - begin).count()};
}

Result run_mpmc(const Options& options) {
  std::vector<salias::Channel> channels;
  channels.reserve(options.producers);
  std::vector<salias::Publisher> publishers;
  publishers.reserve(options.producers);
  std::vector<std::vector<salias::Subscriber>> subscribers(options.consumers);
  for (std::uint32_t producer = 0; producer < options.producers; ++producer) {
    auto channel_result =
        salias::Channel::create(make_config(salias::Mode::Broadcast, options));
    if (!channel_result) {
      throw std::runtime_error("failed to create salias MPMC broadcast shard");
    }
    channels.push_back(std::move(channel_result).value());
    publishers.push_back(channels.back().publisher());
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      subscribers[consumer].push_back(channels.back().subscriber());
    }
  }

  const auto payload = payload_for(options.payload);
  std::atomic<std::uint32_t> ready{0};
  std::atomic<bool> start{false};
  std::atomic<bool> failed{false};
  std::vector<std::uint64_t> consumed(options.consumers, 0);
  std::vector<std::thread> threads;
  threads.reserve(options.producers + options.consumers);
  const std::uint64_t expected_per_consumer =
      static_cast<std::uint64_t>(options.producers) * options.messages;

  for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
    threads.emplace_back([&, consumer] {
      wait_for_start(ready, start);
      std::uint32_t next = 0;
      while (consumed[consumer] < expected_per_consumer) {
        auto& subscriber = subscribers[consumer][next];
        auto message = subscriber.try_recv();
        next = (next + 1) % options.producers;
        if (!message) {
          std::this_thread::yield();
          continue;
        }
        ++consumed[consumer];
        subscriber.release(*message);
      }
    });
  }

  for (std::uint32_t producer = 0; producer < options.producers; ++producer) {
    threads.emplace_back([&, producer] {
      auto& publisher = publishers[producer];
      wait_for_start(ready, start);
      for (std::uint64_t i = 0; i < options.messages; ++i) {
        if (!offer_until_accepted(publisher, payload)) {
          failed.store(true, std::memory_order_release);
          return;
        }
      }
    });
  }

  while (ready.load(std::memory_order_acquire) != options.consumers + options.producers) {
    std::this_thread::yield();
  }
  const auto begin = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }
  const auto end = std::chrono::steady_clock::now();

  std::uint64_t delivered = 0;
  for (const auto value : consumed) {
    if (value != expected_per_consumer) {
      throw std::runtime_error("salias MPMC delivery mismatch");
    }
    delivered += value;
  }
  if (failed.load(std::memory_order_acquire)) {
    throw std::runtime_error("salias MPMC offer failed");
  }

  return Result{.producers = options.producers,
                .consumers = options.consumers,
                .published = static_cast<std::uint64_t>(options.producers) * options.messages,
                .delivered = delivered,
                .payload = options.payload,
                .seconds = std::chrono::duration<double>(end - begin).count()};
}

void print_result(const std::string& scenario, const Result& result) {
  const double publish_rate = static_cast<double>(result.published) / result.seconds;
  const double delivery_rate = static_cast<double>(result.delivered) / result.seconds;
  const double mib_per_second =
      delivery_rate * static_cast<double>(result.payload) / (1024.0 * 1024.0);
  std::cout << "RESULT library=salias"
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

int main(int argc, char** argv) {
  try {
    const Options options = parse_options(argc, argv);
    if (options.scenario == "spsc") {
      print_result(options.scenario, run_spsc(options));
    } else if (options.scenario == "spmc") {
      print_result(options.scenario, run_spmc(options));
    } else if (options.scenario == "mpmc") {
      print_result(options.scenario, run_mpmc(options));
    } else {
      std::cerr << "unknown scenario: " << options.scenario << '\n';
      return 2;
    }
  } catch (const std::exception& error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 1;
  }
  return 0;
}

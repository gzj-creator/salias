#include <benchmark/benchmark.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "salias/salias.hpp"

namespace {

salias::Config make_config(std::string suffix) {
  static std::atomic<std::uint64_t> next{0};
  salias::Config config;
  config.name = "salias-round2-" + std::to_string(::getpid()) + "-" + suffix + "-" +
                std::to_string(next.fetch_add(1));
  config.capacity = 4096;
  return config;
}

void BM_latest_roundtrip(benchmark::State& state) {
  auto config = make_config("latest");
  config.num_consumers = static_cast<std::uint32_t>(state.range(1));
  config.publication_window_messages = static_cast<std::uint64_t>(state.range(2));
  auto channel = salias::LatestMpmcChannel<4096, 50>::create(config);
  if (!channel) {
    state.SkipWithError("channel creation failed");
    return;
  }
  auto publisher = channel->publisher();
  std::vector<salias::LatestMpmcSubscriber> subscribers;
  const auto policy = state.range(0) == 0 ? salias::SubscriptionPolicy::DropOldest
                                          : salias::SubscriptionPolicy::Reliable;
  for (std::uint32_t index = 0; index < config.num_consumers; ++index) {
    auto subscriber = channel->try_subscriber(policy);
    if (!subscriber) {
      state.SkipWithError("subscriber registration failed");
      return;
    }
    subscribers.push_back(std::move(*subscriber));
  }
  std::array<std::byte, 1024> payload{};
  payload.fill(std::byte{0xa5});
  std::uint64_t sequence = 0;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    std::memcpy(payload.data(), &sequence, sizeof(sequence));
    auto offered = publisher.offer(
        std::span<const std::byte>(payload).first(static_cast<std::size_t>(state.range(3))));
    if (!offered || !*offered) {
      state.SkipWithError("offer failed");
      break;
    }
    for (auto& subscriber : subscribers) {
      auto received = subscriber.try_recv_checked();
      std::uint64_t actual = 0;
      if (!received || !*received || (*received)->payload.size() < sizeof(actual)) {
        state.SkipWithError("receive failed");
        break;
      }
      std::memcpy(&actual, (*received)->payload.data(), sizeof(actual));
      if (actual != sequence || (*received)->sequence != sequence ||
          (*received)->payload.size() != static_cast<std::size_t>(state.range(3)) ||
          std::memcmp((*received)->payload.data(), payload.data(), (*received)->payload.size()) !=
              0) {
        state.SkipWithError("corrupt or out-of-order message");
      }
      subscriber.release(**received);
    }
    ++sequence;
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * state.range(3));
}

void latest_arguments(benchmark::internal::Benchmark* target) {
  for (const std::int64_t policy : {0, 1}) {
    for (const std::int64_t consumers : {1, 4, 8}) {
      for (const std::int64_t messages : {0, 1, 16}) {
        for (const std::int64_t payload : {8, 64, 1024}) {
          target->Args({policy, consumers, messages, payload});
        }
      }
    }
  }
}

using PrioritySub = salias::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc>;

salias::Result<salias::PriorityMessage> receive(PrioritySub& reader, std::int64_t policy) {
#ifdef SALIAS_ROUND2_BASELINE
  static_cast<void>(policy);
  return reader.recv();
#else
  return reader.recv(static_cast<salias::PriorityWaitPolicy>(policy));
#endif
}

void BM_priority_roundtrip(benchmark::State& state) {
  auto high = salias::FifoMpscChannel::create(make_config("priority-high"));
  auto low = salias::FifoMpscChannel::create(make_config("priority-low"));
  if (!high || !low) {
    state.SkipWithError("channel creation failed");
    return;
  }
  PrioritySub reader(high->subscriber(), low->subscriber());
  salias::PriorityPublisher<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> publisher(
      high->publisher(), low->publisher());
  std::array<std::byte, 1024> payload{};
  payload.fill(std::byte{0xa5});
  std::uint64_t sequence = 0;
  for (auto iteration : state) {
    static_cast<void>(iteration);
    std::memcpy(payload.data(), &sequence, sizeof(sequence));
    const auto priority = static_cast<std::size_t>(sequence % 2);
    auto offered = publisher.offer(priority, std::span<const std::byte>(payload).first(
                                                 static_cast<std::size_t>(state.range(1))));
    if (!offered || !*offered) {
      state.SkipWithError("offer failed");
      break;
    }
    auto message = receive(reader, state.range(0));
    std::uint64_t actual = 0;
    if (!message || message->message.payload.size() < sizeof(actual)) {
      state.SkipWithError("receive failed");
      break;
    }
    std::memcpy(&actual, message->message.payload.data(), sizeof(actual));
    if (message->priority != priority || actual != sequence ||
        message->message.payload.size() != static_cast<std::size_t>(state.range(1)) ||
        std::memcmp(message->message.payload.data(), payload.data(),
                    message->message.payload.size()) != 0) {
      state.SkipWithError("corrupt or out-of-order message");
    }
    if (!reader.release(*message)) {
      state.SkipWithError("release failed");
      break;
    }
    ++sequence;
  }
#ifdef SALIAS_ROUND2_BASELINE
  state.SetLabel("baseline always uses Yield");
#endif
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * state.range(1));
}

BENCHMARK(BM_latest_roundtrip)->Apply(latest_arguments);
BENCHMARK(BM_priority_roundtrip)->ArgsProduct({{0, 1, 2}, {8, 64, 1024}});

}  // namespace

BENCHMARK_MAIN();

#include <benchmark/benchmark.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include "salias/salias.hpp"

namespace {

struct SmallPayload {
  std::uint32_t producer = 0;
  std::uint32_t seq = 0;
  std::array<std::byte, 56> padding{};
};

salias::Config config_for(salias::Mode mode, std::size_t capacity = 1u << 20) {
  salias::Config config;
  config.mode = mode;
  config.capacity = capacity;
  return config;
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

SmallPayload decode_small(std::span<const std::byte> payload) {
  SmallPayload value{};
  std::memcpy(&value, payload.data(), sizeof(value));
  return value;
}

void BM_spsc_roundtrip_64b(benchmark::State& state) {
  auto channel_result = salias::Channel::create(config_for(salias::Mode::Spsc));
  if (!channel_result) {
    state.SkipWithError("failed to create SPSC channel");
    return;
  }

  auto channel = std::move(channel_result).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  SmallPayload payload{};

  for (auto _ : state) {
    if (!offer_until_accepted(publisher, std::as_bytes(std::span{&payload, 1}))) {
      state.SkipWithError("SPSC offer failed");
      break;
    }
    auto message = subscriber.try_recv();
    if (!message) {
      state.SkipWithError("SPSC receive missed committed message");
      break;
    }
    benchmark::DoNotOptimize(message->payload.data());
    subscriber.release(*message);
  }

  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload)));
}

void BM_broadcast_two_subscribers_64b(benchmark::State& state) {
  auto channel_result = salias::Channel::create(config_for(salias::Mode::Broadcast));
  if (!channel_result) {
    state.SkipWithError("failed to create Broadcast channel");
    return;
  }

  auto channel = std::move(channel_result).value();
  auto publisher = channel.publisher();
  auto first = channel.subscriber();
  auto second = channel.subscriber();
  SmallPayload payload{};

  for (auto _ : state) {
    if (!offer_until_accepted(publisher, std::as_bytes(std::span{&payload, 1}))) {
      state.SkipWithError("Broadcast offer failed");
      break;
    }

    auto first_message = first.try_recv();
    auto second_message = second.try_recv();
    if (!first_message || !second_message) {
      state.SkipWithError("Broadcast subscriber missed committed message");
      break;
    }
    benchmark::DoNotOptimize(first_message->payload.data());
    benchmark::DoNotOptimize(second_message->payload.data());
    first.release(*first_message);
    second.release(*second_message);
  }

  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(sizeof(payload) * 2));
}

void BM_bulk_roundtrip_128k(benchmark::State& state) {
  auto channel_result = salias::Channel::create(config_for(salias::Mode::Bulk));
  if (!channel_result) {
    state.SkipWithError("failed to create Bulk channel");
    return;
  }

  auto channel = std::move(channel_result).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  std::vector<std::byte> payload(128 * 1024, std::byte{0x5A});

  for (auto _ : state) {
    if (!offer_until_accepted(publisher, payload)) {
      state.SkipWithError("Bulk offer failed");
      break;
    }
    auto message = subscriber.try_recv();
    if (!message) {
      state.SkipWithError("Bulk receive missed committed message");
      break;
    }
    benchmark::DoNotOptimize(message->payload.data());
    subscriber.release(*message);
  }

  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(payload.size()));
}

void BM_mpsc_four_producers_64b(benchmark::State& state) {
  constexpr std::uint32_t kProducerCount = 4;
  const auto messages_per_producer = static_cast<std::uint32_t>(state.range(0));
  const std::uint64_t total_messages =
      static_cast<std::uint64_t>(kProducerCount) * messages_per_producer;

  for (auto _ : state) {
    state.PauseTiming();
    auto channel_result = salias::Channel::create(config_for(salias::Mode::Mpsc, 1u << 20));
    if (!channel_result) {
      state.SkipWithError("failed to create MPSC channel");
      return;
    }
    auto channel = std::move(channel_result).value();
    auto subscriber = channel.subscriber();
    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};
    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);

    for (std::uint32_t producer_id = 0; producer_id < kProducerCount; ++producer_id) {
      producers.emplace_back([producer_id, messages_per_producer, &channel, &start, &failed] {
        auto publisher = channel.publisher();
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        for (std::uint32_t seq = 0; seq < messages_per_producer; ++seq) {
          SmallPayload payload{.producer = producer_id, .seq = seq};
          if (!offer_until_accepted(publisher, std::as_bytes(std::span{&payload, 1}))) {
            failed.store(true, std::memory_order_release);
            return;
          }
        }
      });
    }

    std::uint64_t producer_sum = 0;
    std::uint64_t seq_sum = 0;
    state.ResumeTiming();
    start.store(true, std::memory_order_release);

    for (std::uint64_t received = 0; received < total_messages;) {
      auto message = subscriber.try_recv();
      if (!message) {
        std::this_thread::yield();
        continue;
      }
      const SmallPayload payload = decode_small(message->payload);
      producer_sum += payload.producer;
      seq_sum += payload.seq;
      ++received;
      subscriber.release(*message);
    }

    for (auto& producer : producers) {
      producer.join();
    }
    state.PauseTiming();

    const std::uint64_t expected_producer_sum =
        static_cast<std::uint64_t>(messages_per_producer) * (kProducerCount - 1) *
        kProducerCount / 2;
    const std::uint64_t expected_seq_sum =
        static_cast<std::uint64_t>(kProducerCount) * (messages_per_producer - 1) *
        messages_per_producer / 2;
    if (failed.load(std::memory_order_acquire) || producer_sum != expected_producer_sum ||
        seq_sum != expected_seq_sum) {
      state.SkipWithError("MPSC checksum mismatch");
      return;
    }
    state.ResumeTiming();
  }

  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(total_messages));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(total_messages) *
                          static_cast<std::int64_t>(sizeof(SmallPayload)));
}

}  // namespace

BENCHMARK(BM_spsc_roundtrip_64b)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_broadcast_two_subscribers_64b)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_bulk_roundtrip_128k)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_mpsc_four_producers_64b)->Arg(65536)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();

/**
 * @file test/ring/fast_ring_topologies_test.cpp
 * @brief FastSpmcRing、FastMpscRing 与 FastMpmcRing 的拓扑和背压测试。
 */
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

#include "core/ring/fast_mpmc_ring.hpp"
#include "core/ring/fast_mpsc_ring.hpp"
#include "core/ring/fast_spmc_ring.hpp"
#include "core/wait/cpu_relax.hpp"

namespace {

using Error = salias::ring::FastRingError;
using Spsc = salias::ring::FastSpscRing<64, 4>;
using Spmc = salias::ring::FastSpmcRing<64, 4, 2>;
using Mpsc = salias::ring::FastMpscRing<64, 4, 2>;
using Mpmc = salias::ring::FastMpmcRing<64, 4, 2, 2>;

template <typename Ring>
struct alignas(128) Storage {
  std::array<std::byte, Ring::required_storage_size()> bytes{};
};

std::array<std::byte, 64> pattern(std::uint32_t producer, std::uint64_t sequence) {
  std::array<std::byte, 64> payload{};
  std::memcpy(payload.data(), &producer, sizeof(producer));
  std::memcpy(payload.data() + sizeof(producer), &sequence, sizeof(sequence));
  for (std::size_t index = 12; index < payload.size(); ++index) {
    payload[index] = static_cast<std::byte>((sequence + producer * 31 + index) & 0xFFu);
  }
  return payload;
}

void expect_pattern(std::span<const std::byte> payload, std::uint32_t producer,
                    std::uint64_t sequence) {
  const auto expected = pattern(producer, sequence);
  ASSERT_EQ(payload.size(), expected.size());
  EXPECT_EQ(std::memcmp(payload.data(), expected.data(), expected.size()), 0)
      << "producer=" << producer << " sequence=" << sequence;
}

std::pair<std::uint32_t, std::uint64_t> decode(std::span<const std::byte> payload) {
  std::uint32_t producer = 0;
  std::uint64_t sequence = 0;
  std::memcpy(&producer, payload.data(), sizeof(producer));
  std::memcpy(&sequence, payload.data() + sizeof(producer), sizeof(sequence));
  return {producer, sequence};
}

TEST(FastSpmcRingTest, EveryConsumerReceivesAllMessagesAndSlowestBackpressures) {
  Storage<Spmc> storage;
  auto ring = Spmc::create(storage.bytes);
  ASSERT_TRUE(ring) << static_cast<int>(ring.error());
  auto producer = ring->producer();
  auto first = ring->consumer(0);
  auto second = ring->consumer(1);
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);

  for (std::uint64_t sequence = 0; sequence < Spmc::capacity_slots(); ++sequence) {
    ASSERT_TRUE(producer.try_push(pattern(0, sequence)));
  }
  EXPECT_EQ(producer.try_claim().error(), Error::BackPressured);

  for (std::uint64_t sequence = 0; sequence < Spmc::capacity_slots(); ++sequence) {
    std::array<std::byte, 64> payload{};
    ASSERT_TRUE(first->try_pop(payload));
    expect_pattern(payload, 0, sequence);
  }
  EXPECT_EQ(producer.try_claim().error(), Error::BackPressured)
      << "a fast consumer must not overwrite data still pinned by a slow consumer";

  for (std::uint64_t sequence = 0; sequence < Spmc::capacity_slots(); ++sequence) {
    std::array<std::byte, 64> payload{};
    ASSERT_TRUE(second->try_pop(payload));
    expect_pattern(payload, 0, sequence);
  }
  auto reclaimed = producer.try_claim();
  ASSERT_TRUE(reclaimed);
  const auto next = pattern(0, Spmc::capacity_slots());
  std::memcpy(reclaimed->data(), next.data(), next.size());
  ASSERT_TRUE(producer.commit());

  EXPECT_EQ(ring->consumer(Spmc::consumer_count()).error(), Error::InvalidArgument);
}

TEST(FastMpscRingTest, ConsumerPreservesPerProducerOrderAndReclaimsLanesIndependently) {
  Storage<Mpsc> storage;
  auto ring = Mpsc::create(storage.bytes);
  ASSERT_TRUE(ring) << static_cast<int>(ring.error());
  auto first_producer = ring->producer(0);
  auto second_producer = ring->producer(1);
  ASSERT_TRUE(first_producer);
  ASSERT_TRUE(second_producer);
  auto consumer = ring->consumer();

  for (std::uint64_t sequence = 0; sequence < Mpsc::capacity_slots_per_producer(); ++sequence) {
    ASSERT_TRUE(first_producer->try_push(pattern(0, sequence)));
    ASSERT_TRUE(second_producer->try_push(pattern(1, sequence)));
  }
  EXPECT_EQ(first_producer->try_claim().error(), Error::BackPressured);
  EXPECT_EQ(second_producer->try_claim().error(), Error::BackPressured);

  auto first_read = consumer.try_read();
  ASSERT_TRUE(first_read);
  const auto [producer, sequence] = decode(*first_read);
  expect_pattern(*first_read, producer, sequence);
  EXPECT_EQ(first_producer->try_claim().error(), Error::BackPressured);
  ASSERT_TRUE(consumer.release());

  auto reclaimed_producer =
      producer == 0 ? first_producer.operator->() : second_producer.operator->();
  auto reclaimed = reclaimed_producer->try_claim();
  ASSERT_TRUE(reclaimed);
  const auto next = pattern(producer, Mpsc::capacity_slots_per_producer());
  std::memcpy(reclaimed->data(), next.data(), next.size());
  ASSERT_TRUE(reclaimed_producer->commit());

  std::array<std::uint64_t, Mpsc::producer_count()> expected{};
  expected[producer] = sequence + 1;
  for (std::size_t received = 1; received < 2 * Mpsc::capacity_slots_per_producer() + 1;
       ++received) {
    std::array<std::byte, 64> payload{};
    for (;;) {
      auto popped = consumer.try_pop(payload);
      if (popped) {
        break;
      }
      ASSERT_EQ(popped.error(), Error::Empty);
      salias::wait::cpu_relax();
    }
    const auto [lane, position] = decode(payload);
    ASSERT_LT(lane, Mpsc::producer_count());
    expect_pattern(payload, lane, expected[lane]);
    ++expected[lane];
  }

  EXPECT_EQ(ring->producer(Mpsc::producer_count()).error(), Error::InvalidArgument);
}

TEST(FastMpscRingTest, FullHotLaneCannotStarveAnotherLanePastBatchBound) {
  Storage<Mpsc> storage;
  auto ring = Mpsc::create(storage.bytes);
  ASSERT_TRUE(ring);
  auto hot = ring->producer(0);
  auto cold = ring->producer(1);
  ASSERT_TRUE(hot);
  ASSERT_TRUE(cold);
  auto consumer = ring->consumer();

  for (std::uint64_t sequence = 0; sequence < Mpsc::capacity_slots_per_producer(); ++sequence) {
    ASSERT_TRUE(hot->try_push(pattern(0, sequence)));
  }
  ASSERT_TRUE(cold->try_push(pattern(1, 0)));

  std::uint64_t next_hot = Mpsc::capacity_slots_per_producer();
  bool saw_cold = false;
  for (std::size_t received = 0; received < Mpsc::capacity_slots_per_producer() + 1; ++received) {
    std::array<std::byte, 64> payload{};
    ASSERT_TRUE(consumer.try_pop(payload));
    const auto [producer, sequence] = decode(payload);
    if (producer == 1) {
      EXPECT_EQ(sequence, 0u);
      saw_cold = true;
      break;
    }
    ASSERT_EQ(producer, 0u);
    EXPECT_EQ(sequence, received);
    ASSERT_TRUE(hot->try_push(pattern(0, next_hot++)));
  }
  EXPECT_TRUE(saw_cold);
}

TEST(FastMpmcRingTest, EachConsumerObservesEveryProducerInPerProducerOrder) {
  Storage<Mpmc> storage;
  auto ring = Mpmc::create(storage.bytes);
  ASSERT_TRUE(ring) << static_cast<int>(ring.error());
  auto first_producer = ring->producer(0);
  auto second_producer = ring->producer(1);
  auto first_consumer = ring->consumer(0);
  auto second_consumer = ring->consumer(1);
  ASSERT_TRUE(first_producer);
  ASSERT_TRUE(second_producer);
  ASSERT_TRUE(first_consumer);
  ASSERT_TRUE(second_consumer);

  for (std::uint64_t sequence = 0; sequence < Mpmc::capacity_slots_per_producer(); ++sequence) {
    ASSERT_TRUE(first_producer->try_push(pattern(0, sequence)));
    ASSERT_TRUE(second_producer->try_push(pattern(1, sequence)));
  }

  std::array<std::uint64_t, Mpmc::producer_count()> expected{};
  for (std::size_t received = 0; received < 2 * Mpmc::capacity_slots_per_producer(); ++received) {
    std::array<std::byte, 64> payload{};
    auto popped = first_consumer->try_pop(payload);
    if (!popped) {
      ASSERT_EQ(popped.error(), Error::Empty);
      salias::wait::cpu_relax();
      --received;
      continue;
    }
    const auto [producer, sequence] = decode(payload);
    EXPECT_EQ(sequence, expected[producer]);
    expect_pattern(payload, producer, sequence);
    ++expected[producer];
  }
  EXPECT_EQ(first_producer->try_claim().error(), Error::BackPressured);
  EXPECT_EQ(second_producer->try_claim().error(), Error::BackPressured);

  expected.fill(0);
  for (std::size_t received = 0; received < 2 * Mpmc::capacity_slots_per_producer(); ++received) {
    std::array<std::byte, 64> payload{};
    auto popped = second_consumer->try_pop(payload);
    if (!popped) {
      ASSERT_EQ(popped.error(), Error::Empty);
      salias::wait::cpu_relax();
      --received;
      continue;
    }
    const auto [producer, sequence] = decode(payload);
    EXPECT_EQ(sequence, expected[producer]);
    expect_pattern(payload, producer, sequence);
    ++expected[producer];
  }

  EXPECT_TRUE(first_producer->try_claim());
  EXPECT_TRUE(second_producer->try_claim());
  EXPECT_EQ(ring->consumer(Mpmc::consumer_count()).error(), Error::InvalidArgument);
}

TEST(FastMpmcRingTest, CrossProcessTwoProducersDeliverAllMessagesToEveryConsumer) {
  constexpr std::uint64_t kMessagesPerProducer = 20'000;
  void* mapping = ::mmap(nullptr, Mpmc::required_storage_size(), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(mapping, MAP_FAILED);
  const auto bytes =
      std::span<std::byte>(static_cast<std::byte*>(mapping), Mpmc::required_storage_size());
  auto owner = Mpmc::create(bytes);
  ASSERT_TRUE(owner);

  std::array<pid_t, Mpmc::producer_count() + 1> children{};
  for (std::size_t producer = 0; producer < Mpmc::producer_count(); ++producer) {
    children[producer] = ::fork();
    ASSERT_GE(children[producer], 0);
    if (children[producer] == 0) {
      auto attached = Mpmc::attach(bytes);
      if (!attached) {
        _exit(10);
      }
      auto endpoint = attached->producer(producer);
      if (!endpoint) {
        _exit(11);
      }
      for (std::uint64_t sequence = 0; sequence < kMessagesPerProducer; ++sequence) {
        const auto payload = pattern(static_cast<std::uint32_t>(producer), sequence);
        for (;;) {
          auto pushed = endpoint->try_push(payload);
          if (pushed) {
            break;
          }
          if (pushed.error() != Error::BackPressured) {
            _exit(12);
          }
          salias::wait::cpu_relax();
        }
      }
      _exit(0);
    }
  }

  children.back() = ::fork();
  ASSERT_GE(children.back(), 0);
  if (children.back() == 0) {
    auto attached = Mpmc::attach(bytes);
    if (!attached) {
      _exit(20);
    }
    auto endpoint = attached->consumer(1);
    if (!endpoint) {
      _exit(21);
    }
    std::array<std::uint64_t, Mpmc::producer_count()> expected{};
    for (std::size_t received = 0; received < Mpmc::producer_count() * kMessagesPerProducer;
         ++received) {
      std::array<std::byte, 64> payload{};
      for (;;) {
        auto popped = endpoint->try_pop(payload);
        if (popped) {
          break;
        }
        if (popped.error() != Error::Empty) {
          _exit(22);
        }
        salias::wait::cpu_relax();
      }
      const auto [producer, sequence] = decode(payload);
      if (producer >= Mpmc::producer_count() || sequence != expected[producer] ||
          std::memcmp(payload.data(), pattern(producer, sequence).data(), payload.size()) != 0) {
        _exit(23);
      }
      ++expected[producer];
    }
    _exit(0);
  }

  auto first_consumer = owner->consumer(0);
  ASSERT_TRUE(first_consumer);
  std::array<std::uint64_t, Mpmc::producer_count()> expected{};
  for (std::size_t received = 0; received < Mpmc::producer_count() * kMessagesPerProducer;
       ++received) {
    std::array<std::byte, 64> payload{};
    for (;;) {
      auto popped = first_consumer->try_pop(payload);
      if (popped) {
        break;
      }
      ASSERT_EQ(popped.error(), Error::Empty);
      salias::wait::cpu_relax();
    }
    const auto [producer, sequence] = decode(payload);
    ASSERT_LT(producer, Mpmc::producer_count());
    ASSERT_EQ(sequence, expected[producer]);
    expect_pattern(payload, producer, sequence);
    ++expected[producer];
  }

  for (pid_t child : children) {
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
  }
  EXPECT_EQ(::munmap(mapping, Mpmc::required_storage_size()), 0);
}

}  // namespace

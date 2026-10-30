/**
 * @file test/ring/fast_spsc_ring_test.cpp
 * @brief 固定槽 FastSpscRing 的布局、回绕、背压和跨进程测试。
 */
#include "core/ring/fast_spsc_ring.hpp"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

#include "core/wait/cpu_relax.hpp"

namespace {

using Spsc = salias::ring::FastSpscRing<64, 4>;
using Error = salias::ring::FastRingError;

struct alignas(128) Storage {
  std::array<std::byte, Spsc::required_storage_size()> bytes{};
};

std::array<std::byte, 64> pattern(std::uint64_t sequence) {
  std::array<std::byte, 64> payload{};
  std::memcpy(payload.data(), &sequence, sizeof(sequence));
  const auto complement = ~sequence;
  std::memcpy(payload.data() + sizeof(sequence), &complement, sizeof(complement));
  for (std::size_t index = 16; index < payload.size(); ++index) {
    payload[index] = static_cast<std::byte>((sequence + index) & 0xFFu);
  }
  return payload;
}

void expect_pattern(std::span<const std::byte> payload, std::uint64_t sequence) {
  const auto expected = pattern(sequence);
  ASSERT_EQ(payload.size(), expected.size());
  EXPECT_EQ(std::memcmp(payload.data(), expected.data(), expected.size()), 0);
}

TEST(FastSpscRingTest, CreateAttachAndRejectInvalidLayout) {
  Storage storage;
  auto created = Spsc::create(storage.bytes);
  ASSERT_TRUE(created) << static_cast<int>(created.error());

  auto attached = Spsc::attach(storage.bytes);
  ASSERT_TRUE(attached) << static_cast<int>(attached.error());

  auto undersized =
      Spsc::attach(std::span<std::byte>(storage.bytes.data(), Spsc::required_storage_size() - 1));
  ASSERT_FALSE(undersized);
  EXPECT_EQ(undersized.error(), Error::StorageTooSmall);

  std::array<std::byte, Spsc::required_storage_size() + 128> unaligned_bytes{};
  auto unaligned =
      Spsc::attach(std::span<std::byte>(unaligned_bytes.data() + 1, Spsc::required_storage_size()));
  ASSERT_FALSE(unaligned);
  EXPECT_EQ(unaligned.error(), Error::UnalignedStorage);
}

TEST(FastSpscRingTest, EmptyRingReportsEmptyWithoutAdvancing) {
  Storage storage;
  auto ring = Spsc::create(storage.bytes);
  ASSERT_TRUE(ring);
  auto consumer = ring->consumer();

  auto read = consumer.try_read();
  ASSERT_FALSE(read);
  EXPECT_EQ(read.error(), Error::Empty);
  EXPECT_EQ(consumer.consumed_position(), 0u);
}

TEST(FastSpscRingTest, CopyPushPopPreservesPayloadAcrossWrap) {
  Storage storage;
  auto ring = Spsc::create(storage.bytes);
  ASSERT_TRUE(ring);
  auto producer = ring->producer();
  auto consumer = ring->consumer();

  for (std::uint64_t sequence = 0; sequence < 12; ++sequence) {
    const auto expected = pattern(sequence);
    ASSERT_TRUE(producer.try_push(expected));

    std::array<std::byte, 64> actual{};
    ASSERT_TRUE(consumer.try_pop(actual));
    expect_pattern(actual, sequence);
  }
  EXPECT_EQ(producer.published_position(), 12u);
  EXPECT_EQ(consumer.consumed_position(), 12u);
}

TEST(FastSpscRingTest, BackpressureWaitsForConsumerRelease) {
  Storage storage;
  auto ring = Spsc::create(storage.bytes);
  ASSERT_TRUE(ring);
  auto producer = ring->producer();
  auto consumer = ring->consumer();

  for (std::uint64_t sequence = 0; sequence < Spsc::capacity_slots(); ++sequence) {
    ASSERT_TRUE(producer.try_push(pattern(sequence)));
  }
  auto blocked = producer.try_claim();
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), Error::BackPressured);

  auto read = consumer.try_read();
  ASSERT_TRUE(read);
  expect_pattern(*read, 0);
  blocked = producer.try_claim();
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), Error::BackPressured);

  ASSERT_TRUE(consumer.release());
  auto reclaimed = producer.try_claim();
  ASSERT_TRUE(reclaimed);
  const auto payload = pattern(4);
  std::memcpy(reclaimed->data(), payload.data(), payload.size());
  ASSERT_TRUE(producer.commit());

  for (std::uint64_t sequence = 1; sequence <= 4; ++sequence) {
    std::array<std::byte, 64> actual{};
    ASSERT_TRUE(consumer.try_pop(actual));
    expect_pattern(actual, sequence);
  }
}

TEST(FastSpscRingTest, DeferredReleaseKeepsSlotPinnedUntilFlush) {
  Storage storage;
  auto ring = Spsc::create(storage.bytes);
  ASSERT_TRUE(ring);
  auto producer = ring->producer();
  auto consumer = ring->consumer();

  for (std::uint64_t sequence = 0; sequence < Spsc::capacity_slots(); ++sequence) {
    ASSERT_TRUE(producer.try_push(pattern(sequence)));
  }
  auto first = consumer.try_read();
  ASSERT_TRUE(first);
  expect_pattern(*first, 0);
  ASSERT_TRUE(consumer.release(false));
  auto second = consumer.try_read();
  ASSERT_TRUE(second);
  expect_pattern(*second, 1);
  ASSERT_TRUE(consumer.release(false));
  EXPECT_EQ(producer.try_claim().error(), Error::BackPressured);
  ASSERT_TRUE(consumer.flush_release());
  auto reclaimed = producer.try_claim();
  ASSERT_TRUE(reclaimed);
}

TEST(FastSpscRingTest, ClaimIsInvisibleUntilCommitAndReadBlocksReuseUntilRelease) {
  Storage storage;
  auto ring = Spsc::create(storage.bytes);
  ASSERT_TRUE(ring);
  auto producer = ring->producer();
  auto consumer = ring->consumer();

  auto claim = producer.try_claim();
  ASSERT_TRUE(claim);
  const auto payload = pattern(7);
  std::memcpy(claim->data(), payload.data(), payload.size());
  EXPECT_EQ(consumer.try_read().error(), Error::Empty);

  ASSERT_TRUE(producer.commit());
  auto read = consumer.try_read();
  ASSERT_TRUE(read);
  expect_pattern(*read, 7);
  ASSERT_TRUE(consumer.release());
}

TEST(FastSpscRingTest, OutstandingOperationsAreRejectedExplicitly) {
  Storage storage;
  auto ring = Spsc::create(storage.bytes);
  ASSERT_TRUE(ring);
  auto producer = ring->producer();
  auto consumer = ring->consumer();

  auto claim = producer.try_claim();
  ASSERT_TRUE(claim);
  auto second_claim = producer.try_claim();
  ASSERT_FALSE(second_claim);
  EXPECT_EQ(second_claim.error(), Error::ClaimOutstanding);
  EXPECT_EQ(consumer.release().error(), Error::NoOutstandingRead);

  ASSERT_TRUE(producer.commit());
  EXPECT_EQ(producer.commit().error(), Error::NoOutstandingClaim);

  auto read = consumer.try_read();
  ASSERT_TRUE(read);
  EXPECT_EQ(consumer.try_read().error(), Error::ReadOutstanding);
  EXPECT_EQ(producer.commit().error(), Error::NoOutstandingClaim);
  ASSERT_TRUE(consumer.release());
}

TEST(FastSpscRingTest, CrossProcessForkDeliversEveryMessageInOrder) {
  constexpr std::uint64_t kMessages = 50'000;
  void* mapping = ::mmap(nullptr, Spsc::required_storage_size(), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(mapping, MAP_FAILED);
  const auto bytes =
      std::span<std::byte>(static_cast<std::byte*>(mapping), Spsc::required_storage_size());
  auto owner = Spsc::create(bytes);
  ASSERT_TRUE(owner);

  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    auto attached = Spsc::attach(bytes);
    if (!attached) {
      _exit(10);
    }
    auto producer = attached->producer();
    for (std::uint64_t sequence = 0; sequence < kMessages; ++sequence) {
      const auto payload = pattern(sequence);
      for (;;) {
        auto pushed = producer.try_push(payload);
        if (pushed) {
          break;
        }
        if (pushed.error() != Error::BackPressured) {
          _exit(11);
        }
        salias::wait::cpu_relax();
      }
    }
    _exit(0);
  }

  auto consumer = owner->consumer();
  for (std::uint64_t sequence = 0; sequence < kMessages; ++sequence) {
    std::array<std::byte, 64> payload{};
    for (;;) {
      auto popped = consumer.try_pop(payload);
      if (popped) {
        break;
      }
      ASSERT_EQ(popped.error(), Error::Empty);
      salias::wait::cpu_relax();
    }
    expect_pattern(payload, sequence);
  }

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_EQ(::munmap(mapping, Spsc::required_storage_size()), 0);
}

}  // namespace

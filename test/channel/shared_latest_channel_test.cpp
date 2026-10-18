/**
 * @file test/channel/shared_latest_channel_test.cpp
 * @brief LatestMpmc retention/lease handshake and malformed shared-state regressions.
 */
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "core/channel/shared_hybrid_mpsc.hpp"
#include "core/platform/mapping.hpp"

namespace {

using Channel = salias::channel::SharedHybridMpscChannel<salias::channel::Order::Fifo,
                                                         salias::wait::SpinPause, true>;
constexpr std::uint64_t kInactive = std::numeric_limits<std::uint64_t>::max();
constexpr std::size_t kCapacity = 4096;
constexpr std::size_t kRetentionBytes = kCapacity / 2;

class SharedLatestChannelTest : public ::testing::Test {
 protected:
  struct alignas(64) Cursor {
    std::uint64_t position = 0;
    std::uint64_t sequence = 0;
    std::uint64_t lease_start = kInactive;
    std::uint64_t lease_end = 0;
    std::uint64_t lagged_messages = 0;
    std::uint64_t skipped_messages = 0;
  };

  void SetUp() override { reset_channel(); }

  void reset_channel(std::uint64_t initial_sequence = 0) {
    channel_.reset();
    visible_ = 0;
    next_sequence_ = initial_sequence;
    retained_ = 0;
    intent_ = kInactive;
    cursors_ = {};
    Channel::ProducerSharedState producer{
        .visible_producer_pos = &visible_,
        .local_sequence = &next_sequence_,
        .retained_position = &retained_,
        .retention_intent = &intent_,
        .consumers = {},
    };
    for (auto& cursor : cursors_) {
      cursor.sequence = initial_sequence;
      producer.consumers.push_back(Channel::ConsumerSharedState{
          .position = &cursor.position,
          .sequence = &cursor.sequence,
          .lease_start = &cursor.lease_start,
          .lease_end = &cursor.lease_end,
          .lagged_messages = &cursor.lagged_messages,
          .skipped_messages = &cursor.skipped_messages,
      });
    }
    auto mapping = salias::platform::Mapping::create({.size = kCapacity});
    ASSERT_TRUE(mapping);
    bytes_ = mapping->as_ptr();
    auto ring = salias::ring::MagicRing::create(std::move(*mapping));
    ASSERT_TRUE(ring);
    std::vector<salias::ring::MagicRing> rings;
    rings.push_back(std::move(*ring));
    std::vector<Channel::ProducerSharedState> producers;
    producers.push_back(std::move(producer));
    channel_.emplace(std::move(rings), std::move(producers),
                     Channel::SharedControl{.consumer_sequences = {},
                                            .num_producers = &producer_count_,
                                            .num_consumers = &consumer_count_,
                                            .retention_capacity = kRetentionBytes});
  }

  static bool offer(Channel::Tx& tx, std::uint64_t sequence) {
    return tx.offer(std::as_bytes(std::span{&sequence, 1})).has_value();
  }

  static std::uint64_t decode(const salias::flow::Message& message) {
    std::uint64_t value = 0;
    std::memcpy(&value, message.payload.data(), sizeof(value));
    return value;
  }

  alignas(64) std::uint64_t visible_ = 0;
  alignas(64) std::uint64_t next_sequence_ = 0;
  alignas(64) std::uint64_t retained_ = 0;
  alignas(64) std::uint64_t intent_ = kInactive;
  std::uint32_t producer_count_ = 1;
  std::uint32_t consumer_count_ = 2;
  std::array<Cursor, 2> cursors_{};
  std::byte* bytes_ = nullptr;
  std::optional<Channel> channel_;
};

TEST_F(SharedLatestChannelTest, EvictionIntentRejectsReadsWithoutSkippingOrLeakingLeases) {
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  ASSERT_TRUE(offer(tx, 0));
  ASSERT_TRUE(offer(tx, 1));

  // Pause a writer between announcing an eviction and checking the consumer leases.
  std::atomic_ref<std::uint64_t>(intent_).store(16, std::memory_order_seq_cst);
  EXPECT_FALSE(rx.try_recv());
  std::array<salias::flow::Message, 2> messages{};
  EXPECT_EQ(rx.try_recv_run(messages.data(), 2), 0u);
  EXPECT_EQ(cursors_[0].lease_start, kInactive);
  EXPECT_EQ(cursors_[0].position, 0u);
  EXPECT_EQ(retained_, 0u);

  // Aborting that write must leave the original oldest message readable.
  std::atomic_ref<std::uint64_t>(intent_).store(kInactive, std::memory_order_seq_cst);
  auto message = rx.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(decode(*message), 0u);
  rx.release(*message);
}

TEST_F(SharedLatestChannelTest, PinnedWriteFailureDoesNotEvictOrReserveSequenceNumbers) {
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  ASSERT_TRUE(offer(tx, 0));
  auto held = rx.try_recv();
  ASSERT_TRUE(held);
  for (std::uint64_t sequence = 1; sequence < 256; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  auto rejected = tx.claim(sizeof(std::uint64_t));
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::flow::FlowError::BackPressured);
  EXPECT_EQ(retained_, kRetentionBytes);
  EXPECT_EQ(visible_, kCapacity);
  EXPECT_EQ(next_sequence_, 256u);
  EXPECT_EQ(intent_, kInactive);
  EXPECT_EQ(decode(*held), 0u);
  rx.release(*held);

  auto oldest = rx.try_recv();
  ASSERT_TRUE(oldest);
  EXPECT_EQ(oldest->sequence, 128u);
  EXPECT_EQ(decode(*oldest), 128u);
  EXPECT_EQ(cursors_[0].position, oldest->position);
  EXPECT_EQ(cursors_[0].sequence, oldest->sequence);
  rx.release(*oldest);
  ASSERT_TRUE(offer(tx, 256));
  EXPECT_EQ(next_sequence_, 257u);
}

TEST_F(SharedLatestChannelTest, CannotEvictUncommittedClaimsOrExposePublicationHoles) {
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  auto first = tx.claim(1016);
  auto second = tx.claim(1016);
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  std::memset(first->payload.data(), 1, first->payload.size());
  std::memset(second->payload.data(), 2, second->payload.size());
  auto rejected = tx.claim(8);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::flow::FlowError::BackPressured);
  EXPECT_EQ(next_sequence_, 2u);
  EXPECT_EQ(retained_, 0u);
  tx.commit(*second);
  EXPECT_FALSE(rx.try_recv());
  tx.commit(*first);
  for (std::uint64_t sequence = 0; sequence < 2; ++sequence) {
    auto message = rx.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(message->sequence, sequence);
    EXPECT_EQ(message->payload.front(), static_cast<std::byte>(sequence + 1));
    rx.release(*message);
  }
  EXPECT_EQ(intent_, kInactive);
}

TEST_F(SharedLatestChannelTest, RejectsMalformedEvictionHeadersWithoutChangingSharedProgress) {
  auto tx = channel_->tx(0);
  for (std::uint64_t sequence = 0; sequence < 128; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  std::uint64_t original_header = 0;
  std::memcpy(&original_header, bytes_, sizeof(original_header));
  const std::array<std::uint64_t, 2> invalid_headers{
      original_header & ~(std::uint64_t{salias::frame::FLAG_COMMITTED} << 32),
      (original_header & 0xffffffff00000000ull) | static_cast<std::uint64_t>(kCapacity),
  };
  for (const auto header : invalid_headers) {
    std::memcpy(bytes_, &header, sizeof(header));
    auto rejected = tx.claim(8);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), salias::flow::FlowError::BackPressured);
    EXPECT_EQ(retained_, 0u);
    EXPECT_EQ(visible_, kRetentionBytes);
    EXPECT_EQ(next_sequence_, 128u);
    EXPECT_EQ(intent_, kInactive);
  }
  std::memcpy(bytes_, &original_header, sizeof(original_header));
  EXPECT_TRUE(offer(tx, 128));
}

TEST_F(SharedLatestChannelTest, ReconstructsLaggedSequencesAcrossLowBitWrap) {
  constexpr std::uint64_t kInitialSequence =
      (std::uint64_t{1} << salias::frame::kSequenceLowBits) - 260;
  ASSERT_NO_FATAL_FAILURE(reset_channel(kInitialSequence));
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  for (std::uint64_t index = 0; index < 300; ++index) {
    ASSERT_TRUE(offer(tx, kInitialSequence + index));
  }
  for (std::uint64_t index = 172; index < 300; ++index) {
    auto message = rx.try_recv();
    ASSERT_TRUE(message);
    EXPECT_EQ(message->sequence, kInitialSequence + index);
    EXPECT_EQ(decode(*message), kInitialSequence + index);
    rx.release(*message);
  }
  EXPECT_FALSE(rx.try_recv());
}

TEST_F(SharedLatestChannelTest, BatchLeasePinsEveryPayloadUntilFlush) {
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  for (std::uint64_t sequence = 0; sequence < 256; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  std::array<salias::flow::Message, 32> messages{};
  EXPECT_EQ(rx.try_recv_run(nullptr, 32), 0u);
  EXPECT_EQ(rx.try_recv_run(messages.data(), 0), 0u);
  ASSERT_EQ(rx.try_recv_run(messages.data(), 32), 32u);
  EXPECT_EQ(rx.try_recv_run(messages.data(), 32), 0u);
  EXPECT_FALSE(rx.try_recv());
  for (std::uint64_t sequence = 256; sequence < 384; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  EXPECT_FALSE(tx.claim(8));
  for (std::size_t index = 0; index < messages.size(); ++index) {
    EXPECT_EQ(decode(messages[index]), 128u + index);
  }
  rx.flush_progress();
  EXPECT_EQ(cursors_[0].lease_start, kInactive);
  EXPECT_TRUE(offer(tx, 384));
}

TEST_F(SharedLatestChannelTest, DestroyingReceiverClearsAnOutstandingLease) {
  auto tx = channel_->tx(0);
  ASSERT_TRUE(offer(tx, 0));
  {
    auto rx = channel_->rx(0);
    ASSERT_TRUE(rx.try_recv());
    for (std::uint64_t sequence = 1; sequence < 256; ++sequence) {
      ASSERT_TRUE(offer(tx, sequence));
    }
    EXPECT_FALSE(tx.claim(8));
  }
  EXPECT_EQ(cursors_[0].lease_start, kInactive);
  EXPECT_TRUE(offer(tx, 256));
}

TEST_F(SharedLatestChannelTest, MalformedLeaseBoundsCannotEvictRetainedMessages) {
  auto tx = channel_->tx(0);
  for (std::uint64_t sequence = 0; sequence < 256; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  cursors_[0].lease_start = 1;
  for (const auto end : {std::uint64_t{0}, std::uint64_t{kCapacity + 2}}) {
    cursors_[0].lease_end = end;
    EXPECT_FALSE(tx.claim(8));
    EXPECT_EQ(retained_, kRetentionBytes);
    EXPECT_EQ(next_sequence_, 256u);
    EXPECT_EQ(intent_, kInactive);
  }
  cursors_[0].lease_start = kInactive;
  EXPECT_TRUE(offer(tx, 256));
}

TEST_F(SharedLatestChannelTest, CheckedLagPublishesFloorWithoutBorrowingOrDoubleCounting) {
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  for (std::uint64_t sequence = 0; sequence < 300; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  auto lagged = rx.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::channel::ReceiveError::Lagged);
  EXPECT_EQ(rx.lagged_messages(), 172u);
  EXPECT_EQ(cursors_[0].position, retained_);
  EXPECT_EQ(cursors_[0].sequence, 172u);
  EXPECT_EQ(cursors_[0].lease_start, kInactive);
  auto message = rx.try_recv_checked();
  ASSERT_TRUE(message);
  ASSERT_TRUE(*message);
  EXPECT_EQ((**message).sequence, 172u);
  EXPECT_EQ(rx.lagged_messages(), 172u);
  rx.release(**message);
}

TEST_F(SharedLatestChannelTest, SeekAgainstEvictionIntentPreservesProgressAndCounters) {
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  for (std::uint64_t sequence = 0; sequence < 4; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  intent_ = 16;
  auto blocked = rx.skip_to_latest(0);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::channel::ReceiveError::BackPressured);
  EXPECT_EQ(cursors_[0].position, 0u);
  EXPECT_EQ(cursors_[0].lease_start, kInactive);
  EXPECT_EQ(rx.lagged_messages(), 0u);
  EXPECT_EQ(rx.skipped_messages(), 0u);
  intent_ = kInactive;
  auto skipped = rx.skip_to_latest(0);
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 3u);
  EXPECT_EQ(cursors_[0].position, 48u);
  EXPECT_EQ(cursors_[0].sequence, 3u);
  EXPECT_EQ(cursors_[0].lease_start, kInactive);
}

TEST_F(SharedLatestChannelTest, SeekMalformedFramesRollsBackAndClearsItsLease) {
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  for (std::uint64_t sequence = 0; sequence < 128; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  std::uint64_t original_header = 0;
  std::memcpy(&original_header, bytes_ + 16, sizeof(original_header));
  const auto invalid_header =
      (original_header & 0xffffffff00000000ull) | static_cast<std::uint64_t>(kCapacity);
  std::memcpy(bytes_ + 16, &invalid_header, sizeof(invalid_header));
  auto rejected = rx.skip_to_latest(0);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), salias::channel::ReceiveError::BadConfig);
  EXPECT_EQ(cursors_[0].lease_start, kInactive);
  EXPECT_EQ(cursors_[0].position, 0u);
  EXPECT_EQ(rx.lagged_messages(), 0u);
  EXPECT_EQ(rx.skipped_messages(), 0u);
  std::memcpy(bytes_ + 16, &original_header, sizeof(original_header));
  auto skipped = rx.skip_to_latest(0);
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 127u);
}

TEST_F(SharedLatestChannelTest, SeekAndCheckedReceiveCountAcrossLowBitWrap) {
  constexpr auto kInitialSequence = (std::uint64_t{1} << salias::frame::kSequenceLowBits) - 260;
  ASSERT_NO_FATAL_FAILURE(reset_channel(kInitialSequence));
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  for (std::uint64_t index = 0; index < 300; ++index) {
    ASSERT_TRUE(offer(tx, kInitialSequence + index));
  }
  auto checked = channel_->rx(1);
  auto lagged = checked.try_recv_checked();
  ASSERT_FALSE(lagged);
  EXPECT_EQ(lagged.error(), salias::channel::ReceiveError::Lagged);
  EXPECT_EQ(checked.lagged_messages(), 172u);
  auto oldest = checked.try_recv_checked();
  ASSERT_TRUE(oldest);
  ASSERT_TRUE(*oldest);
  EXPECT_EQ((**oldest).sequence, kInitialSequence + 172);
  checked.release(**oldest);
  auto skipped = rx.skip_to_latest(0);
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 299u);
  EXPECT_EQ(rx.lagged_messages(), 172u);
  EXPECT_EQ(rx.skipped_messages(), 127u);
  auto message = rx.try_recv_checked();
  ASSERT_TRUE(message);
  ASSERT_TRUE(*message);
  EXPECT_EQ((**message).sequence, kInitialSequence + 299);
  EXPECT_EQ(decode(**message), kInitialSequence + 299);
  rx.release(**message);
}

TEST_F(SharedLatestChannelTest, SeekCannotReleaseAnOutstandingBatchLease) {
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0);
  for (std::uint64_t sequence = 0; sequence < 256; ++sequence) {
    ASSERT_TRUE(offer(tx, sequence));
  }
  std::array<salias::flow::Message, 16> messages{};
  ASSERT_EQ(rx.try_recv_run(messages.data(), 16), 16u);
  auto blocked = rx.skip_to_latest(0);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::channel::ReceiveError::BackPressured);
  EXPECT_NE(cursors_[0].lease_start, kInactive);
  EXPECT_EQ(rx.lagged_messages(), 128u);
  EXPECT_EQ(rx.skipped_messages(), 0u);
  rx.flush_progress();
  auto skipped = rx.skip_to_latest(0);
  ASSERT_TRUE(skipped);
  EXPECT_EQ(*skipped, 111u);
  EXPECT_EQ(rx.lagged_messages(), 128u);
  EXPECT_EQ(rx.skipped_messages(), 111u);
}

TEST_F(SharedLatestChannelTest, CheckedAndSeekRejectInvalidConsumerAndProducerIndices) {
  auto invalid = channel_->rx(2);
  auto received = invalid.try_recv_checked();
  ASSERT_FALSE(received);
  EXPECT_EQ(received.error(), salias::channel::ReceiveError::BadConfig);
  auto invalid_consumer = invalid.skip_to_latest(0);
  ASSERT_FALSE(invalid_consumer);
  EXPECT_EQ(invalid_consumer.error(), salias::channel::ReceiveError::BadConfig);
  auto rx = channel_->rx(0);
  auto invalid_producer = rx.skip_to_latest(1);
  ASSERT_FALSE(invalid_producer);
  EXPECT_EQ(invalid_producer.error(), salias::channel::ReceiveError::BadConfig);
}

class ReliableRetentionTest : public ::testing::Test {
 protected:
  struct Cursor {
    std::uint64_t position = 0;
    std::uint64_t sequence = 0;
    std::uint64_t messages = 0;
    std::uint64_t lease_start = kInactive;
    std::uint64_t lease_end = 0;
    std::uint32_t policy = salias::channel::consumer_registration::kReliablePolicy;
    std::uint32_t pid = 101;
    std::uint64_t start = 77;
    std::uint64_t generation = 1;
    std::uint64_t state = salias::channel::consumer_registration::make_token(
        1, 101, salias::channel::consumer_registration::kActive);
  };

  void SetUp() override { current_ = this; }
  void TearDown() override { current_ = nullptr; }

  static bool process_alive(std::uint32_t pid, std::uint64_t start) noexcept {
    auto& test = *current_;
    ++test.probes_;
    test.observed_pid_ = pid;
    test.observed_start_ = start;
    if (test.change_token_) {
      std::atomic_ref<std::uint64_t>(test.cursors_[0].state)
          .fetch_add(std::uint64_t{1} << salias::channel::consumer_registration::kGenerationShift,
                     std::memory_order_release);
    }
    return test.alive_ && start == test.expected_start_;
  }

  void build(std::uint64_t messages = 0) {
    Channel::ProducerSharedState producer{
        .visible_producer_pos = &visible_,
        .local_sequence = &sequence_,
        .retained_position = &retained_,
        .retention_intent = &intent_,
        .retained_message_sequence = &retained_messages_,
        .consumers = {},
    };
    for (auto& cursor : cursors_) {
      producer.consumers.push_back(Channel::ConsumerSharedState{
          .position = missing_position_ ? nullptr : &cursor.position,
          .sequence = &cursor.sequence,
          .lease_start = &cursor.lease_start,
          .lease_end = &cursor.lease_end,
          .message_sequence = missing_messages_ ? nullptr : &cursor.messages,
          .process_id = &cursor.pid,
          .process_start = missing_start_ ? nullptr : &cursor.start,
          .delivery_policy = missing_policy_ ? nullptr : &cursor.policy,
          .registration_state = &cursor.state,
          .cursor_generation = missing_generation_ ? nullptr : &cursor.generation,
      });
    }
    auto mapping = salias::platform::Mapping::create({.size = kCapacity});
    ASSERT_TRUE(mapping);
    auto ring = salias::ring::MagicRing::create(std::move(*mapping));
    ASSERT_TRUE(ring);
    std::vector<salias::ring::MagicRing> rings;
    rings.push_back(std::move(*ring));
    std::vector<Channel::ProducerSharedState> producers;
    producers.push_back(std::move(producer));
    channel_.emplace(
        std::move(rings), std::move(producers),
        Channel::SharedControl{.consumer_sequences = {},
                               .num_producers = &producer_count_,
                               .num_consumers = &consumer_count_,
                               .is_process_alive = missing_alive_ ? nullptr : process_alive,
                               .publication_window_messages = messages,
                               .retention_capacity = 32});
  }

  void prefill(Channel::Tx& tx) {
    const std::uint64_t value = 0;
    ASSERT_TRUE(tx.offer(std::as_bytes(std::span{&value, 1})));
    ASSERT_TRUE(tx.offer(std::as_bytes(std::span{&value, 1})));
    ASSERT_EQ(retained_, 0u);
    ASSERT_EQ(probes_, 0u);
  }

  bool next(Channel::Tx& tx) {
    const std::uint64_t value = 1;
    return tx.offer(std::as_bytes(std::span{&value, 1})).has_value();
  }

  static inline ReliableRetentionTest* current_ = nullptr;
  std::array<Cursor, 2> cursors_{};
  std::uint64_t visible_ = 0;
  std::uint64_t sequence_ = 0;
  std::uint64_t retained_ = 0;
  std::uint64_t retained_messages_ = 0;
  std::uint64_t intent_ = kInactive;
  std::uint32_t producer_count_ = 1;
  std::uint32_t consumer_count_ = 2;
  std::uint32_t probes_ = 0;
  std::uint32_t observed_pid_ = 0;
  std::uint64_t observed_start_ = 0;
  std::uint64_t expected_start_ = 77;
  bool alive_ = true;
  bool change_token_ = false;
  bool missing_position_ = false;
  bool missing_messages_ = false;
  bool missing_policy_ = false;
  bool missing_generation_ = false;
  bool missing_start_ = false;
  bool missing_alive_ = false;
  std::optional<Channel> channel_;
};

TEST_F(ReliableRetentionTest, HealthyCursorsAvoidProbesWithBothWindows) {
  for (const auto message_window : {std::uint64_t{0}, std::uint64_t{2}}) {
    channel_.reset();
    visible_ = 0;
    sequence_ = 0;
    retained_ = 0;
    retained_messages_ = 0;
    intent_ = kInactive;
    cursors_ = {};
    ASSERT_NO_FATAL_FAILURE(build(message_window));
    auto tx = channel_->tx(0);
    ASSERT_NO_FATAL_FAILURE(prefill(tx));
    for (std::uint64_t sequence = 0; sequence < 1024; ++sequence) {
      for (auto& cursor : cursors_) {
        cursor.position = 16 * (sequence + 1);
        cursor.messages = sequence + 1;
      }
      ASSERT_TRUE(next(tx));
      ASSERT_EQ(probes_, 0u);
    }
    EXPECT_GT(retained_, kCapacity);
    EXPECT_EQ(retained_messages_, 1024u);
  }
}

TEST_F(ReliableRetentionTest, DisabledMessageWindowDoesNotRequireMessageCursor) {
  missing_messages_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  for (auto& cursor : cursors_) {
    cursor.position = 16;
  }
  EXPECT_TRUE(next(tx));
  EXPECT_EQ(probes_, 0u);
  EXPECT_EQ(retained_messages_, 1u);
}

TEST_F(ReliableRetentionTest, ByteAndMessageBlockersProbeOnceAndNeverPublishFailedFloor) {
  ASSERT_NO_FATAL_FAILURE(build(2));
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 1u);
  EXPECT_EQ(retained_, 0u);
  EXPECT_EQ(retained_messages_, 0u);
  EXPECT_EQ(intent_, kInactive);
  EXPECT_EQ(sequence_, 2u);
  for (auto& cursor : cursors_) {
    cursor.position = 32;
  }
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 2u);
  for (auto& cursor : cursors_) {
    cursor.messages = 2;
  }
  EXPECT_TRUE(next(tx));
  EXPECT_EQ(probes_, 2u);
}

TEST_F(ReliableRetentionTest, ByteOnlyBlockerProbesOnce) {
  ASSERT_NO_FATAL_FAILURE(build(0));
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 1u);
  for (auto& cursor : cursors_) {
    cursor.position = 16;
  }
  EXPECT_TRUE(next(tx));
  EXPECT_EQ(probes_, 1u);
}

TEST_F(ReliableRetentionTest, DifferentReliableConsumersMayBlockDifferentFloors) {
  ASSERT_NO_FATAL_FAILURE(build(2));
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  cursors_[0].position = 16;
  cursors_[0].messages = 0;
  cursors_[1].position = 0;
  cursors_[1].messages = 1;
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 1u);
  cursors_[1].position = 16;
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 2u);
  cursors_[0].messages = 1;
  EXPECT_TRUE(next(tx));
  EXPECT_EQ(probes_, 2u);
}

TEST_F(ReliableRetentionTest, MixedPoliciesSkipDropOldestAndInactiveSlots) {
  cursors_[0].policy = salias::channel::consumer_registration::kDropOldestPolicy;
  cursors_[1].position = 16;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_TRUE(next(tx));
  cursors_[1].state = salias::channel::consumer_registration::kFree;
  EXPECT_TRUE(next(tx));
  EXPECT_EQ(probes_, 0u);
}

TEST_F(ReliableRetentionTest, MissingPolicyPreservesLegacyDropOldestBehavior) {
  missing_policy_ = true;
  ASSERT_NO_FATAL_FAILURE(build(2));
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_TRUE(next(tx));
  EXPECT_EQ(probes_, 0u);
}

TEST_F(ReliableRetentionTest, MissingPositionFailsConservatively) {
  missing_position_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 0u);
}

TEST_F(ReliableRetentionTest, EnabledMessageWindowRequiresMessageCursor) {
  missing_messages_ = true;
  ASSERT_NO_FATAL_FAILURE(build(2));
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 0u);
}

TEST_F(ReliableRetentionTest, MissingGenerationFailsBeforeProbing) {
  missing_generation_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 0u);
}

TEST_F(ReliableRetentionTest, WrongGenerationFailsBeforeProbing) {
  cursors_[0].generation = 2;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 0u);
}

TEST_F(ReliableRetentionTest, UnverifiableBlockerRemainsBackpressured) {
  missing_start_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 0u);
  EXPECT_EQ(retained_, 0u);
}

TEST_F(ReliableRetentionTest, MissingAliveCallbackKeepsBlockerBackpressured) {
  missing_alive_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 0u);
}

TEST_F(ReliableRetentionTest, DeadBlockersUsePidAndStartAndDoNotNeedReplacement) {
  expected_start_ = 88;
  ASSERT_NO_FATAL_FAILURE(build(2));
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_TRUE(next(tx));
  EXPECT_EQ(probes_, 2u);
  EXPECT_EQ(observed_pid_, 101u);
  EXPECT_EQ(observed_start_, 77u);
  EXPECT_EQ(retained_, 16u);
}

TEST_F(ReliableRetentionTest, RegistrationChangeDuringDeadProbeRejectsFloor) {
  alive_ = false;
  change_token_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 1u);
  EXPECT_EQ(retained_, 0u);
  EXPECT_EQ(retained_messages_, 0u);
}

TEST_F(ReliableRetentionTest, RegistrationChangeDuringLiveProbeRejectsFloor) {
  change_token_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 1u);
  EXPECT_EQ(retained_, 0u);
}

TEST_F(ReliableRetentionTest, DeadLeaseRecoveryChecksRegistrationBeforeClearing) {
  for (auto& cursor : cursors_) {
    cursor.policy = salias::channel::consumer_registration::kDropOldestPolicy;
  }
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(0, cursors_[0].state);
  ASSERT_TRUE(next(tx));
  auto held = rx.try_recv();
  ASSERT_TRUE(held);
  for (std::uint64_t sequence = 1; sequence < 256; ++sequence) {
    ASSERT_TRUE(next(tx));
  }
  const auto previous_floor = retained_;
  alive_ = false;
  change_token_ = true;
  EXPECT_FALSE(next(tx));
  EXPECT_EQ(probes_, 1u);
  EXPECT_EQ(retained_, previous_floor);
  EXPECT_EQ(cursors_[0].lease_start, 0u);
  EXPECT_EQ(intent_, kInactive);
  change_token_ = false;
  cursors_[0].generation = salias::channel::consumer_registration::generation(cursors_[0].state);
  EXPECT_TRUE(next(tx));
  EXPECT_EQ(probes_, 2u);
  EXPECT_EQ(cursors_[0].lease_start, kInactive);
  rx.release(*held);
}

}  // namespace

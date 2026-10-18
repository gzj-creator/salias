/**
 * @file test/channel/shared_reliable_channel_test.cpp
 * @brief Reliable FIFO 游标刷新、阻塞者存活探测与注册快照回归测试。
 */
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "core/channel/shared_hybrid_mpsc.hpp"
#include "core/platform/mapping.hpp"

namespace {

using Channel = salias::channel::SharedHybridMpscChannel<salias::channel::Order::Fifo>;
namespace registration = salias::channel::consumer_registration;

/// @brief 分别覆盖字节/消息窗口，以及单帧/批量申请；回调只计数，不访问 /proc。
class SharedReliableProbeTest : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
  struct alignas(64) Cursor {
    std::uint64_t position = 0;
    std::uint64_t sequence = 0;
    std::uint64_t messages = 0;
    std::uint32_t pid = 0;
    std::uint64_t start = 0;
    std::uint64_t generation = 1;
    std::uint64_t state = 0;
    std::uint32_t probes = 0;
    bool alive = true;
  };

  enum class RegistrationChange { None, Replace, Release };

  void SetUp() override {
    current_ = this;
    for (std::size_t index = 0; index < cursors_.size(); ++index) {
      auto& cursor = cursors_[index];
      cursor.pid = static_cast<std::uint32_t>(101 + index);
      cursor.start = live_starts_[index];
      cursor.state = registration::make_token(1, cursor.pid, registration::kActive);
    }
  }

  void TearDown() override { current_ = nullptr; }

  static bool process_alive(std::uint32_t pid, std::uint64_t start) noexcept {
    auto& test = *current_;
    test.observed_pid_ = pid;
    test.observed_start_ = start;
    for (std::size_t index = 0; index < test.cursors_.size(); ++index) {
      auto& cursor = test.cursors_[index];
      if (cursor.pid != pid) {
        continue;
      }
      ++cursor.probes;
      const bool alive = cursor.alive && start == test.live_starts_[index];
      // 故意在探测期间改变同一槽，验证死亡分支也不能跳过注册 token 复核。
      if (test.registration_change_ != RegistrationChange::None) {
        const bool replace = test.registration_change_ == RegistrationChange::Replace;
        std::atomic_ref<std::uint64_t>(cursor.generation)
            .store(2, std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(cursor.position).store(0, std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(cursor.messages).store(0, std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(cursor.state)
            .store(registration::make_token(2, replace ? pid : 0,
                                            replace ? registration::kActive : registration::kFree),
                   std::memory_order_release);
      }
      return alive;
    }
    return true;
  }

  void build(bool include_consumers = true) {
    Channel::ProducerSharedState producer{
        .visible_producer_pos = &visible_,
        .local_sequence = &sequence_,
        .reservation_position = &reserved_position_,
        .reservation_sequence = &reserved_sequence_,
        .claim_active = &claim_active_,
        .reservation_message_sequence = &reserved_messages_,
        .consumers = {},
    };
    if (include_consumers) {
      for (auto& cursor : cursors_) {
        producer.consumers.push_back(Channel::ConsumerSharedState{
            .position = &cursor.position,
            .sequence = &cursor.sequence,
            .message_sequence = &cursor.messages,
            .process_id = &cursor.pid,
            .process_start = missing_start_ ? nullptr : &cursor.start,
            .registration_state = &cursor.state,
            .cursor_generation = missing_generation_ ? nullptr : &cursor.generation,
        });
      }
    }
    auto mapping = salias::platform::Mapping::create({.size = 4096});
    ASSERT_TRUE(mapping);
    auto ring = salias::ring::MagicRing::create(std::move(*mapping));
    ASSERT_TRUE(ring);
    std::vector<salias::ring::MagicRing> rings;
    rings.push_back(std::move(*ring));
    std::vector<Channel::ProducerSharedState> producers;
    producers.push_back(std::move(producer));
    channel_.emplace(
        std::move(rings), std::move(producers),
        Channel::SharedControl{
            .consumer_sequences = {},
            .registration_gate = &gate_,
            .num_producers = &producer_count_,
            .num_consumers = &consumer_count_,
            .is_process_alive = missing_callback_ ? nullptr : process_alive,
            .publication_window = both_windows_ || !std::get<0>(GetParam()) ? 32u : 0u,
            .publication_window_messages = both_windows_ || std::get<0>(GetParam()) ? 2u : 0u,
        });
  }

  /// @brief 共用相同的两帧窗口；批量测试默认也只申请一帧，额外测试部分可用空间。
  std::uint32_t publish(Channel::Tx& tx, std::uint32_t max_frames = 1) {
    if (std::get<1>(GetParam())) {
      auto batch = tx.claim_batch(sizeof(std::uint64_t), max_frames);
      if (!batch) {
        EXPECT_EQ(batch.error(), salias::flow::FlowError::BackPressured);
        return 0;
      }
      for (std::uint32_t index = 0; index < batch->frame_count; ++index) {
        const std::uint64_t value = batch->base_sequence + index;
        std::memcpy(batch->region.data() + batch->frame_len * index + salias::frame::kHeaderSize,
                    &value, sizeof(value));
      }
      tx.commit_batch(*batch);
      return batch->frame_count;
    }
    auto claim = tx.claim(sizeof(std::uint64_t));
    if (!claim) {
      EXPECT_EQ(claim.error(), salias::flow::FlowError::BackPressured);
      return 0;
    }
    std::memcpy(claim->payload.data(), &claim->sequence, sizeof(claim->sequence));
    tx.commit(*claim);
    return 1;
  }

  void prefill(Channel::Tx& tx) {
    ASSERT_EQ(publish(tx), 1u);
    ASSERT_EQ(publish(tx), 1u);
    ASSERT_EQ(visible_, 32u);
    ASSERT_EQ(probes(), 0u);
  }

  void advance(std::size_t index) {
    auto& cursor = cursors_[index];
    std::atomic_ref<std::uint64_t>(cursor.position).store(visible_, std::memory_order_release);
    std::atomic_ref<std::uint64_t>(cursor.sequence).store(sequence_, std::memory_order_release);
    std::atomic_ref<std::uint64_t>(cursor.messages)
        .store(reserved_messages_, std::memory_order_release);
  }

  std::uint32_t probes() const { return cursors_[0].probes + cursors_[1].probes; }

  inline static SharedReliableProbeTest* current_ = nullptr;
  std::array<Cursor, 2> cursors_{};
  std::array<std::uint64_t, 2> live_starts_{77, 78};
  std::uint64_t visible_ = 0;
  std::uint64_t sequence_ = 0;
  std::uint64_t reserved_position_ = 0;
  std::uint64_t reserved_sequence_ = 0;
  std::uint64_t reserved_messages_ = 0;
  std::uint64_t gate_ = 0;
  std::uint32_t claim_active_ = 0;
  std::uint32_t producer_count_ = 1;
  std::uint32_t consumer_count_ = 2;
  std::uint32_t observed_pid_ = 0;
  std::uint64_t observed_start_ = 0;
  bool missing_start_ = false;
  bool missing_generation_ = false;
  bool missing_callback_ = false;
  bool both_windows_ = false;
  RegistrationChange registration_change_ = RegistrationChange::None;
  std::optional<Channel> channel_;
};

TEST_P(SharedReliableProbeTest, HealthyProgressRefreshesWithoutProbes) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  for (std::uint64_t index = 0; index < 1024; ++index) {
    advance(0);
    advance(1);
    ASSERT_EQ(publish(tx), 1u);
    ASSERT_EQ(probes(), 0u);
  }
  EXPECT_GT(visible_, 4096u);
}

TEST_P(SharedReliableProbeTest, HealthyProgressWithBothWindowsAvoidsProbes) {
  both_windows_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  for (std::uint64_t index = 0; index < 1024; ++index) {
    advance(0);
    advance(1);
    ASSERT_EQ(publish(tx), 1u);
    ASSERT_EQ(probes(), 0u);
  }
}

TEST_P(SharedReliableProbeTest, BothWindowsProbeOnlyTheirRespectiveBlockers) {
  both_windows_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  cursors_[0].messages = 0;
  cursors_[1].messages = 2;
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(cursors_[0].probes, 0u);
  EXPECT_EQ(cursors_[1].probes, 1u);
  advance(1);
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(cursors_[0].probes, 1u);
  EXPECT_EQ(cursors_[1].probes, 1u);
  advance(0);
  EXPECT_EQ(publish(tx), 1u);
  EXPECT_EQ(probes(), 2u);
}

TEST_P(SharedReliableProbeTest, RecreatedTxScansAdvancedMessageCursorsWithoutProbes) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  advance(1);
  auto recreated_tx = channel_->tx(0);
  EXPECT_EQ(probes(), 0u);
  EXPECT_EQ(publish(recreated_tx), 1u);
  EXPECT_EQ(probes(), 0u);
}

TEST_P(SharedReliableProbeTest, OnlySlowestConsumerIsProbedAndStillBackpressures) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(cursors_[0].probes, 0u);
  EXPECT_EQ(cursors_[1].probes, 2u);
  EXPECT_EQ(visible_, 32u);
  EXPECT_EQ(reserved_messages_, 2u);
  advance(1);
  EXPECT_EQ(publish(tx), 1u);
  EXPECT_EQ(probes(), 2u);
}

TEST_P(SharedReliableProbeTest, DeadBlockerIsRecheckedWithoutDeathCache) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  cursors_[1].alive = false;
  EXPECT_EQ(publish(tx), 1u);
  EXPECT_EQ(cursors_[0].probes, 0u);
  EXPECT_EQ(cursors_[1].probes, 1u);
  EXPECT_EQ(publish(tx), 1u);
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(cursors_[0].probes, 1u);
  EXPECT_EQ(cursors_[1].probes, 2u);
}

TEST_P(SharedReliableProbeTest, ReusedPidStillChecksOriginalStartToken) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  ++live_starts_[1];
  EXPECT_EQ(publish(tx), 1u);
  EXPECT_EQ(cursors_[0].probes, 0u);
  EXPECT_EQ(cursors_[1].probes, 1u);
  EXPECT_EQ(observed_pid_, 102u);
  EXPECT_EQ(observed_start_, 78u);
}

TEST_P(SharedReliableProbeTest, AllDeadBlockersKeepZeroFallback) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  advance(1);
  ASSERT_EQ(publish(tx), 1u);
  ASSERT_EQ(publish(tx), 1u);
  for (auto& cursor : cursors_) {
    cursor.alive = false;
  }
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(probes(), 2u);
  EXPECT_EQ(visible_, 64u);
  EXPECT_EQ(reserved_messages_, 4u);
}

TEST_P(SharedReliableProbeTest, InactiveSlotsDoNotInventAvailableSpace) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  for (auto& cursor : cursors_) {
    cursor.state = registration::make_token(1, 0, registration::kFree);
  }
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(probes(), 0u);
  EXPECT_EQ(visible_, 32u);
}

TEST_P(SharedReliableProbeTest, EmptyConsumerVectorKeepsInvalidClaimSemantics) {
  ASSERT_NO_FATAL_FAILURE(build(false));
  auto tx = channel_->tx(0);
  if (std::get<1>(GetParam())) {
    const auto claim = tx.claim_batch(sizeof(std::uint64_t), 1);
    ASSERT_FALSE(claim);
    EXPECT_EQ(claim.error(), salias::flow::FlowError::MessageTooLarge);
  } else {
    const auto claim = tx.claim(sizeof(std::uint64_t));
    ASSERT_FALSE(claim);
    EXPECT_EQ(claim.error(), salias::flow::FlowError::MessageTooLarge);
  }
  EXPECT_EQ(probes(), 0u);
}

TEST_P(SharedReliableProbeTest, FastCursorWithWrongGenerationFailsBeforeProbe) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  advance(1);
  cursors_[0].generation = 2;
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(probes(), 0u);
  EXPECT_EQ(visible_, 32u);
  EXPECT_EQ(reserved_messages_, 2u);
}

TEST_P(SharedReliableProbeTest, MissingGenerationFailsBeforeProbe) {
  missing_generation_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  advance(1);
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(probes(), 0u);
}

TEST_P(SharedReliableProbeTest, RegistrationChangeDuringLiveProbeRejectsSnapshot) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  registration_change_ = RegistrationChange::Replace;
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(cursors_[1].probes, 1u);
  EXPECT_EQ(visible_, 32u);
  EXPECT_EQ(reserved_messages_, 2u);
}

TEST_P(SharedReliableProbeTest, RegistrationChangeDuringDeadProbeRejectsSnapshot) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  cursors_[1].alive = false;
  registration_change_ = RegistrationChange::Replace;
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(cursors_[1].probes, 1u);
  EXPECT_EQ(visible_, 32u);
  EXPECT_EQ(reserved_messages_, 2u);
}

TEST_P(SharedReliableProbeTest, UnregisterDuringProbeRejectsSnapshotThenRetries) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  cursors_[1].alive = false;
  registration_change_ = RegistrationChange::Release;
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(visible_, 32u);
  registration_change_ = RegistrationChange::None;
  EXPECT_EQ(publish(tx), 1u);
  EXPECT_EQ(probes(), 1u);
}

TEST_P(SharedReliableProbeTest, RegistrationGateRejectsEvenAdvancedCursors) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  advance(1);
  gate_ = registration::make_token(1, 103, registration::kActive);
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(claim_active_, 0u);
  EXPECT_EQ(probes(), 0u);
  gate_ = 0;
  EXPECT_EQ(publish(tx), 1u);
  EXPECT_EQ(probes(), 0u);
}

TEST_P(SharedReliableProbeTest, MissingStartTokenKeepsBlockerBackpressured) {
  missing_start_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  cursors_[1].alive = false;
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(probes(), 0u);
}

TEST_P(SharedReliableProbeTest, MissingCallbackKeepsBlockerBackpressured) {
  missing_callback_ = true;
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  cursors_[1].alive = false;
  EXPECT_EQ(publish(tx), 0u);
  EXPECT_EQ(probes(), 0u);
}

TEST_P(SharedReliableProbeTest, UnreleasedPayloadIsNotOverwritten) {
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  auto rx = channel_->rx(1, cursors_[1].state);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  auto held = rx.try_recv();
  ASSERT_TRUE(held);
  advance(0);
  EXPECT_EQ(publish(tx), 0u);
  std::uint64_t value = 99;
  std::memcpy(&value, held->payload.data(), sizeof(value));
  EXPECT_EQ(value, 0u);
  EXPECT_EQ(visible_, 32u);
  rx.release(*held);
  ASSERT_EQ(publish(tx), 1u);
  auto unread = rx.try_recv();
  ASSERT_TRUE(unread);
  std::memcpy(&value, unread->payload.data(), sizeof(value));
  EXPECT_EQ(value, 1u);
  EXPECT_EQ(unread->sequence, 1u);
  rx.release(*unread);
}

TEST_P(SharedReliableProbeTest, BatchAllowsOnlySafePrefixWithoutProbing) {
  if (!std::get<1>(GetParam())) {
    GTEST_SKIP() << "仅覆盖批量申请的部分可用空间";
  }
  ASSERT_NO_FATAL_FAILURE(build());
  auto tx = channel_->tx(0);
  ASSERT_NO_FATAL_FAILURE(prefill(tx));
  advance(0);
  cursors_[1].position = 16;
  cursors_[1].messages = 1;
  EXPECT_EQ(publish(tx, 4), 1u);
  EXPECT_EQ(probes(), 0u);
  EXPECT_EQ(publish(tx, 4), 0u);
  EXPECT_EQ(cursors_[0].probes, 0u);
  EXPECT_EQ(cursors_[1].probes, 1u);
}

INSTANTIATE_TEST_SUITE_P(WindowsAndClaims, SharedReliableProbeTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool()));

}  // namespace

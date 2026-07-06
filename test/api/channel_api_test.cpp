#include "salias/salias.hpp"

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace {

inline constexpr std::uint32_t kTestNamedMagic = 0x53414C43u;
inline constexpr std::uint32_t kTestNamedVersion = 1;
inline constexpr std::size_t kTestControlSize = 4096;

struct alignas(64) TestNamedSpscControl {
  std::uint32_t magic = 0;
  std::uint32_t version = 0;
  std::uint32_t mode = 0;
  std::uint32_t flags = 0;
  std::uint64_t capacity = 0;
  std::uint64_t record_size = 0;
  std::uint32_t ready = 0;
  std::uint32_t wait_word = 0;
  std::byte pad[64 - 40]{};
  alignas(64) std::uint64_t producer_pos = 0;
  alignas(64) std::uint64_t consumer_pos = 0;
};

std::string control_shm_name(std::string_view name) {
  return "/salias-" + std::string(name) + "-ctl";
}

salias::Config test_config() {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  EXPECT_GT(raw_page_size, 0);
  salias::Config config;
  config.capacity = static_cast<std::size_t>(raw_page_size);
  return config;
}

std::string unique_name(std::string_view suffix) {
  return "salias-test-" + std::to_string(::getpid()) + "-" + std::string(suffix);
}

void write_control_shm(std::string_view name, const TestNamedSpscControl& control) {
  const std::string control_name = control_shm_name(name);
  static_cast<void>(::shm_unlink(control_name.c_str()));
  const int fd = ::shm_open(control_name.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(kTestControlSize)), 0);
  void* mapped =
      ::mmap(nullptr, kTestControlSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  ASSERT_NE(mapped, MAP_FAILED);
  std::memcpy(mapped, &control, sizeof(control));
  auto* shared = static_cast<TestNamedSpscControl*>(mapped);
  std::atomic_ref<std::uint32_t>(shared->ready).store(control.ready, std::memory_order_release);
  ASSERT_EQ(::munmap(mapped, kTestControlSize), 0);
  ASSERT_EQ(::close(fd), 0);
}

void unlink_control_shm(std::string_view name) {
  const std::string control_name = control_shm_name(name);
  static_cast<void>(::shm_unlink(control_name.c_str()));
}

TEST(ChannelApiTest, InProcessSpscOfferAndTryRecv) {
  auto channel_result = salias::Channel::create(test_config());
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  salias::Publisher publisher = channel.publisher();
  salias::Subscriber subscriber = channel.subscriber();

  constexpr std::array payload{
      std::byte{0x61},
      std::byte{0x62},
      std::byte{0x63},
  };

  auto offered = publisher.offer(payload);
  ASSERT_TRUE(offered);
  EXPECT_TRUE(offered.value());

  auto message = subscriber.try_recv();
  ASSERT_TRUE(message.has_value());
  ASSERT_EQ(message->payload.size(), payload.size());
  for (std::size_t i = 0; i < payload.size(); ++i) {
    EXPECT_EQ(message->payload[i], payload[i]) << "payload byte " << i;
  }

  subscriber.release(*message);
  EXPECT_FALSE(subscriber.try_recv().has_value());
}

TEST(ChannelApiTest, RejectsUnsupportedNamedNonSpscHandshakeForNow) {
  salias::Config config = test_config();
  config.name = unique_name("named-mpsc");
  config.mode = salias::Mode::Mpsc;

  auto channel = salias::Channel::create(config);

  ASSERT_FALSE(channel);
  EXPECT_EQ(channel.error(), salias::Error::BadConfig);
}

TEST(ChannelApiTest, NamedSpscConnectsAcrossForkWithoutDriver) {
  salias::Config config = test_config();
  config.name = unique_name("named-spsc");
  config.mode = salias::Mode::Spsc;

  auto owner_result = salias::Channel::create(config);
  ASSERT_TRUE(owner_result);
  auto owner = std::move(owner_result).value();
  auto publisher = owner.publisher();

  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    auto peer_result = salias::Channel::connect(config.name);
    if (!peer_result) {
      _exit(10);
    }
    auto peer = std::move(peer_result).value();
    auto subscriber = peer.subscriber();

    for (int attempt = 0; attempt < 1000; ++attempt) {
      auto message = subscriber.try_recv();
      if (!message) {
        std::this_thread::yield();
        continue;
      }
      constexpr std::array expected{std::byte{0x41}, std::byte{0x42}, std::byte{0x43}};
      if (message->payload.size() != expected.size()) {
        _exit(11);
      }
      if (std::memcmp(message->payload.data(), expected.data(), expected.size()) != 0) {
        _exit(12);
      }
      subscriber.release(*message);
      _exit(0);
    }
    _exit(13);
  }

  constexpr std::array payload{std::byte{0x41}, std::byte{0x42}, std::byte{0x43}};
  auto offered = publisher.offer(payload);
  ASSERT_TRUE(offered);

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(ChannelApiTest, NamedSpscPeerCanStartBeforeOwnerPublishesReady) {
  salias::Config config = test_config();
  config.name = unique_name("peer-first");
  config.mode = salias::Mode::Spsc;

  int pipe_fds[2]{};
  ASSERT_EQ(::pipe(pipe_fds), 0);

  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    close(pipe_fds[0]);
    const char started = 's';
    if (::write(pipe_fds[1], &started, 1) != 1) {
      _exit(20);
    }
    close(pipe_fds[1]);

    auto peer_result = salias::Channel::connect(config.name);
    if (!peer_result) {
      _exit(21);
    }
    auto peer = std::move(peer_result).value();
    auto subscriber = peer.subscriber();
    for (int attempt = 0; attempt < 1000; ++attempt) {
      auto message = subscriber.try_recv();
      if (!message) {
        std::this_thread::yield();
        continue;
      }
      if (message->payload.size() != 1 || message->payload[0] != std::byte{0x55}) {
        _exit(22);
      }
      subscriber.release(*message);
      _exit(0);
    }
    _exit(23);
  }

  close(pipe_fds[1]);
  char started = 0;
  ASSERT_EQ(::read(pipe_fds[0], &started, 1), 1);
  close(pipe_fds[0]);
  ASSERT_EQ(started, 's');

  int early_status = 0;
  bool child_exited_before_owner = false;
  for (int attempt = 0; attempt < 100; ++attempt) {
    const pid_t observed = ::waitpid(child, &early_status, WNOHANG);
    ASSERT_NE(observed, -1);
    if (observed == child) {
      child_exited_before_owner = true;
      break;
    }
    std::this_thread::yield();
  }
  ASSERT_FALSE(child_exited_before_owner)
      << "connect returned before the owner created the named channel, status=" << early_status;

  auto owner_result = salias::Channel::create(config);
  ASSERT_TRUE(owner_result);
  auto owner = std::move(owner_result).value();
  auto publisher = owner.publisher();
  constexpr std::array payload{std::byte{0x55}};
  ASSERT_TRUE(publisher.offer(payload));

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(ChannelApiTest, NamedConnectRejectsVersionMismatchMetadata) {
  const std::string name = unique_name("bad-version");
  TestNamedSpscControl control{};
  control.magic = kTestNamedMagic;
  control.version = kTestNamedVersion + 1;
  control.mode = static_cast<std::uint32_t>(salias::Mode::Spsc);
  control.capacity = test_config().capacity;
  control.ready = 1;
  write_control_shm(name, control);

  auto connected = salias::Channel::connect(name);

  EXPECT_FALSE(connected);
  EXPECT_EQ(connected.error(), salias::Error::VersionMismatch);
  unlink_control_shm(name);
}

TEST(ChannelApiTest, NamedConnectRejectsDamagedCapacityBeforeOpeningRing) {
  const std::string name = unique_name("bad-capacity");
  TestNamedSpscControl control{};
  control.magic = kTestNamedMagic;
  control.version = kTestNamedVersion;
  control.mode = static_cast<std::uint32_t>(salias::Mode::Spsc);
  control.capacity = 3;
  control.ready = 1;
  write_control_shm(name, control);

  auto connected = salias::Channel::connect(name);

  EXPECT_FALSE(connected);
  EXPECT_EQ(connected.error(), salias::Error::BadConfig);
  unlink_control_shm(name);
}

TEST(ChannelApiTest, NamedConnectRejectsUntrustedMetadataBeforeOpeningRing) {
  struct BadMetaCase {
    const char* suffix;
    std::uint32_t mode;
    std::uint64_t capacity;
    std::uint64_t record_size;
  };

  const std::uint64_t valid_capacity = test_config().capacity;
  const std::array cases{
      BadMetaCase{
          .suffix = "bad-mode",
          .mode = static_cast<std::uint32_t>(salias::Mode::Broadcast),
          .capacity = valid_capacity,
          .record_size = 0,
      },
      BadMetaCase{
          .suffix = "zero-capacity",
          .mode = static_cast<std::uint32_t>(salias::Mode::Spsc),
          .capacity = 0,
          .record_size = 0,
      },
      BadMetaCase{
          .suffix = "non-power-two-capacity",
          .mode = static_cast<std::uint32_t>(salias::Mode::Spsc),
          .capacity = 3,
          .record_size = 0,
      },
      BadMetaCase{
          .suffix = "huge-capacity",
          .mode = static_cast<std::uint32_t>(salias::Mode::Spsc),
          .capacity = std::numeric_limits<std::uint64_t>::max(),
          .record_size = 0,
      },
      BadMetaCase{
          .suffix = "unexpected-record-size",
          .mode = static_cast<std::uint32_t>(salias::Mode::Spsc),
          .capacity = valid_capacity,
          .record_size = 8,
      },
  };

  for (const auto& bad : cases) {
    const std::string name = unique_name(bad.suffix);
    TestNamedSpscControl control{};
    control.magic = kTestNamedMagic;
    control.version = kTestNamedVersion;
    control.mode = bad.mode;
    control.capacity = bad.capacity;
    control.record_size = bad.record_size;
    control.ready = 1;
    write_control_shm(name, control);

    auto connected = salias::Channel::connect(name);

    EXPECT_FALSE(connected) << bad.suffix;
    if (!connected) {
      EXPECT_EQ(connected.error(), salias::Error::BadConfig) << bad.suffix;
    }
    unlink_control_shm(name);
  }
}

TEST(ChannelApiTest, InProcessMpscAcceptsMultiplePublishers) {
  salias::Config config = test_config();
  config.mode = salias::Mode::Mpsc;

  auto channel_result = salias::Channel::create(config);
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  auto first_publisher = channel.publisher();
  auto second_publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  constexpr std::array first_payload{std::byte{0x10}, std::byte{0x11}};
  constexpr std::array second_payload{std::byte{0x20}, std::byte{0x21}};

  ASSERT_TRUE(first_publisher.offer(first_payload));
  ASSERT_TRUE(second_publisher.offer(second_payload));

  auto first = subscriber.try_recv();
  ASSERT_TRUE(first);
  EXPECT_EQ(first->payload[0], std::byte{0x10});
  subscriber.release(*first);

  auto second = subscriber.try_recv();
  ASSERT_TRUE(second);
  EXPECT_EQ(second->payload[0], std::byte{0x20});
  subscriber.release(*second);
}

TEST(ChannelApiTest, InProcessBroadcastSubscribersEachReceiveAllMessages) {
  salias::Config config = test_config();
  config.mode = salias::Mode::Broadcast;

  auto channel_result = salias::Channel::create(config);
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  auto publisher = channel.publisher();
  auto first_subscriber = channel.subscriber();
  auto second_subscriber = channel.subscriber();

  constexpr std::array payload{std::byte{0x31}, std::byte{0x32}, std::byte{0x33}};
  ASSERT_TRUE(publisher.offer(payload));

  auto first = first_subscriber.try_recv();
  ASSERT_TRUE(first);
  EXPECT_EQ(first->payload[0], std::byte{0x31});
  first_subscriber.release(*first);

  auto second = second_subscriber.try_recv();
  ASSERT_TRUE(second);
  EXPECT_EQ(second->payload[0], std::byte{0x31});
  second_subscriber.release(*second);
}

TEST(ChannelApiTest, InProcessBulkAcceptsLargeSingleFrameMessage) {
  salias::Config config;
  config.mode = salias::Mode::Bulk;
  config.capacity = 1u << 20;

  auto channel_result = salias::Channel::create(config);
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::vector<std::byte> payload(128 * 1024, std::byte{0x5A});
  ASSERT_TRUE(publisher.offer(payload));

  auto message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(message->payload.size(), payload.size());
  EXPECT_EQ(message->payload.front(), std::byte{0x5A});
  EXPECT_EQ(message->payload.back(), std::byte{0x5A});
  subscriber.release(*message);
}

TEST(ChannelApiTest, RecreatedPublisherStateStillBackpressuresAfterRingWrap) {
  salias::Config config = test_config();
  auto channel_result = salias::Channel::create(config);
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();
  std::vector<std::byte> payload(512, std::byte{0x6B});

  for (int i = 0; i < 8; ++i) {
    auto offered = publisher.offer(payload);
    ASSERT_TRUE(offered);
    auto message = subscriber.try_recv();
    ASSERT_TRUE(message);
    subscriber.release(*message);
  }

  for (int i = 0; i < 7; ++i) {
    auto offered = publisher.offer(payload);
    ASSERT_TRUE(offered);
  }

  auto blocked = publisher.offer(payload);
  ASSERT_FALSE(blocked);
  EXPECT_EQ(blocked.error(), salias::Error::BackPressured);
}

}  // namespace

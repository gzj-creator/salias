#include "salias/salias.hpp"

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
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

// 构造与生产命名通道约定一致的 POSIX shm 控制块名称。
std::string control_shm_name(std::string_view name) {
  return "/salias-" + std::string(name) + "-ctl";
}

// 为测试创建页大小的默认公共通道配置。
salias::Config test_config() {
  const long raw_page_size = ::sysconf(_SC_PAGESIZE);
  EXPECT_GT(raw_page_size, 0);
  salias::Config config;
  config.capacity = static_cast<std::size_t>(raw_page_size);
  return config;
}

// 为命名通道测试构造当前进程唯一的名称后缀。
std::string unique_name(std::string_view suffix) {
  return "salias-test-" + std::to_string(::getpid()) + "-" + std::string(suffix);
}

// 为 connect() 校验测试写入合成命名通道控制块。
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

// 删除测试创建的合成命名通道控制块。
void unlink_control_shm(std::string_view name) {
  const std::string control_name = control_shm_name(name);
  static_cast<void>(::shm_unlink(control_name.c_str()));
}

// 在限定时间内等待多个子进程报告 ready，避免失败路径让测试永久阻塞在 pipe 上。
bool wait_for_ready_bytes(int fd, std::size_t expected) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    return false;
  }

  std::size_t ready = 0;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (ready < expected && std::chrono::steady_clock::now() < deadline) {
    char byte = 0;
    const ssize_t read_count = ::read(fd, &byte, 1);
    if (read_count == 1) {
      if (byte != 'r') {
        return false;
      }
      ++ready;
      continue;
    }
    if (read_count == 0) {
      return false;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      return false;
    }
    std::this_thread::yield();
  }
  return ready == expected;
}

// 验证公共进程内 SPSC offer/try_recv/release 路径。
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

// 验证批量 poll 会按顺序消费可用消息并自动释放 ring 空间。
TEST(ChannelApiTest, PollDrainsAvailableMessagesAndAutoReleasesThem) {
  auto channel_result = salias::Channel::create(test_config());
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array first{std::byte{0x11}};
  std::array second{std::byte{0x22}};
  ASSERT_TRUE(publisher.offer(first));
  ASSERT_TRUE(publisher.offer(second));

  std::vector<unsigned> seen;
  auto handler = [&seen](const salias::Message& message) noexcept {
    seen.push_back(std::to_integer<unsigned>(message.payload.front()));
  };

  EXPECT_EQ(subscriber.poll(64, handler), 2u);
  ASSERT_EQ(seen.size(), 2u);
  EXPECT_EQ(seen[0], 0x11u);
  EXPECT_EQ(seen[1], 0x22u);
  EXPECT_FALSE(subscriber.try_recv().has_value());

  std::array third{std::byte{0x33}};
  ASSERT_TRUE(publisher.offer(third));
  auto next = subscriber.try_recv();
  ASSERT_TRUE(next);
  EXPECT_EQ(next->payload.front(), std::byte{0x33});
  subscriber.release(*next);
}

// 验证 max_messages 为 0 时 poll 不调用 handler，也不消费消息。
TEST(ChannelApiTest, PollWithZeroLimitDoesNotCallHandler) {
  auto channel_result = salias::Channel::create(test_config());
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array payload{std::byte{0x44}};
  ASSERT_TRUE(publisher.offer(payload));

  bool called = false;
  auto handler = [&called](const salias::Message&) noexcept { called = true; };

  EXPECT_EQ(subscriber.poll(0, handler), 0u);
  EXPECT_FALSE(called);

  auto message = subscriber.try_recv();
  ASSERT_TRUE(message);
  EXPECT_EQ(message->payload.front(), std::byte{0x44});
  subscriber.release(*message);
}

// 验证公共 try_claim/commit 可以原地写入 payload，并且 commit 前消费者不可见。
TEST(ChannelApiTest, PublisherClaimWritesInPlaceAndCommitsMessage) {
  auto channel_result = salias::Channel::create(test_config());
  ASSERT_TRUE(channel_result);
  auto channel = std::move(channel_result).value();

  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  auto claim_result = publisher.try_claim(3);
  ASSERT_TRUE(claim_result);
  auto claim = std::move(claim_result).value();
  ASSERT_EQ(claim.payload().size(), 3u);
  claim.payload()[0] = std::byte{0x41};
  claim.payload()[1] = std::byte{0x42};
  claim.payload()[2] = std::byte{0x43};

  EXPECT_FALSE(subscriber.try_recv().has_value());
  claim.commit();

  auto message = subscriber.try_recv();
  ASSERT_TRUE(message);
  ASSERT_EQ(message->payload.size(), 3u);
  EXPECT_EQ(message->payload[0], std::byte{0x41});
  EXPECT_EQ(message->payload[1], std::byte{0x42});
  EXPECT_EQ(message->payload[2], std::byte{0x43});
  subscriber.release(*message);
}

// 验证当前握手实现仍会拒绝尚未支持的命名 Broadcast 模式。
TEST(ChannelApiTest, RejectsUnsupportedNamedBroadcastHandshakeForNow) {
  salias::Config config = test_config();
  config.name = unique_name("named-broadcast");
  config.mode = salias::Mode::Broadcast;

  auto channel = salias::Channel::create(config);

  ASSERT_FALSE(channel);
  EXPECT_EQ(channel.error(), salias::Error::BadConfig);
}

// 验证命名 MPSC 通道支持多个跨进程 publisher 和单个 owner subscriber。
TEST(ChannelApiTest, NamedMpscAcceptsMultiplePublisherProcesses) {
  salias::Config config = test_config();
  config.name = unique_name("named-mpsc");
  config.mode = salias::Mode::Mpsc;

  auto owner_result = salias::Channel::create(config);
  ASSERT_TRUE(owner_result);
  auto owner = std::move(owner_result).value();
  auto subscriber = owner.subscriber();

  auto spawn_publisher = [&](std::byte value) -> pid_t {
    const pid_t pid = ::fork();
    if (pid < 0) {
      ADD_FAILURE() << "fork failed";
      return -1;
    }
    if (pid == 0) {
      auto peer_result = salias::Channel::connect(config.name);
      if (!peer_result) {
        _exit(10);
      }
      auto peer = std::move(peer_result).value();
      auto publisher = peer.publisher();
      std::array payload{value};
      auto offered = publisher.offer(payload);
      if (!offered || !offered.value()) {
        _exit(11);
      }
      _exit(0);
    }
    return pid;
  };

  const pid_t first_pid = spawn_publisher(std::byte{0x51});
  const pid_t second_pid = spawn_publisher(std::byte{0x52});

  std::vector<unsigned> seen;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (seen.size() < 2 && std::chrono::steady_clock::now() < deadline) {
    auto message = subscriber.try_recv();
    if (!message) {
      std::this_thread::yield();
      continue;
    }
    seen.push_back(std::to_integer<unsigned>(message->payload.front()));
    subscriber.release(*message);
  }

  int first_status = 0;
  int second_status = 0;
  ASSERT_EQ(::waitpid(first_pid, &first_status, 0), first_pid);
  ASSERT_EQ(::waitpid(second_pid, &second_status, 0), second_pid);
  ASSERT_TRUE(WIFEXITED(first_status));
  ASSERT_TRUE(WIFEXITED(second_status));
  EXPECT_EQ(WEXITSTATUS(first_status), 0);
  EXPECT_EQ(WEXITSTATUS(second_status), 0);

  ASSERT_EQ(seen.size(), 2u);
  EXPECT_NE(std::find(seen.begin(), seen.end(), 0x51u), seen.end());
  EXPECT_NE(std::find(seen.begin(), seen.end(), 0x52u), seen.end());
}

// 验证命名 MPMC fanout 语义：每个跨进程 subscriber 都收到每个 publisher 的消息。
TEST(ChannelApiTest, NamedMpmcFanoutDeliversEveryPublisherMessageToEverySubscriberProcess) {
  salias::Config config = test_config();
  config.name = unique_name("named-mpmc");
  config.mode = salias::Mode::Mpmc;

  auto owner_result = salias::Channel::create(config);
  ASSERT_TRUE(owner_result);
  auto owner = std::move(owner_result).value();

  int ready_fds[2]{};
  ASSERT_EQ(::pipe(ready_fds), 0);

  auto spawn_subscriber = [&]() -> pid_t {
    const pid_t pid = ::fork();
    if (pid < 0) {
      ADD_FAILURE() << "fork failed";
      return -1;
    }
    if (pid == 0) {
      close(ready_fds[0]);
      auto peer_result = salias::Channel::connect(config.name);
      if (!peer_result) {
        _exit(20);
      }
      auto peer = std::move(peer_result).value();
      auto subscriber = peer.subscriber();
      const char ready = 'r';
      if (::write(ready_fds[1], &ready, 1) != 1) {
        _exit(21);
      }
      close(ready_fds[1]);

      std::array<bool, 2> seen{};
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
      while ((!seen[0] || !seen[1]) && std::chrono::steady_clock::now() < deadline) {
        auto message = subscriber.try_recv();
        if (!message) {
          std::this_thread::yield();
          continue;
        }
        if (message->payload.size() != 1) {
          _exit(22);
        }
        const unsigned value = std::to_integer<unsigned>(message->payload.front());
        if (value == 0x61u) {
          seen[0] = true;
        } else if (value == 0x62u) {
          seen[1] = true;
        } else {
          _exit(23);
        }
        subscriber.release(*message);
      }
      _exit(seen[0] && seen[1] ? 0 : 24);
    }
    return pid;
  };

  const pid_t first_subscriber = spawn_subscriber();
  const pid_t second_subscriber = spawn_subscriber();
  close(ready_fds[1]);
  ASSERT_TRUE(wait_for_ready_bytes(ready_fds[0], 2));
  close(ready_fds[0]);

  auto spawn_publisher = [&](std::byte value) -> pid_t {
    const pid_t pid = ::fork();
    if (pid < 0) {
      ADD_FAILURE() << "fork failed";
      return -1;
    }
    if (pid == 0) {
      auto peer_result = salias::Channel::connect(config.name);
      if (!peer_result) {
        _exit(30);
      }
      auto peer = std::move(peer_result).value();
      auto publisher = peer.publisher();
      std::array payload{value};
      auto offered = publisher.offer(payload);
      if (!offered || !offered.value()) {
        _exit(31);
      }
      _exit(0);
    }
    return pid;
  };

  const pid_t first_publisher = spawn_publisher(std::byte{0x61});
  const pid_t second_publisher = spawn_publisher(std::byte{0x62});

  std::array pids{first_publisher, second_publisher, first_subscriber, second_subscriber};
  for (const pid_t pid : pids) {
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status)) << "pid=" << pid << " status=" << status;
    EXPECT_EQ(WEXITSTATUS(status), 0) << "pid=" << pid;
  }
}

// 验证命名 SPSC 通道无需驱动进程即可跨 fork 通信。
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

// 验证 connect() 可以先于拥有者发布 ready metadata 启动。
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

// 验证 connect() 会拒绝不兼容的命名通道 metadata 版本。
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

// 验证 connect() 在映射 ring 前会拒绝损坏的容量 metadata。
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

// 验证 connect() 会拒绝不可信的命名通道 metadata 字段。
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

// 验证公共 MPSC 外观接受多个 publisher 的 offer。
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

// 验证独立 broadcast 订阅者都会收到同一条已发布消息。
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

// 验证公共 bulk 外观接受大的单帧 payload。
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

// 验证 ring 回绕后新建 publisher 端点状态仍遵守背压。
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

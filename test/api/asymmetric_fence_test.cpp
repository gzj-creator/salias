/** @file test/api/asymmetric_fence_test.cpp
 * @brief P3-1 非对称屏障（membarrier）的跨进程正确性测试。
 * @details 覆盖四类风险：
 *          1. 发布端 claim 只付编译器屏障后，注册/注销握手仍然互斥：
 *             一个进程持续 claim/commit，另一个进程循环 acquire/release 订阅者 10,000 次，
 *             每个新订阅者从自己的注册快照起校验序号连续、无缺、无重；
 *             asymmetric 与 SALIAS_DISABLE_ASYMMETRIC_FENCE=1 强制 seq_cst 各跑一遍。
 *          2. fork 后子进程重新创建 Publisher 仍能正常发布（membarrier 注册状态随 mm 继承）。
 *          3. 故障注入：heavy fence 失败时注册必须失败且不损坏消息；
 *             发布端注册失败时退回 seq_cst 且照常发布。
 *          4. 协议边界：v14 控制段与未知协议值都必须被连接端拒绝；
 *             注销路径在 heavy fence 失败时 fail closed（不清理槽位）。
 *          线程/进程模型：所有子进程一律 _exit()，避免继承来的 Channel 句柄在子进程里
 *          析构并 unlink 父进程仍在使用的共享段。
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include "core/platform/asymmetric_fence.hpp"
#include "salias/salias.hpp"

namespace {

/// @brief 测试负载：单调递增计数器，用于校验交付序号连续。
struct CounterMessage {
  std::uint64_t value = 0;
};

/// @brief 跨进程控制标志（MAP_SHARED 匿名映射，fork 前建立）。
struct SharedFlags {
  std::atomic<std::uint32_t> stop{0};      ///< 父进程置 1 后发布端退出。
  std::atomic<std::uint32_t> go{0};        ///< 订阅者就绪后放行发布端。
  std::atomic<std::uint32_t> failure{0};   ///< 任一子进程检测到错误时置 1。
};

constexpr std::size_t kStressCapacity = 1u << 20;      ///< 1 MiB，容纳约 1.4 万条 8B 帧。
constexpr std::uint32_t kMessagesPerSession = 8;       ///< 每次订阅至少校验的消息条数。
constexpr std::uint32_t kAcquireReleaseCycles = 10'000;  ///< §5.6 要求的 acquire/release 次数。
constexpr auto kSessionTimeout = std::chrono::seconds(5);

/// @brief 生成进程内唯一的通道名。
std::string unique_name(std::string_view suffix) {
  static std::atomic<std::uint32_t> counter = 0;
  return "fence-" + std::to_string(::getpid()) + "-" +
         std::to_string(counter.fetch_add(1, std::memory_order_relaxed)) + "-" +
         std::string(suffix);
}

/// @brief 建立一块 fork 前共享的标志区。
SharedFlags* make_shared_flags() {
  void* memory = ::mmap(nullptr, sizeof(SharedFlags), PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (memory == MAP_FAILED) {
    return nullptr;
  }
  return std::construct_at(static_cast<SharedFlags*>(memory));
}

void destroy_shared_flags(SharedFlags* flags) {
  if (flags != nullptr) {
    static_cast<void>(::munmap(flags, sizeof(SharedFlags)));
  }
}

/// @brief 作用域内设置/清理运维开关，避免污染同进程内的其他用例。
class DisableAsymmetricFenceScope {
 public:
  explicit DisableAsymmetricFenceScope(bool disable) : disable_(disable) {
    if (disable_) {
      static_cast<void>(::setenv("SALIAS_DISABLE_ASYMMETRIC_FENCE", "1", 1));
    } else {
      static_cast<void>(::unsetenv("SALIAS_DISABLE_ASYMMETRIC_FENCE"));
    }
  }

  ~DisableAsymmetricFenceScope() { static_cast<void>(::unsetenv("SALIAS_DISABLE_ASYMMETRIC_FENCE")); }

  DisableAsymmetricFenceScope(const DisableAsymmetricFenceScope&) = delete;
  DisableAsymmetricFenceScope& operator=(const DisableAsymmetricFenceScope&) = delete;

 private:
  bool disable_;
};

/// @brief 读取控制段中 claim_fence_protocol 字段所在的原始字节（白盒检查）。
/// @details 这里刻意复制建表端的命名规则（/salias-<FNV-1a 16 位十六进制>-c），
///          以便在不扩展公共 API 的前提下验证协议字段确实写入了共享控制段。
std::string control_shm_name(std::string_view name) {
  constexpr std::uint64_t offset_basis = 14695981039346656037ull;
  constexpr std::uint64_t prime = 1099511628211ull;
  std::uint64_t hash = offset_basis;
  for (const unsigned char character : name) {
    hash ^= character;
    hash *= prime;
  }
  constexpr std::array<char, 16> hex_digits = {'0', '1', '2', '3', '4', '5', '6', '7',
                                               '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::array<char, 16> encoded{};
  for (std::size_t index = encoded.size(); index != 0; --index) {
    encoded[index - 1] = hex_digits[hash & 0x0fu];
    hash >>= 4;
  }
  return "/salias-" + std::string(encoded.data(), encoded.size()) + "-c";
}

/// @brief 控制段头部字段的字节偏移（与 NamedControl 布局一致，见 channel.cpp 的 static_assert）。
constexpr std::size_t kVersionOffset = 4;
constexpr std::size_t kClaimFenceProtocolOffset = 104;

/// @brief 直接改写控制段中的单字节字段，模拟旧版本或未知协议的对端。
bool patch_control_byte(std::string_view channel_name, std::size_t offset, std::uint8_t value) {
  const std::string shm_name = control_shm_name(channel_name);
  const int fd = ::shm_open(shm_name.c_str(), O_RDWR, 0);
  if (fd < 0) {
    return false;
  }
  void* mapping = ::mmap(nullptr, 16u * 1024u, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  static_cast<void>(::close(fd));
  if (mapping == MAP_FAILED) {
    return false;
  }
  static_cast<std::uint8_t*>(mapping)[offset] = value;
  static_cast<void>(::munmap(mapping, 16u * 1024u));
  return true;
}

/// @brief 发布端主体：持续发布自增计数器，直到 stop 置位。
/// @return 进程退出码（0 表示未观测到异常）。
int run_producer(std::string_view name, SharedFlags* flags) {
  auto connected = salias::FifoMpscChannel::connect(name);
  if (!connected) {
    return 2;
  }
  auto publisher = connected->publisher();
  std::uint64_t counter = 0;
  while (flags->stop.load(std::memory_order_acquire) == 0) {
    const CounterMessage message{.value = counter};
    const auto payload = std::as_bytes(std::span<const CounterMessage>(&message, 1));
    auto offered = publisher.offer(payload);
    if (!offered) {
      if (offered.error() != salias::Error::BackPressured) {
        return 3;
      }
      std::this_thread::yield();
      continue;
    }
    if (!offered.value()) {
      std::this_thread::yield();
      continue;
    }
    ++counter;
  }
  return 0;
}

/// @brief 订阅端主体：循环 acquire/release，逐个会话校验序号连续性。
/// @return 进程退出码（0 表示全部会话通过）。
int run_consumer(std::string_view name, SharedFlags* flags, std::uint32_t cycles) {
  auto connected = salias::FifoMpscChannel::connect(name);
  if (!connected) {
    return 2;
  }
  for (std::uint32_t cycle = 0; cycle < cycles; ++cycle) {
    auto created = connected->try_subscriber();
    if (!created) {
      return 3;
    }
    {
      auto subscriber = std::move(*created);
      const auto deadline = std::chrono::steady_clock::now() + kSessionTimeout;
      std::uint64_t expected = 0;
      std::uint32_t received = 0;
      while (received < kMessagesPerSession) {
        auto message = subscriber.try_recv_copy<CounterMessage>();
        if (!message) {
          return 4;
        }
        if (!message->has_value()) {
          if (std::chrono::steady_clock::now() >= deadline) {
            return 5;
          }
          std::this_thread::yield();
          continue;
        }
        const std::uint64_t value = (**message).value;
        if (received == 0) {
          expected = value + 1;
        } else if (value != expected) {
          // 序号必须逐条 +1：出现缺号或重号说明注册快照与 claim 握手失效。
          return 6;
        } else {
          ++expected;
        }
        ++received;
      }
    }
    // subscriber 已析构：本次会执行 release_consumer（含 heavy fence）。
  }
  static_cast<void>(flags);
  return 0;
}

}  // namespace

/// @brief 跨进程 claim/注册压力：asymmetric 与强制 seq_cst 两种协议各 10,000 次注册。
class AsymmetricFenceStressTest : public ::testing::TestWithParam<bool> {};

TEST_P(AsymmetricFenceStressTest, AcquireReleaseKeepsSequenceContinuity) {
  const bool force_seq_cst = GetParam();
  if (!force_seq_cst && !salias::platform::fence::global_expedited_supported()) {
    GTEST_SKIP() << "membarrier GLOBAL_EXPEDITED unsupported on this kernel";
  }
  DisableAsymmetricFenceScope scope(force_seq_cst);

  const std::string name = unique_name(force_seq_cst ? "stress-seqcst" : "stress-asym");
  salias::Config config;
  config.name = name;
  config.capacity = kStressCapacity;
  config.num_producers = 1;
  config.num_consumers = 1;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created.has_value()) << "create failed with error " << static_cast<int>(created.error());
  // 保持 owner 句柄存活：fork 出的子进程通过同名通道 connect。
  auto channel = std::move(*created);
  static_cast<void>(channel);

  SharedFlags* flags = make_shared_flags();
  ASSERT_NE(flags, nullptr);
  struct FlagCleanup {
    SharedFlags* flags;
    ~FlagCleanup() { destroy_shared_flags(flags); }
  } cleanup{flags};

  const pid_t producer = ::fork();
  ASSERT_NE(producer, -1);
  if (producer == 0) {
    std::_Exit(run_producer(name, flags));
  }

  const pid_t consumer = ::fork();
  ASSERT_NE(consumer, -1);
  if (consumer == 0) {
    std::_Exit(run_consumer(name, flags, kAcquireReleaseCycles));
  }

  int consumer_status = 0;
  ASSERT_EQ(::waitpid(consumer, &consumer_status, 0), consumer);
  ASSERT_TRUE(WIFEXITED(consumer_status)) << "consumer terminated abnormally";
  EXPECT_EQ(WEXITSTATUS(consumer_status), 0)
      << "consumer failed in cycle validation (exit code " << WEXITSTATUS(consumer_status) << ")";

  flags->stop.store(1, std::memory_order_release);
  int producer_status = 0;
  ASSERT_EQ(::waitpid(producer, &producer_status, 0), producer);
  ASSERT_TRUE(WIFEXITED(producer_status)) << "producer terminated abnormally";
  EXPECT_EQ(WEXITSTATUS(producer_status), 0);
}

INSTANTIATE_TEST_SUITE_P(Protocols, AsymmetricFenceStressTest, ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                           return info.param ? std::string("ForcedSeqCst")
                                             : std::string("Asymmetric");
                         });

/// @brief fork 后子进程重新创建 Publisher 仍可发布，且 membarrier 注册状态随 mm 继承。
TEST(AsymmetricFenceForkTest, ChildPublisherAfterFork) {
  if (!salias::platform::fence::global_expedited_supported()) {
    GTEST_SKIP() << "membarrier GLOBAL_EXPEDITED unsupported on this kernel";
  }
  DisableAsymmetricFenceScope scope(false);

  const std::string name = unique_name("fork");
  salias::Config config;
  config.name = name;
  config.capacity = kStressCapacity;
  config.num_producers = 2;  // 父进程先占用 producer 0 触发注册，子进程使用 producer 1。
  config.num_consumers = 1;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created.has_value());
  auto& channel = *created;

  // 父进程先创建 Publisher：把本进程注册为 GLOBAL_EXPEDITED 接收方。
  auto parent_publisher = channel.publisher();
  ASSERT_TRUE(salias::platform::fence::publisher_light_path_ready());

  SharedFlags* flags = make_shared_flags();
  ASSERT_NE(flags, nullptr);
  struct FlagCleanup {
    SharedFlags* flags;
    ~FlagCleanup() { destroy_shared_flags(flags); }
  } cleanup{flags};

  auto subscribed = channel.try_subscriber();
  ASSERT_TRUE(subscribed.has_value()) << "subscribe failed with error "
                                      << static_cast<int>(subscribed.error());
  auto subscriber = std::move(*subscribed);

  constexpr std::uint32_t kMessages = 64;
  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
#if defined(__linux__)
    // 子进程继承父进程的 mm，因此注册状态也随之继承；GET_REGISTRATIONS 必须仍报告已注册。
    // GET_REGISTRATIONS 报告的是“已下发的注册命令”位：GLOBAL_EXPEDITED 注册成功后
    // 对应位是 MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED（本机实测为 4）。
    const bool inherited =
        (salias::platform::fence::registered_commands() &
         static_cast<std::uint32_t>(MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED)) != 0;
    if (!inherited) {
      std::_Exit(7);
    }
#endif
    while (flags->go.load(std::memory_order_acquire) == 0) {
      std::this_thread::yield();
    }
    auto publisher = channel.publisher();
    for (std::uint32_t index = 0; index < kMessages; ++index) {
      const CounterMessage message{.value = index};
      const auto payload = std::as_bytes(std::span<const CounterMessage>(&message, 1));
      for (;;) {
        auto offered = publisher.offer(payload);
        if (offered && offered.value()) {
          break;
        }
        if (offered && !offered.value()) {
          std::this_thread::yield();
          continue;
        }
        std::_Exit(3);
      }
    }
    std::_Exit(0);
  }

  flags->go.store(1, std::memory_order_release);
  std::uint32_t received = 0;
  const auto deadline = std::chrono::steady_clock::now() + kSessionTimeout;
  while (received < kMessages && std::chrono::steady_clock::now() < deadline) {
    auto message = subscriber.try_recv_copy<CounterMessage>();
    ASSERT_TRUE(message.has_value()) << "recv failed with error " << static_cast<int>(message.error());
    if (!message->has_value()) {
      std::this_thread::yield();
      continue;
    }
    EXPECT_EQ((**message).value, received);
    ++received;
  }
  EXPECT_EQ(received, kMessages);
  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0) << "child exited with code " << WEXITSTATUS(status);
}

/// @brief heavy fence 失败时 acquire_consumer 必须报错，且不影响已发布消息的完整性。
TEST(AsymmetricFenceFaultTest, HeavyFenceFailureFailsClosed) {
  if (!salias::platform::fence::global_expedited_supported()) {
    GTEST_SKIP() << "membarrier GLOBAL_EXPEDITED unsupported on this kernel";
  }
  DisableAsymmetricFenceScope scope(false);

  salias::Config config;
  config.name = unique_name("heavy-fail");
  config.capacity = kStressCapacity;
  config.num_producers = 1;
  config.num_consumers = 1;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created.has_value());
  auto& channel = *created;

  auto publisher = channel.publisher();

  salias::platform::fence::set_heavy_fence_failure_for_testing(true);
  auto failed = channel.try_subscriber();
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error(), salias::Error::PlatformFail);
  salias::platform::fence::clear_failure_injection_for_testing();

  // 故障注入期间没有产生任何半初始化槽位：清空后订阅成功，且消息内容完整。
  auto subscribed = channel.try_subscriber();
  ASSERT_TRUE(subscribed.has_value()) << "subscribe failed with error "
                                      << static_cast<int>(subscribed.error());
  auto subscriber = std::move(*subscribed);

  constexpr std::uint32_t kMessages = 32;
  for (std::uint32_t index = 0; index < kMessages; ++index) {
    const CounterMessage message{.value = 0x1000u + index};
    const auto payload = std::as_bytes(std::span<const CounterMessage>(&message, 1));
    auto offered = publisher.offer(payload);
    ASSERT_TRUE(offered.has_value());
    ASSERT_TRUE(offered.value());
  }
  for (std::uint32_t index = 0; index < kMessages; ++index) {
    auto message = subscriber.try_recv_copy<CounterMessage>();
    ASSERT_TRUE(message.has_value());
    ASSERT_TRUE(message->has_value());
    EXPECT_EQ((**message).value, 0x1000u + index);
  }
}

/// @brief 发布端注册失败时退回 seq_cst 路径，仍必须能正常发布与接收。
TEST(AsymmetricFenceFaultTest, RegistrationFailureFallsBackToSeqCst) {
  if (!salias::platform::fence::global_expedited_supported()) {
    GTEST_SKIP() << "membarrier GLOBAL_EXPEDITED unsupported on this kernel";
  }
  DisableAsymmetricFenceScope scope(false);

  salias::Config config;
  config.name = unique_name("register-fail");
  config.capacity = kStressCapacity;
  config.num_producers = 1;
  config.num_consumers = 1;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created.has_value());
  auto& channel = *created;

  // 先把用户态状态机复位，才能在同一进程内重复观测“注册失败→回退”的结果；
  // 内核里本 mm 的注册位不受影响，后续用例重新注册仍会幂等成功。
  salias::platform::fence::reset_registration_state_for_testing();
  salias::platform::fence::set_registration_failure_for_testing(true);
  auto publisher = channel.publisher();
  salias::platform::fence::clear_failure_injection_for_testing();
  // 本进程回退到 seq_cst，未注册为接收方。
  EXPECT_FALSE(salias::platform::fence::publisher_light_path_ready());

  auto subscribed = channel.try_subscriber();
  ASSERT_TRUE(subscribed.has_value());
  auto subscriber = std::move(*subscribed);

  constexpr std::uint32_t kMessages = 16;
  for (std::uint32_t index = 0; index < kMessages; ++index) {
    const CounterMessage message{.value = 0x2000u + index};
    const auto payload = std::as_bytes(std::span<const CounterMessage>(&message, 1));
    auto offered = publisher.offer(payload);
    ASSERT_TRUE(offered.has_value());
    ASSERT_TRUE(offered.value());
  }
  for (std::uint32_t index = 0; index < kMessages; ++index) {
    auto message = subscriber.try_recv_copy<CounterMessage>();
    ASSERT_TRUE(message.has_value());
    ASSERT_TRUE(message->has_value());
    EXPECT_EQ((**message).value, 0x2000u + index);
  }
}

/// @brief 注销路径在 heavy fence 失败时 fail closed：不清理槽位，注册保持有效。
TEST(AsymmetricFenceFaultTest, ReleaseFailureKeepsRegistration) {
  if (!salias::platform::fence::global_expedited_supported()) {
    GTEST_SKIP() << "membarrier GLOBAL_EXPEDITED unsupported on this kernel";
  }
  DisableAsymmetricFenceScope scope(false);

  salias::Config config;
  config.name = unique_name("release-fail");
  config.capacity = kStressCapacity;
  config.num_producers = 1;
  config.num_consumers = 1;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created.has_value());
  auto& channel = *created;
  ASSERT_EQ(channel.connected_consumer_count(), 0u);

  {
    auto subscribed = channel.try_subscriber();
    ASSERT_TRUE(subscribed.has_value());
    EXPECT_EQ(channel.connected_consumer_count(), 1u);
    salias::platform::fence::set_heavy_fence_failure_for_testing(true);
    // 析构触发 release_consumer；heavy fence 失败后必须放弃清理。
  }
  salias::platform::fence::clear_failure_injection_for_testing();

  EXPECT_EQ(channel.connected_consumer_count(), 1u)
      << "release must fail closed instead of cleaning up without a barrier";
  auto blocked = channel.try_subscriber();
  ASSERT_FALSE(blocked.has_value());
  EXPECT_EQ(blocked.error(), salias::Error::NoSubscriberSlots);
}

/// @brief 版本与协议边界：v14 控制段、未知协议值都必须拒绝连接。
TEST(AsymmetricFenceProtocolTest, RejectsLegacyVersionAndUnknownProtocol) {
  salias::Config config;
  config.name = unique_name("protocol");
  config.capacity = 64u * 1024u;
  config.num_producers = 1;
  config.num_consumers = 1;
  auto created = salias::FifoMpscChannel::create(config);
  ASSERT_TRUE(created.has_value());

  const std::string name = config.name;
  ASSERT_TRUE(patch_control_byte(name, kVersionOffset, 14));
  auto legacy = salias::FifoMpscChannel::connect(name);
  ASSERT_FALSE(legacy.has_value());
  EXPECT_EQ(legacy.error(), salias::Error::VersionMismatch);
  ASSERT_TRUE(patch_control_byte(name, kVersionOffset, 15));

  ASSERT_TRUE(patch_control_byte(name, kClaimFenceProtocolOffset, 2));
  auto unknown = salias::FifoMpscChannel::connect(name);
  ASSERT_FALSE(unknown.has_value());
  EXPECT_EQ(unknown.error(), salias::Error::BadConfig);
  ASSERT_TRUE(patch_control_byte(name, kClaimFenceProtocolOffset, 0));

  auto restored = salias::FifoMpscChannel::connect(name);
  ASSERT_TRUE(restored.has_value());
}

/// @brief 建表端在支持的平台上默认选择 membarrier 协议，运维开关可切回 seq_cst。
TEST(AsymmetricFenceProtocolTest, ProtocolSelectionFollowsEnvironment) {
  const bool supported = salias::platform::fence::global_expedited_supported();
  {
    DisableAsymmetricFenceScope scope(true);
    EXPECT_EQ(salias::platform::fence::select_channel_protocol(),
              salias::platform::fence::ClaimFenceProtocol::kSeqCst);
  }
  {
    DisableAsymmetricFenceScope scope(false);
    EXPECT_EQ(salias::platform::fence::select_channel_protocol() ==
                  salias::platform::fence::ClaimFenceProtocol::kMembarrier,
              supported);
  }
}

/** @file tools/aeron_compare/aeron_ipc_compare.cpp
 * @brief salias 与 Aeron C++ 的 IPC latency/throughput 对照 benchmark 程序。
 * @details 本程序位于 benchmark 工具层，独立于 salias 核心分层，调用真实的 Aeron C++
 * SDK（aeron:ipc 通道）作为对照基线。它通过 fork 派生若干 producer/consumer 子进程，
 * 以共享内存（mmap + MAP_SHARED|MAP_ANONYMOUS）承载跨进程的运行状态与延迟直方图，
 * 在所有 worker 就绪后统一发令开始计时，跑完固定消息数后汇总发布/投递速率与延迟百分位。
 * 支持 SPSC/MPSC/SPMC/MPMC 四种场景、CPU 亲和性绑定（pin）、term-length 调优与
 * 可配置的延迟采样间隔，结果以机器可读的 key=value 形式输出到 stdout。
 */
#include <sched.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "Aeron.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "concurrent/logbuffer/BufferClaim.h"
#include "latency_histogram.hpp"

namespace {
/// 匿名命名空间，集中放置本 benchmark 程序的内部类型与工具函数，限制文件内可见。

/// 工作进程（producer + consumer）数量上限，决定共享状态数组的定长容量。
inline constexpr std::uint32_t kMaxWorkers = 64;
namespace latency = salias::tools::latency;

/// 命令行解析结果：描述一次 Aeron IPC 对照测试的全部可调参数与场景。
struct Options {
  std::string scenario = "spsc";  ///< 场景名：spsc / mpsc / spmc / mpmc。
  std::string dir;                ///< Aeron media driver 的工作目录（aeronDir）。
  std::uint64_t messages = 1'000'000;  ///< 每个 producer 要发布的消息总数。
  std::uint32_t producers = 1;  ///< producer 进程数（由场景默认值填充）。
  std::uint32_t consumers = 1;  ///< consumer 进程数（由场景默认值填充）。
  std::int32_t stream_id = 1001;  ///< Aeron stream id。
  std::int32_t payload = 64;      ///< 单条消息 payload 字节数。
  std::int32_t fragment_limit = 64;  ///< consumer 每次 poll 的 fragment 上限（batch）。
  std::int32_t term_length = 0;  ///< Aeron term 长度，<=0 表示用默认值。
  int cpu_base = -1;  ///< CPU 亲和性起始核号，<0 表示不绑定。
  std::uint32_t cpu_stride = 1;  ///< 相邻 worker 之间的 CPU 核号步长。
  std::uint64_t latency_sample_rate = 0;  ///< 延迟采样间隔，0 表示不采样。
  bool producers_set = false;  ///< 标记 producers 是否由用户显式指定（影响场景默认值）。
  bool consumers_set = false;  ///< 标记 consumers 是否由用户显式指定（影响场景默认值）。
};

/**
 * @brief 跨进程共享的运行状态，承载就绪/发令/失败标志与每个 worker 的统计结果。
 * @details 整块以 mmap 映射为 MAP_SHARED|MAP_ANONYMOUS，供 fork 出的多个子进程共享。
 * ready/start/failed 用 acquire/release 内存序做轻量跨进程同步；consumed 为每个
 * consumer 的投递计数。latency_buckets 用 alignas(64) 填充到独立 cache line，避免
 * 多 consumer 并发累加时产生伪共享（false sharing）。
 */
struct SharedRunState {
  std::atomic<std::uint32_t> ready{0};  ///< 已就绪的 worker 计数，加到总数后父进程发令。
  std::atomic<std::uint32_t> start{0};  ///< 发令标志，置 1 后所有 worker 同时开始收发。
  std::atomic<std::uint32_t> failed{0};  ///< 全局失败标志，任一 worker 异常即置 1。
  std::array<std::atomic<std::uint64_t>, kMaxWorkers> consumed{};  ///< 各 consumer 投递计数。
  std::array<std::uint64_t, kMaxWorkers> latency_samples{};  ///< 各 consumer 延迟采样数。
  std::array<std::uint64_t, kMaxWorkers> latency_max_ns{};  ///< 各 consumer 观测到的最大延迟。
  alignas(64)  // 按 cache line 对齐，避免多 consumer 并发写相邻桶时伪共享。
      std::array<std::array<std::uint64_t, latency::kBucketCount>, kMaxWorkers> latency_buckets{};
};

/**
 * @brief 解析 Aeron IPC 对比工具的命令行参数。
 * @param argc 参数个数。
 * @param argv 参数数组。
 * @return 填充好的 Options；缺少必填项或参数非法时直接 std::exit 终止进程。
 */
// 解析 Aeron IPC 对比工具的命令行参数。
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
    } else if (arg == "--dir") {
      options.dir = require_value("--dir");
    } else if (arg == "--messages") {
      options.messages = std::stoull(require_value("--messages"));
    } else if (arg == "--producers") {
      options.producers = static_cast<std::uint32_t>(std::stoul(require_value("--producers")));
      options.producers_set = true;
    } else if (arg == "--consumers") {
      options.consumers = static_cast<std::uint32_t>(std::stoul(require_value("--consumers")));
      options.consumers_set = true;
    } else if (arg == "--payload") {
      options.payload = static_cast<std::int32_t>(std::stol(require_value("--payload")));
    } else if (arg == "--stream") {
      options.stream_id = static_cast<std::int32_t>(std::stol(require_value("--stream")));
    } else if (arg == "--fragment-limit") {
      options.fragment_limit =
          static_cast<std::int32_t>(std::stol(require_value("--fragment-limit")));
    } else if (arg == "--term-length") {
      options.term_length = static_cast<std::int32_t>(std::stol(require_value("--term-length")));
    } else if (arg == "--cpu-base") {
      options.cpu_base = std::stoi(require_value("--cpu-base"));
    } else if (arg == "--cpu-stride") {
      options.cpu_stride = static_cast<std::uint32_t>(std::stoul(require_value("--cpu-stride")));
    } else if (arg == "--latency-sample-rate") {
      options.latency_sample_rate = std::stoull(require_value("--latency-sample-rate"));
    } else if (arg == "--help") {
      std::cout << "usage: aeron_ipc_compare --dir DIR --scenario spsc|mpsc|spmc|mpmc "
                   "[--messages N] [--producers N] [--consumers N] [--payload N] "
                   "[--fragment-limit N] [--term-length N] [--cpu-base N] [--cpu-stride N] "
                   "[--latency-sample-rate N]\n";
      std::exit(0);
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(2);
    }
  }

  if (options.dir.empty()) {
    std::cerr << "--dir is required\n";
    std::exit(2);
  }
  return options;
}

/// @brief 判断是否启用了 CPU 亲和性绑定（cpu_base >= 0）。
bool pinning_enabled(const Options& options) noexcept { return options.cpu_base >= 0; }

/**
 * @brief 计算指定 worker 应绑定的 CPU 核号。
 * @param options 含 cpu_base 与 cpu_stride 的配置。
 * @param worker_index worker 在全局序中的下标。
 * @return 目标 CPU 核号 = cpu_base + worker_index * cpu_stride。
 */
int worker_cpu(const Options& options, std::uint32_t worker_index) noexcept {
  return options.cpu_base + static_cast<int>(worker_index * options.cpu_stride);
}

/**
 * @brief 若启用 CPU 绑定，则把当前进程/线程固定到对应核上。
 * @param options 含 cpu_base/cpu_stride 的配置。
 * @param worker_index worker 下标。
 * @throw std::runtime_error 当 sched_setaffinity 失败时抛出。
 */
void pin_worker_if_requested(const Options& options, std::uint32_t worker_index) {
  if (!pinning_enabled(options)) {
    return;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(worker_cpu(options, worker_index), &set);
  if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
    throw std::runtime_error("failed to pin Aeron IPC worker");
  }
}

/**
 * @brief 根据配置拼装 Aeron IPC 通道 URI。
 * @param options 含 term_length 的配置。
 * @return "aeron:ipc" 或附加 term-length 参数的变体。
 */
std::string ipc_channel(const Options& options) {
  if (options.term_length <= 0) {
    return "aeron:ipc";
  }
  return "aeron:ipc?term-length=" + std::to_string(options.term_length);
}

/**
 * @brief 用 mmap 创建一块 MAP_SHARED|MAP_ANONYMOUS 共享内存并就地构造 SharedRunState。
 * @return 引用指向映射区首地址的 SharedRunState。
 * @throw std::runtime_error 当 mmap 失败（返回 MAP_FAILED）时抛出。
 * @details 选择匿名共享映射是为了在 fork 后父子进程天然共享同一块物理页，无需命名文件。
 */
SharedRunState& create_shared_state() {
  void* mapping = ::mmap(nullptr, sizeof(SharedRunState), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    throw std::runtime_error("failed to mmap shared run state");
  }
  return *new (mapping) SharedRunState();
}

/**
 * @brief 与 create_shared_state 配对的清理：先析构再 munmap。
 * @param state 之前由 create_shared_state 返回的引用。
 * @note 仅在父进程退出路径调用；子进程通过 _exit 退出，不执行析构。
 */
void destroy_shared_state(SharedRunState& state) noexcept {
  state.~SharedRunState();
  static_cast<void>(::munmap(&state, sizeof(SharedRunState)));
}

/**
 * @brief 标记全局失败，通知所有 worker 提前退出。
 * @details 用 release 语义发布，保证其它 worker 的 acquire 读能看到。
 */
void mark_failed(SharedRunState& state) noexcept {
  state.failed.store(1, std::memory_order_release);
}

/**
 * @brief 查询是否已有任一 worker 标记失败。
 * @details 用 acquire 读，与 mark_failed 的 release 配对，确保可见性。
 */
bool has_failed(SharedRunState& state) noexcept {
  return state.failed.load(std::memory_order_acquire) != 0;
}

/**
 * @brief worker 侧的就绪+等待发令原语：自增 ready 计数后自旋等待 start 置位。
 * @param state 共享运行状态。
 * @retval true 收到开始信号，可以进入正式收发循环。
 * @retval false 期间检测到失败，应立即退出。
 * @details release 发布 ready 自增，让父进程的 acquire 读确认全部就绪；
 * 反向 acquire 读 start，与父进程 release 写 start 配对，保证同时起跑。
 */
bool ready_and_wait_for_start(SharedRunState& state) noexcept {
  state.ready.fetch_add(1, std::memory_order_release);
  while (state.start.load(std::memory_order_acquire) == 0) {
    if (has_failed(state)) {
      return false;
    }
    std::this_thread::yield();  // 自旋间隙让出 CPU，避免空转占用核心。
  }
  return true;
}

/**
 * @brief 在 10s 超时内轮询获取异步 addSubscription 返回的 Subscription 句柄。
 * @param aeron Aeron 客户端实例。
 * @param id addSubscription 返回的注册 id。
 * @param state 共享状态，期间检测到失败则提前返回。
 * @return 获取到的 Subscription；超时或失败返回 nullptr。
 */
std::shared_ptr<aeron::Subscription> find_subscription(aeron::Aeron& aeron, std::int64_t id,
                                                       SharedRunState& state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (has_failed(state)) {
      return nullptr;
    }
    auto subscription = aeron.findSubscription(id);
    if (subscription) {
      return subscription;
    }
    std::this_thread::yield();
  }
  return nullptr;
}

/**
 * @brief 在 10s 超时内轮询获取异步 addExclusivePublication 返回的 Publication 句柄。
 * @param aeron Aeron 客户端实例。
 * @param id addExclusivePublication 返回的注册 id。
 * @param state 共享状态，期间检测到失败则提前返回。
 * @return 获取到的 ExclusivePublication；超时或失败返回 nullptr。
 */
std::shared_ptr<aeron::ExclusivePublication> find_publication(aeron::Aeron& aeron, std::int64_t id,
                                                              SharedRunState& state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (has_failed(state)) {
      return nullptr;
    }
    auto publication = aeron.findExclusivePublication(id);
    if (publication) {
      return publication;
    }
    std::this_thread::yield();
  }
  return nullptr;
}

/**
 * @brief 等待 Subscription 连接并累积到指定数量的 image（对应 producer 个数）。
 * @param subscription 订阅句柄。
 * @param expected_images 期望连接的 image 数（通常等于 producer 数）。
 * @param state 共享状态，期间检测到失败则提前返回。
 * @retval true 已连上且 image 数达标。
 * @retval false 超时或检测到失败。
 */
bool wait_subscription_connected(const std::shared_ptr<aeron::Subscription>& subscription,
                                 std::uint32_t expected_images, SharedRunState& state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (has_failed(state)) {
      return false;
    }
    if (subscription && subscription->isConnected() &&
        subscription->imageCount() >= static_cast<int>(expected_images)) {
      return true;
    }
    std::this_thread::yield();
  }
  return false;
}

/**
 * @brief 等待 ExclusivePublication 连接就绪。
 * @param publication 发布句柄。
 * @param state 共享状态，期间检测到失败则提前返回。
 * @retval true 已连接。
 * @retval false 超时或检测到失败。
 */
bool wait_publication_connected(const std::shared_ptr<aeron::ExclusivePublication>& publication,
                                SharedRunState& state) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < deadline) {
    if (has_failed(state)) {
      return false;
    }
    if (publication && publication->isConnected()) {
      return true;
    }
    std::this_thread::yield();
  }
  return false;
}

/**
 * @brief 一次 benchmark 场景的汇总结果，含吞吐与延迟统计。
 * @details LatencySummary 描述延迟分布的关键百分位（p50/p99/p999）与最大值；
 * 外层 Result 记录 producer/consumer 数、发布与投递总数、耗时（秒）、各 consumer
 * 的延迟摘要以及全体消费者聚合后的延迟摘要，供 print_result 机器可读输出。
 */
struct Result {
  /// 单个 consumer（或聚合）的延迟摘要。
  struct LatencySummary {
    std::uint64_t samples = 0;  ///< 采样数。
    std::uint64_t p50_ns = 0;   ///< 中位数延迟（纳秒）。
    std::uint64_t p99_ns = 0;   ///< p99 延迟（纳秒）。
    std::uint64_t p999_ns = 0;  ///< p99.9 延迟（纳秒）。
    std::uint64_t max_ns = 0;   ///< 观测到的最大延迟（纳秒）。
  };

  std::uint32_t producers = 0;  ///< 本次场景的 producer 进程数。
  std::uint32_t consumers = 0;  ///< 本次场景的 consumer 进程数。
  std::uint64_t published = 0;  ///< 总发布消息数 = producers * messages。
  std::uint64_t delivered = 0;  ///< 总投递消息数（应为 producers*messages*consumers 投递口径）。
  std::int32_t payload = 0;     ///< 单条消息 payload 字节数。
  double seconds = 0.0;         ///< 正式收发阶段总耗时（秒）。
  std::vector<LatencySummary> consumer_latency;  ///< 各 consumer 的延迟摘要。
  LatencySummary aggregate_latency;  ///< 全体 consumer 聚合后的延迟摘要。
};

/**
 * @brief 由直方图桶数据汇总出延迟摘要（p50/p99/p999/max）。
 * @param buckets 已采样的桶数组。
 * @param samples 样本总数。
 * @param max_ns 观测到的最大延迟。
 * @return 填充好的 LatencySummary。
 */
Result::LatencySummary summarize_latency(const std::uint64_t* buckets, std::uint64_t samples,
                                         std::uint64_t max_ns) noexcept {
  return Result::LatencySummary{
      samples,
      latency::percentile(buckets, samples, 50, 100),
      latency::percentile(buckets, samples, 99, 100),
      latency::percentile(buckets, samples, 999, 1000),
      max_ns,
  };
}

/**
 * @brief consumer 子进程主函数：订阅 channel、消费固定消息数、记录延迟。
 * @param options 全局配置。
 * @param consumer_index 本 consumer 在共享状态数组中的下标。
 * @param state 跨进程共享运行状态。
 * @note [[noreturn]]：函数内以 _exit 退出，永不返回父进程。
 * @details 流程：绑定 CPU -> 创建 Aeron 客户端 -> 订阅 -> 等连接 -> 等发令 ->
 * 自旋 poll 直至消费满 expected 条消息；期间按采样率记录延迟。所有异常路径都
 * mark_failed 并 _exit，保证父进程能检测到失败。
 */
[[noreturn]] void run_consumer_child(const Options& options, std::uint32_t consumer_index,
                                     SharedRunState& state) noexcept {
  try {
    pin_worker_if_requested(options, consumer_index);
    aeron::Context context;
    context.aeronDir(options.dir);
    aeron::Aeron aeron(context);

    const std::string channel = ipc_channel(options);
    const auto id = aeron.addSubscription(channel, options.stream_id);
    auto subscription = find_subscription(aeron, id, state);
    if (!subscription || !wait_subscription_connected(subscription, options.producers, state)) {
      mark_failed(state);
      _exit(20);  // 退出码 20：订阅创建/连接失败。
    }
    if (!ready_and_wait_for_start(state)) {
      _exit(21);  // 退出码 21：等待发令期间检测到失败。
    }

    // 每个 consumer 需消费的总量 = producer 数 × 每 producer 消息数。
    const std::uint64_t expected = static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t consumed = 0;
    std::uint64_t sample_count = 0;
    std::uint64_t max_latency_ns = 0;
    auto* const latency_buckets = state.latency_buckets[consumer_index].data();
    aeron::concurrent::BusySpinIdleStrategy idle;
    // poll 回调：每收到一个 fragment 累计计数；开启采样时读取发送时间戳计算单向延迟。
    auto handler = [&](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                       aeron::util::index_t length, aeron::Header&) {
      ++consumed;
      // 未开启采样或 payload 不足 8 字节（放不下时间戳）则只计数不采样。
      if (options.latency_sample_rate == 0 || length < static_cast<aeron::util::index_t>(8)) {
        return;
      }
      const std::uint64_t send_time_ns = static_cast<std::uint64_t>(buffer.getInt64(offset));
      if (send_time_ns == 0) {  // 该帧非采样帧（producer 写入 0 表示未打时间戳）。
        return;
      }
      const std::uint64_t receive_time_ns = latency::monotonic_now_ns();
      if (receive_time_ns < send_time_ns) {  // 时钟异常保护，丢弃负延迟样本。
        return;
      }
      const std::uint64_t elapsed_ns = receive_time_ns - send_time_ns;
      latency::record(latency_buckets, elapsed_ns);
      ++sample_count;
      max_latency_ns = std::max(max_latency_ns, elapsed_ns);
    };
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (consumed < expected) {
      if (has_failed(state) || std::chrono::steady_clock::now() > deadline) {
        mark_failed(state);
        _exit(22);  // 退出码 22：消费超时或全局失败。
      }
      const int fragments = subscription->poll(handler, options.fragment_limit);
      idle.idle(fragments);  // BusySpin：无 fragment 时自旋，CPU 占满换最低延迟。
    }
    state.consumed[consumer_index].store(consumed, std::memory_order_release);  // 发布最终计数。
    state.latency_samples[consumer_index] = sample_count;
    state.latency_max_ns[consumer_index] = max_latency_ns;
    _exit(0);  // 正常退出。
  } catch (...) {
    mark_failed(state);
    _exit(23);  // 退出码 23：未预期异常。
  }
}

/**
 * @brief producer 子进程主函数：创建独占发布、发送固定消息数、可选打时间戳。
 * @param options 全局配置。
 * @param producer_index 本 producer 在全局 worker 序中的下标（消费者之后排）。
 * @param state 跨进程共享运行状态。
 * @note [[noreturn]]：函数内以 _exit 退出。
 * @details 流程：绑定 CPU（下标从 consumers 之后排起）-> 创建 Aeron 客户端 ->
 * 独占发布 -> 等连接 -> 等发令 -> 循环 tryClaim/填充/commit 发布消息。tryClaim 是
 * 零拷贝路径：先 claim 一块 buffer，写入 payload 后 commit 对消费者可见。当 channel
 * 满时 position 返回负值，自旋重试。
 */
[[noreturn]] void run_producer_child(const Options& options, std::uint32_t producer_index,
                                     SharedRunState& state) noexcept {
  try {
    pin_worker_if_requested(options, options.consumers + producer_index);
    aeron::Context context;
    context.aeronDir(options.dir);
    aeron::Aeron aeron(context);

    const std::string channel = ipc_channel(options);
    const auto id = aeron.addExclusivePublication(channel, options.stream_id);
    auto publication = find_publication(aeron, id, state);
    if (!publication || !wait_publication_connected(publication, state)) {
      mark_failed(state);
      _exit(30);  // 退出码 30：发布创建/连接失败。
    }
    if (!ready_and_wait_for_start(state)) {
      _exit(31);  // 退出码 31：等待发令期间检测到失败。
    }

    aeron::concurrent::BusySpinIdleStrategy idle;
    aeron::concurrent::logbuffer::BufferClaim claim;
    for (std::uint64_t i = 0; i < options.messages; ++i) {
      idle.reset();
      for (;;) {
        if (has_failed(state)) {
          _exit(32);  // 退出码 32：发布期间全局失败。
        }
        // tryClaim 零拷贝预留 payload 字节，成功返回正数 position，失败返回负错误码。
        const auto position = publication->tryClaim(options.payload, claim);
        if (position > 0) {
          // marker 高 16 位编码 producer_index，低 48 位编码序号 i，便于多 producer 去重。
          const std::int64_t marker =
              static_cast<std::int64_t>((static_cast<std::uint64_t>(producer_index) << 48) | i);
          const bool sampled =
              options.latency_sample_rate != 0 && i % options.latency_sample_rate == 0;
          if (options.latency_sample_rate != 0) {
            // 仅采样帧写入真实发送时间；非采样帧写 0，consumer 据此跳过。
            const std::uint64_t send_time_ns = sampled ? latency::monotonic_now_ns() : 0;
            claim.buffer().putInt64(claim.offset(), static_cast<std::int64_t>(send_time_ns));
            claim.buffer().putInt64(claim.offset() + 8, marker);
          } else {
            claim.buffer().putInt64(claim.offset(), marker);
          }
          claim.commit();  // commit 后消费者才可见，保证发布语义。
          break;
        }
        if (position == aeron::PUBLICATION_CLOSED || position == aeron::MAX_POSITION_EXCEEDED) {
          mark_failed(state);
          _exit(33);  // 退出码 33：channel 已关闭或超出最大 position。
        }
        idle.idle();  // 背压：channel 满，自旋等待消费者腾出空间。
      }
    }
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(34);  // 退出码 34：未预期异常。
  }
}

/**
 * @brief fork 一个 consumer 子进程并立即进入 run_consumer_child。
 * @return 子进程 pid（父进程视角）；子进程不会返回（run_consumer_child 标记 noreturn）。
 * @throw std::runtime_error 当 fork 失败时抛出。
 */
pid_t fork_consumer(const Options& options, std::uint32_t consumer_index, SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork consumer failed");
  }
  if (pid == 0) {
    run_consumer_child(options, consumer_index, state);
  }
  return pid;
}

/**
 * @brief fork 一个 producer 子进程并立即进入 run_producer_child。
 * @return 子进程 pid（父进程视角）；子进程不会返回。
 * @throw std::runtime_error 当 fork 失败时抛出。
 */
pid_t fork_producer(const Options& options, std::uint32_t producer_index, SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork producer failed");
  }
  if (pid == 0) {
    run_producer_child(options, producer_index, state);
  }
  return pid;
}

/**
 * @brief 按请求的生产者和消费者进程数量运行一个 Aeron IPC benchmark 场景。
 * @param options 已规整好的配置。
 * @return 汇总后的 Result（吞吐 + 延迟）。
 * @throw std::runtime_error 任一子进程失败、投递数不匹配或 waitpid 异常时抛出。
 * @details 流程：创建共享状态 -> fork 全部 consumer 与 producer -> 等所有 worker
 * 就绪 -> 记录起始时间并置 start 发令 -> waitpid 收尸全部子进程 -> 记录结束时间 ->
 * 校验每个 consumer 投递数 -> 汇总各 consumer 与聚合延迟 -> 释放共享状态。异常路径
 * 会 SIGTERM 杀掉残留子进程并回收，避免僵尸进程。
 */
// 按请求的生产者和消费者进程数量运行一个 Aeron IPC benchmark 场景。
Result run_case(const Options& options) {
  SharedRunState& state = create_shared_state();
  std::vector<pid_t> children;
  children.reserve(options.producers + options.consumers);
  try {
    // 先 fork consumer 再 fork producer，确保订阅端先就绪，降低发布端首消息丢失风险。
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      children.push_back(fork_consumer(options, consumer, state));
    }
    for (std::uint32_t producer = 0; producer < options.producers; ++producer) {
      children.push_back(fork_producer(options, producer, state));
    }

    // 等待全部 worker 就绪，期间若任一失败则中止。
    const std::uint32_t expected_ready = options.producers + options.consumers;
    while (state.ready.load(std::memory_order_acquire) != expected_ready) {
      if (has_failed(state)) {
        throw std::runtime_error("Aeron child failed before benchmark start");
      }
      std::this_thread::yield();
    }

    // 发令：记录起始时间后置 start，所有 worker 的 acquire 读会同时观察到。
    const auto begin = std::chrono::steady_clock::now();
    state.start.store(1, std::memory_order_release);

    // 收尸所有子进程；非零退出码视为失败。
    for (const pid_t child : children) {
      int status = 0;
      if (::waitpid(child, &status, 0) != child) {
        mark_failed(state);
        throw std::runtime_error("waitpid failed");
      }
      if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        mark_failed(state);
      }
    }
    const auto end = std::chrono::steady_clock::now();

    if (has_failed(state)) {
      throw std::runtime_error("Aeron IPC child failed");
    }

    // 每个 consumer 应恰好消费 producer 数 × 每 producer 消息数。
    const std::uint64_t expected_per_consumer =
        static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t delivered = 0;
    std::vector<Result::LatencySummary> consumer_latency;
    consumer_latency.reserve(options.consumers);
    std::array<std::uint64_t, latency::kBucketCount> aggregate_buckets{};
    std::uint64_t aggregate_samples = 0;
    std::uint64_t aggregate_max_ns = 0;
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      const auto value = state.consumed[consumer].load(std::memory_order_acquire);  // 读最终计数。
      if (value != expected_per_consumer) {
        throw std::runtime_error("Aeron delivery mismatch");  // 投递数不匹配，判定失败。
      }
      delivered += value;
      const std::uint64_t samples = state.latency_samples[consumer];
      const std::uint64_t max_ns = state.latency_max_ns[consumer];
      consumer_latency.push_back(
          summarize_latency(state.latency_buckets[consumer].data(), samples, max_ns));
      latency::merge(aggregate_buckets.data(), state.latency_buckets[consumer].data());  // 逐桶累加聚合。
      aggregate_samples += samples;
      aggregate_max_ns = std::max(aggregate_max_ns, max_ns);
    }

    const Result::LatencySummary aggregate_latency =
        summarize_latency(aggregate_buckets.data(), aggregate_samples, aggregate_max_ns);

    destroy_shared_state(state);
    return Result{.producers = options.producers,
                  .consumers = options.consumers,
                  .published = static_cast<std::uint64_t>(options.producers) * options.messages,
                  .delivered = delivered,
                  .payload = options.payload,
                  .seconds = std::chrono::duration<double>(end - begin).count(),
                  .consumer_latency = std::move(consumer_latency),
                  .aggregate_latency = aggregate_latency};
  } catch (...) {
    // 异常路径：标记失败、SIGTERM 杀残留子进程并回收，避免僵尸进程。
    mark_failed(state);
    for (const pid_t child : children) {
      static_cast<void>(::kill(child, SIGTERM));
      int status = 0;
      static_cast<void>(::waitpid(child, &status, 0));
    }
    destroy_shared_state(state);
    throw;
  }
}

/**
 * @brief 打印机器可读的 Aeron benchmark 结果（key=value 形式）。
 * @param options 配置，用于输出 pinning/cpu 等元信息。
 * @param scenario 场景名。
 * @param result 已汇总的吞吐与延迟结果。
 * @details 先逐 consumer 输出 LATENCY 行（各百分位），再输出一行 RESULT 汇总
 * （含 publish/delivery 速率、MiB/s 与聚合延迟百分位）。该格式便于脚本解析对比。
 */
// 打印一行机器可读的 Aeron benchmark 结果。
void print_result(const Options& options, const std::string& scenario, const Result& result) {
  const double publish_rate = static_cast<double>(result.published) / result.seconds;
  const double delivery_rate = static_cast<double>(result.delivered) / result.seconds;
  const double mib_per_second =
      delivery_rate * static_cast<double>(result.payload) / (1024.0 * 1024.0);
  for (std::size_t consumer = 0; consumer < result.consumer_latency.size(); ++consumer) {
    const auto& summary = result.consumer_latency[consumer];
    std::cout << "LATENCY library=aeron-cpp" << " scenario=" << scenario << " consumer=" << consumer
              << " samples=" << summary.samples << " p50_ns=" << summary.p50_ns
              << " p99_ns=" << summary.p99_ns << " p999_ns=" << summary.p999_ns
              << " max_ns=" << summary.max_ns << '\n';
  }
  std::cout << "RESULT library=aeron-cpp" << " scenario=" << scenario
            << " pinning=" << (pinning_enabled(options) ? "on" : "off")
            << " cpu_base=" << options.cpu_base << " cpu_stride=" << options.cpu_stride
            << " term_length=" << options.term_length << " producers=" << result.producers
            << " consumers=" << result.consumers << " payload=" << result.payload
            << " published=" << result.published << " delivered=" << result.delivered
            << " seconds=" << result.seconds << " publish_msg_per_sec=" << publish_rate
            << " delivery_msg_per_sec=" << delivery_rate
            << " delivery_mib_per_sec=" << mib_per_second
            << " latency_sample_rate=" << options.latency_sample_rate
            << " latency_samples=" << result.aggregate_latency.samples
            << " latency_p50_ns=" << result.aggregate_latency.p50_ns
            << " latency_p99_ns=" << result.aggregate_latency.p99_ns
            << " latency_p999_ns=" << result.aggregate_latency.p999_ns
            << " latency_max_ns=" << result.aggregate_latency.max_ns << '\n';
}

}  // namespace

/**
 * @brief 程序入口：解析参数、按场景填充默认 producer/consumer 数、校验并运行。
 * @param argc 参数个数。
 * @param argv 参数数组。
 * @retval 0 成功。
 * @retval 1 运行期异常（fork/waitpid/投递不匹配等）。
 * @retval 2 参数非法（未知场景、缺 --dir、数值越界等）。
 * @details 各场景的默认 producer/consumer：spsc=1/1、mpsc=4/1、spmc=1/2、mpmc=4/2；
 * 用户显式指定的值优先。运行成功后打印结果行。
 */
// 规整请求的场景，运行 benchmark，并报告失败原因。
int main(int argc, char** argv) {
  try {
    Options options = parse_options(argc, argv);
    if (options.scenario == "spsc") {
      if (!options.producers_set) {
        options.producers = 1;
      }
      if (!options.consumers_set) {
        options.consumers = 1;
      }
    } else if (options.scenario == "mpsc") {
      if (!options.producers_set) {
        options.producers = 4;
      }
      if (!options.consumers_set) {
        options.consumers = 1;
      }
    } else if (options.scenario == "spmc") {
      if (!options.producers_set) {
        options.producers = 1;
      }
      if (!options.consumers_set) {
        options.consumers = 2;
      }
    } else if (options.scenario == "mpmc") {
      if (!options.producers_set) {
        options.producers = 4;
      }
      if (!options.consumers_set) {
        options.consumers = 2;
      }
    } else {
      std::cerr << "unknown scenario: " << options.scenario << '\n';
      return 2;
    }
    if (options.producers == 0 || options.consumers == 0 ||
        options.producers + options.consumers > kMaxWorkers || options.payload <= 0 ||
        options.fragment_limit <= 0 || options.term_length < 0 || options.cpu_stride == 0) {
      std::cerr << "invalid producers/consumers/payload/fragment-limit/term-length/cpu-stride\n";
      return 2;
    }
    if (options.latency_sample_rate != 0 && options.payload < 16) {
      std::cerr << "latency sampling requires payload >= 16 bytes\n";
      return 2;
    }
    print_result(options, options.scenario, run_case(options));
  } catch (const std::exception& error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 1;
  }
  return 0;
}

/**
 * @file tools/aeron_compare/salias_ipc_compare.cpp
 * @brief salias IPC 跨进程通信延迟与吞吐基准对比工具的主程序。
 * @details 本文件位于 L7 salias 公共 API 之上，是一个独立的 benchmark 程序：
 *   通过 fork() 创建多个 producer（生产者）与 consumer（消费者）子进程，
 *   以 mmap 匿名共享内存作为跨进程的运行态/统计共享区，驱动 salias 具名
 *   channel（FifoMpsc / FifoFanout / OrderedMpsc / OrderedFanout）进行
 *   压测，并采集吞吐(msg/s)、带宽(MiB/s)与延迟分位(p50/p99/p999/max)。
 *   关键不变式：producer/consumer 子进程在就绪后必须等待父进程通过共享
 *   atomic 的 start 标志统一发令；背压(BackPressured)时 producer 自旋退避
 *   重试而非直接失败；延迟采样要求 payload 容纳 8 字节时间戳。
 */
#include <sched.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "latency_histogram.hpp"
#include "salias/salias.hpp"

/// 匿名命名空间：本文件内部使用的工具函数、类型与基准驱动逻辑，
/// 仅在本编译单元可见，避免符号外泄。
namespace {

/// 单次运行允许的最大 worker（producer+consumer）数量上限。
inline constexpr std::uint32_t kMaxWorkers = 64;
/// 1 GiB 的字节数，用作 huge1g 页场景下的默认 capacity。
inline constexpr std::size_t kHuge1GiB = std::size_t{1024} * 1024 * 1024;
/// 延迟直方图工具命名空间别名，简化调用。
namespace latency = salias::tools::latency;

/**
 * @brief 命令行参数解析后的配置集合。
 * @details 承载场景选择、进程拓扑、payload/capacity、批量大小、流控窗口、
 *   CPU 亲和性、延迟采样率等全部压测参数。由 parse_options() 填充后，
 *   传递给各 worker 子进程与 run_case_typed()。所有字段为值类型，可被 fork
 *   后的子进程通过写时复制直接继承。
 */
struct Options {
  std::string scenario = "fifo";  ///< 场景名：fifo（先入先出）或 ordered（保序）
  std::string page = "normal";            ///< 内存页类型：normal / huge2m / huge1g
  std::uint64_t messages = 200'000;       ///< 每个 producer 发布的消息总数
  std::uint32_t producers = 4;            ///< producer 数量
  std::uint32_t consumers = 1;            ///< consumer 数量
  std::size_t payload = 64;               ///< 单条消息 payload 字节数
  std::size_t capacity = 1u << 22;        ///< ring 容量（slot 数），默认 4M
  std::size_t batch_size = 1;  ///< 批量发布每批消息数，1 表示逐条 claim
  std::size_t publication_window = 0;     ///< 流控窗口大小，0 表示使用引擎默认值
  bool capacity_set = false;              ///< 标记 capacity 是否由用户显式指定
  std::uint32_t poll_limit = 64;          ///< consumer 单次 poll 最多处理的消息数
  std::string name;                       ///< 具名 channel 的唯一标识
  int cpu_base = -1;                      ///< CPU 亲和性起始核号，-1 表示不绑定
  std::uint32_t cpu_stride = 1;           ///< 相邻 worker 间的 CPU 核号间隔
  std::uint64_t latency_sample_rate = 0;  ///< 延迟采样周期，0 表示关闭采样
  bool producers_set = false;             ///< 标记 producers 是否由用户显式指定
  bool consumers_set = false;             ///< 标记 consumers 是否由用户显式指定
};

/**
 * @brief 跨进程共享的运行态与统计区，经匿名 mmap 映射在父子进程间共享。
 * @details 通过 std::atomic_ref 对其中的标志字段进行跨进程原子访问，
 *   实现就绪同步、发令、失败传播与结果回收。所有 hot 字段均以 64 字节
 *   对齐并填充（padding），确保不同字段落在不同 cache line 上，避免
 *   producer 与 consumer 之间的伪共享（false sharing）。
 *   线程/进程模型：父进程创建映射，fork 后各子进程继承同一物理页；
 *   全部访问均通过 atomic_ref + acquire/release 内存序保证可见性。
 */
struct alignas(64) SharedRunState {
  std::uint32_t ready = 0;  ///< 已就绪的 worker 计数，由各子进程 fetch_add 自增
  /// ready 的填充，使其独占一个 cache line，避免与 start 伪共享。
  std::byte ready_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::uint32_t start = 0;  ///< 发令标志：父进程在所有 worker 就绪后置 1
  /// start 的填充，独占 cache line。
  std::byte start_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::uint32_t failed = 0;  ///< 全局失败标志，任一子进程出错时置 1
  /// failed 的填充，独占 cache line。
  std::byte failed_pad[64 - sizeof(std::uint32_t)]{};
  /// 每个 consumer 实际消费的消息数（按下标索引）。
  alignas(64) std::array<std::uint64_t, kMaxWorkers> consumed{};
  /// 每个 consumer 采集到的延迟样本数。
  alignas(64) std::array<std::uint64_t, kMaxWorkers> latency_samples{};
  /// 每个 consumer 观测到的最大延迟（纳秒）。
  alignas(64) std::array<std::uint64_t, kMaxWorkers> latency_max_ns{};
  /// 每个 consumer 的延迟直方图桶数组，用于分位数计算与跨 consumer 合并。
  alignas(64)
      std::array<std::array<std::uint64_t, latency::kBucketCount>, kMaxWorkers> latency_buckets{};
};

/// 编译期断言：共享态以 64 字节对齐，保证首字段 cache line 对齐前提。
static_assert(alignof(SharedRunState) == 64);

// 解析 salias IPC 对比工具的命令行参数。
/**
 * @brief 解析命令行参数，填充并返回 Options。
 * @param argc 参数个数。
 * @param argv 参数字符串数组。
 * @return 解析得到的 Options。
 * @throw 遇到未知参数、缺失值或非法取值时，输出错误并以 exit(2) 终止，
 *   不抛 C++ 异常（直接 std::exit）。
 */
Options parse_options(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    /// 取出当前参数的下一个值，缺失则报错并退出。
    auto require_value = [&](const char* name) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << name << '\n';
        std::exit(2);
      }
      return argv[++i];
    };

    if (arg == "--scenario") {
      options.scenario = require_value("--scenario");
    } else if (arg == "--messages") {
      options.messages = std::stoull(require_value("--messages"));
    } else if (arg == "--producers") {
      options.producers = static_cast<std::uint32_t>(std::stoul(require_value("--producers")));
      options.producers_set = true;
    } else if (arg == "--consumers") {
      options.consumers = static_cast<std::uint32_t>(std::stoul(require_value("--consumers")));
      options.consumers_set = true;
    } else if (arg == "--payload") {
      options.payload = static_cast<std::size_t>(std::stoull(require_value("--payload")));
    } else if (arg == "--capacity") {
      options.capacity = static_cast<std::size_t>(std::stoull(require_value("--capacity")));
      options.capacity_set = true;
    } else if (arg == "--batch-size") {
      options.batch_size = static_cast<std::size_t>(std::stoull(require_value("--batch-size")));
    } else if (arg == "--publication-window") {
      options.publication_window =
          static_cast<std::size_t>(std::stoull(require_value("--publication-window")));
    } else if (arg == "--poll-limit") {
      options.poll_limit = static_cast<std::uint32_t>(std::stoul(require_value("--poll-limit")));
    } else if (arg == "--page") {
      options.page = require_value("--page");
    } else if (arg == "--name") {
      options.name = require_value("--name");
    } else if (arg == "--cpu-base") {
      options.cpu_base = std::stoi(require_value("--cpu-base"));
    } else if (arg == "--cpu-stride") {
      options.cpu_stride = static_cast<std::uint32_t>(std::stoul(require_value("--cpu-stride")));
    } else if (arg == "--latency-sample-rate") {
      options.latency_sample_rate = std::stoull(require_value("--latency-sample-rate"));
    } else if (arg == "--help") {
      std::cout << "usage: salias_ipc_compare --scenario fifo|ordered "
                   "[--messages N] [--producers N] [--consumers N] [--payload N] "
                   "[--capacity N] [--batch-size N] [--poll-limit N] "
                   "[--publication-window N] "
                   "[--page normal|huge2m|huge1g] "
                   "[--name NAME] [--cpu-base N] [--cpu-stride N] "
                   "[--latency-sample-rate N]\n";
      std::exit(0);
    } else {
      std::cerr << "unknown argument: " << arg << '\n';
      std::exit(2);
    }
  }

  if (options.scenario == "fifo" || options.scenario == "ordered") {
    if (!options.producers_set) {
      options.producers = 4;
    }
    if (!options.consumers_set) {
      options.consumers = 1;
    }
  } else {
    std::cerr << "unknown scenario: " << options.scenario << '\n';
    std::exit(2);
  }

  if (options.page != "normal" && options.page != "huge2m" && options.page != "huge1g") {
    std::cerr << "unknown page: " << options.page << '\n';
    std::exit(2);
  }
  // huge1g 页要求容量按 1 GiB 对齐，未显式指定 capacity 时取整为 1 GiB。
  if (!options.capacity_set && options.page == "huge1g") {
    options.capacity = kHuge1GiB;
  }

  if (options.producers == 0 || options.consumers == 0 ||
      options.producers + options.consumers > kMaxWorkers || options.payload == 0 ||
      options.batch_size == 0 || options.poll_limit == 0 || options.cpu_stride == 0) {
    std::cerr << "invalid producers/consumers/payload/batch-size/poll-limit/cpu-stride\n";
    std::exit(2);
  }
  // 采样需在 payload 前 8 字节写入时间戳，故要求 payload 至少 16 字节
  // （时间戳 8 字节 + marker 8 字节）。
  if (options.latency_sample_rate != 0 && options.payload < 2 * sizeof(std::uint64_t)) {
    std::cerr << "latency sampling requires payload >= 16 bytes\n";
    std::exit(2);
  }
  // 未指定 channel 名称时，用 PID+场景名生成唯一标识，避免多实例冲突。
  if (options.name.empty()) {
    options.name = "salias-ipc-" + std::to_string(::getpid()) + "-" + options.scenario;
  }
  return options;
}

/// @brief 判断是否启用了 CPU 亲和性绑定（cpu_base >= 0 即启用）。
bool pinning_enabled(const Options& options) noexcept { return options.cpu_base >= 0; }

/**
 * @brief 计算指定 worker 的目标 CPU 核号。
 * @param options 配置。
 * @param worker_index worker 在全局序列中的下标。
 * @return 目标 CPU 核号。
 */
int worker_cpu(const Options& options, std::uint32_t worker_index) noexcept {
  return options.cpu_base + static_cast<int>(worker_index * options.cpu_stride);
}

/**
 * @brief 若启用 CPU 亲和性，则将当前进程绑定到对应核。
 * @param options 配置。
 * @param worker_index worker 下标。
 * @throw std::runtime_error sched_setaffinity 失败时抛出。
 */
void pin_worker_if_requested(const Options& options, std::uint32_t worker_index) {
  if (!pinning_enabled(options)) {
    return;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(worker_cpu(options, worker_index), &set);
  if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
    throw std::runtime_error("failed to pin salias IPC worker");
  }
}

/**
 * @brief 将页类型字符串映射为 salias::HugePage 枚举。
 * @param page 页类型字符串：normal / huge2m / huge1g。
 * @return 对应的 HugePage 枚举值。
 */
salias::HugePage huge_page_for(const std::string& page) noexcept {
  if (page == "huge2m") {
    return salias::HugePage::Size2MB;
  }
  if (page == "huge1g") {
    return salias::HugePage::Size1GB;
  }
  return salias::HugePage::None;
}

/**
 * @brief 根据选项构建具名 channel 的 salias::Config。
 * @param options 已解析的选项。
 * @return 填充好的 Config，包含名称、容量、huge page、拓扑与模式。
 * @details 模式选择逻辑：consumers==1 时使用 MPSC 变体（单消费者多生产者），
 *   否则使用 Fanout 变体（多消费者扇出）；scenario 决定 fifo 或 ordered。
 */
salias::Config make_named_config(const Options& options) {
  salias::Config config;
  config.name = options.name;
  config.capacity = options.capacity;
  config.huge = huge_page_for(options.page);
  config.num_producers = options.producers;
  config.num_consumers = options.consumers;
  config.publication_window = options.publication_window;
  if (options.scenario == "fifo") {
    config.mode = options.consumers == 1 ? salias::Mode::FifoMpsc : salias::Mode::FifoFanout;
  } else {
    config.mode = options.consumers == 1 ? salias::Mode::OrderedMpsc : salias::Mode::OrderedFanout;
  }
  return config;
}

/**
 * @brief 创建匿名 mmap 共享内存并就地构造 SharedRunState。
 * @return 共享态引用，父子进程通过 fork 继承同一物理页。
 * @throw std::runtime_error mmap 失败时抛出。
 * @note 使用 MAP_SHARED | MAP_ANONYMOUS，fork 后父子进程共享同一映射页，
 *   通过 atomic_ref 实现跨进程原子同步。
 */
SharedRunState& create_shared_state() {
  void* mapping = ::mmap(nullptr, sizeof(SharedRunState), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    throw std::runtime_error("failed to mmap shared run state");
  }
  // 在映射首地址就地构造（placement new），不分配新内存。
  auto* state = new (mapping) SharedRunState();
  return *state;
}

/**
 * @brief 析构共享态并解除 mmap 映射。
 * @param state 共享态引用。
 * @note 显式调用析构函数释放非平凡成员资源，再 munmap 归还映射页。
 */
void destroy_shared_state(SharedRunState& state) noexcept {
  state.~SharedRunState();
  static_cast<void>(::munmap(&state, sizeof(SharedRunState)));
}

/**
 * @brief 将全局失败标志置 1（release 语义发布），通知其他 worker 终止。
 * @param state 共享态引用。
 */
void mark_failed(SharedRunState& state) noexcept {
  // release：确保本进程之前的写入对其他读取 failed 的进程可见。
  std::atomic_ref<std::uint32_t>(state.failed).store(1, std::memory_order_release);
}

/**
 * @brief 读取全局失败标志（acquire 语义获取）。
 * @param state 共享态引用。
 * @return 是否已有任一 worker 标记失败。
 */
bool has_failed(SharedRunState& state) noexcept {
  // acquire：与 mark_failed 的 release 配对，看到 failed==1 即看到其之前的写入。
  return std::atomic_ref<std::uint32_t>(state.failed).load(std::memory_order_acquire) != 0;
}

/**
 * @brief 判断给定序列号是否应进行延迟采样。
 * @param options 配置（含采样周期）。
 * @param sequence 当前消息的全局序列号。
 * @return 采样开启且 sequence 为采样周期的整数倍时返回 true。
 */
bool should_sample(const Options& options, std::uint64_t sequence) noexcept {
  return options.latency_sample_rate != 0 && sequence % options.latency_sample_rate == 0;
}

/**
 * @brief 将发送时间戳与 marker 写入 payload。
 * @param payload 目标负载 span。
 * @param marker 生产者标识与本地序列号编码的标记值。
 * @param send_time_ns 发送时刻的单调时间戳（纳秒），0 表示不采样。
 * @param latency_enabled 是否启用延迟采样（决定是否写时间戳）。
 * @details 布局：启用采样时前 8 字节为时间戳，其后为 marker；否则仅写
 *   marker。该布局必须与 consumer 端读取逻辑严格一致。
 */
void write_payload(std::span<std::byte> payload, std::uint64_t marker, std::uint64_t send_time_ns,
                   bool latency_enabled) noexcept {
  // 启用采样时 marker 偏移在时间戳之后，否则置于 payload 起始。
  const std::size_t marker_offset = latency_enabled ? sizeof(send_time_ns) : 0;
  if (latency_enabled && payload.size() >= sizeof(send_time_ns)) {
    std::memcpy(payload.data(), &send_time_ns, sizeof(send_time_ns));
  }
  if (payload.size() >= marker_offset + sizeof(marker)) {
    std::memcpy(payload.data() + marker_offset, &marker, sizeof(marker));
  }
}

/**
 * @brief 子进程就绪并自旋等待父进程发令的过程。
 * @param state 共享态引用。
 * @details 先 fetch_add 自增 ready（release），然后自旋 load start（acquire）
 *   直到父进程置 1。等待期间若检测到 failed 则提前返回。yield 让出 CPU
 *   避免空转占用核。
 */
void ready_and_wait_for_start(SharedRunState& state) noexcept {
  // release：发布本进程的 ready 自增，确保父进程 acquire 时看到。
  std::atomic_ref<std::uint32_t>(state.ready).fetch_add(1, std::memory_order_release);
  // acquire：与父进程 store(start, release) 配对，看到 start==1 即可开始压测。
  while (std::atomic_ref<std::uint32_t>(state.start).load(std::memory_order_acquire) == 0) {
    if (has_failed(state)) {
      return;
    }
    std::this_thread::yield();
  }
}

/**
 * @brief 逐条 claim-写-commit，直到该消息成功发布或检测到失败。
 * @tparam M salias 模式枚举。
 * @param publisher 生产者句柄。
 * @param payload_len payload 字节数。
 * @param marker 写入 payload 的标记值。
 * @param sequence 当前消息序列号（用于决定是否采样）。
 * @param options 全局配置。
 * @param state 共享态。
 * @return 成功 commit 返回 true；任一进程失败或非背压错误返回 false。
 * @details 遇到 BackPressured（流控背压）时不视为失败，yield 退避后重试，
 *   实现流控窗口下的自适应发布。非背压错误则标记全局失败。
 */
template <salias::Mode M>
bool claim_until_committed(salias::Publisher<M>& publisher, std::size_t payload_len,
                           std::uint64_t marker, std::uint64_t sequence, const Options& options,
                           SharedRunState& state) noexcept {
  for (;;) {
    if (has_failed(state)) {
      return false;
    }
    auto claim_result = publisher.try_claim(payload_len);
    if (claim_result) {
      auto claim = std::move(claim_result).value();
      // 仅在采样序列号上取时间戳，避免每条消息都读 TSC 带来的开销。
      const std::uint64_t send_time_ns =
          should_sample(options, sequence) ? latency::monotonic_now_ns() : 0;
      write_payload(claim.payload(), marker, send_time_ns, options.latency_sample_rate != 0);
      // commit 以 release 语义发布该消息，consumer acquire 后可见。
      claim.commit();
      return true;
    }
    // 背压：consumer 尚未消费足够位置，yield 退避后重试。
    if (claim_result.error() != salias::Error::BackPressured) {
      mark_failed(state);
      return false;
    }
    std::this_thread::yield();
  }
}

/**
 * @brief 批量 offer_batch 发布，直到本 producer 的全部消息发布完毕或失败。
 * @tparam M salias 模式枚举。
 * @param publisher 生产者句柄。
 * @param options 全局配置。
 * @param producer_index 本 producer 下标（编码进 marker 高 16 位）。
 * @param state 共享态。
 * @return 全部消息发布成功返回 true；失败返回 false。
 * @details 批量路径预分配一整批 payload 连续缓冲，逐条填充后再一次性
 *   offer，摊薄每条消息的发布开销。尾部不足一批时取剩余消息数。
 *   背压同样 yield 退避重试。
 */
template <salias::Mode M>
bool offer_batch_until_committed(salias::Publisher<M>& publisher, const Options& options,
                                 std::uint32_t producer_index, SharedRunState& state) noexcept {
  // 预分配连续的批量 payload 缓冲与指向各消息的 span 数组。
  std::vector<std::byte> payload_storage(options.batch_size * options.payload);
  std::vector<std::span<const std::byte>> payloads(options.batch_size);
  for (std::size_t i = 0; i < options.batch_size; ++i) {
    payloads[i] =
        std::span<const std::byte>(payload_storage.data() + i * options.payload, options.payload);
  }

  std::uint64_t published = 0;
  while (published < options.messages) {
    if (has_failed(state)) {
      return false;
    }
    // 尾批可能不足 batch_size，取剩余消息数与批次大小的较小值。
    const std::size_t requested = static_cast<std::size_t>(std::min<std::uint64_t>(
        static_cast<std::uint64_t>(options.batch_size), options.messages - published));
    for (std::size_t i = 0; i < requested; ++i) {
      const std::uint64_t sequence = published + i;
      // marker 高 16 位编码 producer_index，低 48 位编码本地序列号。
      const std::uint64_t marker = (static_cast<std::uint64_t>(producer_index) << 48) | sequence;
      const std::uint64_t send_time_ns =
          should_sample(options, sequence) ? latency::monotonic_now_ns() : 0;
      write_payload(
          std::span<std::byte>(payload_storage.data() + i * options.payload, options.payload),
          marker, send_time_ns, options.latency_sample_rate != 0);
    }

    auto offered = publisher.offer_batch(
        std::span<const std::span<const std::byte>>(payloads.data(), requested));
    if (offered) {
      const std::size_t count = offered.value();
      if (count > 0) {
        published += count;
        continue;
      }
    } else if (offered.error() != salias::Error::BackPressured) {
      mark_failed(state);
      return false;
    }
    std::this_thread::yield();
  }
  return true;
}

/**
 * @brief consumer 子进程主逻辑：连接、订阅、轮询消费、采集延迟并退出。
 * @tparam M salias 模式枚举。
 * @param options 全局配置。
 * @param consumer_index 本 consumer 下标，用于写入共享态对应槽位。
 * @param state 共享态。
 * @note 标记 [[noreturn]]：本函数始终以 _exit 终止，不返回到 fork 点。
 *   延迟测量：从 payload 前 8 字节读时间戳，与接收时刻做差得单程延迟。
 *   设 60 秒超时，超时或失败均标记失败并退出。退出码区分错误来源。
 */
template <salias::Mode M>
[[noreturn]] void run_consumer_child(const Options& options, std::uint32_t consumer_index,
                                     SharedRunState& state) noexcept {
  try {
    pin_worker_if_requested(options, consumer_index);
    auto connected = salias::Channel<M>::connect(options.name);
    if (!connected) {
      mark_failed(state);
      _exit(20);
    }
    auto channel = std::move(connected).value();
    auto subscriber = channel.subscriber();
    ready_and_wait_for_start(state);
    if (has_failed(state)) {
      _exit(21);
    }

    // 本 consumer 应消费的消息总数 = producer 数 × 每 producer 消息数。
    const std::uint64_t expected = static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t consumed = 0;
    std::uint64_t sample_count = 0;
    std::uint64_t max_latency_ns = 0;
    auto* const latency_buckets = state.latency_buckets[consumer_index].data();
    /// poll 回调：计消费数，并在启用采样时从 payload 提取时间戳计算延迟。
    auto handler = [&](const salias::Message& message) noexcept {
      ++consumed;
      // 关闭采样或 payload 过短无法读取时间戳，跳过延迟统计。
      if (options.latency_sample_rate == 0 || message.payload.size() < sizeof(std::uint64_t)) {
        return;
      }
      std::uint64_t send_time_ns = 0;
      std::memcpy(&send_time_ns, message.payload.data(), sizeof(send_time_ns));
      // send_time_ns==0 表示该消息不在采样序列上，跳过。
      if (send_time_ns == 0) {
        return;
      }
      const std::uint64_t receive_time_ns = latency::monotonic_now_ns();
      // 单调时钟倒挂保护，避免无符号下溢。
      if (receive_time_ns < send_time_ns) {
        return;
      }
      const std::uint64_t elapsed_ns = receive_time_ns - send_time_ns;
      latency::record(latency_buckets, elapsed_ns);
      ++sample_count;
      max_latency_ns = std::max(max_latency_ns, elapsed_ns);
    };
    // 60 秒硬超时，防止 consumer 卡死导致基准永不结束。
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (consumed < expected) {
      if (has_failed(state) || std::chrono::steady_clock::now() > deadline) {
        mark_failed(state);
        _exit(22);
      }
      // poll_limit 限制单次 poll 处理上限，平衡延迟与吞吐。
      static_cast<void>(subscriber.poll(options.poll_limit, handler));
    }
    // 以 release 语义发布本 consumer 的消费计数，供父进程 acquire 读取核验。
    std::atomic_ref<std::uint64_t>(state.consumed[consumer_index])
        .store(consumed, std::memory_order_release);
    state.latency_samples[consumer_index] = sample_count;
    state.latency_max_ns[consumer_index] = max_latency_ns;
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(23);
  }
}

/**
 * @brief producer 子进程主逻辑：连接 channel、订阅、发令后发布全部消息。
 * @tparam M salias 模式枚举。
 * @param options 全局配置。
 * @param producer_index 本 producer 下标（worker 下标从 consumers 之后开始排）。
 * @param state 共享态。
 * @note [[noreturn]]：始终以 _exit 终止。batch_size==1 走逐条 claim 路径，
 *   否则走批量 offer_batch 路径。退出码区分错误来源。
 */
template <salias::Mode M>
[[noreturn]] void run_producer_child(const Options& options, std::uint32_t producer_index,
                                     SharedRunState& state) noexcept {
  try {
    // producer 的 worker 下标排在所有 consumer 之后，保证 CPU 亲和性不冲突。
    pin_worker_if_requested(options, options.consumers + producer_index);
    auto connected = salias::Channel<M>::connect(options.name);
    if (!connected) {
      mark_failed(state);
      _exit(30);
    }
    auto channel = std::move(connected).value();
    auto publisher = channel.publisher();
    ready_and_wait_for_start(state);
    if (has_failed(state)) {
      _exit(31);
    }

    if (options.batch_size == 1) {
      for (std::uint64_t i = 0; i < options.messages; ++i) {
        const std::uint64_t marker = (static_cast<std::uint64_t>(producer_index) << 48) | i;
        if (!claim_until_committed<M>(publisher, options.payload, marker, i, options, state)) {
          _exit(32);
        }
      }
    } else {
      if (!offer_batch_until_committed<M>(publisher, options, producer_index, state)) {
        _exit(32);
      }
    }
    _exit(0);
  } catch (...) {
    mark_failed(state);
    _exit(33);
  }
}

/**
 * @brief fork 出一个 consumer 子进程并运行其主逻辑。
 * @tparam M salias 模式枚举。
 * @param options 全局配置。
 * @param consumer_index consumer 下标。
 * @param state 共享态。
 * @return 子进程 PID（父进程视角）；子进程内不会返回。
 * @throw std::runtime_error fork 失败时抛出。
 */
template <salias::Mode M>
pid_t fork_consumer(const Options& options, std::uint32_t consumer_index, SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork consumer failed");
  }
  if (pid == 0) {
    run_consumer_child<M>(options, consumer_index, state);
  }
  return pid;
}

/**
 * @brief fork 出一个 producer 子进程并运行其主逻辑。
 * @tparam M salias 模式枚举。
 * @param options 全局配置。
 * @param producer_index producer 下标。
 * @param state 共享态。
 * @return 子进程 PID（父进程视角）；子进程内不会返回。
 * @throw std::runtime_error fork 失败时抛出。
 */
template <salias::Mode M>
pid_t fork_producer(const Options& options, std::uint32_t producer_index, SharedRunState& state) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork producer failed");
  }
  if (pid == 0) {
    run_producer_child<M>(options, producer_index, state);
  }
  return pid;
}

/**
 * @brief 单次基准运行的结果集合，包含吞吐量与延迟统计。
 * @details 由 run_case_typed() 填充后交由 print_result() 输出。LatencySummary
 *   以指定名称设计（designated initializers）构造，字段顺序无关。
 */
struct Result {
  /**
   * @brief 延迟统计摘要：分位数与最大值（纳秒）。
   */
  struct LatencySummary {
    std::uint64_t samples = 0;  ///< 采样样本数
    std::uint64_t p50_ns = 0;   ///< 中位数延迟（纳秒）
    std::uint64_t p99_ns = 0;   ///< P99 延迟（纳秒）
    std::uint64_t p999_ns = 0;  ///< P99.9 延迟（纳秒）
    std::uint64_t max_ns = 0;   ///< 观测到的最大延迟（纳秒）
  };

  std::uint32_t producers = 0;                  ///< 本次运行的 producer 数
  std::uint32_t consumers = 0;                  ///< 本次运行的 consumer 数
  std::uint64_t published = 0;  ///< 应发布消息总数（producer 数 × messages）
  std::uint64_t delivered = 0;                  ///< 实际投递消息总数
  std::size_t payload = 0;                      ///< 单条 payload 字节数
  std::size_t capacity = 0;                     ///< ring 容量
  std::uint32_t poll_limit = 0;                 ///< consumer 单次 poll 上限
  double seconds = 0.0;                         ///< 基准运行耗时（秒）
  std::vector<LatencySummary> consumer_latency; ///< 各 consumer 的延迟摘要
  LatencySummary aggregate_latency;             ///< 跨 consumer 合并后的聚合延迟摘要
};

/**
 * @brief 由直方图桶与样本数计算延迟分位数摘要。
 * @param buckets 直方图桶数组首地址。
 * @param samples 总样本数。
 * @param max_ns 观测到的最大延迟（纳秒）。
 * @return 包含 p50/p99/p999/max 的 LatencySummary。
 */
Result::LatencySummary summarize_latency(const std::uint64_t* buckets, std::uint64_t samples,
                                         std::uint64_t max_ns) noexcept {
  return Result::LatencySummary{
      .samples = samples,
      .p50_ns = latency::percentile(buckets, samples, 50, 100),
      .p99_ns = latency::percentile(buckets, samples, 99, 100),
      .p999_ns = latency::percentile(buckets, samples, 999, 1000),
      .max_ns = max_ns,
  };
}

/**
 * @brief 类型化基准运行主体：创建 channel、fork 子进程、发令、回收结果。
 * @tparam M salias 模式枚举，决定 channel 与 publisher/subscriber 的具体实现。
 * @param options 全局配置。
 * @return 本次运行的 Result（吞吐与延迟统计）。
 * @throw std::runtime_error 创建失败、子进程失败、投递量不匹配等异常。
 * @details 流程：父进程作为 channel owner 创建具名 IPC，再 fork 出全部
 *   consumer 与 producer 子进程。所有子进程就绪后，父进程记录起始时间并以
 *   release 语义写入 start=1 发令。waitpid 等待全部子进程退出后记录结束
 *   时间，回收各 consumer 的消费计数与延迟直方图。异常路径向存活子进程
 *   发 SIGTERM 并回收僵尸，最后重新抛出异常。
 */
template <salias::Mode M>
Result run_case_typed(const Options& options) {
  auto owner_result = salias::Channel<M>::create(make_named_config(options));
  if (!owner_result) {
    throw std::runtime_error("failed to create named salias IPC channel");
  }
  auto owner = std::move(owner_result).value();
  SharedRunState& state = create_shared_state();

  std::vector<pid_t> children;
  children.reserve(options.producers + options.consumers);
  try {
    // 先 fork 全部 consumer 再 fork producer，consumer 先就绪避免发布丢失。
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      children.push_back(fork_consumer<M>(options, consumer, state));
    }
    for (std::uint32_t producer = 0; producer < options.producers; ++producer) {
      children.push_back(fork_producer<M>(options, producer, state));
    }

    // 等待全部子进程就绪（ready == producer+consumer 总数），再统一发令。
    const std::uint32_t expected_ready = options.producers + options.consumers;
    while (std::atomic_ref<std::uint32_t>(state.ready).load(std::memory_order_acquire) !=
           expected_ready) {
      if (has_failed(state)) {
        throw std::runtime_error("child failed before benchmark start");
      }
      std::this_thread::yield();
    }

    // 所有子进程就绪，记录起始时刻后发令。
    const auto begin = std::chrono::steady_clock::now();
    // release：发布 start=1，子进程 acquire 看到后立即开始压测。
    std::atomic_ref<std::uint32_t>(state.start).store(1, std::memory_order_release);

    // 阻塞等待全部子进程退出，任一非正常退出则标记全局失败。
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
      throw std::runtime_error("salias IPC child failed");
    }

    // 每个 consumer 应消费的消息数 = producer 数 × 每 producer 消息数。
    const std::uint64_t expected_per_consumer =
        static_cast<std::uint64_t>(options.producers) * options.messages;
    std::uint64_t delivered = 0;
    std::vector<Result::LatencySummary> consumer_latency;
    consumer_latency.reserve(options.consumers);
    std::array<std::uint64_t, latency::kBucketCount> aggregate_buckets{};
    std::uint64_t aggregate_samples = 0;
    std::uint64_t aggregate_max_ns = 0;
    for (std::uint32_t consumer = 0; consumer < options.consumers; ++consumer) {
      // acquire：与子进程 store(consumed, release) 配对，读取最终消费计数。
      const auto value =
          std::atomic_ref<std::uint64_t>(state.consumed[consumer]).load(std::memory_order_acquire);
      // 投递量校验：每个 consumer 必须恰好消费 expected_per_consumer 条。
      if (value != expected_per_consumer) {
        throw std::runtime_error("salias IPC delivery mismatch");
      }
      delivered += value;
      const std::uint64_t samples = state.latency_samples[consumer];
      const std::uint64_t max_ns = state.latency_max_ns[consumer];
      consumer_latency.push_back(
          summarize_latency(state.latency_buckets[consumer].data(), samples, max_ns));
      // 将各 consumer 的直方图合并到聚合桶，用于计算整体延迟分位。
      latency::merge(aggregate_buckets.data(), state.latency_buckets[consumer].data());
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
                  .capacity = options.capacity,
                  .poll_limit = options.poll_limit,
                  .seconds = std::chrono::duration<double>(end - begin).count(),
                  .consumer_latency = std::move(consumer_latency),
                  .aggregate_latency = aggregate_latency};
  } catch (...) {
    // 异常路径：标记失败，向存活子进程发 SIGTERM 并回收，避免僵尸进程。
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
 * @brief 根据场景与消费者数选择对应的模板实例运行基准。
 * @param options 全局配置。
 * @return 对应模式的基准 Result。
 * @details 映射规则：fifo 场景下单 consumer 用 FifoMpsc，多 consumer 用 FifoFanout；
 *   ordered 场景同理对应 OrderedMpsc / OrderedFanout。
 */
Result run_case(const Options& options) {
  if (options.scenario == "fifo") {
    return options.consumers == 1 ? run_case_typed<salias::Mode::FifoMpsc>(options)
                                  : run_case_typed<salias::Mode::FifoFanout>(options);
  }
  return options.consumers == 1 ? run_case_typed<salias::Mode::OrderedMpsc>(options)
                                : run_case_typed<salias::Mode::OrderedFanout>(options);
}

/**
 * @brief 将基准结果以 key=value 机器可解析格式输出到 stdout。
 * @param options 全局配置（用于输出场景、页类型、CPU 配置等元信息）。
 * @param result 本次运行的 Result。
 * @details 先逐 consumer 输出 LATENCY 行，再输出一行汇总 RESULT 行，包含
 *   吞吐（publish/delivery msg/s）、带宽（MiB/s）与聚合延迟分位数。
 *   字段命名以 salias-ipc 前缀标识，便于与 Aeron 对照工具的输出统一解析。
 */
void print_result(const Options& options, const Result& result) {
  const double publish_rate = static_cast<double>(result.published) / result.seconds;
  const double delivery_rate = static_cast<double>(result.delivered) / result.seconds;
  // 带宽 = 投递速率 × payload 字节数，换算为 MiB/s。
  const double mib_per_second =
      delivery_rate * static_cast<double>(result.payload) / (1024.0 * 1024.0);
  for (std::size_t consumer = 0; consumer < result.consumer_latency.size(); ++consumer) {
    const auto& summary = result.consumer_latency[consumer];
    std::cout << "LATENCY library=salias-ipc" << " scenario=" << options.scenario
              << " consumer=" << consumer << " samples=" << summary.samples
              << " p50_ns=" << summary.p50_ns << " p99_ns=" << summary.p99_ns
              << " p999_ns=" << summary.p999_ns << " max_ns=" << summary.max_ns << '\n';
  }
  std::cout << "RESULT library=salias-ipc" << " scenario=" << options.scenario
            << " page=" << options.page << " pinning=" << (pinning_enabled(options) ? "on" : "off")
            << " cpu_base=" << options.cpu_base << " cpu_stride=" << options.cpu_stride
            << " producers=" << result.producers << " consumers=" << result.consumers
            << " payload=" << result.payload << " capacity=" << result.capacity
            << " batch_size=" << options.batch_size
            << " publication_window=" << options.publication_window
            << " poll_limit=" << result.poll_limit
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
 * @brief 程序入口：解析参数、运行基准、输出结果。
 * @param argc 参数个数。
 * @param argv 参数数组。
 * @return 0 成功，1 出错（异常信息输出到 stderr）。
 */
int main(int argc, char** argv) {
  try {
    const Options options = parse_options(argc, argv);
    print_result(options, run_case(options));
  } catch (const std::exception& error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 1;
  }
  return 0;
}

/**
 * @file src/salias/src/channel.cpp
 * @brief L7 公共 API 层：具名跨进程 channel 的实现，桥接共享内存 ring 与端点。
 * @details 本文件位于分层架构最上层 L7 salias 公共 API，向下依赖 L5 channel
 *（SharedHybridMpscChannel）、L1 ring（MagicRing）与 L0 平台层
 *（mmap/shm_open/hugetlbfs）。核心职责是把 per-producer 的共享环形缓冲封装为
 * 可跨进程发现的具名 channel：首个进程以 create() 创建控制段与 ring 段并发布
 * ready 标志，后续进程以 connect() 等待 ready 后挂载。
 * 关键不变式：控制段（NamedControl）固定 16 KiB，所有热点计数器按 cache line
 * 对齐并填充，避免跨进程 false sharing；ready 字段以 acquire/release 内存序
 * 实现发布-订阅同步。
 */

#include "salias/channel.hpp"

#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <libproc.h>
#include <sys/proc_info.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/channel/shared_hybrid_mpsc.hpp"
#include "core/platform/monotonic_clock.hpp"
#include "core/platform/asymmetric_fence.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias {
/// @brief salias 顶层命名空间，聚合公共 API（channel/publisher/subscriber）及其实现细节。

namespace {
/// @brief 匿名命名空间，仅本翻译单元可见的具名通道辅助类型与函数。

inline constexpr std::uint32_t kNamedMagic = 0x53414C43u;  // ASCII "SALC"，校验控制段归属。
inline constexpr std::uint32_t kNamedVersion = 15;  // schema 元数据协议版本，connect 须严格匹配。
inline constexpr std::size_t kControlSize = 16u * 1024u;  // 控制段固定 16 KiB。
inline constexpr std::size_t kCacheLineGuard = 128;  // 双 cache line 填充宽度，规避伪共享。
inline constexpr std::size_t kMaxProducers = 16;  // 单 channel 最大 producer 数。
inline constexpr std::size_t kMaxConsumers = 8;   // 单 channel 最大 consumer 数。
inline constexpr std::size_t kHugePage2MiB = std::size_t{2} * 1024 * 1024;     // 2 MiB huge page。
inline constexpr std::size_t kHugePage1GiB = std::size_t{1024} * 1024 * 1024;  // 1 GiB huge page。
inline constexpr std::uint32_t kNamedFlagHugetlbfs = 1u;  // flags 位 0：是否用 hugetlbfs。
inline constexpr std::uint32_t kNamedHugeShift = 1u;      // huge page 编码起始位移。
inline constexpr std::uint32_t kNamedHugeMask = 0x6u;     // huge page 编码 2 位掩码。
// 非法端点哨兵值，用于标记未分配或无效的 producer/consumer id。
inline constexpr std::uint32_t kInvalidEndpoint = std::numeric_limits<std::uint32_t>::max();
inline constexpr std::uint64_t kDeadProcessStartToken = std::numeric_limits<std::uint64_t>::max();
inline constexpr const char* kDefaultHugetlbfsDir = "/dev/hugepages";  // hugetlbfs 默认挂载点。
// 发布窗口下限（字节）：非 0 窗口须不小于一页，避免小于一次 poll 批量导致长期回压。
inline constexpr std::size_t kMinPublicationWindow = 4096;

#if defined(MAP_POPULATE)
inline constexpr int kPopulateFlag = MAP_POPULATE;
#else
inline constexpr int kPopulateFlag = 0;
#endif

/// @brief 具名 ring 的后端存储类型。
/// @details PosixShm 使用 shm_open 共享内存；Hugetlbfs 使用挂载在 hugetlbfs 的普通文件，
/// 后者走 huge page 以降低 TLB miss，适合大容量低延迟场景。
enum class NamedRingBackend { PosixShm, Hugetlbfs };

/// @brief 具名 ring 的后端与 huge page 规格。
/// @details 在 create/connect 之间以 flags 编码透传，保证两端选用一致的后端。
struct NamedRingSpec {
  NamedRingBackend backend = NamedRingBackend::PosixShm;  ///< 共享内存后端。
  HugePage huge = HugePage::None;  ///< huge page 规格（None 表示普通页）。
};

/// @brief 单个 consumer sequence 的共享槽，64 字节对齐并填充至整条 cache line。
/// @details 一个 consumer 对应一个此槽，存放该 consumer 已消费到的 sequence；
/// 填充确保不同 consumer 的 sequence 落在不同 cache line，避免跨进程 false sharing。
struct alignas(64) NamedPositionSlot {
  std::uint64_t value = 0;                      ///< consumer 当前的消费 position/sequence。
  std::byte pad[64 - sizeof(std::uint64_t)]{};  ///< 填充至 64 字节。
};

/// @brief 一个订阅者注册槽，记录进程身份以区分存活与已退出的消费者。
struct alignas(64) NamedConsumerSlot {
  std::uint64_t state = 0;  ///< 注册代际与阶段，原子发布。
  std::uint32_t pid = 0;    ///< 注册该订阅者的进程 ID。
  std::uint32_t delivery_policy = channel::consumer_registration::kReliablePolicy;
  std::uint64_t process_start = 0;  ///< 进程启动标识，用于识别 PID 复用。
  std::uint64_t notification_ticket = 0;  ///< Adaptive one-shot registration token。
  std::byte pad[64 - (3 * sizeof(std::uint64_t) + 2 * sizeof(std::uint32_t))]{};
};

/// @brief 某 producer 视角下一个 consumer 的游标（position + sequence），64 字节对齐。
/// @details position 指消费到的字节位置，sequence 指消费到的序列号；二者打包进同一
/// cache line 以保证读取一致性，填充避免相邻 consumer 游标伪共享。
struct alignas(64) NamedConsumerCursorSlot {
  std::uint64_t position = 0;  ///< consumer 在该 producer ring 内的消费位置（字节偏移）。
  std::uint64_t sequence = 0;          ///< consumer 已消费到的序列号。
  std::uint64_t message_sequence = 0;  ///< consumer 已消费消息数(per producer)。
  std::uint64_t generation = 0;        ///< 游标所属的订阅代际。
  std::uint64_t lease_start = std::numeric_limits<std::uint64_t>::max();  ///< 借用区间起点。
  std::uint64_t lease_end = 0;  ///< 借用区间末端；仅在 lease_start 有效时读取。
  std::uint64_t lagged_messages = 0;   ///< 该 consumer 在此 ring 观测到的淘汰消息数。
  std::uint64_t skipped_messages = 0;  ///< 该 consumer 在此 ring 主动跳过的消息数。
};

/// @brief 单个 producer 的共享状态槽，双 cache line 对齐。
/// @details 存放该 producer 对外可见的发布 position 与本地序列号；尾部内嵌每个 consumer 的
/// 游标数组，使流控判断（能否继续 publish）所需的全部信息集中在同一 producer 槽内，
/// 减少跨 cache line 访问。填充宽度 kCacheLineGuard=128 隔离头部热字段与尾部 consumer 数组。
struct alignas(kCacheLineGuard) NamedProducerSlot {
  std::uint64_t visible_producer_pos = 0;  ///< 已对外可见（已发布）的 position。
  std::uint64_t local_sequence = 0;  ///< 本地已 claim 的序列号（可能尚未发布）。
  std::uint64_t reservation_position = 0;  ///< 当前 producer 已预留到的写尾。
  std::uint64_t reservation_sequence = 0;  ///< reservation_position 对应的下一序列号。
  std::uint64_t retained_position = 0;     ///< 丢旧模式下最早仍可读取的帧位置。
  std::uint64_t retention_intent = std::numeric_limits<std::uint64_t>::max();  ///< 计划淘汰位置。
  std::uint64_t reservation_message_sequence = 0;  ///< producer 已预留消息数。
  std::uint64_t retained_message_sequence = 0;     ///< 最早保留帧对应的消息数。
  std::byte hot_pad[kCacheLineGuard - 8 * sizeof(std::uint64_t)]{};  ///< 隔离发布热游标。

  alignas(kCacheLineGuard) std::uint32_t producer_pid = 0;  ///< 当前 producer 槽所有者。
  std::uint64_t producer_process_start = 0;                 ///< producer 进程启动标识。
  std::uint32_t claim_active = 0;  ///< claim 正在更新 reservation 时置位。
  std::byte
      coordination_pad[kCacheLineGuard - (3 * sizeof(std::uint32_t) + sizeof(std::uint64_t))]{};
  std::array<NamedConsumerCursorSlot, kMaxConsumers> consumers{};  ///< 各 consumer 游标。
};

/// @brief 控制段布局，固定 16 KiB，跨进程共享，存放 channel 元数据与全部 producer/consumer 游标。
/// @details 所有计数器按 cache line 对齐：global_sequence、next_producer 等热字段各自独占一条
/// cache line，避免多核并发读写引发 false sharing。ready 字段配合 acquire/release 内存序，
/// 作为创建端发布、连接端等待的同步点。不变式：magic/version 由创建端写入且此后不变；
/// ready 由 0→1 单调翻转一次。
struct alignas(kCacheLineGuard) NamedControl {
  std::uint32_t magic = 0;     ///< 魔数 "SALC"，校验控制段归属。
  std::uint32_t version = 0;   ///< 布局版本号，connect 端必须严格匹配。
  std::uint32_t mode = 0;      ///< channel 模式（Mode 枚举值），决定 ordering/fanout。
  std::uint32_t flags = 0;     ///< 后端/huge page 编码标志位。
  std::uint64_t capacity = 0;  ///< 单 ring 字节容量（2 的幂且页对齐）。
  std::uint64_t fixed_payload_size = 0;  ///< 0 表示变长 payload；非零时接收路径使用定长帧。
  std::uint32_t ready = 0;  ///< 就绪标志：创建端置 1（release），连接端等 1（acquire）。
  std::uint32_t wait_word = 0;           ///< 等待策略唤醒字（wait strategy 回填）。
  std::uint32_t notification_enabled = 0;  ///< Adaptive 通知 sticky 开关。
  std::uint64_t publication_window = 0;  ///< 发布流控窗口（0 表示不限流）。
  std::uint32_t high_watermark_percent = 90;      ///< 丢旧模式的保留水位比例。
  std::uint64_t publication_window_messages = 0;  ///< 每 producer ring 的消息数窗口。
  std::uint64_t registration_gate = 0;            ///< 注册订阅者时暂时阻止新 claim。
  std::uint64_t registration_gate_process_start = 0;  ///< gate 所有者进程启动标识。
  std::uint64_t schema_id = 0;                        ///< 应用声明的稳定 wire schema ID。
  std::uint32_t schema_version = 0;       ///< 应用显式递增的 wire schema 版本。
  std::uint32_t schema_flags = 0;         ///< bit 0 表示绑定了固定消息 schema。
  std::uint8_t claim_fence_protocol = 0;  ///< claim 屏障协议（0=seq_cst，1=membarrier）。
  std::byte pad[kCacheLineGuard - 97]{};  ///< 头部填充，隔离后续热字段。

  alignas(kCacheLineGuard) std::uint64_t global_sequence = 0;  ///< 全局单调序列号。
  alignas(kCacheLineGuard) std::uint32_t next_producer = 0;  ///< 下一个待分配 producer id。
  alignas(kCacheLineGuard) std::uint32_t num_producers = 0;  ///< 预设 producer 总数。
  alignas(kCacheLineGuard) std::uint32_t next_consumer = 0;  ///< 下一个待分配 consumer id。
  alignas(kCacheLineGuard) std::uint32_t num_consumers = 0;  ///< 预设 consumer 总数。
  alignas(kCacheLineGuard) std::array<NamedConsumerSlot, kMaxConsumers> consumers{};
  // 各 consumer 的消费 sequence 槽。
  alignas(kCacheLineGuard) std::array<NamedPositionSlot, kMaxConsumers> consumer_sequences{};
  // 各 producer 的共享状态槽（含其 consumer 游标）。
  alignas(kCacheLineGuard) std::array<NamedProducerSlot, kMaxProducers> producers{};
};

// 布局断言：确保 cache line 填充正确，槽大小符合预期，控制段不超过 16 KiB。
static_assert(sizeof(NamedPositionSlot) == 64);
static_assert(sizeof(NamedConsumerSlot) == 64);
static_assert(sizeof(NamedConsumerCursorSlot) == 64);
static_assert(sizeof(NamedProducerSlot) == 2 * kCacheLineGuard + 64 * kMaxConsumers);
static_assert(sizeof(NamedControl) <= kControlSize);
// 协议升级（v14→v15）只在内联填充区里新增 claim_fence_protocol，控制段尺寸与
// global_sequence 等热字段的偏移必须保持逐字节不变，避免引入任何布局效应。
static_assert(sizeof(NamedControl) == 14'208);
static_assert(offsetof(NamedControl, global_sequence) == 2 * kCacheLineGuard);

/// @brief 依据 channel 模式推导 ordering 语义。
/// @details FifoMpsc/FifoMpmc 采用 FIFO 顺序；OrderedMpsc/OrderedMpmc 采用有序（按
/// 序列号）语义。
template <Mode M>
inline constexpr channel::Order kOrdering =
    M == Mode::FifoMpsc || M == Mode::FifoMpmc || M == Mode::LatestMpmc ? channel::Order::Fifo
                                                                        : channel::Order::Ordered;

/// @brief 依据 channel 模式判断是否为 fanout（多消费者广播）模式。
template <Mode M>
inline constexpr bool kFanout =
    M == Mode::FifoMpmc || M == Mode::OrderedMpmc || M == Mode::LatestMpmc;

/// @brief 依据 channel 模式判断是否启用覆盖旧数据的保留窗口。
template <Mode M>
inline constexpr bool kDropOldest = M == Mode::LatestMpmc;

/// @brief 校验具名 channel 名称合法性，仅允许字母数字与 -_. 且长度在 [1,128]。
/// @param name 待校验的名称。
/// @return 合法返回 true。
bool is_valid_channel_name(std::string_view name) noexcept {
  if (name.empty() || name.size() > 128) {
    return false;
  }
  for (const unsigned char character : name) {
    if (std::isalnum(character) == 0 && character != '-' && character != '_' && character != '.') {
      return false;
    }
  }
  return true;
}

/// @brief 将公共 channel 名编码为固定长度 POSIX shm 键，规避 macOS 的短名称限制。
std::uint64_t channel_name_hash(std::string_view name) noexcept {
  constexpr std::uint64_t offset_basis = 14695981039346656037ull;
  constexpr std::uint64_t prime = 1099511628211ull;
  std::uint64_t hash = offset_basis;
  for (const unsigned char character : name) {
    hash ^= character;
    hash *= prime;
  }
  return hash;
}

std::string encoded_channel_name(std::string_view name) {
  constexpr std::array<char, 16> hex_digits = {'0', '1', '2', '3', '4', '5', '6', '7',
                                               '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::array<char, 16> encoded{};
  std::uint64_t hash = channel_name_hash(name);
  for (std::size_t index = encoded.size(); index != 0; --index) {
    encoded[index - 1] = hex_digits[hash & 0x0fu];
    hash >>= 4;
  }
  return std::string(encoded.data(), encoded.size());
}

/// @brief 判断 value 是否为 2 的幂。
/// @details ring 容量必须为 2 的幂，以便用位与（而非取模）完成序列号到槽位的映射。
bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

// 发布窗口合法性：0 表示不限流；非 0 时必须落在 [一页, capacity] 之内。
bool is_valid_publication_window(std::uint64_t window, std::uint64_t capacity) noexcept {
  return window == 0 || (window >= kMinPublicationWindow && window <= capacity);
}

/// 丢旧水位为容量的静态百分比，必须落在 [1, 100]。
bool is_valid_high_watermark_percent(std::uint32_t percent) noexcept {
  return percent >= 1 && percent <= 100;
}

/// @brief 读取指定进程启动标识，避免仅凭 PID 将复用后的新进程误认为旧订阅者。
std::uint64_t process_start_token(std::uint32_t pid) noexcept {
#if defined(__linux__)
  char pid_text[16]{};
  const auto conversion = std::to_chars(pid_text, pid_text + sizeof(pid_text) - 1, pid);
  if (conversion.ec != std::errc{}) {
    return 0;
  }
  char path[32] = "/proc/";
  const std::size_t pid_length = static_cast<std::size_t>(conversion.ptr - pid_text);
  std::memcpy(path + 6, pid_text, pid_length);
  std::memcpy(path + 6 + pid_length, "/stat", 6);
  const int fd = ::open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return 0;
  }
  char content[4096]{};
  const ssize_t length = ::read(fd, content, sizeof(content) - 1);
  static_cast<void>(::close(fd));
  if (length <= 0) {
    return 0;
  }

  char* field = std::strrchr(content, ')');
  if (field == nullptr) {
    return 0;
  }
  ++field;
  while (*field == ' ') {
    ++field;
  }
  if (*field == 'Z' || *field == 'X' || *field == 'x') {
    return kDeadProcessStartToken;
  }
  for (int index = 3; index < 22; ++index) {
    while (*field == ' ') {
      ++field;
    }
    while (*field != '\0' && *field != ' ') {
      ++field;
    }
  }
  while (*field == ' ') {
    ++field;
  }
  std::uint64_t token = 0;
  const char* end = content + length;
  const auto parsed = std::from_chars(field, end, token);
  return parsed.ec == std::errc{} ? token : 0;
#elif defined(__APPLE__)
  proc_bsdinfo info{};
  if (::proc_pidinfo(static_cast<int>(pid), PROC_PIDTBSDINFO, 0, &info, sizeof(info)) !=
      static_cast<int>(sizeof(info))) {
    return 0;
  }
  return static_cast<std::uint64_t>(info.pbi_start_tvsec) * 1'000'000u +
         static_cast<std::uint64_t>(info.pbi_start_tvusec) + 1u;
#else
  static_cast<void>(pid);
  return 0;
#endif
}

/// @brief 校验 consumer 注册时的进程仍是当前占用该 PID 的同一进程。
bool is_registered_process_alive(std::uint32_t pid, std::uint64_t start_token) noexcept {
  if (pid == 0) {
    return false;
  }
  const std::uint64_t current_token = process_start_token(pid);
  if (current_token == kDeadProcessStartToken) {
    return false;
  }
  if (start_token != 0 && current_token != 0) {
    return current_token == start_token;
  }
  if (::kill(static_cast<pid_t>(pid), 0) == 0) {
    return true;
  }
  return errno == EPERM;
}

/// @brief 校验 ring 容量合法性：非 0、2 的幂、不超过 size_t 一半、且对齐到系统页大小。
/// @param capacity 待校验容量（字节）。
/// @return 合法返回 true。
/// @note 容量上限取 size_t/2，确保序列号环绕运算不会溢出；页对齐保证 mmap 映射无残余。
bool is_valid_ring_capacity(std::uint64_t capacity) noexcept {
  if (capacity == 0 || capacity > std::numeric_limits<std::size_t>::max()) {
    return false;
  }
  const auto size = static_cast<std::size_t>(capacity);
  if (!is_power_of_two(size) || size > std::numeric_limits<std::size_t>::max() / 2) {
    return false;
  }
  const long page_size = ::sysconf(_SC_PAGESIZE);
  return page_size > 0 && size % static_cast<std::size_t>(page_size) == 0;
}

/// @brief 返回指定 huge page 规格对应的字节大小，None 返回 0。
/// @param huge huge page 规格。
/// @return 字节大小；None 为 0。
std::size_t huge_page_size(HugePage huge) noexcept {
  switch (huge) {
    case HugePage::None:
      return 0;
    case HugePage::Size2MB:
      return kHugePage2MiB;
    case HugePage::Size1GB:
      return kHugePage1GiB;
  }
  return 0;
}

/// @brief 将公共 API 层的 HugePage 枚举转换为平台层 platform::HugePage 枚举。
/// @param huge 公共层 huge page 规格。
/// @return 平台层对应值。
platform::HugePage to_platform_huge_page(HugePage huge) noexcept {
  switch (huge) {
    case HugePage::None:
      return platform::HugePage::None;
    case HugePage::Size2MB:
      return platform::HugePage::Size2MB;
    case HugePage::Size1GB:
      return platform::HugePage::Size1GB;
  }
  return platform::HugePage::None;
}

/// @brief 校验容量是否对齐到指定 huge page 大小；非 huge page（None）时恒为合法。
/// @param capacity 容量（字节）。
/// @param huge huge page 规格。
/// @return 容量整除页大小返回 true。
bool is_valid_huge_capacity(std::size_t capacity, HugePage huge) noexcept {
  const std::size_t page_size = huge_page_size(huge);
  return page_size == 0 || capacity % page_size == 0;
}

/// @brief 构造控制段的 POSIX 共享内存名称（/salias-<name>-ctl）。
std::string control_shm_name(std::string_view name) {
  return "/salias-" + encoded_channel_name(name) + "-c";
}

/// @brief 构造某 producer 的 ring 的 POSIX 共享内存名称（/salias-<name>-ring-<id>）。
std::string ring_shm_name(std::string_view name, std::uint32_t producer_id) {
  return "/salias-" + encoded_channel_name(name) + "-r-" + std::to_string(producer_id);
}

/// @brief 构造某 producer 的 ring 在 hugetlbfs 文件系统中的公开名（不含目录）。
std::string ring_public_name(std::string_view name, std::uint32_t producer_id) {
  return std::string(name) + "-producer-" + std::to_string(producer_id);
}

/// @brief 读取 hugetlbfs 挂载目录，优先取环境变量 SALIAS_HUGETLBFS_DIR，否则用默认值。
std::string hugetlbfs_dir() {
  if (const char* directory = std::getenv("SALIAS_HUGETLBFS_DIR");
      directory != nullptr && directory[0] != '\0') {
    return directory;
  }
  return kDefaultHugetlbfsDir;
}

/// @brief 构造某 ring 在 hugetlbfs 中的完整文件路径，自动去除尾部多余斜杠。
std::string huge_ring_path(std::string_view name) {
  std::string directory = hugetlbfs_dir();
  if (!directory.empty() && directory.back() == '/') {
    directory.pop_back();
  }
  return directory + "/salias-" + std::string(name) + "-ring";
}

/// @brief 将 HugePage 规格编码为控制段 flags 位字段（bit0=hugetlbfs，bit1-2=huge 编码）。
/// @param huge huge page 规格。
/// @return 编码后的 flags 值。
std::uint32_t named_flags_for(HugePage huge) noexcept {
  switch (huge) {
    case HugePage::None:
      return 0;
    case HugePage::Size2MB:
      return kNamedFlagHugetlbfs | (1u << kNamedHugeShift);
    case HugePage::Size1GB:
      return kNamedFlagHugetlbfs | (2u << kNamedHugeShift);
  }
  return 0;
}

/// @brief 反向解码控制段 flags 为 NamedRingSpec，用于 connect 端重建后端信息。
/// @param flags 控制段 flags 字段。
/// @retval std::nullopt flags 含未知位或 huge 编码非法。
/// @retval NamedRingSpec 解析成功。
/// @note 拒绝任何未知位，保证未来扩展时旧端点不会误用新标志。
std::optional<NamedRingSpec> named_ring_spec_from_flags(std::uint32_t flags) noexcept {
  constexpr std::uint32_t known_mask = kNamedFlagHugetlbfs | kNamedHugeMask;
  if ((flags & ~known_mask) != 0) {
    return std::nullopt;
  }
  const bool hugetlbfs = (flags & kNamedFlagHugetlbfs) != 0;
  const std::uint32_t huge_code = (flags & kNamedHugeMask) >> kNamedHugeShift;
  if (!hugetlbfs) {
    return huge_code == 0 ? std::optional<NamedRingSpec>{NamedRingSpec{}} : std::nullopt;
  }
  if (huge_code == 1) {
    return NamedRingSpec{.backend = NamedRingBackend::Hugetlbfs, .huge = HugePage::Size2MB};
  }
  if (huge_code == 2) {
    return NamedRingSpec{.backend = NamedRingBackend::Hugetlbfs, .huge = HugePage::Size1GB};
  }
  return std::nullopt;
}

/// @brief 依据 HugePage 规格正向构造 NamedRingSpec，用于 create 端。
NamedRingSpec named_ring_spec_for(HugePage huge) noexcept {
  return huge == HugePage::None
             ? NamedRingSpec{}
             : NamedRingSpec{.backend = NamedRingBackend::Hugetlbfs, .huge = huge};
}

/// @brief 返回某 ring 在清理时使用的名称：hugetlbfs 用文件路径，PosixShm 用 shm 名称。
std::string ring_cleanup_name(std::string_view public_name, const std::string& shm_name,
                              NamedRingSpec spec) {
  return spec.backend == NamedRingBackend::Hugetlbfs ? huge_ring_path(public_name) : shm_name;
}

/// @brief 若 fd 有效则关闭，忽略返回值，供 RAII 与错误路径统一使用。
void close_if_open(int fd) noexcept {
  if (fd >= 0) {
    static_cast<void>(::close(fd));
  }
}

/// @brief 以 O_CREAT|O_EXCL 创建指定大小的 POSIX 共享内存段，失败时自动回收。
/// @param name shm 名称（以 / 开头）。
/// @param size 段大小（字节）。
/// @retval fd>=0 成功。
/// @retval -1 创建或 ftruncate 失败（已清理半成品）。
/// @note O_EXCL 保证首个创建者独占；失败时 unlink 防止遗留空段。
int create_sized_shm(const std::string& name, std::size_t size) noexcept {
  const int fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    return -1;
  }
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
    close_if_open(fd);
    static_cast<void>(::shm_unlink(name.c_str()));
    return -1;
  }
  return fd;
}

/// @brief 在 hugetlbfs 挂载点创建指定大小的文件，失败时自动回收。
/// @param name ring 公开名（不含目录）。
/// @param size 文件大小（字节）。
/// @retval fd>=0 成功。
/// @retval -1 创建或 ftruncate 失败（已清理半成品）。
int create_sized_hugetlbfs_file(std::string_view name, std::size_t size) {
  const std::string path = huge_ring_path(name);
  const int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    return -1;
  }
  if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
    close_if_open(fd);
    static_cast<void>(::unlink(path.c_str()));
    return -1;
  }
  return fd;
}

/// @brief RAII 封装控制段的 mmap 映射与文件描述符生命周期。
/// @details 持有 mmap 基址、长度与 fd，析构时自动 munmap 并 close；仅 move 语义，
/// 禁止拷贝以避免双重释放。映射为 MAP_SHARED|MAP_POPULATE，跨进程可见且预填充页表。
class ControlMapping {
 public:
  /// @brief 创建并以创建者身份映射控制段（O_CREAT|O_EXCL）。
  /// @param name shm 名称。
  /// @retval ControlMapping 成功。
  /// @retval Error::PlatformFail 创建或映射失败。
  static Result<ControlMapping> create(const std::string& name) noexcept {
    const int fd = create_sized_shm(name, kControlSize);
    if (fd < 0) {
      return std::unexpected(Error::PlatformFail);
    }
    return map_fd(fd);
  }

  /// @brief 以连接者身份打开并映射已存在的控制段。
  /// @param name shm 名称。
  /// @retval ControlMapping 成功。
  /// @retval Error::NotFound 段不存在或尚未完成定长初始化。
  /// @retval Error::PlatformFail 段状态无法查询。
  /// @note 创建端会先 shm_open 再 ftruncate；连接端必须等文件达到 kControlSize 后再 mmap，
  /// 否则零长映射上的原子读取会触发 SIGBUS。
  static Result<ControlMapping> open(const std::string& name) noexcept {
    const int fd = ::shm_open(name.c_str(), O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
      return std::unexpected(Error::NotFound);
    }
    struct stat status {};
    if (::fstat(fd, &status) != 0) {
      close_if_open(fd);
      return std::unexpected(Error::PlatformFail);
    }
    if (status.st_size != static_cast<off_t>(kControlSize)) {
      close_if_open(fd);
      return std::unexpected(Error::NotFound);
    }
    return map_fd(fd);
  }

  ControlMapping() noexcept = default;
  ControlMapping(ControlMapping&& other) noexcept
      : base_(std::exchange(other.base_, nullptr)),
        len_(std::exchange(other.len_, 0)),
        fd_(std::exchange(other.fd_, -1)) {}
  ControlMapping& operator=(ControlMapping&& other) noexcept {
    if (this != &other) {
      reset();
      base_ = std::exchange(other.base_, nullptr);
      len_ = std::exchange(other.len_, 0);
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }
  ControlMapping(const ControlMapping&) = delete;
  ControlMapping& operator=(const ControlMapping&) = delete;
  ~ControlMapping() noexcept { reset(); }

  /// @brief 返回映射为控制段布局的可写指针。
  NamedControl* control() noexcept { return reinterpret_cast<NamedControl*>(base_); }
  /// @brief 返回映射为控制段布局的只读指针。
  const NamedControl* control() const noexcept {
    return reinterpret_cast<const NamedControl*>(base_);
  }

 private:
  /// @brief 将已打开的 fd 以 MAP_SHARED|MAP_POPULATE 映射，失败时关闭 fd。
  static Result<ControlMapping> map_fd(int fd) noexcept {
    void* mapped =
        ::mmap(nullptr, kControlSize, PROT_READ | PROT_WRITE, MAP_SHARED | kPopulateFlag, fd, 0);
    if (mapped == MAP_FAILED) {
      close_if_open(fd);
      return std::unexpected(Error::PlatformFail);
    }
    return ControlMapping(static_cast<std::byte*>(mapped), kControlSize, fd);
  }

  ControlMapping(std::byte* base, std::size_t len, int fd) noexcept
      : base_(base), len_(len), fd_(fd) {}

  /// @brief 释放映射与 fd，置空成员，可重复调用（幂等）。
  void reset() noexcept {
    if (base_ != nullptr) {
      static_cast<void>(::munmap(base_, len_));
    }
    close_if_open(fd_);
    base_ = nullptr;
    len_ = 0;
    fd_ = -1;
  }

  std::byte* base_ = nullptr;  ///< mmap 基址，nullptr 表示未映射。
  std::size_t len_ = 0;        ///< 映射长度。
  int fd_ = -1;                ///< 文件描述符，-1 表示无效。
};

/// @brief 将 flow 层错误映射为公共 API 层 Error。
Error map_flow_error(flow::FlowError error) noexcept {
  switch (error) {
    case flow::FlowError::Ok:
      return Error::Ok;
    case flow::FlowError::BackPressured:
      return Error::BackPressured;
    case flow::FlowError::MessageTooLarge:
      return Error::MessageTooLarge;
  }
  return Error::BadConfig;
}

Result<std::uint64_t> expiry_deadline(std::chrono::nanoseconds ttl) noexcept {
  if (ttl.count() < 0) {
    return std::unexpected(Error::BadConfig);
  }
  const auto now = platform::monotonic_now_ns();
  if (!now) {
    return std::unexpected(Error::PlatformFail);
  }
  const auto duration = static_cast<std::uint64_t>(ttl.count());
  if (duration > std::numeric_limits<std::uint64_t>::max() - *now) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return *now + duration;
}

Error map_receive_error(channel::ReceiveError error) noexcept {
  switch (error) {
    case channel::ReceiveError::BadConfig:
      return Error::BadConfig;
    case channel::ReceiveError::BackPressured:
      return Error::BackPressured;
    case channel::ReceiveError::Lagged:
      return Error::Lagged;
  }
  return Error::BadConfig;
}

/// @brief 将平台层错误映射为公共 API 层 Error：尺寸非法→BadConfig，其余→PlatformFail。
Error map_platform_error(platform::PlatformError error) noexcept {
  return error == platform::PlatformError::InvalidSize ? Error::BadConfig : Error::PlatformFail;
}

/// @brief 以共享内存映射 fd 并构造 MagicRing；映射后立即关闭 fd（映射保持有效）。
/// @param fd 已打开的文件描述符。
/// @param capacity ring 容量。
/// @param huge huge page 规格。
/// @retval MagicRing 构造成功。
/// @retval Error 映射或 ring 创建失败。
/// @note fd 在本函数内必定被关闭，调用者无需再管。
Result<ring::MagicRing> map_ring_from_fd(int fd, std::size_t capacity, HugePage huge) noexcept {
  auto mapped = platform::Mapping::map_shared_fd(
      fd, platform::MapOptions{.size = capacity, .huge = to_platform_huge_page(huge)});
  close_if_open(fd);
  if (!mapped) {
    return std::unexpected(map_platform_error(mapped.error()));
  }
  auto ring = ring::MagicRing::create(std::move(mapped).value());
  if (!ring) {
    return std::unexpected(Error::PlatformFail);
  }
  return std::move(ring).value();
}

/// @brief 以创建者身份新建并映射某 producer 的 ring（PosixShm 或 hugetlbfs）。
/// @param public_name hugetlbfs 公开名。
/// @param shm_name PosixShm 名称。
/// @param capacity ring 容量。
/// @param spec 后端与 huge page 规格。
/// @retval MagicRing 成功。
/// @retval Error::PlatformFail 创建失败。
Result<ring::MagicRing> create_named_ring(std::string_view public_name, const std::string& shm_name,
                                          std::size_t capacity, NamedRingSpec spec) {
  const int fd = spec.backend == NamedRingBackend::Hugetlbfs
                     ? create_sized_hugetlbfs_file(public_name, capacity)
                     : create_sized_shm(shm_name, capacity);
  if (fd < 0) {
    return std::unexpected(Error::PlatformFail);
  }
  return map_ring_from_fd(fd, capacity, spec.huge);
}

/// @brief 以连接者身份打开已存在的某 producer ring 并映射。
/// @param public_name hugetlbfs 公开名。
/// @param shm_name PosixShm 名称。
/// @param capacity ring 容量。
/// @param spec 后端与 huge page 规格。
/// @retval MagicRing 成功。
/// @retval Error::NotFound 段不存在。
Result<ring::MagicRing> open_named_ring(std::string_view public_name, const std::string& shm_name,
                                        std::size_t capacity, NamedRingSpec spec) {
  const int fd = spec.backend == NamedRingBackend::Hugetlbfs
                     ? ::open(huge_ring_path(public_name).c_str(), O_RDWR | O_CLOEXEC)
                     : ::shm_open(shm_name.c_str(), O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    return std::unexpected(Error::NotFound);
  }
  return map_ring_from_fd(fd, capacity, spec.huge);
}

/// @brief 连接端阻塞等待控制段就绪：先轮询 shm_open 成功，再轮询 ready==1。
/// @param name 控制段 shm 名称。
/// @retval ControlMapping 就绪后的映射。
/// @retval Error::NotFound 超时仍未就绪。
/// @note ready 以 acquire 内存序读取，与创建端 release 写入配对，保证可见性。
Result<ControlMapping> open_control_when_ready(const std::string& name) {
  Error open_error = Error::NotFound;
  for (int attempt = 0; attempt < 100000; ++attempt) {
    auto opened = ControlMapping::open(name);
    if (opened) {
      auto mapping = std::move(opened).value();
      NamedControl* control = mapping.control();
      for (int ready_attempt = 0; ready_attempt < 100000; ++ready_attempt) {
        if (std::atomic_ref<std::uint32_t>(control->ready).load(std::memory_order_acquire) == 1) {
          return mapping;
        }
        std::this_thread::yield();
      }
      return std::unexpected(Error::NotFound);
    }
    open_error = opened.error();
    if (open_error != Error::NotFound) {
      return std::unexpected(open_error);
    }
    std::this_thread::yield();
  }
  return std::unexpected(open_error);
}

/// @brief 按 Mode 选定底层 SharedHybridMpscChannel 的 ordering 类型的别名。
template <Mode M>
using SharedChannel =
    channel::SharedHybridMpscChannel<kOrdering<M>, wait::AdaptiveFutex, kDropOldest<M>>;

/// @brief 从控制段提取 channel 级共享状态（全局序列号、consumer sequences 等）。
/// @param control 控制段引用。
/// @return 填充好的 SharedControl，字段指针指向控制段内对应槽位。
template <Mode M>
typename SharedChannel<M>::SharedControl make_shared_control(NamedControl& control) {
  typename SharedChannel<M>::SharedControl shared{};
  shared.global_seq = &control.global_sequence;
  shared.wait_word = &control.wait_word;
  shared.notification_enabled = &control.notification_enabled;
  shared.registration_gate = &control.registration_gate;
  shared.claim_fence_protocol =
      control.claim_fence_protocol ==
              static_cast<std::uint8_t>(platform::fence::ClaimFenceProtocol::kMembarrier)
          ? platform::fence::ClaimFenceProtocol::kMembarrier
          : platform::fence::ClaimFenceProtocol::kSeqCst;
  shared.num_producers = &control.num_producers;
  shared.num_consumers = &control.num_consumers;
  shared.is_process_alive = is_registered_process_alive;
  shared.publication_window = control.publication_window;
  shared.publication_window_messages = control.publication_window_messages;
  if constexpr (kDropOldest<M>) {
    const std::size_t capacity = static_cast<std::size_t>(control.capacity);
    const std::size_t percent = control.high_watermark_percent;
    const std::size_t whole = (capacity / 100) * percent;
    const std::size_t fraction = ((capacity % 100) * percent) / 100;
    shared.retention_capacity = std::max<std::size_t>(1, whole + fraction);
  }
  shared.consumer_sequences.reserve(control.num_consumers);
  for (std::uint32_t consumer_id = 0; consumer_id < control.num_consumers; ++consumer_id) {
    shared.consumer_sequences.push_back(&control.consumer_sequences[consumer_id].value);
  }
  return shared;
}

/// @brief 从控制段提取每个 producer 的共享状态（可见 position、本地序列号、其 consumer 游标）。
/// @param control 控制段引用。
/// @return 每个 producer 一个 ProducerSharedState，内部指针指向控制段对应槽位。
template <Mode M>
std::vector<typename SharedChannel<M>::ProducerSharedState> make_producer_states(
    NamedControl& control) {
  std::vector<typename SharedChannel<M>::ProducerSharedState> states;
  states.reserve(control.num_producers);
  for (std::uint32_t producer_id = 0; producer_id < control.num_producers; ++producer_id) {
    typename SharedChannel<M>::ProducerSharedState state{};
    state.visible_producer_pos = &control.producers[producer_id].visible_producer_pos;
    state.local_sequence = &control.producers[producer_id].local_sequence;
    state.reservation_position = &control.producers[producer_id].reservation_position;
    state.reservation_sequence = &control.producers[producer_id].reservation_sequence;
    state.reservation_message_sequence =
        &control.producers[producer_id].reservation_message_sequence;
    state.retained_message_sequence = &control.producers[producer_id].retained_message_sequence;
    state.claim_active = &control.producers[producer_id].claim_active;
    state.producer_pid = &control.producers[producer_id].producer_pid;
    state.producer_process_start = &control.producers[producer_id].producer_process_start;
    state.retained_position = &control.producers[producer_id].retained_position;
    state.retention_intent = &control.producers[producer_id].retention_intent;
    state.consumers.reserve(control.num_consumers);
    for (std::uint32_t consumer_id = 0; consumer_id < control.num_consumers; ++consumer_id) {
      state.consumers.push_back(typename SharedChannel<M>::ConsumerSharedState{
          .position = &control.producers[producer_id].consumers[consumer_id].position,
          .sequence = &control.producers[producer_id].consumers[consumer_id].sequence,
          .lease_start = &control.producers[producer_id].consumers[consumer_id].lease_start,
          .lease_end = &control.producers[producer_id].consumers[consumer_id].lease_end,
          .message_sequence =
              &control.producers[producer_id].consumers[consumer_id].message_sequence,
          .process_id = &control.consumers[consumer_id].pid,
          .process_start = &control.consumers[consumer_id].process_start,
          .delivery_policy = &control.consumers[consumer_id].delivery_policy,
          .registration_state = &control.consumers[consumer_id].state,
          .notification_ticket = &control.consumers[consumer_id].notification_ticket,
          .cursor_generation = &control.producers[producer_id].consumers[consumer_id].generation,
          .lagged_messages = &control.producers[producer_id].consumers[consumer_id].lagged_messages,
          .skipped_messages =
              &control.producers[producer_id].consumers[consumer_id].skipped_messages,
      });
    }
    states.push_back(std::move(state));
  }
  return states;
}

/// @brief 具名 channel 的运行时状态：持有控制段映射、底层 channel 与共享段名称，负责清理。
/// @details owns_names_ 区分创建者（析构时 unlink 共享段）与连接者（仅 munmap）；
/// hugetlbfs_ 决定 ring 清理走 unlink 还是 shm_unlink。仅 move 语义。
/// @tparam M channel 模式。
template <Mode M>
class NamedChannelState {
 public:
  class ConsumerRegistration {
   public:
    ConsumerRegistration() noexcept = default;
    ConsumerRegistration(NamedChannelState* owner, std::uint32_t consumer_id,
                         std::uint64_t token) noexcept
        : owner_(owner), consumer_id_(consumer_id), token_(token) {}

    ConsumerRegistration(ConsumerRegistration&& other) noexcept
        : owner_(std::exchange(other.owner_, nullptr)),
          consumer_id_(other.consumer_id_),
          token_(std::exchange(other.token_, 0)) {}

    ConsumerRegistration& operator=(ConsumerRegistration&& other) noexcept {
      if (this != &other) {
        reset();
        owner_ = std::exchange(other.owner_, nullptr);
        consumer_id_ = other.consumer_id_;
        token_ = std::exchange(other.token_, 0);
      }
      return *this;
    }

    ConsumerRegistration(const ConsumerRegistration&) = delete;
    ConsumerRegistration& operator=(const ConsumerRegistration&) = delete;
    ~ConsumerRegistration() noexcept { reset(); }

    std::uint32_t consumer_id() const noexcept { return consumer_id_; }
    std::uint64_t token() const noexcept { return token_; }

   private:
    void reset() noexcept {
      if (owner_ != nullptr) {
        if (channel::consumer_registration::process_id(token_) ==
            static_cast<std::uint32_t>(::getpid())) {
          owner_->release_consumer(consumer_id_, token_);
        }
        owner_ = nullptr;
        token_ = 0;
      }
    }

    NamedChannelState* owner_ = nullptr;
    std::uint32_t consumer_id_ = 0;
    std::uint64_t token_ = 0;
  };

  /// @brief 构造完整状态。
  /// @param control 控制段映射。
  /// @param channel 底层 channel 引擎。
  /// @param control_name 控制段 shm 名称。
  /// @param ring_names 各 ring 的清理用名称。
  /// @param owns_names 是否为创建者（析构时清理共享段）。
  /// @param hugetlbfs ring 是否走 hugetlbfs 后端。
  NamedChannelState(ControlMapping control, SharedChannel<M> channel, std::string control_name,
                    std::vector<std::string> ring_names, bool owns_names, bool hugetlbfs) noexcept
      : control_(std::move(control)),
        channel_(std::move(channel)),
        control_name_(std::move(control_name)),
        ring_names_(std::move(ring_names)),
        owns_names_(owns_names),
        hugetlbfs_(hugetlbfs) {}

  NamedChannelState(NamedChannelState&& other) noexcept
      : control_(std::move(other.control_)),
        channel_(std::move(other.channel_)),
        control_name_(std::move(other.control_name_)),
        ring_names_(std::move(other.ring_names_)),
        owns_names_(std::exchange(other.owns_names_, false)),
        hugetlbfs_(other.hugetlbfs_) {}

  NamedChannelState& operator=(NamedChannelState&& other) noexcept {
    if (this != &other) {
      cleanup();
      control_ = std::move(other.control_);
      channel_ = std::move(other.channel_);
      control_name_ = std::move(other.control_name_);
      ring_names_ = std::move(other.ring_names_);
      owns_names_ = std::exchange(other.owns_names_, false);
      hugetlbfs_ = other.hugetlbfs_;
    }
    return *this;
  }

  NamedChannelState(const NamedChannelState&) = delete;
  NamedChannelState& operator=(const NamedChannelState&) = delete;
  ~NamedChannelState() noexcept { cleanup(); }

  /// @brief 获取指定 producer 的发送端句柄。
  auto tx(std::uint32_t producer_id) noexcept { return channel_.tx(producer_id); }
  /// @brief 获取指定 registration 对应的接收端句柄。
  auto rx(std::uint32_t consumer_id, std::uint64_t token) {
    return channel_.rx(consumer_id, token);
  }

  /// @brief 统计注册槽位中仍由原进程持有的订阅者数量。
  std::uint32_t connected_consumer_count() noexcept {
    NamedControl* control = control_.control();
    if (control == nullptr) {
      return 0;
    }
    std::uint32_t connected = 0;
    for (std::uint32_t consumer_id = 0; consumer_id < control->num_consumers; ++consumer_id) {
      auto& slot = control->consumers[consumer_id];
      const auto state = std::atomic_ref<std::uint64_t>(slot.state).load(std::memory_order_acquire);
      if ((state & channel::consumer_registration::kPhaseMask) !=
          channel::consumer_registration::kActive) {
        continue;
      }
      const auto pid = std::atomic_ref<std::uint32_t>(slot.pid).load(std::memory_order_relaxed);
      const auto start_token =
          std::atomic_ref<std::uint64_t>(slot.process_start).load(std::memory_order_relaxed);
      if (is_registered_process_alive(pid, start_token)) {
        ++connected;
      }
    }
    return connected;
  }

  std::size_t capacity() const noexcept {
    const NamedControl* control = control_.control();
    return control == nullptr ? 0 : static_cast<std::size_t>(control->capacity);
  }

  std::uint32_t high_watermark_percent() const noexcept {
    const NamedControl* control = control_.control();
    return control == nullptr || control->mode != static_cast<std::uint32_t>(Mode::LatestMpmc)
               ? 0
               : control->high_watermark_percent;
  }

  /// @brief 跨进程原子分配一个 producer id，耗尽则返回 nullopt。
  /// @retval std::optional<producer_id> 成功分配。
  /// @retval std::nullopt 超出 num_producers 上限。
  /// @note 使用 acq_rel（获取/释放语义）的 fetch_add，确保各 producer 看到一致的递增次序。
  std::optional<std::uint32_t> acquire_producer_id() noexcept {
    NamedControl* control = control_.control();
    const std::uint32_t producer_id = std::atomic_ref<std::uint32_t>(control->next_producer)
                                          .fetch_add(1, std::memory_order_acq_rel);
    if (producer_id >= control->num_producers) {
      return std::nullopt;
    }
    auto& producer = control->producers[producer_id];
    const auto pid = static_cast<std::uint32_t>(::getpid());
    std::atomic_ref<std::uint32_t>(producer.producer_pid).store(pid, std::memory_order_relaxed);
    std::atomic_ref<std::uint64_t>(producer.producer_process_start)
        .store(process_start_token(pid), std::memory_order_release);
    return producer_id;
  }

  Result<ConsumerRegistration> acquire_consumer(SubscriptionPolicy policy) noexcept {
    NamedControl* control = control_.control();
    if (control == nullptr) {
      return std::unexpected(Error::BadConfig);
    }
    auto gate = acquire_registration_gate(*control);
    if (!gate) {
      return std::unexpected(gate.error());
    }
    RegistrationGateGuard gate_guard(control, *gate);
    if (!enter_registration_barrier(*control)) {
      return std::unexpected(Error::PlatformFail);
    }
    wait_for_claims(*control);

    const auto pid = static_cast<std::uint32_t>(::getpid());
    const auto start = process_start_token(pid);
    for (std::uint32_t consumer_id = 0; consumer_id < control->num_consumers; ++consumer_id) {
      auto& slot = control->consumers[consumer_id];
      auto state = std::atomic_ref<std::uint64_t>(slot.state);
      std::uint64_t observed = state.load(std::memory_order_acquire);
      const auto phase = observed & channel::consumer_registration::kPhaseMask;
      if (phase != channel::consumer_registration::kFree) {
        const auto owner_pid = channel::consumer_registration::process_id(observed);
        const auto owner_start =
            std::atomic_ref<std::uint64_t>(slot.process_start).load(std::memory_order_relaxed);
        if (is_registered_process_alive(owner_pid, owner_start)) {
          continue;
        }
        const auto generation = channel::consumer_registration::generation(observed);
        const auto reclaiming = channel::consumer_registration::make_token(
            generation, owner_pid, channel::consumer_registration::kReclaiming);
        if (!state.compare_exchange_strong(observed, reclaiming, std::memory_order_acq_rel,
                                           std::memory_order_acquire)) {
          continue;
        }
        std::atomic_ref<std::uint64_t>(slot.notification_ticket)
            .store(0, std::memory_order_seq_cst);
        clear_consumer_cursors(*control, consumer_id, generation);
        std::atomic_ref<std::uint32_t>(slot.pid).store(0, std::memory_order_relaxed);
        std::atomic_ref<std::uint32_t>(slot.delivery_policy)
            .store(channel::consumer_registration::kReliablePolicy, std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(slot.process_start).store(0, std::memory_order_relaxed);
        state.store(channel::consumer_registration::make_token(
                        generation, 0, channel::consumer_registration::kFree),
                    std::memory_order_release);
        observed = state.load(std::memory_order_acquire);
      }

      const auto generation = (channel::consumer_registration::generation(observed) + 1) &
                              channel::consumer_registration::kGenerationMask;
      const auto initializing = channel::consumer_registration::make_token(
          generation, pid, channel::consumer_registration::kInitializing);
      if (!state.compare_exchange_strong(observed, initializing, std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
        continue;
      }

      std::atomic_ref<std::uint32_t>(slot.pid).store(pid, std::memory_order_relaxed);
      std::atomic_ref<std::uint32_t>(slot.delivery_policy)
          .store(static_cast<std::uint32_t>(policy), std::memory_order_relaxed);
      std::atomic_ref<std::uint64_t>(slot.process_start).store(start, std::memory_order_relaxed);
      std::atomic_ref<std::uint64_t>(slot.notification_ticket)
          .store(0, std::memory_order_seq_cst);
      const auto ordered_sequence = [&] {
        if constexpr (kOrdering<M> == channel::Order::Ordered) {
          return std::atomic_ref<std::uint64_t>(control->global_sequence)
              .load(std::memory_order_acquire);
        }
        return std::uint64_t{0};
      }();
      for (std::uint32_t producer_id = 0; producer_id < control->num_producers; ++producer_id) {
        auto& producer = control->producers[producer_id];
        const auto cursor_generation =
            std::atomic_ref<std::uint64_t>(producer.consumers[consumer_id].generation);
        cursor_generation.store(generation, std::memory_order_relaxed);
        const auto position = std::atomic_ref<std::uint64_t>(producer.reservation_position)
                                  .load(std::memory_order_acquire);
        auto sequence = ordered_sequence;
        if constexpr (kOrdering<M> == channel::Order::Fifo) {
          sequence = std::atomic_ref<std::uint64_t>(producer.reservation_sequence)
                         .load(std::memory_order_relaxed);
        }
        std::atomic_ref<std::uint64_t>(producer.consumers[consumer_id].position)
            .store(position, std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(producer.consumers[consumer_id].sequence)
            .store(sequence, std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(producer.consumers[consumer_id].message_sequence)
            .store(std::atomic_ref<std::uint64_t>(producer.reservation_message_sequence)
                       .load(std::memory_order_relaxed),
                   std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(producer.consumers[consumer_id].lease_start)
            .store(std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(producer.consumers[consumer_id].lease_end)
            .store(0, std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(producer.consumers[consumer_id].lagged_messages)
            .store(0, std::memory_order_relaxed);
        std::atomic_ref<std::uint64_t>(producer.consumers[consumer_id].skipped_messages)
            .store(0, std::memory_order_relaxed);
      }
      if constexpr (kOrdering<M> == channel::Order::Ordered) {
        std::atomic_ref<std::uint64_t>(control->consumer_sequences[consumer_id].value)
            .store(ordered_sequence, std::memory_order_relaxed);
      }
      const auto active = channel::consumer_registration::make_token(
          generation, pid, channel::consumer_registration::kActive);
      state.store(active, std::memory_order_release);
      return ConsumerRegistration(this, consumer_id, active);
    }
    return std::unexpected(Error::NoSubscriberSlots);
  }

 private:
  class RegistrationGateGuard {
   public:
    RegistrationGateGuard(NamedControl* control, std::uint64_t token) noexcept
        : control_(control), token_(token) {}
    RegistrationGateGuard(const RegistrationGateGuard&) = delete;
    RegistrationGateGuard& operator=(const RegistrationGateGuard&) = delete;
    ~RegistrationGateGuard() noexcept {
      if (control_ != nullptr) {
        auto expected = token_;
        static_cast<void>(std::atomic_ref<std::uint64_t>(control_->registration_gate)
                              .compare_exchange_strong(expected, 0, std::memory_order_release,
                                                       std::memory_order_relaxed));
      }
    }

   private:
    NamedControl* control_;
    std::uint64_t token_;
  };

  static Result<std::uint64_t> acquire_registration_gate(NamedControl& control) noexcept {
    const auto pid = static_cast<std::uint32_t>(::getpid());
    const auto preparing = channel::consumer_registration::make_token(
        0, pid, channel::consumer_registration::kPreparing);
    const auto active =
        channel::consumer_registration::make_token(0, pid, channel::consumer_registration::kActive);
    auto gate = std::atomic_ref<std::uint64_t>(control.registration_gate);
    for (;;) {
      std::uint64_t expected = 0;
      if (gate.compare_exchange_weak(expected, preparing, std::memory_order_seq_cst,
                                     std::memory_order_acquire)) {
        std::atomic_ref<std::uint64_t>(control.registration_gate_process_start)
            .store(process_start_token(pid), std::memory_order_relaxed);
        gate.store(active, std::memory_order_seq_cst);
        return active;
      }

      const auto owner_pid = channel::consumer_registration::process_id(expected);
      const auto phase = expected & channel::consumer_registration::kPhaseMask;
      const auto owner_start =
          phase == channel::consumer_registration::kPreparing
              ? 0
              : std::atomic_ref<std::uint64_t>(control.registration_gate_process_start)
                    .load(std::memory_order_relaxed);
      if (!is_registered_process_alive(owner_pid, owner_start)) {
        static_cast<void>(gate.compare_exchange_strong(expected, 0, std::memory_order_acq_rel,
                                                       std::memory_order_relaxed));
        continue;
      }
      std::this_thread::yield();
    }
  }

  /**
   * @brief 在写下 registration_gate 之后、读取 claim_active 之前建立跨进程完整屏障。
   * @param control 控制段引用。
   * @return true 表示可以安全读取 claim_active；false 表示必须 fail closed。
   * @note 通道协议为 kSeqCst 时无需额外屏障：发布端的两次 seq_cst store 已各自是完整屏障，
   *       本端随后读取 claim_active 与写 gate 之间由 seq_cst 序保证。协议为 kMembarrier 时，
   *       发布端只留编译器屏障，必须由本端用 membarrier 补上另一半，否则 Dekker 握手失效。
   */
  static bool enter_registration_barrier(NamedControl& control) noexcept {
    if (control.claim_fence_protocol !=
        static_cast<std::uint8_t>(platform::fence::ClaimFenceProtocol::kMembarrier)) {
      return true;
    }
    return platform::fence::heavy_fence();
  }

  static void wait_for_claims(NamedControl& control) noexcept {
    for (std::uint32_t producer_id = 0; producer_id < control.num_producers; ++producer_id) {
      auto& producer = control.producers[producer_id];
      auto active = std::atomic_ref<std::uint32_t>(producer.claim_active);
      for (;;) {
        const auto token = active.load(std::memory_order_seq_cst);
        if (token == 0) {
          break;
        }
        const auto producer_pid = static_cast<std::uint32_t>(
            std::atomic_ref<std::uint32_t>(producer.producer_pid).load(std::memory_order_relaxed));
        const auto producer_start = std::atomic_ref<std::uint64_t>(producer.producer_process_start)
                                        .load(std::memory_order_relaxed);
        if (!is_registered_process_alive(producer_pid, producer_start)) {
          auto stale = token;
          static_cast<void>(active.compare_exchange_strong(stale, 0, std::memory_order_acq_rel,
                                                           std::memory_order_relaxed));
          continue;
        }
        std::this_thread::yield();
      }
    }
  }

  static void clear_consumer_cursors(NamedControl& control, std::uint32_t consumer_id,
                                     std::uint64_t generation) noexcept {
    for (std::uint32_t producer_id = 0; producer_id < control.num_producers; ++producer_id) {
      auto& cursor = control.producers[producer_id].consumers[consumer_id];
      std::atomic_ref<std::uint64_t>(cursor.position).store(0, std::memory_order_relaxed);
      std::atomic_ref<std::uint64_t>(cursor.sequence).store(0, std::memory_order_relaxed);
      std::atomic_ref<std::uint64_t>(cursor.message_sequence).store(0, std::memory_order_relaxed);
      std::atomic_ref<std::uint64_t>(cursor.generation)
          .store(generation, std::memory_order_relaxed);
      std::atomic_ref<std::uint64_t>(cursor.lease_start)
          .store(std::numeric_limits<std::uint64_t>::max(), std::memory_order_seq_cst);
      std::atomic_ref<std::uint64_t>(cursor.lease_end).store(0, std::memory_order_relaxed);
      std::atomic_ref<std::uint64_t>(cursor.lagged_messages).store(0, std::memory_order_relaxed);
      std::atomic_ref<std::uint64_t>(cursor.skipped_messages).store(0, std::memory_order_relaxed);
    }
    std::atomic_ref<std::uint64_t>(control.consumer_sequences[consumer_id].value)
        .store(0, std::memory_order_relaxed);
  }

  void release_consumer(std::uint32_t consumer_id, std::uint64_t token) noexcept {
    NamedControl* control = control_.control();
    if (control == nullptr || consumer_id >= control->num_consumers ||
        (token & channel::consumer_registration::kPhaseMask) !=
            channel::consumer_registration::kActive) {
      return;
    }
    auto gate = acquire_registration_gate(*control);
    if (!gate) {
      return;
    }
    RegistrationGateGuard gate_guard(control, *gate);
    if (!enter_registration_barrier(*control)) {
      // fail closed：没有屏障就不能读 claim_active，也不能继续清理该槽位，
      // 保持注册原样（消费者继续有效）比在缺失屏障时读写游标安全。
      return;
    }
    wait_for_claims(*control);

    auto& slot = control->consumers[consumer_id];
    auto state = std::atomic_ref<std::uint64_t>(slot.state);
    const auto releasing = channel::consumer_registration::make_token(
        channel::consumer_registration::generation(token),
        channel::consumer_registration::process_id(token),
        channel::consumer_registration::kReleasing);
    auto expected = token;
    if (!state.compare_exchange_strong(expected, releasing, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
      return;
    }
    auto ticket = token;
    static_cast<void>(std::atomic_ref<std::uint64_t>(slot.notification_ticket)
                          .compare_exchange_strong(ticket, 0, std::memory_order_seq_cst,
                                                   std::memory_order_relaxed));
    const auto generation = channel::consumer_registration::generation(token);
    clear_consumer_cursors(*control, consumer_id, generation);
    std::atomic_ref<std::uint32_t>(slot.pid).store(0, std::memory_order_relaxed);
    std::atomic_ref<std::uint32_t>(slot.delivery_policy)
        .store(channel::consumer_registration::kReliablePolicy, std::memory_order_relaxed);
    std::atomic_ref<std::uint64_t>(slot.process_start).store(0, std::memory_order_relaxed);
    state.store(channel::consumer_registration::make_token(generation, 0,
                                                           channel::consumer_registration::kFree),
                std::memory_order_release);
  }

  /// @brief 创建者析构时清理全部共享段（控制段 + 各 ring），连接者不清理。
  void cleanup() noexcept {
    if (!owns_names_) {
      return;
    }
    static_cast<void>(::shm_unlink(control_name_.c_str()));
    for (const auto& ring_name : ring_names_) {
      if (hugetlbfs_) {
        static_cast<void>(::unlink(ring_name.c_str()));
      } else {
        static_cast<void>(::shm_unlink(ring_name.c_str()));
      }
    }
    owns_names_ = false;
  }

  ControlMapping control_;
  SharedChannel<M> channel_;
  std::string control_name_;
  std::vector<std::string> ring_names_;
  bool owns_names_ = false;
  bool hugetlbfs_ = false;
};

/// @brief 创建者建立具名 channel：校验配置→建控制段与各 producer ring→写元数据→发布 ready。
/// @param config channel 配置。
/// @retval NamedChannelState 创建成功（owns_names=true）。
/// @retval Error::BadConfig 配置非法。
/// @retval Error::PlatformFail 共享段创建/映射失败。
/// @note 任意 ring 创建失败时回滚已建段；最后以 release 内存序写 ready=1 唤醒连接端。
template <Mode M>
Result<NamedChannelState<M>> create_named_state(const Config& config,
                                                detail::SchemaDescriptor schema) {
  // Ordered 模式每个 producer 独立序列号域，故序列号域数等于 producer 数；FIFO 共享一域。
  const std::uint32_t sequence_domains =
      kOrdering<M> == channel::Order::Ordered ? config.num_producers : 1;
  if (!is_valid_channel_name(config.name) || config.mode != M ||
      !is_valid_ring_capacity(config.capacity) || config.num_producers == 0 ||
      config.num_producers > kMaxProducers || config.num_consumers == 0 ||
      config.num_consumers > kMaxConsumers || (!kFanout<M> && config.num_consumers != 1) ||
      !is_valid_publication_window(config.publication_window, config.capacity) ||
      (kDropOldest<M> && (!is_valid_high_watermark_percent(config.high_watermark_percent) ||
                          config.publication_window != 0)) ||
      !channel::hybrid_detail::sequence_low_window_fits(sequence_domains, config.capacity)) {
    return std::unexpected(Error::BadConfig);
  }

#if !defined(__linux__)
  if (config.huge != HugePage::None) {
    return std::unexpected(Error::PlatformFail);
  }
#endif

  std::string control_name = control_shm_name(config.name);
  const NamedRingSpec ring_spec = named_ring_spec_for(config.huge);
  auto control_mapping = ControlMapping::create(control_name);
  if (!control_mapping) {
    return std::unexpected(control_mapping.error());
  }

  std::vector<ring::MagicRing> rings;
  std::vector<std::string> cleanup_names;
  auto cleanup_created_names = [&]() noexcept {
    for (const auto& cleanup_name : cleanup_names) {
      if (ring_spec.backend == NamedRingBackend::Hugetlbfs) {
        static_cast<void>(::unlink(cleanup_name.c_str()));
      } else {
        static_cast<void>(::shm_unlink(cleanup_name.c_str()));
      }
    }
    static_cast<void>(::shm_unlink(control_name.c_str()));
  };

  try {
    rings.reserve(config.num_producers);
    cleanup_names.reserve(config.num_producers);
    // 逐个创建 per-producer ring；任一失败则回滚已创建的 ring 与控制段。
    for (std::uint32_t producer_id = 0; producer_id < config.num_producers; ++producer_id) {
      const std::string shm_name = ring_shm_name(config.name, producer_id);
      const std::string public_name = ring_public_name(config.name, producer_id);
      std::string cleanup_name = ring_cleanup_name(public_name, shm_name, ring_spec);
      auto ring = create_named_ring(public_name, shm_name, config.capacity, ring_spec);
      if (!ring) {
        cleanup_created_names();
        return std::unexpected(ring.error());
      }
      cleanup_names.push_back(std::move(cleanup_name));
      rings.push_back(std::move(ring).value());
    }

    NamedControl* control = control_mapping->control();
    std::construct_at(control);  // placement-new 初始化控制段为默认值。
    // 写入 channel 元数据；随后以 release 写 ready，对连接端形成发布屏障。
    control->magic = kNamedMagic;
    control->version = kNamedVersion;
    control->mode = static_cast<std::uint32_t>(M);
    control->flags = named_flags_for(config.huge);
    control->capacity = config.capacity;
    control->fixed_payload_size = schema.payload_size;
    control->schema_id = schema.schema_id;
    control->schema_version = schema.version;
    control->schema_flags = schema.typed ? 1u : 0u;
    control->num_producers = config.num_producers;
    control->num_consumers = config.num_consumers;
    control->publication_window = config.publication_window;
    control->publication_window_messages = config.publication_window_messages;
    control->high_watermark_percent = config.high_watermark_percent;
    // claim 屏障协议在 ready=1 之前写入，此后不可变。
    control->claim_fence_protocol =
        static_cast<std::uint8_t>(platform::fence::select_channel_protocol());

    SharedChannel<M> channel(std::move(rings), make_producer_states<M>(*control),
                             make_shared_control<M>(*control));
    // release 写 ready=1：与 open_control_when_ready 的 acquire 读配对，保证元数据可见。
    std::atomic_ref<std::uint32_t>(control->ready).store(1, std::memory_order_release);
    return NamedChannelState<M>(std::move(control_mapping).value(), std::move(channel),
                                std::move(control_name), std::move(cleanup_names), true,
                                ring_spec.backend == NamedRingBackend::Hugetlbfs);
  } catch (const std::bad_alloc&) {
    cleanup_created_names();
    return std::unexpected(Error::OutOfMemory);
  }
}

/// @brief 以连接者身份挂载已存在的具名 channel：等待 ready→校验元数据→打开各 ring。
/// @param name channel 名称。
/// @retval NamedChannelState 连接成功（owns_names=false，不清理共享段）。
/// @retval Error::NotFound 控制段不存在或未就绪。
/// @retval Error::VersionMismatch magic/version 不匹配。
/// @retval Error::BadConfig 元数据非法或与请求模式不符。
/// @note 连接者不做任何 unlink，仅映射；owns_names 恒为 false。
template <Mode M>
Result<NamedChannelState<M>> connect_named_state(std::string_view name,
                                                 detail::SchemaDescriptor schema) {
  if (!is_valid_channel_name(name)) {
    return std::unexpected(Error::BadConfig);
  }
  const std::string control_name = control_shm_name(name);
  auto control_mapping = open_control_when_ready(control_name);
  if (!control_mapping) {
    return std::unexpected(control_mapping.error());
  }
  NamedControl* control = control_mapping->control();
  if (control->magic != kNamedMagic || control->version != kNamedVersion) {
    return std::unexpected(Error::VersionMismatch);
  }
  if (control->fixed_payload_size != schema.payload_size ||
      control->schema_id != schema.schema_id || control->schema_version != schema.version ||
      control->schema_flags != (schema.typed ? 1u : 0u)) {
    return std::unexpected(Error::SchemaMismatch);
  }
  const std::uint32_t sequence_domains =
      kOrdering<M> == channel::Order::Ordered ? control->num_producers : 1;

  // 未知协议值必须显式拒绝：否则本端可能不发 heavy fence，却与依赖它的发布端通信。
  if (control->claim_fence_protocol >
      static_cast<std::uint8_t>(platform::fence::ClaimFenceProtocol::kMembarrier)) {
    return std::unexpected(Error::BadConfig);
  }
  if (control->mode != static_cast<std::uint32_t>(M) ||
      !is_valid_ring_capacity(control->capacity) || control->num_producers == 0 ||
      control->num_producers > kMaxProducers || control->num_consumers == 0 ||
      control->num_consumers > kMaxConsumers || (!kFanout<M> && control->num_consumers != 1) ||
      !is_valid_publication_window(control->publication_window, control->capacity) ||
      (kDropOldest<M> && (!is_valid_high_watermark_percent(control->high_watermark_percent) ||
                          control->publication_window != 0)) ||
      !channel::hybrid_detail::sequence_low_window_fits(sequence_domains, control->capacity)) {
    return std::unexpected(Error::BadConfig);
  }
  const auto ring_spec = named_ring_spec_from_flags(control->flags);
  if (!ring_spec || !is_valid_huge_capacity(control->capacity, ring_spec->huge)) {
    return std::unexpected(Error::BadConfig);
  }

  std::vector<ring::MagicRing> rings;
  std::vector<std::string> ring_names;
  rings.reserve(control->num_producers);
  ring_names.reserve(control->num_producers);
  for (std::uint32_t producer_id = 0; producer_id < control->num_producers; ++producer_id) {
    const std::string shm_name = ring_shm_name(name, producer_id);
    const std::string public_name = ring_public_name(name, producer_id);
    auto ring = open_named_ring(public_name, shm_name, control->capacity, *ring_spec);
    if (!ring) {
      return std::unexpected(ring.error());
    }
    ring_names.push_back(ring_cleanup_name(public_name, shm_name, *ring_spec));
    rings.push_back(std::move(ring).value());
  }

  SharedChannel<M> channel(std::move(rings), make_producer_states<M>(*control),
                           make_shared_control<M>(*control));
  return NamedChannelState<M>(std::move(control_mapping).value(), std::move(channel), control_name,
                              std::move(ring_names), false,
                              ring_spec->backend == NamedRingBackend::Hugetlbfs);
}

/// @brief channel 模式特征，统一 create/connect 的入口分发。
/// @details 将具名状态工厂以静态方法暴露，供 Channel<M> 转发。
template <Mode M>
struct ChannelTraits {
  using State = NamedChannelState<M>;  ///< 该模式对应的具名状态类型。
  /// @brief 创建具名 channel。
  static Result<State> create(const Config& config, detail::SchemaDescriptor schema) {
    return create_named_state<M>(config, schema);
  }
  /// @brief 连接已存在的具名 channel。
  static Result<State> connect(std::string_view name, detail::SchemaDescriptor schema) {
    return connect_named_state<M>(name, schema);
  }
};

}  // namespace

/// @brief 持有具名状态的共享包装，通过 shared_ptr 在端点间共享 channel 生命周期。
/// @tparam M channel 模式。
template <Mode M>
struct ChannelState {
  using State = typename ChannelTraits<M>::State;  ///< 具名状态类型。
  explicit ChannelState(State state) noexcept : channel(std::move(state)) {}
  State channel;  ///< 实际的具名状态。
};

/// @brief publisher 端点：绑定 channel 状态与某 producer 的发送句柄。
/// @details valid 标记 producer id 是否有效（kInvalidEndpoint 表示分配失败）。
/// @tparam M channel 模式。
template <Mode M>
struct PublisherEndpoint {
  using Tx = decltype(std::declval<typename ChannelState<M>::State&>().tx(0));  ///< 发送句柄类型。

  /// @brief 构造端点并立即获取发送句柄。
  /// @param state_value channel 状态（共享所有权）。
  /// @param producer_id 已分配的 producer id。
  PublisherEndpoint(std::shared_ptr<ChannelState<M>> state_value,
                    std::uint32_t producer_id) noexcept
      : state(std::move(state_value)),
        tx(state->channel.tx(producer_id)),
        valid(producer_id != kInvalidEndpoint) {}

  std::shared_ptr<ChannelState<M>> state;  ///< channel 状态，保活。
  Tx tx;                                   ///< 发送句柄。
  bool valid = false;                      ///< producer id 是否有效。
};

/// @brief subscriber 端点：绑定 channel 状态与某 consumer 的接收句柄。
/// @details valid 标记 consumer id 是否有效（kInvalidEndpoint 表示分配失败）。
/// @tparam M channel 模式。
template <Mode M>
struct SubscriberEndpoint {
  using Rx =
      decltype(std::declval<typename ChannelState<M>::State&>().rx(0, 0));  ///< 接收句柄类型。

  /// @brief 构造端点并立即获取接收句柄。
  /// @param state_value channel 状态（共享所有权）。
  /// @param consumer_id 已分配的 consumer id。
  SubscriberEndpoint(std::shared_ptr<ChannelState<M>> state_value,
                     typename ChannelState<M>::State::ConsumerRegistration registration)
      : state(std::move(state_value)),
        registration(std::move(registration)),
        rx(state->channel.rx(this->registration.consumer_id(), this->registration.token())),
        valid(true) {}

  std::shared_ptr<ChannelState<M>> state;  ///< channel 状态，保活。
  typename ChannelState<M>::State::ConsumerRegistration registration;  ///< 注册槽 RAII 所有权。
  Rx rx;                                                               ///< 接收句柄。
  bool valid = false;  ///< consumer id 是否有效。
};

/// @brief 构造 channel，接管共享状态。
template <Mode M>
Channel<M, RawPayloadSchema>::Channel(std::shared_ptr<ChannelState<M>> state) noexcept
    : state_(std::move(state)) {}

/// @brief 静态工厂：创建具名 channel。
/// @param config 配置。
/// @retval Channel 创建成功。
/// @retval Error 配置非法或平台创建失败。
/// @note 公共入口校验后再委托 ChannelTraits 创建具名状态。
template <Mode M>
Result<Channel<M>> Channel<M, RawPayloadSchema>::create(const Config& config)
  requires(M != Mode::LatestMpmc)
{
  return create_with_schema(config, detail::kRawPayloadSchema);
}

template <Mode M>
Result<Channel<M>> Channel<M, RawPayloadSchema>::create_with_schema(
    const Config& config, detail::SchemaDescriptor schema) {
  try {
    if (config.name.empty() || !is_valid_ring_capacity(config.capacity) ||
        !is_valid_huge_capacity(config.capacity, config.huge)) {
      return std::unexpected(Error::BadConfig);
    }
    Config typed_config = config;
    typed_config.mode = M;  // 强制模式与本模板实例一致。
    auto named = ChannelTraits<M>::create(typed_config, schema);
    if (!named) {
      return std::unexpected(named.error());
    }
    return Channel(std::make_shared<ChannelState<M>>(std::move(named).value()));
  } catch (const std::bad_alloc&) {
    return std::unexpected(Error::OutOfMemory);
  }
}

/// @brief 静态工厂：连接已存在的具名 channel。
/// @param name channel 名称。
/// @retval Channel 连接成功。
/// @retval Error 未找到、版本不匹配或配置非法。
template <Mode M>
Result<Channel<M>> Channel<M, RawPayloadSchema>::connect(std::string_view name)
  requires(M != Mode::LatestMpmc)
{
  return connect_with_schema(name, detail::kRawPayloadSchema);
}

template <Mode M>
Result<Channel<M>> Channel<M, RawPayloadSchema>::connect_with_schema(
    std::string_view name, detail::SchemaDescriptor schema) {
  try {
    auto named = ChannelTraits<M>::connect(name, schema);
    if (!named) {
      return std::unexpected(named.error());
    }
    return Channel(std::make_shared<ChannelState<M>>(std::move(named).value()));
  } catch (const std::bad_alloc&) {
    return std::unexpected(Error::OutOfMemory);
  }
}

/// @brief 分配一个 producer 并返回其 Publisher。
/// @return Publisher；若 producer id 耗尽则返回无效端点。
template <Mode M>
Publisher<M> Channel<M, RawPayloadSchema>::publisher() {
  const std::uint32_t producer_id =
      state_->channel.acquire_producer_id().value_or(kInvalidEndpoint);
  return Publisher<M>(std::make_shared<PublisherEndpoint<M>>(state_, producer_id));
}

/// @brief 查询共享控制块中进程仍存活的订阅注册数量。
template <Mode M>
std::uint32_t Channel<M, RawPayloadSchema>::connected_consumer_count() const noexcept {
  return state_ == nullptr ? 0 : state_->channel.connected_consumer_count();
}

template <Mode M>
std::size_t Channel<M, RawPayloadSchema>::capacity() const noexcept {
  return state_ == nullptr ? 0 : state_->channel.capacity();
}

template <Mode M>
std::uint32_t Channel<M, RawPayloadSchema>::high_watermark_percent() const noexcept {
  return state_ == nullptr ? 0 : state_->channel.high_watermark_percent();
}

/// @brief 分配一个 consumer 并返回其 Subscriber。
/// @return Subscriber；若 consumer id 耗尽则返回无效端点。
template <Mode M>
Subscriber<M> Channel<M, RawPayloadSchema>::subscriber() {
  constexpr auto default_policy =
      M == Mode::LatestMpmc ? SubscriptionPolicy::DropOldest : SubscriptionPolicy::Reliable;
  return subscriber(default_policy);
}

template <Mode M>
Subscriber<M> Channel<M, RawPayloadSchema>::subscriber(SubscriptionPolicy policy) {
  auto created = try_subscriber(policy);
  if (!created) {
    if (created.error() == Error::OutOfMemory) {
      throw std::bad_alloc();
    }
    return Subscriber<M>{};
  }
  return std::move(*created);
}

template <Mode M>
Result<Subscriber<M>> Channel<M, RawPayloadSchema>::try_subscriber() {
  constexpr auto default_policy =
      M == Mode::LatestMpmc ? SubscriptionPolicy::DropOldest : SubscriptionPolicy::Reliable;
  return try_subscriber(default_policy);
}

template <Mode M>
Result<Subscriber<M>> Channel<M, RawPayloadSchema>::try_subscriber(SubscriptionPolicy policy) {
  if (state_ == nullptr) {
    return std::unexpected(Error::BadConfig);
  }
  if ((policy != SubscriptionPolicy::Reliable && policy != SubscriptionPolicy::DropOldest) ||
      (M != Mode::LatestMpmc && policy == SubscriptionPolicy::DropOldest)) {
    return std::unexpected(Error::BadConfig);
  }
  try {
    auto registration = state_->channel.acquire_consumer(policy);
    if (!registration) {
      return std::unexpected(registration.error());
    }
    return Subscriber<M>(
        std::make_shared<SubscriberEndpoint<M>>(state_, std::move(registration).value()));
  } catch (const std::bad_alloc&) {
    return std::unexpected(Error::OutOfMemory);
  }
}

/// @brief 构造 Publisher，接管端点。
template <Mode M>
Publisher<M, RawPayloadSchema>::Publisher(std::shared_ptr<PublisherEndpoint<M>> endpoint) noexcept
    : endpoint_(std::move(endpoint)) {}

/// @brief 构造一个未提交的 claim，只记录发布端身份与 claim 句柄。
/// @param owner 申请该 claim 的发布端；仅作身份令牌，不参与生命周期管理。
/// @param claim 底层 flow 层返回的 claim（含 payload 区域与 producer id）。
template <Mode M>
PublishClaim<M, RawPayloadSchema>::PublishClaim(const PublisherEndpoint<M>* owner,
                                                flow::Claim claim) noexcept
    : owner_(owner), claim_(claim), committed_(false) {}

/// @brief 移动构造；源对象置为已提交空状态，确保其析构不再 commit。
template <Mode M>
PublishClaim<M, RawPayloadSchema>::PublishClaim(PublishClaim&& other) noexcept
    : owner_(other.owner_), claim_(other.claim_), committed_(other.committed_) {
  other.owner_ = nullptr;
  other.claim_ = {};
  other.committed_ = true;
}

/// @brief 移动赋值；自赋值保护，源对象置为已提交空状态。
template <Mode M>
PublishClaim<M>& PublishClaim<M, RawPayloadSchema>::operator=(PublishClaim&& other) noexcept {
  if (this != &other) {
    // 目标对象仍持有未提交预留时，覆盖它会丢失 ring 内的位置信息；调试构建直接拒绝。
    assert(committed_ && "PublishClaim move assignment overwrites an uncommitted claim");
    owner_ = other.owner_;
    claim_ = other.claim_;
    committed_ = other.committed_;
    other.owner_ = nullptr;
    other.claim_ = {};
    other.committed_ = true;
  }
  return *this;
}

/// @brief 析构时检查未提交 reservation；Release 构建不保留该检查。
template <Mode M>
PublishClaim<M, RawPayloadSchema>::~PublishClaim() noexcept {
  assert(committed_ && "PublishClaim destroyed without publisher.commit()");
}

/// @brief 返回可写的 payload 区域供调用方填充帧体。
/// @return payload 字节视图。
template <Mode M>
std::span<std::byte> PublishClaim<M, RawPayloadSchema>::payload() noexcept {
  return claim_.payload;
}

/// @brief 提交由当前 Publisher 申请的 claim，将帧对外可见。
/// @note 只比较发布端身份，不解引用 claim 中保存的指针；提交使用当前 Publisher 的端点。
template <Mode M>
void Publisher<M, RawPayloadSchema>::commit(PublishClaim<M, RawPayloadSchema>&& claim) noexcept {
  if (claim.committed_ || claim.owner_ == nullptr) {
    return;
  }
  // 跨 Publisher 提交会把预留位置交给另一个生产端；调试构建直接断言，发布构建拒绝。
  if (claim.owner_ != endpoint_.get()) {
    claim.mark_committed();
    assert(false && "PublishClaim does not belong to this Publisher");
    return;
  }
  if (endpoint_ == nullptr || !endpoint_->valid) {
    claim.mark_committed();
    return;
  }
  endpoint_->tx.commit(claim.claim_);
  claim.mark_committed();
}

/// @brief 尝试申请指定长度的 payload 区域，返回调用方填充的 claim。
/// @param payload_len payload 字节数。
/// @retval PublishClaim 申请成功（需调用方填充后 commit）。
/// @retval Error::MessageTooLarge 超出 uint32 上限。
/// @retval Error::BadConfig 端点无效。
/// @retval Error 转发自 flow 层（背压等）。
template <Mode M>
Result<PublishClaim<M>> Publisher<M, RawPayloadSchema>::try_claim(
    std::size_t payload_len) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid ||
      payload_len > std::numeric_limits<std::uint32_t>::max()) {
    return std::unexpected(payload_len > std::numeric_limits<std::uint32_t>::max()
                               ? Error::MessageTooLarge
                               : Error::BadConfig);
  }
  auto claim = endpoint_->tx.claim(static_cast<std::uint32_t>(payload_len));
  if (!claim) {
    return std::unexpected(map_flow_error(claim.error()));
  }
  return PublishClaim<M>(endpoint_.get(), claim.value());
}

template <Mode M>
Result<PublishClaim<M>> Publisher<M, RawPayloadSchema>::try_claim_for(
    std::size_t payload_len, std::chrono::nanoseconds ttl) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid ||
      payload_len > std::numeric_limits<std::uint32_t>::max()) {
    return std::unexpected(payload_len > std::numeric_limits<std::uint32_t>::max()
                               ? Error::MessageTooLarge
                               : Error::BadConfig);
  }
  auto deadline = expiry_deadline(ttl);
  if (!deadline) {
    return std::unexpected(deadline.error());
  }
  auto claim = endpoint_->tx.claim(static_cast<std::uint32_t>(payload_len), *deadline, true);
  if (!claim) {
    return std::unexpected(map_flow_error(claim.error()));
  }
  return PublishClaim<M>(endpoint_.get(), claim.value());
}

/// @brief 单帧零拷贝发布：拷贝 payload 后立即提交。
/// @param payload 待发布数据。
/// @retval bool 是否成功（背压时为 false）。
/// @retval Error 端点无效或 flow 层错误。
template <Mode M>
Result<bool> Publisher<M, RawPayloadSchema>::offer(std::span<const std::byte> payload) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  auto offered = endpoint_->tx.offer(payload);
  if (!offered) {
    return std::unexpected(map_flow_error(offered.error()));
  }
  return offered.value();
}

template <Mode M>
Result<bool> Publisher<M, RawPayloadSchema>::offer_for(std::span<const std::byte> payload,
                                                       std::chrono::nanoseconds ttl) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
    return std::unexpected(Error::MessageTooLarge);
  }
  auto deadline = expiry_deadline(ttl);
  if (!deadline) {
    return std::unexpected(deadline.error());
  }
  auto offered = endpoint_->tx.offer(payload, *deadline, true);
  if (!offered) {
    return std::unexpected(map_flow_error(offered.error()));
  }
  return offered.value();
}

/// @brief 批量发布：等长帧走 claim_batch 单次提交，变长帧退化为逐帧 offer。
/// @param payloads 多帧 payload 数组。
/// @retval std::size_t 成功发布的帧数。
/// @retval Error 全部失败时返回 flow 错误；部分成功时返回已发布帧数。
/// @note 等长路径用 claim_batch 申请连续区域并 memcpy 各帧 payload（跳过帧头 kHeaderSize），
/// 仅一次提交，显著降低 per-frame 开销；变长路径逐帧 offer。
template <Mode M>
Result<std::size_t> Publisher<M, RawPayloadSchema>::offer_batch(
    std::span<const std::span<const std::byte>> payloads) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  if (payloads.empty()) {
    return std::size_t{0};
  }
  const std::size_t first_size = payloads.front().size();
  bool uniform = true;  // 检测是否所有帧等长，以决定走批量优化路径。
  for (const auto payload : payloads) {
    if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
      return std::unexpected(Error::MessageTooLarge);
    }
    uniform = uniform && payload.size() == first_size;
  }
  if (uniform) {
    // 等长批量路径：一次性 claim 连续区域，逐帧 memcpy payload 后统一提交。
    const std::uint32_t limit = payloads.size() > std::numeric_limits<std::uint32_t>::max()
                                    ? std::numeric_limits<std::uint32_t>::max()
                                    : static_cast<std::uint32_t>(payloads.size());
    auto batch = endpoint_->tx.claim_batch(static_cast<std::uint32_t>(first_size), limit);
    if (!batch) {
      return std::unexpected(map_flow_error(batch.error()));
    }
    for (std::uint32_t index = 0; index < batch->frame_count; ++index) {
      // 每帧在连续区域中的偏移 = 帧序号 * 帧长 + 帧头大小，跳过帧头写入 payload。
      auto destination = batch->region.subspan(
          static_cast<std::size_t>(index) * batch->frame_len + frame::kHeaderSize, first_size);
      if (first_size != 0) {
        std::memcpy(destination.data(), payloads[index].data(), first_size);
      }
    }
    endpoint_->tx.commit_batch(batch.value());
    return batch->frame_count;
  }
  // 变长路径：逐帧 offer；部分成功时返回已发布帧数而非错误。
  std::size_t published = 0;
  for (const auto payload : payloads) {
    auto offered = endpoint_->tx.offer(payload);
    if (!offered) {
      return published == 0 ? Result<std::size_t>{std::unexpected(map_flow_error(offered.error()))}
                            : Result<std::size_t>{published};
    }
    ++published;
  }
  return published;
}

template <Mode M>
Result<std::size_t> Publisher<M, RawPayloadSchema>::offer_batch_for(
    std::span<const std::span<const std::byte>> payloads, std::chrono::nanoseconds ttl) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  auto deadline = expiry_deadline(ttl);
  if (!deadline) {
    return std::unexpected(deadline.error());
  }
  if (payloads.empty()) {
    return std::size_t{0};
  }

  const std::size_t first_size = payloads.front().size();
  bool uniform = true;
  for (const auto payload : payloads) {
    if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
      return std::unexpected(Error::MessageTooLarge);
    }
    uniform = uniform && payload.size() == first_size;
  }
  if (uniform) {
    const std::uint32_t limit = payloads.size() > std::numeric_limits<std::uint32_t>::max()
                                    ? std::numeric_limits<std::uint32_t>::max()
                                    : static_cast<std::uint32_t>(payloads.size());
    auto batch =
        endpoint_->tx.claim_batch(static_cast<std::uint32_t>(first_size), limit, *deadline, true);
    if (!batch) {
      return std::unexpected(map_flow_error(batch.error()));
    }
    for (std::uint32_t index = 0; index < batch->frame_count; ++index) {
      auto destination = batch->region.subspan(static_cast<std::size_t>(index) * batch->frame_len +
                                                   frame::kHeaderSize + sizeof(std::uint64_t),
                                               first_size);
      if (first_size != 0) {
        std::memcpy(destination.data(), payloads[index].data(), first_size);
      }
    }
    endpoint_->tx.commit_batch(*batch);
    return batch->frame_count;
  }

  std::size_t published = 0;
  for (const auto payload : payloads) {
    auto offered = endpoint_->tx.offer(payload, *deadline, true);
    if (!offered) {
      return published == 0 ? Result<std::size_t>{std::unexpected(map_flow_error(offered.error()))}
                            : Result<std::size_t>{published};
    }
    ++published;
  }
  return published;
}

/// @brief 构造 Subscriber，接管端点。
template <Mode M>
Subscriber<M, RawPayloadSchema>::Subscriber(
    std::shared_ptr<SubscriberEndpoint<M>> endpoint) noexcept
    : endpoint_(std::move(endpoint)) {}

/// @brief 非阻塞尝试接收一条消息。
/// @retval Message 收到消息。
/// @retval std::nullopt 无消息或端点无效。
template <Mode M>
std::optional<Message> Subscriber<M, RawPayloadSchema>::try_recv() noexcept {
  return endpoint_ == nullptr || !endpoint_->valid ? std::nullopt : endpoint_->rx.try_recv();
}

template <Mode M>
Result<std::optional<Message>> Subscriber<M, RawPayloadSchema>::try_recv_checked() noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  auto received = endpoint_->rx.try_recv_checked();
  if (!received) {
    return std::unexpected(map_receive_error(received.error()));
  }
  return *received;
}

template <Mode M>
Result<std::optional<Message>> Subscriber<M, RawPayloadSchema>::try_recv_checked_fixed(
    std::uint32_t payload_size) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  auto received = endpoint_->rx.template try_recv_checked_payload<true>(payload_size);
  if (!received) {
    return std::unexpected(map_receive_error(received.error()));
  }
  return *received;
}

template <Mode M>
Result<std::uint64_t> Subscriber<M, RawPayloadSchema>::skip_to_latest_fixed(
    std::uint32_t producer_id, std::uint32_t payload_size) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  auto skipped = endpoint_->rx.template skip_to_latest_payload<true>(producer_id, payload_size);
  if (!skipped) {
    return std::unexpected(map_receive_error(skipped.error()));
  }
  return *skipped;
}

template <Mode M>
std::uint64_t Subscriber<M, RawPayloadSchema>::lagged_messages() const noexcept {
  return endpoint_ == nullptr || !endpoint_->valid ? 0 : endpoint_->rx.lagged_messages();
}

template <Mode M>
std::uint64_t Subscriber<M, RawPayloadSchema>::skipped_messages() const noexcept {
  return endpoint_ == nullptr || !endpoint_->valid ? 0 : endpoint_->rx.skipped_messages();
}

template <Mode M>
std::uint64_t Subscriber<M, RawPayloadSchema>::expired_messages() const noexcept {
  return endpoint_ == nullptr || !endpoint_->valid ? 0 : endpoint_->rx.expired_messages();
}

template <Mode M>
Result<std::uint64_t> Subscriber<M, RawPayloadSchema>::skip_to_latest(
    std::uint32_t producer_id) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  auto skipped = endpoint_->rx.skip_to_latest(producer_id);
  if (!skipped) {
    return std::unexpected(map_receive_error(skipped.error()));
  }
  return *skipped;
}

/// @brief 批量接收消息到输出数组，但不刷新消费进度（需配合 flush_batch 提交进度）。
/// @param out 输出消息数组。
/// @param cap 数组容量。
/// @return 实际接收条数；端点无效或参数非法返回 0。
/// @note 与 flush_batch 配合可实现"读取-处理-确认"语义，处理失败时不推进进度。
template <Mode M>
std::size_t Subscriber<M, RawPayloadSchema>::fetch_batch(Message* out, std::uint32_t cap) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid || out == nullptr || cap == 0) {
    return 0;
  }
  return endpoint_->rx.try_recv_run(out, cap);
}

/// @brief 提交 fetch_batch 累积的消费进度，使已读消息对生产者可见（释放槽位）。
template <Mode M>
void Subscriber<M, RawPayloadSchema>::flush_batch() noexcept {
  if (endpoint_ != nullptr && endpoint_->valid) {
    endpoint_->rx.flush_progress();
  }
}

/// @brief 回调式轮询消费：分块接收并逐条回调，每块处理完即刷新进度。
/// @param max_messages 本次最多消费条数。
/// @param callback 每条消息的回调。
/// @param user 回调用户数据指针。
/// @return 实际消费条数。
/// @note 以 32 条为一块（kChunk）平衡栈空间与刷新频率；无消息即提前退出。
template <Mode M>
std::size_t Subscriber<M, RawPayloadSchema>::poll(std::uint32_t max_messages, PollCallback callback,
                                                  void* user) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid || max_messages == 0 || callback == nullptr) {
    return 0;
  }
  constexpr std::uint32_t kChunk = 32;  // 每块栈上缓冲 32 条，避免大栈帧。
  detail::PollMessageBuffer<kChunk> storage;
  Message* buffer = storage.data();
  std::size_t consumed = 0;
  while (consumed < max_messages) {
    const std::uint32_t remaining =
        static_cast<std::uint32_t>(std::min<std::size_t>(kChunk, max_messages - consumed));
    const std::size_t got = endpoint_->rx.try_recv_run(buffer, remaining);
    if (got == 0) {
      break;  // 当前无消息可读，提前结束。
    }
    for (std::size_t i = 0; i < got; ++i) {
      callback(buffer[i], user);
    }
    endpoint_->rx.flush_progress();  // 每块回调完成后刷新进度，及时释放槽位。
    consumed += got;
  }
  return consumed;
}

/// @brief 阻塞接收一条消息（内部按等待策略自旋/阻塞）。
/// @return 收到的消息。
template <Mode M>
Message Subscriber<M, RawPayloadSchema>::recv() noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return {};
  }
  return endpoint_->rx.recv();
}

template <Mode M>
Message Subscriber<M, RawPayloadSchema>::recv_fixed(std::uint32_t payload_size) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return {};
  }
  return endpoint_->rx.template recv_payload<true>(payload_size);
}

template <Mode M>
bool Subscriber<M, RawPayloadSchema>::valid() const noexcept {
  return endpoint_ != nullptr && endpoint_->valid;
}

/// @brief 显式释放一条消息占用的槽位（配合 fetch_batch 手动管理进度时使用）。
/// @param message 待释放的消息。
template <Mode M>
void Subscriber<M, RawPayloadSchema>::release(const Message& message) noexcept {
  if (endpoint_ != nullptr && endpoint_->valid) {
    endpoint_->rx.release(message);
  }
}

// 显式实例化五种 channel 模式的全部模板类，使头文件中的声明在本翻译单元落地为符号。
// 注意：宏体使用反斜杠续行，此处不得在续行行尾追加 // 注释。
#define SALIAS_INSTANTIATE_MODE(mode)      \
  template class Channel<Mode::mode>;      \
  template class Publisher<Mode::mode>;    \
  template class PublishClaim<Mode::mode>; \
  template class Subscriber<Mode::mode>

SALIAS_INSTANTIATE_MODE(FifoMpsc);     // 多生产者单消费者，FIFO 顺序。
SALIAS_INSTANTIATE_MODE(FifoMpmc);     // 多生产者多消费者广播，FIFO 顺序。
SALIAS_INSTANTIATE_MODE(OrderedMpsc);  // 多生产者单消费者，按序列号有序。
SALIAS_INSTANTIATE_MODE(OrderedMpmc);  // 多生产者多消费者广播，按序列号有序。
SALIAS_INSTANTIATE_MODE(LatestMpmc);   // 慢消费者跳过保留窗口外的旧数据。

#undef SALIAS_INSTANTIATE_MODE

}  // namespace salias

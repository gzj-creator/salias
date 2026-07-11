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
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/channel/shared_hybrid_mpsc.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias {
/// @brief salias 顶层命名空间，聚合公共 API（channel/publisher/subscriber）及其实现细节。

namespace {
/// @brief 匿名命名空间，仅本翻译单元可见的具名通道辅助类型与函数。

inline constexpr std::uint32_t kNamedMagic = 0x53414C43u;  // ASCII "SALC"，校验控制段归属。
inline constexpr std::uint32_t kNamedVersion = 4;  // 控制段布局版本号，connect 须严格匹配。
inline constexpr std::size_t kControlSize = 16u * 1024u;  // 控制段固定 16 KiB。
inline constexpr std::size_t kCacheLineGuard = 128;  // 双 cache line 填充宽度，规避伪共享。
inline constexpr std::size_t kMaxProducers = 16;  // 单 channel 最大 producer 数。
inline constexpr std::size_t kMaxConsumers = 8;  // 单 channel 最大 consumer 数。
inline constexpr std::size_t kHugePage2MiB = std::size_t{2} * 1024 * 1024;  // 2 MiB huge page。
inline constexpr std::size_t kHugePage1GiB = std::size_t{1024} * 1024 * 1024;  // 1 GiB huge page。
inline constexpr std::uint32_t kNamedFlagHugetlbfs = 1u;  // flags 位 0：是否用 hugetlbfs。
inline constexpr std::uint32_t kNamedHugeShift = 1u;  // huge page 编码起始位移。
inline constexpr std::uint32_t kNamedHugeMask = 0x6u;  // huge page 编码 2 位掩码。
// 非法端点哨兵值，用于标记未分配或无效的 producer/consumer id。
inline constexpr std::uint32_t kInvalidEndpoint = std::numeric_limits<std::uint32_t>::max();
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
  std::uint64_t value = 0;  ///< consumer 当前的消费 position/sequence。
  std::byte pad[64 - sizeof(std::uint64_t)]{};  ///< 填充至 64 字节。
};

/// @brief 某 producer 视角下一个 consumer 的游标（position + sequence），64 字节对齐。
/// @details position 指消费到的字节位置，sequence 指消费到的序列号；二者打包进同一
/// cache line 以保证读取一致性，填充避免相邻 consumer 游标伪共享。
struct alignas(64) NamedConsumerCursorSlot {
  std::uint64_t position = 0;  ///< consumer 在该 producer ring 内的消费位置（字节偏移）。
  std::uint64_t sequence = 0;  ///< consumer 已消费到的序列号。
  std::byte pad[64 - (2 * sizeof(std::uint64_t))]{};  ///< 填充至 64 字节。
};

/// @brief 单个 producer 的共享状态槽，双 cache line 对齐。
/// @details 存放该 producer 对外可见的发布 position 与本地序列号；尾部内嵌每个 consumer 的
/// 游标数组，使流控判断（能否继续 publish）所需的全部信息集中在同一 producer 槽内，
/// 减少跨 cache line 访问。填充宽度 kCacheLineGuard=128 隔离头部热字段与尾部 consumer 数组。
struct alignas(kCacheLineGuard) NamedProducerSlot {
  std::uint64_t visible_producer_pos = 0;  ///< 已对外可见（已发布）的 position。
  std::uint64_t local_sequence = 0;  ///< 本地已 claim 的序列号（可能尚未发布）。
  std::byte pad[kCacheLineGuard - (2 * sizeof(std::uint64_t))]{};  ///< 头部填充。
  std::array<NamedConsumerCursorSlot, kMaxConsumers> consumers{};  ///< 各 consumer 游标。
};

/// @brief 控制段布局，固定 16 KiB，跨进程共享，存放 channel 元数据与全部 producer/consumer 游标。
/// @details 所有计数器按 cache line 对齐：global_sequence、next_producer 等热字段各自独占一条
/// cache line，避免多核并发读写引发 false sharing。ready 字段配合 acquire/release 内存序，
/// 作为创建端发布、连接端等待的同步点。不变式：magic/version 由创建端写入且此后不变；
/// ready 由 0→1 单调翻转一次。
struct alignas(kCacheLineGuard) NamedControl {
  std::uint32_t magic = 0;  ///< 魔数 "SALC"，校验控制段归属。
  std::uint32_t version = 0;  ///< 布局版本号，connect 端必须严格匹配。
  std::uint32_t mode = 0;  ///< channel 模式（Mode 枚举值），决定 ordering/fanout。
  std::uint32_t flags = 0;  ///< 后端/huge page 编码标志位。
  std::uint64_t capacity = 0;  ///< 单 ring 字节容量（2 的幂且页对齐）。
  std::uint64_t record_size = 0;  ///< 定长记录大小，变长模式须为 0。
  std::uint32_t ready = 0;  ///< 就绪标志：创建端置 1（release），连接端等 1（acquire）。
  std::uint32_t wait_word = 0;  ///< 等待策略唤醒字（wait strategy 回填）。
  std::uint64_t publication_window = 0;  ///< 发布流控窗口（0 表示不限流）。
  std::byte pad[kCacheLineGuard - 48]{};  ///< 头部填充，隔离后续热字段。

  alignas(kCacheLineGuard) std::uint64_t global_sequence = 0;  ///< 全局单调序列号。
  alignas(kCacheLineGuard) std::uint32_t next_producer = 0;  ///< 下一个待分配 producer id。
  alignas(kCacheLineGuard) std::uint32_t num_producers = 0;  ///< 预设 producer 总数。
  alignas(kCacheLineGuard) std::uint32_t next_consumer = 0;  ///< 下一个待分配 consumer id。
  alignas(kCacheLineGuard) std::uint32_t num_consumers = 0;  ///< 预设 consumer 总数。
  // 各 consumer 的消费 sequence 槽。
  alignas(kCacheLineGuard) std::array<NamedPositionSlot, kMaxConsumers> consumer_sequences{};
  // 各 producer 的共享状态槽（含其 consumer 游标）。
  alignas(kCacheLineGuard) std::array<NamedProducerSlot, kMaxProducers> producers{};
};

// 布局断言：确保 cache line 填充正确，槽大小符合预期，控制段不超过 16 KiB。
static_assert(sizeof(NamedPositionSlot) == 64);
static_assert(sizeof(NamedConsumerCursorSlot) == 64);
static_assert(sizeof(NamedProducerSlot) == kCacheLineGuard + 64 * kMaxConsumers);
static_assert(sizeof(NamedControl) <= kControlSize);

/// @brief 依据 channel 模式推导 ordering 语义。
/// @details FifoMpsc/FifoFanout 采用 FIFO 顺序；OrderedMpsc/OrderedFanout 采用有序（按
/// 序列号）语义。
template <Mode M>
inline constexpr channel::Order kOrdering =
    M == Mode::FifoMpsc || M == Mode::FifoFanout ? channel::Order::Fifo : channel::Order::Ordered;

/// @brief 依据 channel 模式判断是否为 fanout（多消费者广播）模式。
template <Mode M>
inline constexpr bool kFanout = M == Mode::FifoFanout || M == Mode::OrderedFanout;

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

/// @brief 判断 value 是否为 2 的幂。
/// @details ring 容量必须为 2 的幂，以便用位与（而非取模）完成序列号到槽位的映射。
bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

// 发布窗口合法性：0 表示不限流；非 0 时必须落在 [一页, capacity] 之内。
bool is_valid_publication_window(std::uint64_t window, std::uint64_t capacity) noexcept {
  return window == 0 || (window >= kMinPublicationWindow && window <= capacity);
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
  return "/salias-" + std::string(name) + "-ctl";
}

/// @brief 构造某 producer 的 ring 的 POSIX 共享内存名称（/salias-<name>-ring-<id>）。
std::string ring_shm_name(std::string_view name, std::uint32_t producer_id) {
  return "/salias-" + std::string(name) + "-ring-" + std::to_string(producer_id);
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
int create_sized_hugetlbfs_file(std::string_view name, std::size_t size) noexcept {
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

/// @brief 按后端类型 unlink 某 ring，hugetlbfs 走文件 unlink，PosixShm 走 shm_unlink。
void unlink_ring(std::string_view public_name, const std::string& shm_name,
                 NamedRingSpec spec) noexcept {
  if (spec.backend == NamedRingBackend::Hugetlbfs) {
    static_cast<void>(::unlink(huge_ring_path(public_name).c_str()));
  } else {
    static_cast<void>(::shm_unlink(shm_name.c_str()));
  }
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
  /// @retval Error::NotFound 段不存在。
  static Result<ControlMapping> open(const std::string& name) noexcept {
    const int fd = ::shm_open(name.c_str(), O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
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
  std::size_t len_ = 0;  ///< 映射长度。
  int fd_ = -1;  ///< 文件描述符，-1 表示无效。
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
                                          std::size_t capacity, NamedRingSpec spec) noexcept {
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
                                        std::size_t capacity, NamedRingSpec spec) noexcept {
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
using SharedChannel = channel::SharedHybridMpscChannel<kOrdering<M>>;

/// @brief 从控制段提取 channel 级共享状态（全局序列号、consumer sequences 等）。
/// @param control 控制段引用。
/// @return 填充好的 SharedControl，字段指针指向控制段内对应槽位。
template <Mode M>
typename SharedChannel<M>::SharedControl make_shared_control(NamedControl& control) {
  typename SharedChannel<M>::SharedControl shared{};
  shared.global_seq = &control.global_sequence;
  shared.wait_word = &control.wait_word;
  shared.num_producers = &control.num_producers;
  shared.num_consumers = &control.num_consumers;
  shared.publication_window = control.publication_window;
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
    state.consumers.reserve(control.num_consumers);
    for (std::uint32_t consumer_id = 0; consumer_id < control.num_consumers; ++consumer_id) {
      state.consumers.push_back(typename SharedChannel<M>::ConsumerSharedState{
          .position = &control.producers[producer_id].consumers[consumer_id].position,
          .sequence = &control.producers[producer_id].consumers[consumer_id].sequence,
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
  /// @brief 获取指定 consumer 的接收端句柄。
  auto rx(std::uint32_t consumer_id) noexcept { return channel_.rx(consumer_id); }

  /// @brief 跨进程原子分配一个 producer id，耗尽则返回 nullopt。
  /// @retval std::optional<producer_id> 成功分配。
  /// @retval std::nullopt 超出 num_producers 上限。
  /// @note 使用 acq_rel（获取/释放语义）的 fetch_add，确保各 producer 看到一致的递增次序。
  std::optional<std::uint32_t> acquire_producer_id() noexcept {
    NamedControl* control = control_.control();
    const std::uint32_t producer_id = std::atomic_ref<std::uint32_t>(control->next_producer)
                                          .fetch_add(1, std::memory_order_acq_rel);
    return producer_id < control->num_producers ? std::optional{producer_id} : std::nullopt;
  }

  /// @brief 跨进程原子分配一个 consumer id；非 fanout 模式固定返回 0。
  /// @retval std::optional<consumer_id> 成功分配。
  /// @retval std::nullopt 超出 num_consumers 上限。
  /// @note 非 fanout（单消费者）模式下无需分配，直接返回唯一 consumer id 0。
  std::optional<std::uint32_t> acquire_consumer_id() noexcept {
    if constexpr (!kFanout<M>) {
      return 0;
    }
    NamedControl* control = control_.control();
    const std::uint32_t consumer_id = std::atomic_ref<std::uint32_t>(control->next_consumer)
                                          .fetch_add(1, std::memory_order_acq_rel);
    return consumer_id < control->num_consumers ? std::optional{consumer_id} : std::nullopt;
  }

 private:
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
Result<NamedChannelState<M>> create_named_state(const Config& config) {
  // Ordered 模式每个 producer 独立序列号域，故序列号域数等于 producer 数；FIFO 共享一域。
  const std::uint32_t sequence_domains =
      kOrdering<M> == channel::Order::Ordered ? config.num_producers : 1;
  if (!is_valid_channel_name(config.name) || config.mode != M ||
      !is_valid_ring_capacity(config.capacity) || config.num_producers == 0 ||
      config.num_producers > kMaxProducers || config.num_consumers == 0 ||
      config.num_consumers > kMaxConsumers || (!kFanout<M> && config.num_consumers != 1) ||
      !is_valid_publication_window(config.publication_window, config.capacity) ||
      !channel::hybrid_detail::sequence_low_window_fits(sequence_domains, config.capacity)) {
    return std::unexpected(Error::BadConfig);
  }

  const std::string control_name = control_shm_name(config.name);
  const NamedRingSpec ring_spec = named_ring_spec_for(config.huge);
  auto control_mapping = ControlMapping::create(control_name);
  if (!control_mapping) {
    return std::unexpected(control_mapping.error());
  }

  std::vector<ring::MagicRing> rings;
  std::vector<std::string> cleanup_names;
  rings.reserve(config.num_producers);
  cleanup_names.reserve(config.num_producers);
  // 逐个创建 per-producer ring；任一失败则回滚已创建的 ring 与控制段。
  for (std::uint32_t producer_id = 0; producer_id < config.num_producers; ++producer_id) {
    const std::string shm_name = ring_shm_name(config.name, producer_id);
    const std::string public_name = ring_public_name(config.name, producer_id);
    auto ring = create_named_ring(public_name, shm_name, config.capacity, ring_spec);
    if (!ring) {
      for (std::uint32_t cleanup_id = 0; cleanup_id < producer_id; ++cleanup_id) {
        unlink_ring(ring_public_name(config.name, cleanup_id),
                    ring_shm_name(config.name, cleanup_id), ring_spec);
      }
      static_cast<void>(::shm_unlink(control_name.c_str()));
      return std::unexpected(ring.error());
    }
    cleanup_names.push_back(ring_cleanup_name(public_name, shm_name, ring_spec));
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
  control->num_producers = config.num_producers;
  control->num_consumers = config.num_consumers;
  control->publication_window = config.publication_window;

  SharedChannel<M> channel(std::move(rings), make_producer_states<M>(*control),
                           make_shared_control<M>(*control));
  // release 写 ready=1：与 open_control_when_ready 的 acquire 读配对，保证元数据可见。
  std::atomic_ref<std::uint32_t>(control->ready).store(1, std::memory_order_release);
  return NamedChannelState<M>(std::move(control_mapping).value(), std::move(channel), control_name,
                              std::move(cleanup_names), true,
                              ring_spec.backend == NamedRingBackend::Hugetlbfs);
}

/// @brief 以连接者身份挂载已存在的具名 channel：等待 ready→校验元数据→打开各 ring。
/// @param name channel 名称。
/// @retval NamedChannelState 连接成功（owns_names=false，不清理共享段）。
/// @retval Error::NotFound 控制段不存在或未就绪。
/// @retval Error::VersionMismatch magic/version 不匹配。
/// @retval Error::BadConfig 元数据非法或与请求模式不符。
/// @note 连接者不做任何 unlink，仅映射；owns_names 恒为 false。
template <Mode M>
Result<NamedChannelState<M>> connect_named_state(std::string_view name) {
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
  const std::uint32_t sequence_domains =
      kOrdering<M> == channel::Order::Ordered ? control->num_producers : 1;
  if (control->mode != static_cast<std::uint32_t>(M) ||
      !is_valid_ring_capacity(control->capacity) || control->record_size != 0 ||
      control->num_producers == 0 || control->num_producers > kMaxProducers ||
      control->num_consumers == 0 || control->num_consumers > kMaxConsumers ||
      (!kFanout<M> && control->num_consumers != 1) ||
      !is_valid_publication_window(control->publication_window, control->capacity) ||
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
  static Result<State> create(const Config& config) { return create_named_state<M>(config); }
  /// @brief 连接已存在的具名 channel。
  static Result<State> connect(std::string_view name) { return connect_named_state<M>(name); }
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
  Tx tx;  ///< 发送句柄。
  bool valid = false;  ///< producer id 是否有效。
};

/// @brief subscriber 端点：绑定 channel 状态与某 consumer 的接收句柄。
/// @details valid 标记 consumer id 是否有效（kInvalidEndpoint 表示分配失败）。
/// @tparam M channel 模式。
template <Mode M>
struct SubscriberEndpoint {
  using Rx = decltype(std::declval<typename ChannelState<M>::State&>().rx(0));  ///< 接收句柄类型。

  /// @brief 构造端点并立即获取接收句柄。
  /// @param state_value channel 状态（共享所有权）。
  /// @param consumer_id 已分配的 consumer id。
  SubscriberEndpoint(std::shared_ptr<ChannelState<M>> state_value,
                     std::uint32_t consumer_id) noexcept
      : state(std::move(state_value)),
        rx(state->channel.rx(consumer_id)),
        valid(consumer_id != kInvalidEndpoint) {}

  std::shared_ptr<ChannelState<M>> state;  ///< channel 状态，保活。
  Rx rx;  ///< 接收句柄。
  bool valid = false;  ///< consumer id 是否有效。
};

/// @brief 构造 channel，接管共享状态。
template <Mode M>
Channel<M>::Channel(std::shared_ptr<ChannelState<M>> state) noexcept : state_(std::move(state)) {}

/// @brief 静态工厂：创建具名 channel。
/// @param config 配置。
/// @retval Channel 创建成功。
/// @retval Error 配置非法或平台创建失败。
/// @note 公共入口校验后再委托 ChannelTraits 创建具名状态。
template <Mode M>
Result<Channel<M>> Channel<M>::create(const Config& config) {
  if (config.name.empty() || config.fixed_size || config.record_size != 0 ||
      !is_valid_ring_capacity(config.capacity) ||
      !is_valid_huge_capacity(config.capacity, config.huge)) {
    return std::unexpected(Error::BadConfig);
  }
  Config typed_config = config;
  typed_config.mode = M;  // 强制模式与本模板实例一致。
  auto named = ChannelTraits<M>::create(typed_config);
  if (!named) {
    return std::unexpected(named.error());
  }
  return Channel(std::make_shared<ChannelState<M>>(std::move(named).value()));
}

/// @brief 静态工厂：连接已存在的具名 channel。
/// @param name channel 名称。
/// @retval Channel 连接成功。
/// @retval Error 未找到、版本不匹配或配置非法。
template <Mode M>
Result<Channel<M>> Channel<M>::connect(std::string_view name) {
  auto named = ChannelTraits<M>::connect(name);
  if (!named) {
    return std::unexpected(named.error());
  }
  return Channel(std::make_shared<ChannelState<M>>(std::move(named).value()));
}

/// @brief 分配一个 producer 并返回其 Publisher。
/// @return Publisher；若 producer id 耗尽则返回无效端点。
template <Mode M>
Publisher<M> Channel<M>::publisher() noexcept {
  const std::uint32_t producer_id =
      state_->channel.acquire_producer_id().value_or(kInvalidEndpoint);
  return Publisher<M>(std::make_shared<PublisherEndpoint<M>>(state_, producer_id));
}

/// @brief 分配一个 consumer 并返回其 Subscriber。
/// @return Subscriber；若 consumer id 耗尽则返回无效端点。
template <Mode M>
Subscriber<M> Channel<M>::subscriber() noexcept {
  const std::uint32_t consumer_id =
      state_->channel.acquire_consumer_id().value_or(kInvalidEndpoint);
  return Subscriber<M>(std::make_shared<SubscriberEndpoint<M>>(state_, consumer_id));
}

/// @brief 构造 Publisher，接管端点。
template <Mode M>
Publisher<M>::Publisher(std::shared_ptr<PublisherEndpoint<M>> endpoint) noexcept
    : endpoint_(std::move(endpoint)) {}

/// @brief 构造一个未提交的 claim，持有端点与 claim 句柄。
/// @param endpoint publisher 端点。
/// @param claim 底层 flow 层返回的 claim（含 payload 区域与 producer id）。
template <Mode M>
PublishClaim<M>::PublishClaim(std::shared_ptr<PublisherEndpoint<M>> endpoint,
                              flow::Claim claim) noexcept
    : endpoint_(std::move(endpoint)), claim_(claim), committed_(false) {}

/// @brief 移动构造；源对象置为已提交空状态，确保其析构不再 commit。
template <Mode M>
PublishClaim<M>::PublishClaim(PublishClaim&& other) noexcept
    : endpoint_(std::move(other.endpoint_)), claim_(other.claim_), committed_(other.committed_) {
  other.claim_ = {};
  other.committed_ = true;
}

/// @brief 移动赋值；自赋值保护，源对象置为已提交空状态。
template <Mode M>
PublishClaim<M>& PublishClaim<M>::operator=(PublishClaim&& other) noexcept {
  if (this != &other) {
    endpoint_ = std::move(other.endpoint_);
    claim_ = other.claim_;
    committed_ = other.committed_;
    other.claim_ = {};
    other.committed_ = true;
  }
  return *this;
}

/// @brief 返回可写的 payload 区域供调用方填充帧体。
/// @return payload 字节视图。
template <Mode M>
std::span<std::byte> PublishClaim<M>::payload() noexcept {
  return claim_.payload;
}

/// @brief 提交本次 claim，将帧对外可见。
/// @note 幂等：已提交或无效端点直接返回；提交后立即释放端点引用以缩短生命周期。
template <Mode M>
void PublishClaim<M>::commit() noexcept {
  if (committed_ || endpoint_ == nullptr || !endpoint_->valid ||
      claim_.producer_id == kInvalidEndpoint) {
    return;
  }
  endpoint_->tx.commit(claim_);
  claim_ = {};
  committed_ = true;
  endpoint_.reset();
}

/// @brief 尝试申请指定长度的 payload 区域，返回调用方填充的 claim。
/// @param payload_len payload 字节数。
/// @retval PublishClaim 申请成功（需调用方填充后 commit）。
/// @retval Error::MessageTooLarge 超出 uint32 上限。
/// @retval Error::BadConfig 端点无效。
/// @retval Error 转发自 flow 层（背压等）。
template <Mode M>
Result<PublishClaim<M>> Publisher<M>::try_claim(std::size_t payload_len) noexcept {
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
  return PublishClaim<M>(endpoint_, claim.value());
}

/// @brief 单帧零拷贝发布：拷贝 payload 后立即提交。
/// @param payload 待发布数据。
/// @retval bool 是否成功（背压时为 false）。
/// @retval Error 端点无效或 flow 层错误。
template <Mode M>
Result<bool> Publisher<M>::offer(std::span<const std::byte> payload) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid) {
    return std::unexpected(Error::BadConfig);
  }
  auto offered = endpoint_->tx.offer(payload);
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
Result<std::size_t> Publisher<M>::offer_batch(
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

/// @brief 构造 Subscriber，接管端点。
template <Mode M>
Subscriber<M>::Subscriber(std::shared_ptr<SubscriberEndpoint<M>> endpoint) noexcept
    : endpoint_(std::move(endpoint)) {}

/// @brief 非阻塞尝试接收一条消息。
/// @retval Message 收到消息。
/// @retval std::nullopt 无消息或端点无效。
template <Mode M>
std::optional<Message> Subscriber<M>::try_recv() noexcept {
  return endpoint_ == nullptr || !endpoint_->valid ? std::nullopt : endpoint_->rx.try_recv();
}

/// @brief 批量接收消息到输出数组，但不刷新消费进度（需配合 flush_batch 提交进度）。
/// @param out 输出消息数组。
/// @param cap 数组容量。
/// @return 实际接收条数；端点无效或参数非法返回 0。
/// @note 与 flush_batch 配合可实现"读取-处理-确认"语义，处理失败时不推进进度。
template <Mode M>
std::size_t Subscriber<M>::fetch_batch(Message* out, std::uint32_t cap) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid || out == nullptr || cap == 0) {
    return 0;
  }
  return endpoint_->rx.try_recv_run(out, cap);
}

/// @brief 提交 fetch_batch 累积的消费进度，使已读消息对生产者可见（释放槽位）。
template <Mode M>
void Subscriber<M>::flush_batch() noexcept {
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
std::size_t Subscriber<M>::poll(std::uint32_t max_messages, PollCallback callback,
                                void* user) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid || max_messages == 0 || callback == nullptr) {
    return 0;
  }
  constexpr std::uint32_t kChunk = 32;  // 每块栈上缓冲 32 条，避免大栈帧。
  Message buffer[kChunk];
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
Message Subscriber<M>::recv() noexcept {
  return endpoint_->rx.recv();
}

/// @brief 显式释放一条消息占用的槽位（配合 fetch_batch 手动管理进度时使用）。
/// @param message 待释放的消息。
template <Mode M>
void Subscriber<M>::release(const Message& message) noexcept {
  if (endpoint_ != nullptr && endpoint_->valid) {
    endpoint_->rx.release(message);
  }
}

// 显式实例化四种 channel 模式的全部模板类，使头文件中的声明在本翻译单元落地为符号。
// 注意：宏体使用反斜杠续行，此处不得在续行行尾追加 // 注释。
#define SALIAS_INSTANTIATE_MODE(mode)      \
  template class Channel<Mode::mode>;      \
  template class Publisher<Mode::mode>;    \
  template class PublishClaim<Mode::mode>; \
  template class Subscriber<Mode::mode>

SALIAS_INSTANTIATE_MODE(FifoMpsc);  // 多生产者单消费者，FIFO 顺序。
SALIAS_INSTANTIATE_MODE(FifoFanout);  // 多生产者多消费者广播，FIFO 顺序。
SALIAS_INSTANTIATE_MODE(OrderedMpsc);  // 多生产者单消费者，按序列号有序。
SALIAS_INSTANTIATE_MODE(OrderedFanout);  // 多生产者多消费者广播，按序列号有序。

#undef SALIAS_INSTANTIATE_MODE

}  // namespace salias

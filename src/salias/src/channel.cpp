#include "salias/channel.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

#include "core/channel/broadcast.hpp"
#include "core/channel/bulk.hpp"
#include "core/channel/mpsc.hpp"
#include "core/channel/shared_mpmc.hpp"
#include "core/channel/shared_mpsc.hpp"
#include "core/channel/shared_spsc.hpp"
#include "core/channel/spsc.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias {

namespace {

inline constexpr std::uint32_t kNamedMagic = 0x53414C43u;  // "SALC"
inline constexpr std::uint32_t kNamedVersion = 1;
inline constexpr std::size_t kControlSize = 4096;
inline constexpr std::size_t kNamedMpmcMaxSubscribers = 8;

struct alignas(64) NamedPositionSlot {
  std::uint64_t value = 0;
  std::byte pad[64 - sizeof(std::uint64_t)]{};
};

static_assert(sizeof(NamedPositionSlot) == 64);
static_assert(alignof(NamedPositionSlot) == 64);

struct alignas(64) NamedControl {
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
  alignas(64) std::uint32_t subscriber_count = 0;
  std::byte subscriber_count_pad[64 - sizeof(std::uint32_t)]{};
  alignas(64) std::array<NamedPositionSlot, kNamedMpmcMaxSubscribers> subscriber_heads{};
};

static_assert(alignof(NamedControl) == 64);
static_assert(offsetof(NamedControl, producer_pos) % 64 == 0);
static_assert(offsetof(NamedControl, consumer_pos) % 64 == 0);
static_assert(offsetof(NamedControl, subscriber_count) % 64 == 0);
static_assert(offsetof(NamedControl, subscriber_heads) % 64 == 0);
static_assert(sizeof(NamedControl) <= kControlSize);

// 校验用户提供的名称，避免生成非法 POSIX shm 路径。
bool is_valid_channel_name(std::string_view name) noexcept {
  if (name.empty() || name.size() > 128) {
    return false;
  }
  for (const unsigned char c : name) {
    if (!(std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.')) {
      return false;
    }
  }
  return true;
}

// 判断 value 是否为非零 2 的幂。
bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

// 校验 ring 容量的大小、2 的幂、溢出和页对齐约束。
bool is_valid_ring_capacity(std::uint64_t capacity) noexcept {
  if (capacity == 0 || capacity > std::numeric_limits<std::size_t>::max()) {
    return false;
  }
  const auto size = static_cast<std::size_t>(capacity);
  if (!is_power_of_two(size) || size > (std::numeric_limits<std::size_t>::max() / 2)) {
    return false;
  }
  const long page_size = ::sysconf(_SC_PAGESIZE);
  return page_size > 0 && size % static_cast<std::size_t>(page_size) == 0;
}

// 构造命名通道控制块的 POSIX 共享内存名称。
std::string control_shm_name(std::string_view name) {
  return "/salias-" + std::string(name) + "-ctl";
}

// 构造命名通道 ring 的 POSIX 共享内存名称。
std::string ring_shm_name(std::string_view name) {
  return "/salias-" + std::string(name) + "-ring";
}

// 当 fd 表示已打开描述符时关闭它。
void close_if_open(int fd) noexcept {
  if (fd >= 0) {
    static_cast<void>(::close(fd));
  }
}

// 创建并设置 POSIX 共享内存对象大小；失败时返回 -1。
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

class ControlMapping {
 public:
  // 创建并映射新的命名通道控制块。
  static Result<ControlMapping> create(const std::string& name) noexcept {
    const int fd = create_sized_shm(name, kControlSize);
    if (fd < 0) {
      return std::unexpected(Error::PlatformFail);
    }
    return map_fd(fd);
  }

  // 打开并映射已有命名通道控制块。
  static Result<ControlMapping> open(const std::string& name) noexcept {
    const int fd = ::shm_open(name.c_str(), O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
      return std::unexpected(Error::NotFound);
    }
    return map_fd(fd);
  }

  // 创建空映射句柄。
  ControlMapping() noexcept = default;
  // 从另一个句柄转移映射所有权。
  ControlMapping(ControlMapping&& other) noexcept
      : base_(other.base_), len_(other.len_), fd_(other.fd_) {
    other.base_ = nullptr;
    other.len_ = 0;
    other.fd_ = -1;
  }
  // 释放当前所有权后，从另一个句柄转移映射所有权。
  ControlMapping& operator=(ControlMapping&& other) noexcept {
    if (this != &other) {
      reset();
      base_ = other.base_;
      len_ = other.len_;
      fd_ = other.fd_;
      other.base_ = nullptr;
      other.len_ = 0;
      other.fd_ = -1;
    }
    return *this;
  }
  // 禁止拷贝；映射持有 mmap 和 fd。
  ControlMapping(const ControlMapping&) = delete;
  // 禁止拷贝赋值；映射持有 mmap 和 fd。
  ControlMapping& operator=(const ControlMapping&) = delete;
  // 释放已映射控制块并关闭 fd。
  ~ControlMapping() noexcept { reset(); }

  // 返回固定的命名通道控制块前缀。
  NamedControl* control() noexcept {
    // 安全性：ControlMapping 只映射 kControlSize 大小的 shm 对象。
    // 固定前缀 ABI 是 NamedControl，由拥有者在发布 ready 前写入。
    return reinterpret_cast<NamedControl*>(base_);
  }

 private:
  // 映射已经打开的控制块 fd。
  static Result<ControlMapping> map_fd(int fd) noexcept {
    // 安全性：fd 指向由 create_sized_shm() 或拥有者在发布 ready 前截断为 kControlSize 的 shm 对象。
    // MAP_SHARED 让固定控制块 ABI 对双方可见。
    void* mapped = ::mmap(nullptr, kControlSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
      close_if_open(fd);
      return std::unexpected(Error::PlatformFail);
    }
    return ControlMapping(static_cast<std::byte*>(mapped), kControlSize, fd);
  }

  // 保存映射成功的控制块。
  ControlMapping(std::byte* base, std::size_t len, int fd) noexcept
      : base_(base), len_(len), fd_(fd) {}

  // 解除控制块映射、关闭 fd，并重置句柄。
  void reset() noexcept {
    if (base_ != nullptr && len_ != 0) {
      static_cast<void>(::munmap(base_, len_));
    }
    close_if_open(fd_);
    base_ = nullptr;
    len_ = 0;
    fd_ = -1;
  }

  std::byte* base_ = nullptr;
  std::size_t len_ = 0;
  int fd_ = -1;
};

// 将 flow 层发布错误转换为公共 API 错误。
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

// 将核心通道创建错误转换为公共 API 错误。
Error map_channel_error(channel::ChannelError error) noexcept {
  switch (error) {
    case channel::ChannelError::Ok:
      return Error::Ok;
    case channel::ChannelError::BadConfig:
      return Error::BadConfig;
    case channel::ChannelError::PlatformFail:
    case channel::ChannelError::RingFail:
      return Error::PlatformFail;
  }
  return Error::BadConfig;
}

// 将公共配置转换为核心通道使用的配置子集。
channel::ChannelConfig core_config(const Config& config) noexcept {
  return channel::ChannelConfig{
      .capacity = config.capacity,
      .fixed_size = config.fixed_size,
      .record_size = config.record_size,
  };
}

// 将 flow 层消息视图转换为公共消息类型。
Message to_public_message(const flow::Message& message) noexcept {
  return Message{
      .payload = message.payload,
      .position = message.position,
      .next_position = message.next_position,
      .meta = message.meta,
  };
}

// 将公共消息视图转换回 flow 层消息，用于 release。
flow::Message to_flow_message(const Message& message) noexcept {
  return flow::Message{
      .payload = message.payload,
      .position = message.position,
      .next_position = message.next_position,
      .meta = message.meta,
  };
}

// 将平台映射错误转换为公共 API 错误。
Error map_platform_error(platform::PlatformError error) noexcept {
  switch (error) {
    case platform::PlatformError::InvalidSize:
      return Error::BadConfig;
    case platform::PlatformError::Ok:
    case platform::PlatformError::MemfdCreateFailed:
    case platform::PlatformError::FtruncateFailed:
    case platform::PlatformError::ReserveFailed:
    case platform::PlatformError::MapFixedFailed:
    case platform::PlatformError::UnmapFailed:
    case platform::PlatformError::HugePageUnavailable:
    case platform::PlatformError::NumaUnavailable:
    case platform::PlatformError::FutexFailed:
      return Error::PlatformFail;
  }
  return Error::PlatformFail;
}

// 将已打开 fd 映射为双映射 ring，并关闭原始 fd。
Result<ring::MagicRing> map_ring_from_fd(int fd, std::size_t capacity) noexcept {
  auto mapped = platform::Mapping::map_shared_fd(fd, platform::MapOptions{.size = capacity});
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

// 创建命名共享内存 ring，并映射为 MagicRing。
Result<ring::MagicRing> create_named_ring(const std::string& name,
                                          std::size_t capacity) noexcept {
  const int fd = create_sized_shm(name, capacity);
  if (fd < 0) {
    return std::unexpected(Error::PlatformFail);
  }
  return map_ring_from_fd(fd, capacity);
}

// 打开已有命名共享内存 ring，并映射为 MagicRing。
Result<ring::MagicRing> open_named_ring(const std::string& name,
                                        std::size_t capacity) noexcept {
  const int fd = ::shm_open(name.c_str(), O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    return std::unexpected(Error::NotFound);
  }
  return map_ring_from_fd(fd, capacity);
}

}  // namespace

struct NamedSpscState {
  // 接管命名通道资源，并记录当前端是否拥有 shm 名称。
  NamedSpscState(ControlMapping control_value, channel::SharedSpscChannel<> channel_value,
                 std::string control_name_value, std::string ring_name_value,
                 bool owns_names_value) noexcept
      : control(std::move(control_value)),
        channel(std::move(channel_value)),
        control_name(std::move(control_name_value)),
        ring_name(std::move(ring_name_value)),
        owns_names(owns_names_value) {}

  // 转移命名通道资源所有权，并清除来源对象的 unlink 所有权。
  NamedSpscState(NamedSpscState&& other) noexcept
      : control(std::move(other.control)),
        channel(std::move(other.channel)),
        control_name(std::move(other.control_name)),
        ring_name(std::move(other.ring_name)),
        owns_names(other.owns_names) {
    other.owns_names = false;
  }
  // 释放当前持有名称后，转移命名通道资源所有权。
  NamedSpscState& operator=(NamedSpscState&& other) noexcept {
    if (this != &other) {
      cleanup();
      control = std::move(other.control);
      channel = std::move(other.channel);
      control_name = std::move(other.control_name);
      ring_name = std::move(other.ring_name);
      owns_names = other.owns_names;
      other.owns_names = false;
    }
    return *this;
  }
  // 禁止拷贝；状态持有映射和 shm 名称生命周期。
  NamedSpscState(const NamedSpscState&) = delete;
  // 禁止拷贝赋值；状态持有映射和 shm 名称生命周期。
  NamedSpscState& operator=(const NamedSpscState&) = delete;
  // unlink 当前持有的名称，但保留已有对端映射有效。
  ~NamedSpscState() noexcept { cleanup(); }

  // 为底层 shared SPSC 通道创建生产者端点。
  auto tx() noexcept { return channel.tx(); }
  // 为底层 shared SPSC 通道创建消费者端点。
  auto rx() noexcept { return channel.rx(); }

  ControlMapping control;
  channel::SharedSpscChannel<> channel;
  std::string control_name;
  std::string ring_name;
  bool owns_names = false;

 private:
  // 当当前状态拥有名称时 unlink 命名共享内存对象。
  void cleanup() noexcept {
    if (owns_names) {
      // 安全性：拥有者只 unlink 自己创建的名称。
      // shm_unlink 后已有对端映射仍保持有效，名称只是不再供后续 connect() 查找。
      static_cast<void>(::shm_unlink(control_name.c_str()));
      static_cast<void>(::shm_unlink(ring_name.c_str()));
      owns_names = false;
    }
  }
};

struct NamedMpscState {
  // 接管命名 MPSC 资源，并记录当前端是否拥有 shm 名称。
  NamedMpscState(ControlMapping control_value, channel::SharedMpscChannel<> channel_value,
                 std::string control_name_value, std::string ring_name_value,
                 bool owns_names_value) noexcept
      : control(std::move(control_value)),
        channel(std::move(channel_value)),
        control_name(std::move(control_name_value)),
        ring_name(std::move(ring_name_value)),
        owns_names(owns_names_value) {}

  // 转移命名 MPSC 资源所有权，并清除来源对象的 unlink 所有权。
  NamedMpscState(NamedMpscState&& other) noexcept
      : control(std::move(other.control)),
        channel(std::move(other.channel)),
        control_name(std::move(other.control_name)),
        ring_name(std::move(other.ring_name)),
        owns_names(other.owns_names) {
    other.owns_names = false;
  }
  // 释放当前持有名称后，转移命名 MPSC 资源所有权。
  NamedMpscState& operator=(NamedMpscState&& other) noexcept {
    if (this != &other) {
      cleanup();
      control = std::move(other.control);
      channel = std::move(other.channel);
      control_name = std::move(other.control_name);
      ring_name = std::move(other.ring_name);
      owns_names = other.owns_names;
      other.owns_names = false;
    }
    return *this;
  }
  // 禁止拷贝；状态持有映射和 shm 名称生命周期。
  NamedMpscState(const NamedMpscState&) = delete;
  // 禁止拷贝赋值；状态持有映射和 shm 名称生命周期。
  NamedMpscState& operator=(const NamedMpscState&) = delete;
  // unlink 当前持有的名称，但保留已有对端映射有效。
  ~NamedMpscState() noexcept { cleanup(); }

  // 为底层 shared MPSC 通道创建生产者端点。
  auto tx() noexcept { return channel.tx(); }
  // 为底层 shared MPSC 通道创建消费者端点。
  auto rx() noexcept { return channel.rx(); }

  ControlMapping control;
  channel::SharedMpscChannel<> channel;
  std::string control_name;
  std::string ring_name;
  bool owns_names = false;

 private:
  // 当当前状态拥有名称时 unlink 命名共享内存对象。
  void cleanup() noexcept {
    if (owns_names) {
      static_cast<void>(::shm_unlink(control_name.c_str()));
      static_cast<void>(::shm_unlink(ring_name.c_str()));
      owns_names = false;
    }
  }
};

struct NamedMpmcState {
  static constexpr std::uint32_t kInvalidSubscriber =
      channel::SharedMpmcChannel<>::kInvalidSubscriber;

  // 接管命名 MPMC 资源，并记录当前端是否拥有 shm 名称。
  NamedMpmcState(ControlMapping control_value, channel::SharedMpmcChannel<> channel_value,
                 std::string control_name_value, std::string ring_name_value,
                 bool owns_names_value) noexcept
      : control(std::move(control_value)),
        channel(std::move(channel_value)),
        control_name(std::move(control_name_value)),
        ring_name(std::move(ring_name_value)),
        owns_names(owns_names_value) {}

  // 转移命名 MPMC 资源所有权，并清除来源对象的 unlink 所有权。
  NamedMpmcState(NamedMpmcState&& other) noexcept
      : control(std::move(other.control)),
        channel(std::move(other.channel)),
        control_name(std::move(other.control_name)),
        ring_name(std::move(other.ring_name)),
        owns_names(other.owns_names) {
    other.owns_names = false;
  }
  // 释放当前持有名称后，转移命名 MPMC 资源所有权。
  NamedMpmcState& operator=(NamedMpmcState&& other) noexcept {
    if (this != &other) {
      cleanup();
      control = std::move(other.control);
      channel = std::move(other.channel);
      control_name = std::move(other.control_name);
      ring_name = std::move(other.ring_name);
      owns_names = other.owns_names;
      other.owns_names = false;
    }
    return *this;
  }
  // 禁止拷贝；状态持有映射和 shm 名称生命周期。
  NamedMpmcState(const NamedMpmcState&) = delete;
  // 禁止拷贝赋值；状态持有映射和 shm 名称生命周期。
  NamedMpmcState& operator=(const NamedMpmcState&) = delete;
  // unlink 当前持有的名称，但保留已有对端映射有效。
  ~NamedMpmcState() noexcept { cleanup(); }

  // 为底层 shared MPMC 通道创建生产者端点。
  auto tx() noexcept { return channel.tx(); }
  // 分配 fanout 订阅者槽位。
  auto subscribe_index() noexcept { return channel.subscribe_index(); }
  // 为已有订阅者索引创建订阅端点。
  auto rx(std::uint32_t index) noexcept { return channel.rx(index); }

  ControlMapping control;
  channel::SharedMpmcChannel<> channel;
  std::string control_name;
  std::string ring_name;
  bool owns_names = false;

 private:
  // 当当前状态拥有名称时 unlink 命名共享内存对象。
  void cleanup() noexcept {
    if (owns_names) {
      static_cast<void>(::shm_unlink(control_name.c_str()));
      static_cast<void>(::shm_unlink(ring_name.c_str()));
      owns_names = false;
    }
  }
};

namespace {

// 从命名控制块构造 flow 位置指针。
flow::Positions named_positions(NamedControl& control) noexcept {
  return flow::Positions{
      .producer = &control.producer_pos,
      .consumer = &control.consumer_pos,
      .cap = static_cast<std::size_t>(control.capacity),
  };
}

channel::SharedMpmcChannel<>::SubscriberHeads named_subscriber_heads(
    NamedControl& control) noexcept {
  channel::SharedMpmcChannel<>::SubscriberHeads heads{};
  for (std::size_t i = 0; i < heads.size(); ++i) {
    heads[i] = &control.subscriber_heads[i].value;
  }
  return heads;
}

// 创建命名 SPSC 拥有者端，并发布 ready 元数据。
Result<NamedSpscState> create_named_spsc_state(const Config& config) {
  if (!is_valid_channel_name(config.name) || config.mode != Mode::Spsc ||
      !is_valid_ring_capacity(config.capacity)) {
    return std::unexpected(Error::BadConfig);
  }

  const std::string control_name = control_shm_name(config.name);
  const std::string ring_name = ring_shm_name(config.name);

  auto control_mapping = ControlMapping::create(control_name);
  if (!control_mapping) {
    return std::unexpected(control_mapping.error());
  }

  auto ring = create_named_ring(ring_name, config.capacity);
  if (!ring) {
    static_cast<void>(::shm_unlink(control_name.c_str()));
    static_cast<void>(::shm_unlink(ring_name.c_str()));
    return std::unexpected(ring.error());
  }

  NamedControl* control = control_mapping.value().control();
  control->magic = kNamedMagic;
  control->version = kNamedVersion;
  control->mode = static_cast<std::uint32_t>(Mode::Spsc);
  control->flags = 0;
  control->capacity = config.capacity;
  control->record_size = 0;
  control->wait_word = 0;
  control->producer_pos = 0;
  control->consumer_pos = 0;

  channel::SharedSpscChannel<> channel(std::move(ring).value(), named_positions(*control),
                                       &control->wait_word);

  // 安全性：上面已初始化所有 ChannelMeta/control 字段和共享位置单元。
  // 对端在信任本进程写入的字段前会 acquire-load ready。
  std::atomic_ref<std::uint32_t>(control->ready).store(1, std::memory_order_release);

  return NamedSpscState(std::move(control_mapping).value(), std::move(channel), control_name,
                        ring_name, true);
}

Result<NamedMpscState> create_named_mpsc_state(const Config& config) {
  if (!is_valid_channel_name(config.name) || config.mode != Mode::Mpsc ||
      !is_valid_ring_capacity(config.capacity)) {
    return std::unexpected(Error::BadConfig);
  }

  const std::string control_name = control_shm_name(config.name);
  const std::string ring_name = ring_shm_name(config.name);

  auto control_mapping = ControlMapping::create(control_name);
  if (!control_mapping) {
    return std::unexpected(control_mapping.error());
  }

  auto ring = create_named_ring(ring_name, config.capacity);
  if (!ring) {
    static_cast<void>(::shm_unlink(control_name.c_str()));
    static_cast<void>(::shm_unlink(ring_name.c_str()));
    return std::unexpected(ring.error());
  }

  NamedControl* control = control_mapping.value().control();
  control->magic = kNamedMagic;
  control->version = kNamedVersion;
  control->mode = static_cast<std::uint32_t>(Mode::Mpsc);
  control->flags = 0;
  control->capacity = config.capacity;
  control->record_size = 0;
  control->wait_word = 0;
  control->producer_pos = 0;  // MPSC reserved tail.
  control->consumer_pos = 0;

  channel::SharedMpscChannel<> channel(std::move(ring).value(), &control->producer_pos,
                                       &control->consumer_pos, &control->wait_word);

  // 安全性：上面已初始化所有 control 字段和共享位置单元。
  // 对端在信任本进程写入的字段前会 acquire-load ready。
  std::atomic_ref<std::uint32_t>(control->ready).store(1, std::memory_order_release);

  return NamedMpscState(std::move(control_mapping).value(), std::move(channel), control_name,
                        ring_name, true);
}

Result<NamedMpmcState> create_named_mpmc_state(const Config& config) {
  if (!is_valid_channel_name(config.name) || config.mode != Mode::Mpmc ||
      !is_valid_ring_capacity(config.capacity)) {
    return std::unexpected(Error::BadConfig);
  }

  const std::string control_name = control_shm_name(config.name);
  const std::string ring_name = ring_shm_name(config.name);

  auto control_mapping = ControlMapping::create(control_name);
  if (!control_mapping) {
    return std::unexpected(control_mapping.error());
  }

  auto ring = create_named_ring(ring_name, config.capacity);
  if (!ring) {
    static_cast<void>(::shm_unlink(control_name.c_str()));
    static_cast<void>(::shm_unlink(ring_name.c_str()));
    return std::unexpected(ring.error());
  }

  NamedControl* control = control_mapping.value().control();
  control->magic = kNamedMagic;
  control->version = kNamedVersion;
  control->mode = static_cast<std::uint32_t>(Mode::Mpmc);
  control->flags = 0;
  control->capacity = config.capacity;
  control->record_size = 0;
  control->wait_word = 0;
  control->producer_pos = 0;  // MPMC reserved tail.
  control->consumer_pos = 0;
  control->subscriber_count = 0;
  for (auto& head : control->subscriber_heads) {
    head.value = 0;
  }

  channel::SharedMpmcChannel<> channel(std::move(ring).value(), &control->producer_pos,
                                       &control->subscriber_count,
                                       named_subscriber_heads(*control), &control->wait_word);

  // 安全性：上面已初始化所有 control 字段和共享 fanout 位置单元。
  // 对端在信任本进程写入的字段前会 acquire-load ready。
  std::atomic_ref<std::uint32_t>(control->ready).store(1, std::memory_order_release);

  return NamedMpmcState(std::move(control_mapping).value(), std::move(channel), control_name,
                        ring_name, true);
}

// 有限等待控制块和 ready 元数据后连接命名 SPSC 拥有者。
Result<NamedSpscState> connect_named_spsc_state(std::string_view name) {
  if (!is_valid_channel_name(name)) {
    return std::unexpected(Error::BadConfig);
  }

  const std::string control_name = control_shm_name(name);
  const std::string ring_name = ring_shm_name(name);
  std::optional<ControlMapping> control_mapping;
  Error open_error = Error::NotFound;
  for (int attempt = 0; attempt < 100000; ++attempt) {
    auto opened = ControlMapping::open(control_name);
    if (opened) {
      control_mapping.emplace(std::move(opened).value());
      break;
    }
    open_error = opened.error();
    if (open_error != Error::NotFound) {
      return std::unexpected(open_error);
    }
    std::this_thread::yield();
  }
  if (!control_mapping.has_value()) {
    return std::unexpected(open_error);
  }

  NamedControl* control = control_mapping->control();
  bool ready = false;
  for (int attempt = 0; attempt < 100000; ++attempt) {
    // 安全性：拥有者初始化所有字段后以 release 语义发布 ready。
    // 这里的 acquire load 防止对端读到部分初始化的 ChannelMeta/control 块。
    if (std::atomic_ref<std::uint32_t>(control->ready).load(std::memory_order_acquire) == 1) {
      ready = true;
      break;
    }
    std::this_thread::yield();
  }
  if (!ready) {
    return std::unexpected(Error::NotFound);
  }

  if (control->magic != kNamedMagic || control->version != kNamedVersion) {
    return std::unexpected(Error::VersionMismatch);
  }
  if (control->mode != static_cast<std::uint32_t>(Mode::Spsc) ||
      !is_valid_ring_capacity(control->capacity) || control->record_size != 0) {
    return std::unexpected(Error::BadConfig);
  }

  auto ring = open_named_ring(ring_name, static_cast<std::size_t>(control->capacity));
  if (!ring) {
    return std::unexpected(ring.error());
  }

  channel::SharedSpscChannel<> channel(std::move(ring).value(), named_positions(*control),
                                       &control->wait_word);
  return NamedSpscState(std::move(*control_mapping), std::move(channel), control_name, ring_name,
                        false);
}

// 有限等待控制块和 ready 元数据后连接命名 MPSC 拥有者。
Result<NamedMpscState> connect_named_mpsc_state(std::string_view name) {
  if (!is_valid_channel_name(name)) {
    return std::unexpected(Error::BadConfig);
  }

  const std::string control_name = control_shm_name(name);
  const std::string ring_name = ring_shm_name(name);
  std::optional<ControlMapping> control_mapping;
  Error open_error = Error::NotFound;
  for (int attempt = 0; attempt < 100000; ++attempt) {
    auto opened = ControlMapping::open(control_name);
    if (opened) {
      control_mapping.emplace(std::move(opened).value());
      break;
    }
    open_error = opened.error();
    if (open_error != Error::NotFound) {
      return std::unexpected(open_error);
    }
    std::this_thread::yield();
  }
  if (!control_mapping.has_value()) {
    return std::unexpected(open_error);
  }

  NamedControl* control = control_mapping->control();
  bool ready = false;
  for (int attempt = 0; attempt < 100000; ++attempt) {
    if (std::atomic_ref<std::uint32_t>(control->ready).load(std::memory_order_acquire) == 1) {
      ready = true;
      break;
    }
    std::this_thread::yield();
  }
  if (!ready) {
    return std::unexpected(Error::NotFound);
  }

  if (control->magic != kNamedMagic || control->version != kNamedVersion) {
    return std::unexpected(Error::VersionMismatch);
  }
  if (control->mode != static_cast<std::uint32_t>(Mode::Mpsc) ||
      !is_valid_ring_capacity(control->capacity) || control->record_size != 0) {
    return std::unexpected(Error::BadConfig);
  }

  auto ring = open_named_ring(ring_name, static_cast<std::size_t>(control->capacity));
  if (!ring) {
    return std::unexpected(ring.error());
  }

  channel::SharedMpscChannel<> channel(std::move(ring).value(), &control->producer_pos,
                                       &control->consumer_pos, &control->wait_word);
  return NamedMpscState(std::move(*control_mapping), std::move(channel), control_name, ring_name,
                        false);
}

Result<NamedMpmcState> connect_named_mpmc_state(std::string_view name) {
  if (!is_valid_channel_name(name)) {
    return std::unexpected(Error::BadConfig);
  }

  const std::string control_name = control_shm_name(name);
  const std::string ring_name = ring_shm_name(name);
  std::optional<ControlMapping> control_mapping;
  Error open_error = Error::NotFound;
  for (int attempt = 0; attempt < 100000; ++attempt) {
    auto opened = ControlMapping::open(control_name);
    if (opened) {
      control_mapping.emplace(std::move(opened).value());
      break;
    }
    open_error = opened.error();
    if (open_error != Error::NotFound) {
      return std::unexpected(open_error);
    }
    std::this_thread::yield();
  }
  if (!control_mapping.has_value()) {
    return std::unexpected(open_error);
  }

  NamedControl* control = control_mapping->control();
  bool ready = false;
  for (int attempt = 0; attempt < 100000; ++attempt) {
    if (std::atomic_ref<std::uint32_t>(control->ready).load(std::memory_order_acquire) == 1) {
      ready = true;
      break;
    }
    std::this_thread::yield();
  }
  if (!ready) {
    return std::unexpected(Error::NotFound);
  }

  if (control->magic != kNamedMagic || control->version != kNamedVersion) {
    return std::unexpected(Error::VersionMismatch);
  }
  if (control->mode != static_cast<std::uint32_t>(Mode::Mpmc) ||
      !is_valid_ring_capacity(control->capacity) || control->record_size != 0) {
    return std::unexpected(Error::BadConfig);
  }

  auto ring = open_named_ring(ring_name, static_cast<std::size_t>(control->capacity));
  if (!ring) {
    return std::unexpected(ring.error());
  }

  channel::SharedMpmcChannel<> channel(std::move(ring).value(), &control->producer_pos,
                                       &control->subscriber_count,
                                       named_subscriber_heads(*control), &control->wait_word);
  return NamedMpmcState(std::move(*control_mapping), std::move(channel), control_name, ring_name,
                        false);
}

}  // namespace

struct ChannelState {
  using Storage = std::variant<channel::SpscChannel<>, channel::MpscChannel<>,
                               channel::BroadcastChannel<>, channel::BulkChannel<>,
                               NamedSpscState, NamedMpscState, NamedMpmcState>;

  // 将任意受支持核心通道实现存入外观层状态 variant。
  template <class CoreChannel>
  explicit ChannelState(CoreChannel channel_value) noexcept : channel(std::move(channel_value)) {}

  Storage channel;
};

// 保存公共通道句柄共享状态。
Channel::Channel(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}

// 根据公共配置创建进程内或命名通道。
Result<Channel> Channel::create(const Config& config) {
  if (config.fixed_size || config.record_size != 0 || config.wait != WaitKind::SpinPause) {
    return std::unexpected(Error::BadConfig);
  }

  if (!config.name.empty()) {
    switch (config.mode) {
      case Mode::Spsc: {
        auto named = create_named_spsc_state(config);
        if (!named) {
          return std::unexpected(named.error());
        }
        return Channel(std::make_shared<ChannelState>(std::move(named).value()));
      }
      case Mode::Mpsc: {
        auto named = create_named_mpsc_state(config);
        if (!named) {
          return std::unexpected(named.error());
        }
        return Channel(std::make_shared<ChannelState>(std::move(named).value()));
      }
      case Mode::Mpmc: {
        auto named = create_named_mpmc_state(config);
        if (!named) {
          return std::unexpected(named.error());
        }
        return Channel(std::make_shared<ChannelState>(std::move(named).value()));
      }
      case Mode::Broadcast:
      case Mode::Bulk:
        return std::unexpected(Error::BadConfig);
    }
  }

  auto create_core = [&config]<class CoreChannel>() -> Result<Channel> {
    auto created = CoreChannel::create(core_config(config));
    if (!created) {
      return std::unexpected(map_channel_error(created.error()));
    }
    return Channel(std::make_shared<ChannelState>(std::move(created).value()));
  };

  switch (config.mode) {
    case Mode::Spsc:
      return create_core.template operator()<channel::SpscChannel<>>();
    case Mode::Mpsc:
      return create_core.template operator()<channel::MpscChannel<>>();
    case Mode::Broadcast:
      return create_core.template operator()<channel::BroadcastChannel<>>();
    case Mode::Bulk:
      return create_core.template operator()<channel::BulkChannel<>>();
    case Mode::Mpmc:
      return std::unexpected(Error::BadConfig);
  }
  return std::unexpected(Error::BadConfig);
}

// 连接已有命名 SPSC 通道。
Result<Channel> Channel::connect(std::string_view name) {
  auto named = connect_named_spsc_state(name);
  if (named) {
    return Channel(std::make_shared<ChannelState>(std::move(named).value()));
  }
  if (named.error() != Error::BadConfig) {
    return std::unexpected(named.error());
  }

  auto mpsc = connect_named_mpsc_state(name);
  if (mpsc) {
    return Channel(std::make_shared<ChannelState>(std::move(mpsc).value()));
  }
  if (mpsc.error() != Error::BadConfig) {
    return std::unexpected(mpsc.error());
  }

  auto mpmc = connect_named_mpmc_state(name);
  if (!mpmc) {
    return std::unexpected(mpmc.error());
  }
  return Channel(std::make_shared<ChannelState>(std::move(mpmc).value()));
}

// 创建共享当前通道状态的发布外观。
Publisher Channel::publisher() noexcept {
  return Publisher(state_);
}

// 创建订阅外观；broadcast 模式下会分配订阅索引。
Subscriber Channel::subscriber() noexcept {
  std::uint32_t subscription_index = 0;
  std::visit(
      [&subscription_index](auto& core_channel) {
        using Core = std::decay_t<decltype(core_channel)>;
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>> ||
                      std::is_same_v<Core, NamedMpmcState>) {
          auto index = core_channel.subscribe_index();
          subscription_index = index.value_or(Core::kInvalidSubscriber);
        }
      },
      state_->channel);
  return Subscriber(state_, subscription_index);
}

// 保存发布端使用的共享通道状态。
Publisher::Publisher(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}

// 创建绑定到共享通道状态的公共发布 claim。
PublishClaim::PublishClaim(std::shared_ptr<ChannelState> state, std::span<std::byte> payload,
                           std::uint64_t start_pos, std::uint32_t payload_len,
                           std::uint32_t meta) noexcept
    : state_(std::move(state)),
      payload_(payload),
      start_pos_(start_pos),
      payload_len_(payload_len),
      meta_(meta),
      committed_(false) {}

// 转移 claim 所有权，并使来源对象不能再提交。
PublishClaim::PublishClaim(PublishClaim&& other) noexcept
    : state_(std::move(other.state_)),
      payload_(other.payload_),
      start_pos_(other.start_pos_),
      payload_len_(other.payload_len_),
      meta_(other.meta_),
      committed_(other.committed_) {
  other.payload_ = {};
  other.committed_ = true;
}

// 丢弃当前 claim 句柄后，转移另一个 claim 的提交权。
PublishClaim& PublishClaim::operator=(PublishClaim&& other) noexcept {
  if (this != &other) {
    state_ = std::move(other.state_);
    payload_ = other.payload_;
    start_pos_ = other.start_pos_;
    payload_len_ = other.payload_len_;
    meta_ = other.meta_;
    committed_ = other.committed_;
    other.payload_ = {};
    other.committed_ = true;
  }
  return *this;
}

// 返回 claim 预留的可写 payload 区域。
std::span<std::byte> PublishClaim::payload() noexcept { return payload_; }

// 通过当前激活的核心通道发布 claim。
void PublishClaim::commit() noexcept {
  if (committed_ || !state_) {
    return;
  }

  flow::Claim claim{
      .payload = payload_,
      .start_pos = start_pos_,
      .payload_len = payload_len_,
      .meta = meta_,
  };
  std::visit(
      [&claim](auto& core_channel) {
        auto tx = core_channel.tx();
        tx.commit(claim);
      },
      state_->channel);

  payload_ = {};
  committed_ = true;
  state_.reset();
}

// 通过当前激活的核心通道预留一条可原地写入的 payload。
Result<PublishClaim> Publisher::try_claim(std::size_t payload_len) noexcept {
  if (payload_len > std::numeric_limits<std::uint32_t>::max()) {
    return std::unexpected(Error::MessageTooLarge);
  }

  std::optional<flow::FlowError> error;
  std::optional<flow::Claim> claim;
  const auto len = static_cast<std::uint32_t>(payload_len);
  std::visit(
      [len, &error, &claim](auto& core_channel) {
        auto tx = core_channel.tx();
        auto claimed = tx.claim(len);
        if (!claimed) {
          error = claimed.error();
          return;
        }
        claim = claimed.value();
      },
      state_->channel);

  if (error.has_value()) {
    return std::unexpected(map_flow_error(*error));
  }
  return PublishClaim(state_, claim->payload, claim->start_pos, claim->payload_len, claim->meta);
}

// 通过当前激活的核心通道实现发布一条 payload。
Result<bool> Publisher::offer(std::span<const std::byte> payload) noexcept {
  std::optional<flow::FlowError> error;
  bool value = false;
  std::visit(
      [&payload, &error, &value](auto& core_channel) {
        auto tx = core_channel.tx();
        auto offered = tx.offer(payload);
        if (!offered) {
          error = offered.error();
          return;
        }
        value = offered.value();
      },
      state_->channel);
  if (error.has_value()) {
    return std::unexpected(map_flow_error(*error));
  }
  return value;
}

// 保存共享通道状态和可选 broadcast 订阅索引。
Subscriber::Subscriber(std::shared_ptr<ChannelState> state,
                       std::uint32_t subscription_index) noexcept
    : state_(std::move(state)), subscription_index_(subscription_index) {}

// 通过当前激活的核心通道实现非阻塞接收一条消息。
std::optional<Message> Subscriber::try_recv() noexcept {
  std::optional<flow::Message> message;
  std::visit(
      [this, &message](auto& core_channel) {
        using Core = std::decay_t<decltype(core_channel)>;
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>> ||
                      std::is_same_v<Core, NamedMpmcState>) {
          auto rx = core_channel.rx(subscription_index_);
          message = rx.try_recv();
        } else {
          auto rx = core_channel.rx();
          message = rx.try_recv();
        }
      },
      state_->channel);
  if (!message) {
    return std::nullopt;
  }
  return to_public_message(*message);
}

// 批量轮询当前激活的核心通道，并在回调后释放每条消息。
std::size_t Subscriber::poll(std::uint32_t max_messages, PollCallback callback,
                             void* user) noexcept {
  if (max_messages == 0 || callback == nullptr) {
    return 0;
  }

  std::size_t consumed = 0;
  std::visit(
      [this, callback, user, max_messages, &consumed](auto& core_channel) {
        using Core = std::decay_t<decltype(core_channel)>;
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>> ||
                      std::is_same_v<Core, NamedMpmcState>) {
          auto rx = core_channel.rx(subscription_index_);
          while (consumed < max_messages) {
            auto message = rx.try_recv();
            if (!message) {
              return;
            }
            const Message public_message = to_public_message(*message);
            callback(public_message, user);
            rx.release(*message);
            ++consumed;
          }
        } else {
          auto rx = core_channel.rx();
          while (consumed < max_messages) {
            auto message = rx.try_recv();
            if (!message) {
              return;
            }
            const Message public_message = to_public_message(*message);
            callback(public_message, user);
            rx.release(*message);
            ++consumed;
          }
        }
      },
      state_->channel);
  return consumed;
}

// 通过当前激活的核心通道实现等待接收一条消息。
Message Subscriber::recv() noexcept {
  std::optional<flow::Message> message;
  std::visit(
      [this, &message](auto& core_channel) {
        using Core = std::decay_t<decltype(core_channel)>;
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>> ||
                      std::is_same_v<Core, NamedMpmcState>) {
          auto rx = core_channel.rx(subscription_index_);
          message = rx.recv();
        } else {
          auto rx = core_channel.rx();
          message = rx.recv();
        }
      },
      state_->channel);
  return to_public_message(*message);
}

// 通过当前激活的核心通道实现释放一条消息。
void Subscriber::release(const Message& message) noexcept {
  const flow::Message flow_message = to_flow_message(message);
  std::visit(
      [this, &flow_message](auto& core_channel) {
        using Core = std::decay_t<decltype(core_channel)>;
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>> ||
                      std::is_same_v<Core, NamedMpmcState>) {
          auto rx = core_channel.rx(subscription_index_);
          rx.release(flow_message);
        } else {
          auto rx = core_channel.rx();
          rx.release(flow_message);
        }
      },
      state_->channel);
}

}  // namespace salias

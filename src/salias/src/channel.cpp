#include "salias/channel.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
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
#include "core/channel/shared_spsc.hpp"
#include "core/channel/spsc.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias {

namespace {

inline constexpr std::uint32_t kNamedMagic = 0x53414C43u;  // "SALC"
inline constexpr std::uint32_t kNamedVersion = 1;
inline constexpr std::size_t kControlSize = 4096;

struct alignas(64) NamedSpscControl {
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

static_assert(alignof(NamedSpscControl) == 64);
static_assert(offsetof(NamedSpscControl, producer_pos) % 64 == 0);
static_assert(offsetof(NamedSpscControl, consumer_pos) % 64 == 0);

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

  // 返回固定的命名 SPSC 控制块前缀。
  NamedSpscControl* control() noexcept {
    // 安全性：ControlMapping 只映射 kControlSize 大小的 shm 对象。
    // 固定前缀 ABI 是 NamedSpscControl，由拥有者在发布 ready 前写入。
    return reinterpret_cast<NamedSpscControl*>(base_);
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

namespace {

// 从命名控制块构造 flow 位置指针。
flow::Positions named_positions(NamedSpscControl& control) noexcept {
  return flow::Positions{
      .producer = &control.producer_pos,
      .consumer = &control.consumer_pos,
      .cap = static_cast<std::size_t>(control.capacity),
  };
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

  NamedSpscControl* control = control_mapping.value().control();
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

  NamedSpscControl* control = control_mapping->control();
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

}  // namespace

struct ChannelState {
  using Storage = std::variant<channel::SpscChannel<>, channel::MpscChannel<>,
                               channel::BroadcastChannel<>, channel::BulkChannel<>,
                               NamedSpscState>;

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
    auto named = create_named_spsc_state(config);
    if (!named) {
      return std::unexpected(named.error());
    }
    return Channel(std::make_shared<ChannelState>(std::move(named).value()));
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
  }
  return std::unexpected(Error::BadConfig);
}

// 连接已有命名 SPSC 通道。
Result<Channel> Channel::connect(std::string_view name) {
  auto named = connect_named_spsc_state(name);
  if (!named) {
    return std::unexpected(named.error());
  }
  return Channel(std::make_shared<ChannelState>(std::move(named).value()));
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
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>>) {
          auto index = core_channel.subscribe_index();
          subscription_index = index.value_or(Core::kInvalidSubscriber);
        }
      },
      state_->channel);
  return Subscriber(state_, subscription_index);
}

// 保存发布端使用的共享通道状态。
Publisher::Publisher(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}

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
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>>) {
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

// 通过当前激活的核心通道实现等待接收一条消息。
Message Subscriber::recv() noexcept {
  std::optional<flow::Message> message;
  std::visit(
      [this, &message](auto& core_channel) {
        using Core = std::decay_t<decltype(core_channel)>;
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>>) {
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
        if constexpr (std::is_same_v<Core, channel::BroadcastChannel<>>) {
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

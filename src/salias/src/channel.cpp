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

bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

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

std::string control_shm_name(std::string_view name) {
  return "/salias-" + std::string(name) + "-ctl";
}

std::string ring_shm_name(std::string_view name) {
  return "/salias-" + std::string(name) + "-ring";
}

void close_if_open(int fd) noexcept {
  if (fd >= 0) {
    static_cast<void>(::close(fd));
  }
}

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
  static Result<ControlMapping> create(const std::string& name) noexcept {
    const int fd = create_sized_shm(name, kControlSize);
    if (fd < 0) {
      return Result<ControlMapping>::failure(Error::PlatformFail);
    }
    return map_fd(fd);
  }

  static Result<ControlMapping> open(const std::string& name) noexcept {
    const int fd = ::shm_open(name.c_str(), O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
      return Result<ControlMapping>::failure(Error::NotFound);
    }
    return map_fd(fd);
  }

  ControlMapping() noexcept = default;
  ControlMapping(ControlMapping&& other) noexcept
      : base_(other.base_), len_(other.len_), fd_(other.fd_) {
    other.base_ = nullptr;
    other.len_ = 0;
    other.fd_ = -1;
  }
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
  ControlMapping(const ControlMapping&) = delete;
  ControlMapping& operator=(const ControlMapping&) = delete;
  ~ControlMapping() noexcept { reset(); }

  NamedSpscControl* control() noexcept {
    // SAFETY: ControlMapping only maps kControlSize shm objects, and NamedSpscControl is the fixed
    // prefix ABI written by the owner before ready publication.
    return reinterpret_cast<NamedSpscControl*>(base_);
  }

 private:
  static Result<ControlMapping> map_fd(int fd) noexcept {
    // SAFETY: fd refers to a shm object truncated to kControlSize by create_sized_shm() or by the
    // owner before ready publication. MAP_SHARED makes the fixed control ABI visible to both sides.
    void* mapped = ::mmap(nullptr, kControlSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
      close_if_open(fd);
      return Result<ControlMapping>::failure(Error::PlatformFail);
    }
    return Result<ControlMapping>::success(
        ControlMapping(static_cast<std::byte*>(mapped), kControlSize, fd));
  }

  ControlMapping(std::byte* base, std::size_t len, int fd) noexcept
      : base_(base), len_(len), fd_(fd) {}

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

channel::ChannelConfig core_config(const Config& config) noexcept {
  return channel::ChannelConfig{
      .capacity = config.capacity,
      .fixed_size = config.fixed_size,
      .record_size = config.record_size,
  };
}

Message to_public_message(const flow::Message& message) noexcept {
  return Message{
      .payload = message.payload,
      .position = message.position,
      .next_position = message.next_position,
      .meta = message.meta,
  };
}

flow::Message to_flow_message(const Message& message) noexcept {
  return flow::Message{
      .payload = message.payload,
      .position = message.position,
      .next_position = message.next_position,
      .meta = message.meta,
  };
}

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

Result<ring::MagicRing> map_ring_from_fd(int fd, std::size_t capacity) noexcept {
  auto mapped = platform::Mapping::map_shared_fd(fd, platform::MapOptions{.size = capacity});
  close_if_open(fd);
  if (!mapped) {
    return Result<ring::MagicRing>::failure(map_platform_error(mapped.error()));
  }

  auto ring = ring::MagicRing::create(std::move(mapped).value());
  if (!ring) {
    return Result<ring::MagicRing>::failure(Error::PlatformFail);
  }
  return Result<ring::MagicRing>::success(std::move(ring).value());
}

Result<ring::MagicRing> create_named_ring(const std::string& name,
                                          std::size_t capacity) noexcept {
  const int fd = create_sized_shm(name, capacity);
  if (fd < 0) {
    return Result<ring::MagicRing>::failure(Error::PlatformFail);
  }
  return map_ring_from_fd(fd, capacity);
}

Result<ring::MagicRing> open_named_ring(const std::string& name,
                                        std::size_t capacity) noexcept {
  const int fd = ::shm_open(name.c_str(), O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    return Result<ring::MagicRing>::failure(Error::NotFound);
  }
  return map_ring_from_fd(fd, capacity);
}

}  // namespace

struct NamedSpscState {
  NamedSpscState(ControlMapping control_value, channel::SharedSpscChannel<> channel_value,
                 std::string control_name_value, std::string ring_name_value,
                 bool owns_names_value) noexcept
      : control(std::move(control_value)),
        channel(std::move(channel_value)),
        control_name(std::move(control_name_value)),
        ring_name(std::move(ring_name_value)),
        owns_names(owns_names_value) {}

  NamedSpscState(NamedSpscState&& other) noexcept
      : control(std::move(other.control)),
        channel(std::move(other.channel)),
        control_name(std::move(other.control_name)),
        ring_name(std::move(other.ring_name)),
        owns_names(other.owns_names) {
    other.owns_names = false;
  }
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
  NamedSpscState(const NamedSpscState&) = delete;
  NamedSpscState& operator=(const NamedSpscState&) = delete;
  ~NamedSpscState() noexcept { cleanup(); }

  auto tx() noexcept { return channel.tx(); }
  auto rx() noexcept { return channel.rx(); }

  ControlMapping control;
  channel::SharedSpscChannel<> channel;
  std::string control_name;
  std::string ring_name;
  bool owns_names = false;

 private:
  void cleanup() noexcept {
    if (owns_names) {
      // SAFETY: the owner unlinks only the names it created. Existing peer mappings remain valid
      // after shm_unlink; the names simply disappear for future connect() calls.
      static_cast<void>(::shm_unlink(control_name.c_str()));
      static_cast<void>(::shm_unlink(ring_name.c_str()));
      owns_names = false;
    }
  }
};

namespace {

flow::Positions named_positions(NamedSpscControl& control) noexcept {
  return flow::Positions{
      .producer = &control.producer_pos,
      .consumer = &control.consumer_pos,
      .cap = static_cast<std::size_t>(control.capacity),
  };
}

Result<NamedSpscState> create_named_spsc_state(const Config& config) {
  if (!is_valid_channel_name(config.name) || config.mode != Mode::Spsc ||
      !is_valid_ring_capacity(config.capacity)) {
    return Result<NamedSpscState>::failure(Error::BadConfig);
  }

  const std::string control_name = control_shm_name(config.name);
  const std::string ring_name = ring_shm_name(config.name);

  auto control_mapping = ControlMapping::create(control_name);
  if (!control_mapping) {
    return Result<NamedSpscState>::failure(control_mapping.error());
  }

  auto ring = create_named_ring(ring_name, config.capacity);
  if (!ring) {
    static_cast<void>(::shm_unlink(control_name.c_str()));
    static_cast<void>(::shm_unlink(ring_name.c_str()));
    return Result<NamedSpscState>::failure(ring.error());
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

  // SAFETY: all ChannelMeta/control fields and shared position cells are initialized above. The
  // peer acquire-loads ready before trusting any field from this process.
  std::atomic_ref<std::uint32_t>(control->ready).store(1, std::memory_order_release);

  return Result<NamedSpscState>::success(NamedSpscState(std::move(control_mapping).value(),
                                                        std::move(channel), control_name,
                                                        ring_name, true));
}

Result<NamedSpscState> connect_named_spsc_state(std::string_view name) {
  if (!is_valid_channel_name(name)) {
    return Result<NamedSpscState>::failure(Error::BadConfig);
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
      return Result<NamedSpscState>::failure(open_error);
    }
    std::this_thread::yield();
  }
  if (!control_mapping.has_value()) {
    return Result<NamedSpscState>::failure(open_error);
  }

  NamedSpscControl* control = control_mapping->control();
  bool ready = false;
  for (int attempt = 0; attempt < 100000; ++attempt) {
    // SAFETY: owner publishes ready with release after initializing all fields. This acquire load
    // prevents the peer from reading a partially initialized ChannelMeta/control block.
    if (std::atomic_ref<std::uint32_t>(control->ready).load(std::memory_order_acquire) == 1) {
      ready = true;
      break;
    }
    std::this_thread::yield();
  }
  if (!ready) {
    return Result<NamedSpscState>::failure(Error::NotFound);
  }

  if (control->magic != kNamedMagic || control->version != kNamedVersion) {
    return Result<NamedSpscState>::failure(Error::VersionMismatch);
  }
  if (control->mode != static_cast<std::uint32_t>(Mode::Spsc) ||
      !is_valid_ring_capacity(control->capacity) || control->record_size != 0) {
    return Result<NamedSpscState>::failure(Error::BadConfig);
  }

  auto ring = open_named_ring(ring_name, static_cast<std::size_t>(control->capacity));
  if (!ring) {
    return Result<NamedSpscState>::failure(ring.error());
  }

  channel::SharedSpscChannel<> channel(std::move(ring).value(), named_positions(*control),
                                       &control->wait_word);
  return Result<NamedSpscState>::success(NamedSpscState(std::move(*control_mapping),
                                                        std::move(channel), control_name,
                                                        ring_name, false));
}

}  // namespace

struct ChannelState {
  using Storage = std::variant<channel::SpscChannel<>, channel::MpscChannel<>,
                               channel::BroadcastChannel<>, channel::BulkChannel<>,
                               NamedSpscState>;

  template <class CoreChannel>
  explicit ChannelState(CoreChannel channel_value) noexcept : channel(std::move(channel_value)) {}

  Storage channel;
};

Channel::Channel(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}

Result<Channel> Channel::create(const Config& config) {
  if (config.fixed_size || config.record_size != 0 || config.wait != WaitKind::SpinPause) {
    return Result<Channel>::failure(Error::BadConfig);
  }

  if (!config.name.empty()) {
    auto named = create_named_spsc_state(config);
    if (!named) {
      return Result<Channel>::failure(named.error());
    }
    return Result<Channel>::success(
        Channel(std::make_shared<ChannelState>(std::move(named).value())));
  }

  auto create_core = [&config]<class CoreChannel>() -> Result<Channel> {
    auto created = CoreChannel::create(core_config(config));
    if (!created) {
      return Result<Channel>::failure(map_channel_error(created.error()));
    }
    return Result<Channel>::success(
        Channel(std::make_shared<ChannelState>(std::move(created).value())));
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
  return Result<Channel>::failure(Error::BadConfig);
}

Result<Channel> Channel::connect(std::string_view name) {
  auto named = connect_named_spsc_state(name);
  if (!named) {
    return Result<Channel>::failure(named.error());
  }
  return Result<Channel>::success(
      Channel(std::make_shared<ChannelState>(std::move(named).value())));
}

Publisher Channel::publisher() noexcept {
  return Publisher(state_);
}

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

Publisher::Publisher(std::shared_ptr<ChannelState> state) noexcept : state_(std::move(state)) {}

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
    return Result<bool>::failure(map_flow_error(*error));
  }
  return Result<bool>::success(value);
}

Subscriber::Subscriber(std::shared_ptr<ChannelState> state,
                       std::uint32_t subscription_index) noexcept
    : state_(std::move(state)), subscription_index_(subscription_index) {}

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

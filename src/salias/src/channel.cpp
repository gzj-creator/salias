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

namespace {

inline constexpr std::uint32_t kNamedMagic = 0x53414C43u;
inline constexpr std::uint32_t kNamedVersion = 4;
inline constexpr std::size_t kControlSize = 16u * 1024u;
inline constexpr std::size_t kCacheLineGuard = 128;
inline constexpr std::size_t kMaxProducers = 16;
inline constexpr std::size_t kMaxConsumers = 8;
inline constexpr std::size_t kHugePage2MiB = std::size_t{2} * 1024 * 1024;
inline constexpr std::size_t kHugePage1GiB = std::size_t{1024} * 1024 * 1024;
inline constexpr std::uint32_t kNamedFlagHugetlbfs = 1u;
inline constexpr std::uint32_t kNamedHugeShift = 1u;
inline constexpr std::uint32_t kNamedHugeMask = 0x6u;
inline constexpr std::uint32_t kInvalidEndpoint = std::numeric_limits<std::uint32_t>::max();
inline constexpr const char* kDefaultHugetlbfsDir = "/dev/hugepages";
// 发布窗口下限（字节）：非 0 窗口必须不小于一页，避免窗口小于一次 poll 批量导致生产者长期回压。
inline constexpr std::size_t kMinPublicationWindow = 4096;

enum class NamedRingBackend { PosixShm, Hugetlbfs };

struct NamedRingSpec {
  NamedRingBackend backend = NamedRingBackend::PosixShm;
  HugePage huge = HugePage::None;
};

struct alignas(64) NamedPositionSlot {
  std::uint64_t value = 0;
  std::byte pad[64 - sizeof(std::uint64_t)]{};
};

struct alignas(64) NamedConsumerCursorSlot {
  std::uint64_t position = 0;
  std::uint64_t sequence = 0;
  std::byte pad[64 - (2 * sizeof(std::uint64_t))]{};
};

struct alignas(kCacheLineGuard) NamedProducerSlot {
  std::uint64_t visible_producer_pos = 0;
  std::uint64_t local_sequence = 0;
  std::byte pad[kCacheLineGuard - (2 * sizeof(std::uint64_t))]{};
  std::array<NamedConsumerCursorSlot, kMaxConsumers> consumers{};
};

struct alignas(kCacheLineGuard) NamedControl {
  std::uint32_t magic = 0;
  std::uint32_t version = 0;
  std::uint32_t mode = 0;
  std::uint32_t flags = 0;
  std::uint64_t capacity = 0;
  std::uint64_t record_size = 0;
  std::uint32_t ready = 0;
  std::uint32_t wait_word = 0;
  std::uint64_t publication_window = 0;
  std::byte pad[kCacheLineGuard - 48]{};

  alignas(kCacheLineGuard) std::uint64_t global_sequence = 0;
  alignas(kCacheLineGuard) std::uint32_t next_producer = 0;
  alignas(kCacheLineGuard) std::uint32_t num_producers = 0;
  alignas(kCacheLineGuard) std::uint32_t next_consumer = 0;
  alignas(kCacheLineGuard) std::uint32_t num_consumers = 0;
  alignas(kCacheLineGuard) std::array<NamedPositionSlot, kMaxConsumers> consumer_sequences{};
  alignas(kCacheLineGuard) std::array<NamedProducerSlot, kMaxProducers> producers{};
};

static_assert(sizeof(NamedPositionSlot) == 64);
static_assert(sizeof(NamedConsumerCursorSlot) == 64);
static_assert(sizeof(NamedProducerSlot) == kCacheLineGuard + 64 * kMaxConsumers);
static_assert(sizeof(NamedControl) <= kControlSize);

template <Mode M>
inline constexpr channel::Order kOrdering =
    M == Mode::FifoMpsc || M == Mode::FifoFanout ? channel::Order::Fifo : channel::Order::Ordered;

template <Mode M>
inline constexpr bool kFanout = M == Mode::FifoFanout || M == Mode::OrderedFanout;

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

bool is_power_of_two(std::size_t value) noexcept {
  return value != 0 && (value & (value - 1)) == 0;
}

// 发布窗口合法性：0 表示不限流；非 0 时必须落在 [一页, capacity] 之内。
bool is_valid_publication_window(std::uint64_t window, std::uint64_t capacity) noexcept {
  return window == 0 || (window >= kMinPublicationWindow && window <= capacity);
}

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

bool is_valid_huge_capacity(std::size_t capacity, HugePage huge) noexcept {
  const std::size_t page_size = huge_page_size(huge);
  return page_size == 0 || capacity % page_size == 0;
}

std::string control_shm_name(std::string_view name) {
  return "/salias-" + std::string(name) + "-ctl";
}

std::string ring_shm_name(std::string_view name, std::uint32_t producer_id) {
  return "/salias-" + std::string(name) + "-ring-" + std::to_string(producer_id);
}

std::string ring_public_name(std::string_view name, std::uint32_t producer_id) {
  return std::string(name) + "-producer-" + std::to_string(producer_id);
}

std::string hugetlbfs_dir() {
  if (const char* directory = std::getenv("SALIAS_HUGETLBFS_DIR");
      directory != nullptr && directory[0] != '\0') {
    return directory;
  }
  return kDefaultHugetlbfsDir;
}

std::string huge_ring_path(std::string_view name) {
  std::string directory = hugetlbfs_dir();
  if (!directory.empty() && directory.back() == '/') {
    directory.pop_back();
  }
  return directory + "/salias-" + std::string(name) + "-ring";
}

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

NamedRingSpec named_ring_spec_for(HugePage huge) noexcept {
  return huge == HugePage::None
             ? NamedRingSpec{}
             : NamedRingSpec{.backend = NamedRingBackend::Hugetlbfs, .huge = huge};
}

std::string ring_cleanup_name(std::string_view public_name, const std::string& shm_name,
                              NamedRingSpec spec) {
  return spec.backend == NamedRingBackend::Hugetlbfs ? huge_ring_path(public_name) : shm_name;
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

void unlink_ring(std::string_view public_name, const std::string& shm_name,
                 NamedRingSpec spec) noexcept {
  if (spec.backend == NamedRingBackend::Hugetlbfs) {
    static_cast<void>(::unlink(huge_ring_path(public_name).c_str()));
  } else {
    static_cast<void>(::shm_unlink(shm_name.c_str()));
  }
}

class ControlMapping {
 public:
  static Result<ControlMapping> create(const std::string& name) noexcept {
    const int fd = create_sized_shm(name, kControlSize);
    if (fd < 0) {
      return std::unexpected(Error::PlatformFail);
    }
    return map_fd(fd);
  }

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

  NamedControl* control() noexcept { return reinterpret_cast<NamedControl*>(base_); }

 private:
  static Result<ControlMapping> map_fd(int fd) noexcept {
    void* mapped =
        ::mmap(nullptr, kControlSize, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, 0);
    if (mapped == MAP_FAILED) {
      close_if_open(fd);
      return std::unexpected(Error::PlatformFail);
    }
    return ControlMapping(static_cast<std::byte*>(mapped), kControlSize, fd);
  }

  ControlMapping(std::byte* base, std::size_t len, int fd) noexcept
      : base_(base), len_(len), fd_(fd) {}

  void reset() noexcept {
    if (base_ != nullptr) {
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

Error map_platform_error(platform::PlatformError error) noexcept {
  return error == platform::PlatformError::InvalidSize ? Error::BadConfig : Error::PlatformFail;
}

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

template <Mode M>
using SharedChannel = channel::SharedHybridMpscChannel<kOrdering<M>>;

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

template <Mode M>
class NamedChannelState {
 public:
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

  auto tx(std::uint32_t producer_id) noexcept { return channel_.tx(producer_id); }
  auto rx(std::uint32_t consumer_id) noexcept { return channel_.rx(consumer_id); }

  std::optional<std::uint32_t> acquire_producer_id() noexcept {
    NamedControl* control = control_.control();
    const std::uint32_t producer_id = std::atomic_ref<std::uint32_t>(control->next_producer)
                                          .fetch_add(1, std::memory_order_acq_rel);
    return producer_id < control->num_producers ? std::optional{producer_id} : std::nullopt;
  }

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

template <Mode M>
Result<NamedChannelState<M>> create_named_state(const Config& config) {
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
  std::construct_at(control);
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
  std::atomic_ref<std::uint32_t>(control->ready).store(1, std::memory_order_release);
  return NamedChannelState<M>(std::move(control_mapping).value(), std::move(channel), control_name,
                              std::move(cleanup_names), true,
                              ring_spec.backend == NamedRingBackend::Hugetlbfs);
}

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

template <Mode M>
struct ChannelTraits {
  using State = NamedChannelState<M>;
  static Result<State> create(const Config& config) { return create_named_state<M>(config); }
  static Result<State> connect(std::string_view name) { return connect_named_state<M>(name); }
};

}  // namespace

template <Mode M>
struct ChannelState {
  using State = typename ChannelTraits<M>::State;
  explicit ChannelState(State state) noexcept : channel(std::move(state)) {}
  State channel;
};

template <Mode M>
struct PublisherEndpoint {
  using Tx = decltype(std::declval<typename ChannelState<M>::State&>().tx(0));

  PublisherEndpoint(std::shared_ptr<ChannelState<M>> state_value,
                    std::uint32_t producer_id) noexcept
      : state(std::move(state_value)),
        tx(state->channel.tx(producer_id)),
        valid(producer_id != kInvalidEndpoint) {}

  std::shared_ptr<ChannelState<M>> state;
  Tx tx;
  bool valid = false;
};

template <Mode M>
struct SubscriberEndpoint {
  using Rx = decltype(std::declval<typename ChannelState<M>::State&>().rx(0));

  SubscriberEndpoint(std::shared_ptr<ChannelState<M>> state_value,
                     std::uint32_t consumer_id) noexcept
      : state(std::move(state_value)),
        rx(state->channel.rx(consumer_id)),
        valid(consumer_id != kInvalidEndpoint) {}

  std::shared_ptr<ChannelState<M>> state;
  Rx rx;
  bool valid = false;
};

template <Mode M>
Channel<M>::Channel(std::shared_ptr<ChannelState<M>> state) noexcept : state_(std::move(state)) {}

template <Mode M>
Result<Channel<M>> Channel<M>::create(const Config& config) {
  if (config.name.empty() || config.fixed_size || config.record_size != 0 ||
      !is_valid_ring_capacity(config.capacity) ||
      !is_valid_huge_capacity(config.capacity, config.huge)) {
    return std::unexpected(Error::BadConfig);
  }
  Config typed_config = config;
  typed_config.mode = M;
  auto named = ChannelTraits<M>::create(typed_config);
  if (!named) {
    return std::unexpected(named.error());
  }
  return Channel(std::make_shared<ChannelState<M>>(std::move(named).value()));
}

template <Mode M>
Result<Channel<M>> Channel<M>::connect(std::string_view name) {
  auto named = ChannelTraits<M>::connect(name);
  if (!named) {
    return std::unexpected(named.error());
  }
  return Channel(std::make_shared<ChannelState<M>>(std::move(named).value()));
}

template <Mode M>
Publisher<M> Channel<M>::publisher() noexcept {
  const std::uint32_t producer_id =
      state_->channel.acquire_producer_id().value_or(kInvalidEndpoint);
  return Publisher<M>(std::make_shared<PublisherEndpoint<M>>(state_, producer_id));
}

template <Mode M>
Subscriber<M> Channel<M>::subscriber() noexcept {
  const std::uint32_t consumer_id =
      state_->channel.acquire_consumer_id().value_or(kInvalidEndpoint);
  return Subscriber<M>(std::make_shared<SubscriberEndpoint<M>>(state_, consumer_id));
}

template <Mode M>
Publisher<M>::Publisher(std::shared_ptr<PublisherEndpoint<M>> endpoint) noexcept
    : endpoint_(std::move(endpoint)) {}

template <Mode M>
PublishClaim<M>::PublishClaim(std::shared_ptr<PublisherEndpoint<M>> endpoint,
                              flow::Claim claim) noexcept
    : endpoint_(std::move(endpoint)), claim_(claim), committed_(false) {}

template <Mode M>
PublishClaim<M>::PublishClaim(PublishClaim&& other) noexcept
    : endpoint_(std::move(other.endpoint_)), claim_(other.claim_), committed_(other.committed_) {
  other.claim_ = {};
  other.committed_ = true;
}

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

template <Mode M>
std::span<std::byte> PublishClaim<M>::payload() noexcept {
  return claim_.payload;
}

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
    auto batch = endpoint_->tx.claim_batch(static_cast<std::uint32_t>(first_size), limit);
    if (!batch) {
      return std::unexpected(map_flow_error(batch.error()));
    }
    for (std::uint32_t index = 0; index < batch->frame_count; ++index) {
      auto destination = batch->region.subspan(
          static_cast<std::size_t>(index) * batch->frame_len + frame::kHeaderSize, first_size);
      if (first_size != 0) {
        std::memcpy(destination.data(), payloads[index].data(), first_size);
      }
    }
    endpoint_->tx.commit_batch(batch.value());
    return batch->frame_count;
  }
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
Subscriber<M>::Subscriber(std::shared_ptr<SubscriberEndpoint<M>> endpoint) noexcept
    : endpoint_(std::move(endpoint)) {}

template <Mode M>
std::optional<Message> Subscriber<M>::try_recv() noexcept {
  return endpoint_ == nullptr || !endpoint_->valid ? std::nullopt : endpoint_->rx.try_recv();
}

template <Mode M>
std::size_t Subscriber<M>::fetch_batch(Message* out, std::uint32_t cap) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid || out == nullptr || cap == 0) {
    return 0;
  }
  return endpoint_->rx.try_recv_run(out, cap);
}

template <Mode M>
void Subscriber<M>::flush_batch() noexcept {
  if (endpoint_ != nullptr && endpoint_->valid) {
    endpoint_->rx.flush_progress();
  }
}

template <Mode M>
std::size_t Subscriber<M>::poll(std::uint32_t max_messages, PollCallback callback,
                                void* user) noexcept {
  if (endpoint_ == nullptr || !endpoint_->valid || max_messages == 0 || callback == nullptr) {
    return 0;
  }
  constexpr std::uint32_t kChunk = 32;
  Message buffer[kChunk];
  std::size_t consumed = 0;
  while (consumed < max_messages) {
    const std::uint32_t remaining =
        static_cast<std::uint32_t>(std::min<std::size_t>(kChunk, max_messages - consumed));
    const std::size_t got = endpoint_->rx.try_recv_run(buffer, remaining);
    if (got == 0) {
      break;
    }
    for (std::size_t i = 0; i < got; ++i) {
      callback(buffer[i], user);
    }
    endpoint_->rx.flush_progress();
    consumed += got;
  }
  return consumed;
}

template <Mode M>
Message Subscriber<M>::recv() noexcept {
  return endpoint_->rx.recv();
}

template <Mode M>
void Subscriber<M>::release(const Message& message) noexcept {
  if (endpoint_ != nullptr && endpoint_->valid) {
    endpoint_->rx.release(message);
  }
}

#define SALIAS_INSTANTIATE_MODE(mode)      \
  template class Channel<Mode::mode>;      \
  template class Publisher<Mode::mode>;    \
  template class PublishClaim<Mode::mode>; \
  template class Subscriber<Mode::mode>

SALIAS_INSTANTIATE_MODE(FifoMpsc);
SALIAS_INSTANTIATE_MODE(FifoFanout);
SALIAS_INSTANTIATE_MODE(OrderedMpsc);
SALIAS_INSTANTIATE_MODE(OrderedFanout);

#undef SALIAS_INSTANTIATE_MODE

}  // namespace salias

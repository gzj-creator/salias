/**
 * @file src/salias/include/salias/fixed_channel.hpp
 * @brief 固定 schema、固定参与者的同机可靠 IPC 快速通道。
 * @details FixedChannel 只支持编译期固定 payload 与固定 slot 数量，参与者数量由
 *          topology 和模板参数确定。它直接使用固定槽 Ring，不经过通用 Channel 的
 *          变长 frame、动态订阅、TTL 或 Latest 语义。create()/connect() 通过具名
 *          POSIX 共享内存发布控制段，连接端在 ready 可见后校验协议版本、schema
 *          和完整布局，再附加 ring。
 *
 *          线程模型：每个 producer index 只能由一个 producer 线程独占，每个
 *          consumer index 只能由一个 consumer 线程独占。端点创建属于初始化阶段，
 *          流量开始后不得增加新的参与者。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

#include "core/ring/fast_mpmc_ring.hpp"
#include "core/ring/fast_mpsc_ring.hpp"
#include "core/ring/fast_ring_error.hpp"
#include "core/ring/fast_spmc_ring.hpp"
#include "core/ring/fast_spsc_ring.hpp"
#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/message.hpp"

namespace salias {

/// @brief FixedChannel 的四种固定参与者拓扑。
enum class FixedTopology : std::uint8_t { Spsc, Mpsc, Spmc, Mpmc };

/// @brief 具名固定通道的创建与连接配置。
struct FixedConfig {
  std::string_view name;  ///< 通道名称，仅允许字母、数字和 -_.，最长 128 字节。
  HugePage huge = HugePage::None;  ///< ring 数据区页类型；控制段始终使用普通页。
};

namespace detail {

Error map_fixed_ring_error(ring::FastRingError error) noexcept;

/// @brief FixedChannel 在共享控制段中持久化的固定布局摘要。
struct FixedLayout {
  std::uint64_t payload_size = 0;
  std::uint64_t ring_storage_size = 0;
  std::uint64_t capacity_slots = 0;
  std::uint32_t producer_count = 0;
  std::uint32_t consumer_count = 0;
};

/**
 * @brief 固定通道具名共享内存段句柄。
 * @details 由 FixedChannel 内部使用：create() 建立控制段与 ring 数据段，connect()
 *          等待 ready 并校验布局。控制段使用普通 POSIX shm，数据段可位于普通 shm
 *          或 hugetlbfs。创建者析构时解除名称，连接者只解除自己的映射。
 */
class FixedSharedSegment {
 public:
  FixedSharedSegment();
  FixedSharedSegment(FixedSharedSegment&&) noexcept;
  FixedSharedSegment& operator=(FixedSharedSegment&&) noexcept;
  FixedSharedSegment(const FixedSharedSegment&) = delete;
  FixedSharedSegment& operator=(const FixedSharedSegment&) = delete;
  ~FixedSharedSegment();

  static Result<FixedSharedSegment> create(std::string_view name, HugePage huge,
                                           SchemaDescriptor schema, FixedLayout layout);
  static Result<FixedSharedSegment> connect(std::string_view name, SchemaDescriptor schema,
                                            FixedLayout layout);

  /// @return ring 数据段内的固定存储视图；空句柄返回空 span。
  std::span<std::byte> ring_storage() const noexcept;

  /// @brief 在 ring 自身完成初始化后发布控制段 ready。
  void publish_ready() noexcept;

 private:
  struct State;
  explicit FixedSharedSegment(std::shared_ptr<State> state) noexcept;

  std::shared_ptr<State> state_;
};

}  // namespace detail

template <class Ring, detail::WireSchema Schema>
class FixedPublisher;

template <class Ring, detail::WireSchema Schema>
class FixedSubscriber;

/** @brief 尚未提交的 schema-bound 固定消息 claim。 */
template <class Ring>
class FixedPublishClaim {
 public:
  FixedPublishClaim(FixedPublishClaim&&) noexcept = default;
  FixedPublishClaim& operator=(FixedPublishClaim&&) noexcept = default;
  FixedPublishClaim(const FixedPublishClaim&) = delete;
  FixedPublishClaim& operator=(const FixedPublishClaim&) = delete;
  ~FixedPublishClaim() noexcept = default;

  /// @return 指向共享 ring 中、长度恰为 Schema::message_type 大小的写入区。
  std::span<std::byte> payload() noexcept { return payload_; }

 private:
  FixedPublishClaim(std::shared_ptr<void> owner, std::shared_ptr<typename Ring::Producer> producer,
                    std::span<std::byte> payload) noexcept
      : owner_(std::move(owner)), producer_(std::move(producer)), payload_(payload) {}

  std::shared_ptr<void> owner_;
  std::shared_ptr<typename Ring::Producer> producer_;
  std::span<std::byte> payload_;

  template <class, detail::WireSchema>
  friend class FixedPublisher;
};

/**
 * @brief 绑定固定 producer index 的 schema-only 发布端。
 * @details send() 复制一次对象表示；try_claim()/commit() 提供零拷贝路径。端点不可
 *          拷贝，避免同一 producer slot 被同一进程内的两个句柄同时声明未完成 claim。
 */
template <class Ring, detail::WireSchema Schema>
class FixedPublisher {
 public:
  using T = typename Schema::message_type;

  FixedPublisher(FixedPublisher&&) noexcept = default;
  FixedPublisher& operator=(FixedPublisher&&) noexcept = default;
  FixedPublisher(const FixedPublisher&) = delete;
  FixedPublisher& operator=(const FixedPublisher&) = delete;
  ~FixedPublisher() noexcept = default;

  /// @brief 复制一条固定类型消息并发布。
  Result<void> send(const T& value) noexcept {
    auto pushed = producer_->try_push(detail::as_bytes(value));
    if (!pushed) {
      return std::unexpected(detail::map_fixed_ring_error(pushed.error()));
    }
    return {};
  }

  /// @brief 预留一条固定大小的共享 ring 写入区。
  Result<FixedPublishClaim<Ring>> try_claim() noexcept {
    auto claimed = producer_->try_claim();
    if (!claimed) {
      return std::unexpected(detail::map_fixed_ring_error(claimed.error()));
    }
    return FixedPublishClaim<Ring>(owner_, producer_, *claimed);
  }

  /// @brief 提交由当前 producer 申请的 claim。
  Result<void> commit(FixedPublishClaim<Ring>&& claim) noexcept {
    if (claim.owner_.get() != owner_.get() || claim.producer_.get() != producer_.get()) {
      return std::unexpected(Error::BadConfig);
    }
    auto committed = producer_->commit();
    if (!committed) {
      return std::unexpected(detail::map_fixed_ring_error(committed.error()));
    }
    claim.payload_ = {};
    return {};
  }

 private:
  FixedPublisher(std::shared_ptr<void> owner, typename Ring::Producer producer)
      : owner_(owner), producer_(std::make_shared<typename Ring::Producer>(std::move(producer))) {}

  std::shared_ptr<void> owner_;
  std::shared_ptr<typename Ring::Producer> producer_;

  template <FixedTopology, detail::WireSchema, std::size_t, std::size_t, std::size_t>
  friend class FixedChannel;
};

/**
 * @brief 绑定固定 consumer index 的 schema-only 订阅端。
 * @details 只返回本地值，不暴露可越界借用的共享 span。每次成功接收都会立即推进该
 *          consumer 的独立 cursor；fanout 背压由最慢 consumer 决定。
 */
template <class Ring, detail::WireSchema Schema>
class FixedSubscriber {
 public:
  using T = typename Schema::message_type;

  FixedSubscriber(FixedSubscriber&&) noexcept = default;
  FixedSubscriber& operator=(FixedSubscriber&&) noexcept = default;
  FixedSubscriber(const FixedSubscriber&) = delete;
  FixedSubscriber& operator=(const FixedSubscriber&) = delete;
  ~FixedSubscriber() noexcept = default;

  Result<std::optional<T>> try_recv_copy() noexcept {
    auto read = consumer_->try_read();
    if (!read) {
      if (read.error() == ring::FastRingError::Empty) {
        return std::optional<T>{};
      }
      return std::unexpected(detail::map_fixed_ring_error(read.error()));
    }
    const T value = detail::copy_from_bytes<T>(*read);
    auto released = consumer_->release();
    if (!released) {
      return std::unexpected(detail::map_fixed_ring_error(released.error()));
    }
    return std::optional<T>{value};
  }

  /**
   * @brief 最多复制接收 max_messages 条消息并逐条调用本地 handler。
   * @param max_messages 本次最多接收条数。
   * @param handler noexcept callable，签名等价于 void(const T&)。
   * @return 实际交付条数；遇到空 ring 即返回，不等待。
   */
  template <class Handler>
    requires std::is_nothrow_invocable_v<Handler&, const T&>
  std::size_t poll(std::uint32_t max_messages, Handler&& handler) noexcept {
    std::size_t received = 0;
    while (received < max_messages) {
      auto value = try_recv_copy();
      if (!value || !*value) {
        break;
      }
      handler(**value);
      ++received;
    }
    return received;
  }

 private:
  FixedSubscriber(std::shared_ptr<void> owner, typename Ring::Consumer consumer)
      : owner_(owner), consumer_(std::make_shared<typename Ring::Consumer>(std::move(consumer))) {}

  std::shared_ptr<void> owner_;
  std::shared_ptr<typename Ring::Consumer> consumer_;

  template <FixedTopology, detail::WireSchema, std::size_t, std::size_t, std::size_t>
  friend class FixedChannel;
};

/**
 * @brief 具名、固定 schema、固定参与者的可靠 IPC 快速通道。
 * @tparam Topology SPSC、MPSC、SPMC 或 MPMC。
 * @tparam Schema 固定布局、平凡可复制的 wire schema。
 * @tparam CapacitySlotCount 每个 producer lane 的固定 slot 数，必须为 2 的幂。
 * @tparam ProducerCount producer 数量；SPSC/SPMC 必须为 1。
 * @tparam ConsumerCount consumer 数量；SPSC/MPSC 必须为 1。
 */
template <FixedTopology Topology, detail::WireSchema Schema, std::size_t CapacitySlotCount,
          std::size_t ProducerCount =
              (Topology == FixedTopology::Spsc || Topology == FixedTopology::Spmc ? 1 : 2),
          std::size_t ConsumerCount =
              (Topology == FixedTopology::Spsc || Topology == FixedTopology::Mpsc ? 1 : 2)>
class FixedChannel {
  static_assert(Topology != FixedTopology::Spsc || (ProducerCount == 1 && ConsumerCount == 1),
                "Fixed Spsc topology requires one producer and one consumer");
  static_assert(Topology != FixedTopology::Mpsc || ConsumerCount == 1,
                "Fixed Mpsc topology requires one consumer");
  static_assert(Topology != FixedTopology::Spmc || ProducerCount == 1,
                "Fixed Spmc topology requires one producer");

 public:
  using SchemaType = Schema;
  using T = typename Schema::message_type;
  using Ring = std::conditional_t<
      Topology == FixedTopology::Spsc, ring::FastSpscRing<sizeof(T), CapacitySlotCount>,
      std::conditional_t<
          Topology == FixedTopology::Mpsc,
          ring::FastMpscRing<sizeof(T), CapacitySlotCount, ProducerCount>,
          std::conditional_t<
              Topology == FixedTopology::Spmc,
              ring::FastSpmcRing<sizeof(T), CapacitySlotCount, ConsumerCount>,
              ring::FastMpmcRing<sizeof(T), CapacitySlotCount, ProducerCount, ConsumerCount>>>>;
  using Publisher = FixedPublisher<Ring, Schema>;
  using Subscriber = FixedSubscriber<Ring, Schema>;
  using CreateResult = Result<FixedChannel>;

  FixedChannel(FixedChannel&&) noexcept = default;
  FixedChannel& operator=(FixedChannel&&) noexcept = default;
  FixedChannel(const FixedChannel&) = default;
  FixedChannel& operator=(const FixedChannel&) = default;
  ~FixedChannel() noexcept = default;

  static Result<FixedChannel> create(const FixedConfig& config) {
    try {
      auto segment = detail::FixedSharedSegment::create(
          config.name, config.huge, detail::schema_descriptor<Schema>(), layout());
      if (!segment) {
        return std::unexpected(segment.error());
      }
      auto created = Ring::create(segment->ring_storage());
      if (!created) {
        return std::unexpected(detail::map_fixed_ring_error(created.error()));
      }
      segment->publish_ready();
      return FixedChannel(std::make_shared<State>(std::move(*segment), std::move(*created)));
    } catch (const std::bad_alloc&) {
      return std::unexpected(Error::OutOfMemory);
    }
  }

  static Result<FixedChannel> create(std::string_view name, HugePage huge = HugePage::None) {
    return create(FixedConfig{.name = name, .huge = huge});
  }

  static Result<FixedChannel> connect(std::string_view name) {
    try {
      auto segment =
          detail::FixedSharedSegment::connect(name, detail::schema_descriptor<Schema>(), layout());
      if (!segment) {
        return std::unexpected(segment.error());
      }
      auto attached = Ring::attach(segment->ring_storage());
      if (!attached) {
        return std::unexpected(detail::map_fixed_ring_error(attached.error()));
      }
      return FixedChannel(std::make_shared<State>(std::move(*segment), std::move(*attached)));
    } catch (const std::bad_alloc&) {
      return std::unexpected(Error::OutOfMemory);
    }
  }

  Result<Publisher> publisher(std::size_t producer_index) {
    if (producer_index >= ProducerCount || state_ == nullptr) {
      return std::unexpected(Error::BadConfig);
    }
    try {
      if constexpr (ProducerCount == 1) {
        return Publisher(std::shared_ptr<void>(state_), state_->ring.producer());
      } else {
        auto producer = state_->ring.producer(producer_index);
        if (!producer) {
          return std::unexpected(detail::map_fixed_ring_error(producer.error()));
        }
        return Publisher(std::shared_ptr<void>(state_), std::move(*producer));
      }
    } catch (const std::bad_alloc&) {
      return std::unexpected(Error::OutOfMemory);
    }
  }

  Result<Subscriber> subscriber(std::size_t consumer_index) {
    if (consumer_index >= ConsumerCount || state_ == nullptr) {
      return std::unexpected(Error::BadConfig);
    }
    try {
      if constexpr (ConsumerCount == 1) {
        return Subscriber(std::shared_ptr<void>(state_), state_->ring.consumer());
      } else {
        auto consumer = state_->ring.consumer(consumer_index);
        if (!consumer) {
          return std::unexpected(detail::map_fixed_ring_error(consumer.error()));
        }
        return Subscriber(std::shared_ptr<void>(state_), std::move(*consumer));
      }
    } catch (const std::bad_alloc&) {
      return std::unexpected(Error::OutOfMemory);
    }
  }

  static constexpr std::size_t payload_size() noexcept { return sizeof(T); }
  static constexpr std::size_t capacity_slots() noexcept { return CapacitySlotCount; }
  static constexpr std::size_t producer_count() noexcept { return ProducerCount; }
  static constexpr std::size_t consumer_count() noexcept { return ConsumerCount; }

 private:
  struct State {
    State(detail::FixedSharedSegment created_segment, Ring created_ring)
        : segment(std::move(created_segment)), ring(std::move(created_ring)) {}

    detail::FixedSharedSegment segment;
    Ring ring;
  };

  static constexpr detail::FixedLayout layout() noexcept {
    return {
        .payload_size = sizeof(T),
        .ring_storage_size = Ring::required_storage_size(),
        .capacity_slots = CapacitySlotCount,
        .producer_count = static_cast<std::uint32_t>(ProducerCount),
        .consumer_count = static_cast<std::uint32_t>(ConsumerCount),
    };
  }

  explicit FixedChannel(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}

  std::shared_ptr<State> state_;
};

}  // namespace salias

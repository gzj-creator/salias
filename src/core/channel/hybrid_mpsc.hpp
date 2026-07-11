/**
 * @file src/core/channel/hybrid_mpsc.hpp
 * @brief 进程内混合 MPSC channel：每生产者独立 ring，单消费者轮询消费。
 * @details 位于 L5 channel 层，组合下层能力：L0 平台(mmap/大页/NUMA)经
 *          platform::Mapping 提供内存；L1 ring(MagicRing)提供双映射环形寻址；
 *          L2 frame 帧编解码定义帧头与序列号低位编码；L3 flow 生产/消费
 *          流控提供 claim/commit 与消费进度模型；L4 等待策略经模板注入。
 *
 *          核心设计：每个 producer 拥有独立 MagicRing(避免生产者间写冲突)，
 *          单 consumer 轮询所有 ring。生产采用 claim→写 payload→commit 两阶段，
 *          commit 以 release 语义推进 visible_producer_pos 使帧对消费者可见；
 *          消费者以 acquire 语义观察该位置。支持 Order::Ordered(全局单调)
 *          与 Order::Fifo(每环内 FIFO)两种序。流控窗口(publication_window)
 *          限制生产者领先消费者的在途字节数，实现背压。
 *
 *          线程模型：每个 producer 独占一个 ring 与其 Tx；consumer 独占 Rx。
 *          producer_pos、visible_producer_pos 独占不同缓存行(128B 对齐)避免
 *          false sharing。不变式：consumer 不会读到未 commit 的帧；序列号低位
 *          环绕由 sequence_low_window_fits 在建表时静态校验。
 */
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/channel/error.hpp"
#include "core/channel/hybrid_control.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/frame/sequence.hpp"
#include "core/platform/map_options.hpp"
#include "core/platform/mapping.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

/// channel 层：在 ring 与 flow 之上封装生产/消费端句柄与等待策略。
namespace salias::channel {

/// hybrid_detail：HybridMpscChannel 内部使用的无状态工具函数集。
namespace hybrid_detail {

/**
 * @brief 判定 ring 是否有足够剩余空间容纳 need 字节。
 * @param capacity 有效容量(可能被流控窗口收窄)。
 * @param tail 生产者尾位置(单调递增，非取模)。
 * @param head 消费者头位置(单调递增)。
 * @param need 本次申请字节数。
 * @return true 当已用空间不超过容量且 need 不超过剩余空间。
 * @note 位置为单调递增的全量值，用减法得到已用字节，避免取模回绕。
 */
inline bool has_capacity(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                         std::size_t need) noexcept {
  const std::uint64_t used = tail - head;
  return used <= capacity && need <= capacity - used;
}

/**
 * @brief 计算定长批量申请可容纳的帧数。
 * @param capacity 有效容量(可能被流控窗口收窄)。
 * @param tail 生产者尾位置(单调递增)。
 * @param head 消费者头位置(单调递增)。
 * @param frame_len 单帧长度(含帧头+payload+对齐)。
 * @param max_frames 调用方期望的最大帧数上限。
 * @return 可容纳帧数(0 表示无空间)；不超过 max_frames。
 * @note 批量边界对齐：所有帧等长，整除得可容纳帧数，便于消费端解码。
 */
inline std::uint32_t batch_fit(std::size_t capacity, std::uint64_t tail, std::uint64_t head,
                               std::size_t frame_len, std::uint32_t max_frames) noexcept {
  if (frame_len == 0 || max_frames == 0) {
    return 0;
  }
  const std::uint64_t used = tail - head;
  if (used > capacity) {
    return 0;
  }
  const std::uint64_t free_bytes = capacity - used;
  const std::uint64_t fit = free_bytes / frame_len;
  if (fit == 0) {
    return 0;
  }
  return fit < max_frames ? static_cast<std::uint32_t>(fit) : max_frames;
}

/**
 * @brief 静态校验：序列号低位(24 bit)窗口是否足以支撑给定生产者数。
 * @param num_producers 生产者(或序列域)数量。
 * @param capacity_per_producer 单环字节容量。
 * @return true 配置安全，不会因低位回绕混淆跨环序列号；false 应拒绝建表。
 * @details 帧头仅存序列号低 24 位。在缺少高位旁路存储时，若某环能容纳
 *          一个低位周期(kSequenceLowPeriod=2^24 帧的领先量)，消费者可能将本环
 *          超过一个低位周期(2^24 帧领先量)，消费者可能将本环某帧低位
 *          环的同低位混淆。单生产者恒安全；多生产者需保证其余环不可能
 *          领先预期序列达一个完整低位周期。该不变式建表时一次性校验。
 */
inline bool sequence_low_window_fits(std::uint32_t num_producers,
                                     std::size_t capacity_per_producer) noexcept {
  if (num_producers == 0 || capacity_per_producer == 0) {
    return false;
  }
  if (num_producers == 1) {
    return true;
  }

  const std::uint64_t frames_per_ring =
      static_cast<std::uint64_t>(capacity_per_producer / frame::kFrameAlign);
  if (frames_per_ring == 0) {
    return false;
  }

  // 帧头仅存低 24 位序列号；在补齐高位旁路存储前，禁止其它生产者环
  // 领先预期序列达一个完整低位周期，否则消费者无法凭低位区分来源。
  constexpr std::uint64_t kSequenceLowPeriod = std::uint64_t{1} << frame::kSequenceLowBits;
  return static_cast<std::uint64_t>(num_producers - 1) <
         ((kSequenceLowPeriod + frames_per_ring - 1) / frames_per_ring);
}

/**
 * @brief 为给定序列号构造标准单帧的元数据字(含低位序列号与标志位)。
 * @param sequence 全局序列号，其低 24 位被编码进 meta 高位。
 * @param committed 是否已置 COMMITTED 标志(消费者据此判断帧可见)。
 * @return 32 位元数据字：低字节 flags(BEGIN|END[|COMMITTED])，高位序列号低位。
 * @note sequence_high 在 hybrid 模式下不单独存储(见 sequence_low_window_fits)。
 */
inline std::uint32_t meta_for_sequence(std::uint64_t sequence, bool committed) noexcept {
  std::uint32_t meta = frame::FLAG_BEGIN | frame::FLAG_END;
  if (committed) {
    meta |= frame::FLAG_COMMITTED;
  }
  std::uint64_t sequence_high = 0;
  frame::encode_sequence(sequence, meta, sequence_high);
  return meta;
}

/// @brief 由字节 position 求帧槽索引(capacity 须为 2 的幂，按帧对齐取整)。
inline std::size_t sequence_slot(std::uint64_t position, std::size_t capacity) noexcept {
  return static_cast<std::size_t>(position & (capacity - 1u)) / frame::kFrameAlign;
}

/**
 * @brief 将 payload_len 与 meta 打包为一个 64 位帧头字。
 * @details 布局：低 32 位 payload_len，高 32 位 meta；与 FrameHeader 小端一致。
 */
inline std::uint64_t pack_header(std::uint32_t payload_len, std::uint32_t meta) noexcept {
  return static_cast<std::uint64_t>(payload_len) | (static_cast<std::uint64_t>(meta) << 32u);
}

/**
 * @brief 以指定内存序原子写入 8 字节帧头到 ring 的 position 处。
 * @param ring 目标 ring。
 * @param position 帧起始字节偏移。
 * @param payload_len 负载字节长度(将写入帧头低 32 位)。
 * @param meta 帧元数据(将写入帧头高 32 位)。
 * @param order 写入内存序：claim 用 relaxed，commit 用 release(发布可见)。
 * @note 通过 std::atomic_ref 对普通内存做原子写，避免帧头独占原子成员。
 */
inline void store_header(ring::MagicRing& ring, std::uint64_t position, std::uint32_t payload_len,
                         std::uint32_t meta, std::memory_order order) noexcept {
  auto header_dst = ring.slice_mut(position, frame::kHeaderSize);
  auto* header_ptr = reinterpret_cast<std::uint64_t*>(header_dst.data());
  std::atomic_ref<std::uint64_t>(*header_ptr).store(pack_header(payload_len, meta), order);
}

/**
 * @brief 以 acquire 语义原子读取 position 处的 8 字节帧头。
 * @return 打包后的 64 位帧头字(低 32 位 payload_len，高 32 位 meta)。
 * @note acquire load 与生产者 commit 阶段的 release store 配对，确保读到帧头时
 *       payload 数据亦已对当前线程可见，避免读到半写帧。
 */
inline std::uint64_t load_header_acquire(const ring::MagicRing& ring,
                                         std::uint64_t position) noexcept {
  auto header_src = ring.slice(position, frame::kHeaderSize);
  auto* header_ptr =
      const_cast<std::uint64_t*>(reinterpret_cast<const std::uint64_t*>(header_src.data()));
  return std::atomic_ref<std::uint64_t>(*header_ptr).load(std::memory_order_acquire);
}

/**
 * @brief 预取 position 处帧头到 L1(只读)，减少批量消费缓存未命中。
 * @note GCC/Clang 下用 __builtin_prefetch 提示 locality=3(尽量留在所有层)；
 *       其它平台为空实现以保持编译通过。
 */
inline void prefetch_header(const ring::MagicRing& ring, std::uint64_t position) noexcept {
#if defined(__GNUC__) || defined(__clang__)
  __builtin_prefetch(ring.slice(position, 1).data(), 0, 3);
#else
  static_cast<void>(ring);
  static_cast<void>(position);
#endif
}

}  // namespace hybrid_detail

/**
 * @brief 进程内混合 MPSC channel：每生产者独立 ring，单消费者轮询消费。
 * @tparam Ordering 序模式：Order::Ordered(全局单调序列号)或 Order::Fifo(每环 FIFO)。
 * @tparam Wait 等待策略类型，须满足 wait::WaitStrategy 概念(默认 SpinPause)。
 * @details 内存模型：每个 producer 拥有独立 MagicRing(经 mmap 分配，可选
 *          绑定)，ring 容量须为 2 的幂以支持掩码寻址。所有权：channel 持有
 *          HybridSharedControl、producer_rings_ 与 consumer_sequences_，move-only。
 *          线程安全：Tx/Rx 分别为单生产者/单消费者独占使用，非线程安全；
 *          底层原子操作保证跨生产者-消费者可见性。
 *          关键不变式：consumer 仅消费已 COMMITTED 且序列号匹配的帧；
 *          序列号低位回绕安全由 create() 经 sequence_low_window_fits 静态保证。
 */
template <Order Ordering = Order::Ordered, wait::WaitStrategy Wait = wait::SpinPause>
class HybridMpscChannel {
 public:
  /**
   * @brief channel 建表配置。
   * @details ring_capacity_per_producer 须为 2 的幂(由 MagicRing 校验)。
   *          publication_window 为流控窗口(字节)，0 表示满环可用；非 0 时限制
   *          生产者领先消费者的在途字节数以实现背压。huge 启用大页降低
   *          miss；numa_node=-1 不绑定。
   */
  struct Config {
    std::uint32_t num_producers = 1;       ///< 生产者(独立 ring)数量
    std::uint32_t num_consumers = 1;       ///< 消费者数量
    std::size_t ring_capacity_per_producer = 4u * 1024u * 1024u;  ///< 单环容量(须 2 的幂)
    platform::HugePage huge = platform::HugePage::None;  ///< 是否启用大页(huge page)
    int numa_node = -1;                    ///< NUMA 绑定节点，-1 表示不绑定
    std::size_t publication_window = 0;    ///< 发布流控窗口(字节)，0 表示满环
  };

  /// @brief create() 结果类型：成功返回 channel，失败返回 ChannelError。
  using CreateResult = std::expected<HybridMpscChannel, ChannelError>;
  /// @brief offer() 结果类型：成功返回 true，失败返回 FlowError(背压/过大)。
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;  ///< 生产端句柄(前向声明)
  class Rx;  ///< 消费端句柄(前向声明)

  /**
   * @brief 创建进程内混合 MPSC channel：为每个 producer 建独立 ring，初始化控制
   *        结构与消费者序列槽。
   * @param config 建表配置(生产者/消费者数、环容量、大页、NUMA、流控)。
   * @retval HybridMpscChannel 建表成功。
   * @retval BadConfig 生产者/消费者数为 0、环容量为 0 或低位窗口校验失败。
   * @retval PlatformFail mmap 映射失败。
   * @retval RingFail MagicRing 创建(双映射)失败。
   * @note noexcept 但内部通过 std::expected 传播错误而非抛异常。Ordered 模式
   *       序列域数等于生产者数；Fifo 模式仅 1 个序列域(各环独立计数)。
   */
  // 创建进程内混合 MPSC channel：每个 producer 拥有独立的 MagicRing。
  static CreateResult create(const Config& config) noexcept {
    const std::uint32_t sequence_domains = Ordering == Order::Ordered ? config.num_producers : 1;
    if (config.num_producers == 0 || config.num_consumers == 0 ||
        config.ring_capacity_per_producer == 0 ||
        !hybrid_detail::sequence_low_window_fits(sequence_domains,
                                                 config.ring_capacity_per_producer)) {
      return std::unexpected(ChannelError::BadConfig);
    }

    auto control = std::make_unique<HybridSharedControl>();
    control->num_producers = config.num_producers;
    control->publication_window = config.publication_window;

    std::vector<ProducerRing> producer_rings;
    producer_rings.reserve(config.num_producers);
    for (std::uint32_t i = 0; i < config.num_producers; ++i) {
      auto mapping = platform::Mapping::create(platform::MapOptions{
          .size = config.ring_capacity_per_producer,
          .huge = config.huge,
          .numa_node = config.numa_node,
      });
      if (!mapping) {
        return std::unexpected(ChannelError::PlatformFail);
      }

      auto ring = ring::MagicRing::create(std::move(mapping).value());
      if (!ring) {
        return std::unexpected(ChannelError::RingFail);
      }
      producer_rings.emplace_back(std::move(ring).value(), config.num_consumers);
    }

    auto consumer_sequences = std::make_unique<std::atomic<std::uint64_t>[]>(config.num_consumers);
    for (std::uint32_t consumer_id = 0; consumer_id < config.num_consumers; ++consumer_id) {
      consumer_sequences[consumer_id].store(0, std::memory_order_relaxed);
    }

    return HybridMpscChannel(std::move(control), std::move(producer_rings),
                             std::move(consumer_sequences), Wait{});
  }

  HybridMpscChannel(HybridMpscChannel&&) noexcept = default;            ///< move 构造(默认)
  HybridMpscChannel& operator=(HybridMpscChannel&&) noexcept = default;  ///< move 赋值(默认)
  HybridMpscChannel(const HybridMpscChannel&) = delete;      ///< 禁止拷贝(独占映射)
  HybridMpscChannel& operator=(const HybridMpscChannel&) = delete;      ///< 禁止拷贝赋值

  /// @brief 返回生产者(独立 ring)数量；channel 已 move-out 后返回 0。
  std::uint32_t producer_count() const noexcept {
    return control_ == nullptr ? 0u : control_->num_producers;
  }

  /// @brief 返回单环字节容量；channel 已 move-out 后返回 0。
  std::size_t ring_capacity_per_producer() const noexcept {
    return producer_rings_.empty() ? 0u : producer_rings_.front().ring.capacity();
  }

  /// @brief 获取指定 producer 的生产端句柄 Tx；调用方须保证 producer_id 合法。
  Tx tx(std::uint32_t producer_id) noexcept { return Tx(*this, producer_id); }
  /// @brief 获取指定 consumer 的消费端句柄 Rx；默认 consumer_id=0。
  Rx rx(std::uint32_t consumer_id = 0) noexcept { return Rx(*this, consumer_id); }

 private:
  /**
   * @brief 单个 producer 的私有 ring 及其位置/统计状态。
   * @details 128 字节对齐隔离缓存行。producer_pos 为写位置(独占，无需原子)；
   *          visible_producer_pos 为已 commit 的可见位置(原子，release 写/
   *          acquire 读)，独占缓存行避免与 producer_pos 乒乓。consumer_positions
   *          为本环上每个 consumer 的消费进度，生产者据此判定可回收空间。
   *          所有权：持有 ring 映射与 consumer_positions 数组。线程模型：
   *          同一 ProducerRing 仅由单一 producer 线程写入 producer_pos/
   *          visible_producer_pos；consumer_positions 由各 consumer 线程
   *          release 写、本 producer acquire 读。
   */
  struct alignas(128) ProducerRing {
    /**
     * @brief 构造并初始化一个生产者环。
     * @param created_ring 已建好的 MagicRing(转移所有权)。
     * @param created_consumer_count 消费者数量，决定 consumer_positions 数组长度。
     */
    ProducerRing(ring::MagicRing created_ring, std::uint32_t created_consumer_count)
        : ring(std::move(created_ring)),
          consumer_positions(
              std::make_unique<std::atomic<std::uint64_t>[]>(created_consumer_count)),
          consumer_count(created_consumer_count) {
      for (std::uint32_t consumer_id = 0; consumer_id < consumer_count; ++consumer_id) {
        consumer_positions[consumer_id].store(0, std::memory_order_relaxed);
      }
    }

    /// @brief move 构造；visible_producer_pos 以 relaxed load 转移。
    ProducerRing(ProducerRing&& other) noexcept
        : ring(std::move(other.ring)),
          producer_pos(other.producer_pos),
          cached_consumer_pos(other.cached_consumer_pos),
          visible_producer_pos(other.visible_producer_pos.load(std::memory_order_relaxed)),
          consumer_positions(std::move(other.consumer_positions)),
          consumer_count(other.consumer_count),
          total_written(other.total_written),
          backpressure_count(other.backpressure_count) {}

    /// @brief move 赋值；自赋值保护，visible_producer_pos 用 relaxed store 转移。
    ProducerRing& operator=(ProducerRing&& other) noexcept {
      if (this != &other) {
        ring = std::move(other.ring);
        producer_pos = other.producer_pos;
        cached_consumer_pos = other.cached_consumer_pos;
        visible_producer_pos.store(other.visible_producer_pos.load(std::memory_order_relaxed),
                                   std::memory_order_relaxed);
        consumer_positions = std::move(other.consumer_positions);
        consumer_count = other.consumer_count;
        total_written = other.total_written;
        backpressure_count = other.backpressure_count;
      }
      return *this;
    }

    ProducerRing(const ProducerRing&) = delete;             ///< 禁止拷贝(独占映射所有权)
    ProducerRing& operator=(const ProducerRing&) = delete;  ///< 禁止拷贝赋值

    ring::MagicRing ring;                                   ///< 该生产者的私有 ring
    // 写位置(独占缓存行)：单生产者独占递增，无需原子；单调全量偏移。
    alignas(128) std::uint64_t producer_pos = 0;
    std::uint64_t cached_consumer_pos = 0;  ///< 消费者最小位置本地缓存，减少跨核读
    // 可见位置(独占缓存行)：commit 后 release 写，消费者 acquire 读。
    alignas(128) std::atomic<std::uint64_t> visible_producer_pos{0};
    // 每个 consumer 在本环的消费进度；consumer release 写、producer acquire 读。
    std::unique_ptr<std::atomic<std::uint64_t>[]> consumer_positions;
    std::uint32_t consumer_count = 0;       ///< 本环订阅的 consumer 数量
    std::uint64_t total_written = 0;        ///< 累计已写帧数(Fifo 模式下作序列号源)
    std::uint64_t backpressure_count = 0;   ///< 累计背压次数(诊断用)
  };

  /// @brief 私有构造：由 create() 在完成所有 ring/控制结构分配后调用。
  explicit HybridMpscChannel(std::unique_ptr<HybridSharedControl> control,
                             std::vector<ProducerRing> producer_rings,
                             std::unique_ptr<std::atomic<std::uint64_t>[]> consumer_sequences,
                             Wait wait) noexcept
      : control_(std::move(control)),
        producer_rings_(std::move(producer_rings)),
        consumer_sequences_(std::move(consumer_sequences)),
        wait_(std::move(wait)) {}

  /// @brief 判定 producer_id 是否在合法范围内。
  bool has_producer(std::uint32_t producer_id) const noexcept {
    return producer_id < producer_rings_.size();
  }

  /// @brief 判定 consumer_id 是否合法(channel 非空且 id 小于 consumer_count)。
  bool has_consumer(std::uint32_t consumer_id) const noexcept {
    return !producer_rings_.empty() && consumer_id < producer_rings_.front().consumer_count;
  }

  /**
   * @brief 求本环所有 consumer 消费进度的最小值(最慢消费者位置)。
   * @return 最小消费位置；生产者据此回收环形空间。
   * @note 以 acquire load 读取每个 consumer 位置，与 consumer 端 release store 配对，
   *       确保读到的进度对应已对生产者可见的消费完成。
   */
  static std::uint64_t minimum_consumer_position(const ProducerRing& producer) noexcept {
    std::uint64_t minimum = producer.consumer_positions[0].load(std::memory_order_acquire);
    for (std::uint32_t consumer_id = 1; consumer_id < producer.consumer_count; ++consumer_id) {
      const std::uint64_t position =
          producer.consumer_positions[consumer_id].load(std::memory_order_acquire);
      if (position < minimum) {
        minimum = position;
      }
    }
    return minimum;
  }

  // 有效流控容量：窗口为 0 时即环容量，否则取 min(环容量, 窗口)。
  // 不改变 ring 寻址与回绕，仅收窄 claim 空间判定，实现发布流控(背压)。
  std::size_t effective_capacity(std::size_t ring_capacity) const noexcept {
    if (control_ == nullptr || control_->publication_window == 0) {
      return ring_capacity;
    }
    const auto window = static_cast<std::size_t>(control_->publication_window);
    return window < ring_capacity ? window : ring_capacity;
  }

  /**
   * @brief claim 阶段写入"未提交"帧头(无 COMMITTED 标志，relaxed 序)。
   * @details 此时帧对消费者尚不可见(visible_producer_pos 未推进)；relaxed 写
   *          足够，因为随后 commit 会以 release 序重写帧头并推进可见位置。
   */
  void write_uncommitted_header(ProducerRing& producer, std::uint64_t position,
                                std::uint32_t payload_len, std::uint64_t sequence) noexcept {
    const std::uint32_t meta = hybrid_detail::meta_for_sequence(sequence, false);
    hybrid_detail::store_header(producer.ring, position, payload_len, meta,
                                std::memory_order_relaxed);
  }

  /**
   * @brief 尝试从指定 producer 环的 position 读一条已提交帧。
   * @param producer_id 生产者环索引。
   * @param position 帧起始字节偏移。
   * @param target_sequence 期望的序列号(用于校验帧归属)。
   * @param visible_bound 可见上界(position >= visible_bound 表示尚不可读)。
   * @return 可读则返回 Message；未就绪/未提交/序列号不符/越界返回 nullopt。
   * @note 以 acquire 读帧头(与 commit 的 release 配对)；帧未置 COMMITTED 则视为
   *       不可读，避免消费半写帧。序列号低位匹配校验防止跨环/跨帧混淆
   *       (见 sequence_low_window_fits)。
   *       next_position 越过 visible_bound 也判为不可读，保证不读未发布区域。
   */
  std::optional<flow::Message> try_read_from_ring(std::uint32_t producer_id, std::uint64_t position,
                                                  std::uint64_t target_sequence,
                                                  std::uint64_t visible_bound) noexcept {
    auto& producer = producer_rings_[producer_id];
    if (position >= visible_bound) {
      return std::nullopt;
    }

    const std::uint64_t header = hybrid_detail::load_header_acquire(producer.ring, position);
    const std::uint32_t payload_len = static_cast<std::uint32_t>(header);
    const std::uint32_t meta = static_cast<std::uint32_t>(header >> 32u);
    const std::uint32_t flags = meta & 0xFFu;
    if ((flags & frame::FLAG_COMMITTED) == 0) {
      return std::nullopt;
    }
    if (frame::sequence_low_from_meta(meta) != frame::sequence_low(target_sequence)) {
      return std::nullopt;
    }

    const std::uint64_t next_position = position + frame::frame_len(payload_len);
    if (frame::frame_len(payload_len) > producer.ring.capacity() ||
        next_position > visible_bound) {
      return std::nullopt;
    }

    return flow::Message{
        .payload = producer.ring.slice(position + frame::kHeaderSize, payload_len),
        .position = position,
        .next_position = next_position,
        .meta = meta,
        .sequence = target_sequence,
        .producer_id = producer_id,
    };
  }

  std::unique_ptr<HybridSharedControl> control_;  ///< 共享控制(序列号、流控、wait_word)
  std::vector<ProducerRing> producer_rings_;      ///< 每 producer 一个独立 ring 及其状态
  // 各 consumer 的期望序列(全局有序)；长度等于 num_consumers。
  std::unique_ptr<std::atomic<std::uint64_t>[]> consumer_sequences_;
  Wait wait_;  ///< 等待策略实例(模板注入，零开销)

 public:
  /**
   * @brief 生产端句柄：单 producer 独占，提供 claim/commit 与批量接口。
   * @details 非持有：仅保存 channel 指针与 producer_id。线程安全：单写者。
   *          同一 Tx 不可被多线程并发调用。生命周期：须保证引用的 channel
   *          存活；channel move-out 后调用将返回错误。
   */
  class Tx {
   public:
    /// @brief claim_batch() 的结果类型：成功返回 BatchClaim，失败返回 FlowError。
    using BatchClaimResult = std::expected<flow::BatchClaim, flow::FlowError>;

    /// @brief 绑定 channel 与 producer_id；调用方须保证 producer_id 合法。
    explicit Tx(HybridMpscChannel& channel, std::uint32_t producer_id) noexcept
        : channel_(&channel), producer_id_(producer_id) {}

    /**
     * @brief 单帧 claim：在 ring 预留 payload 区域并写未提交帧头。
     * @param payload_len 期望的负载字节数(不含帧头)。
     * @retval Claim 预留成功，payload 为 ring 内独占可写区，commit() 前写完。
     * @retval MessageTooLarge 帧长超过 ring 总容量或 producer_id 非法。
     * @retval BackPressured 流控窗口/环空间不足，需等待消费者推进或重试。
     * @note 两阶段发布：claim 先写未提交帧头(relaxed)并推进 producer_pos 与可见
     *       位置(release)，但帧头未置 COMMITTED 故消费者跳过；commit() 再以
     *       release 重写帧头置 COMMITTED。背压处理：先用本地缓存
     *       cached_consumer_pos 快判，失效时以 acquire 重读最小消费位置。
     *       Ordered 模式序列号取自全局 global_seq(fetch_add relaxed，单调即可)。
     */
    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_)) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      const std::size_t need = frame::frame_len(payload_len);
      if (need > producer.ring.capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      // 先用本地缓存的消费者位置快判容量，避免每次都跨核 acquire 读。
      if (!hybrid_detail::has_capacity(channel_->effective_capacity(producer.ring.capacity()),
                                       producer.producer_pos, producer.cached_consumer_pos, need)) {
        // 缓存失效：以 acquire 重新读取最慢消费者位置，再判一次。
        producer.cached_consumer_pos = channel_->minimum_consumer_position(producer);
        if (!hybrid_detail::has_capacity(channel_->effective_capacity(producer.ring.capacity()),
                                         producer.producer_pos, producer.cached_consumer_pos,
                                         need)) {
          ++producer.backpressure_count;
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      const std::uint64_t sequence = [&producer, this] {
        if constexpr (Ordering == Order::Ordered) {
          return channel_->control_->global_seq.fetch_add(1, std::memory_order_relaxed);
        }
        return producer.total_written;
      }();
      const std::uint64_t position = producer.producer_pos;
      producer.producer_pos += need;
      ++producer.total_written;
      channel_->write_uncommitted_header(producer, position, payload_len, sequence);
      // release 推进可见位置：与消费者 acquire load 配对，保证 payload 可见。
      producer.visible_producer_pos.store(producer.producer_pos, std::memory_order_release);

      return flow::Claim{
          .payload = producer.ring.slice_mut(position + frame::kHeaderSize, payload_len),
          .start_pos = position,
          .payload_len = payload_len,
          .meta = hybrid_detail::meta_for_sequence(sequence, false),
          .sequence = sequence,
          .producer_id = producer_id_,
      };
    }

    /**
     * @brief 批量 claim：一次预留最多 max_frames 个等长帧，返回连续可写区。
     * @param payload_len 单帧负载字节数(所有帧等长，便于消费端定长步进)。
     * @param max_frames 期望的最大帧数上限。
     * @retval BatchClaim 预留成功，region 为连续可写区，frame_count 为批大小。
     * @retval MessageTooLarge 单帧超过环容量、producer_id 非法或 max_frames 为 0。
     * @retval BackPressured 剩余空间不足以容纳任何一帧，需重试或等待。
     * @note 批量边界对齐：所有帧等长，base_sequence 为首帧序号，后续帧 +1。
     *       Ordered 模式下一次 fetch_add(fit) 批量领取序列号，减少原子争用。
     *       各帧逐个写未提交帧头，最后统一 release 推进可见位置。
     */
    BatchClaimResult claim_batch(std::uint32_t payload_len, std::uint32_t max_frames) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_) || max_frames == 0) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      const std::size_t per = frame::frame_len(payload_len);
      if (per > producer.ring.capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      std::uint32_t fit = hybrid_detail::batch_fit(
          channel_->effective_capacity(producer.ring.capacity()), producer.producer_pos,
          producer.cached_consumer_pos, per, max_frames);
      if (fit == 0) {
        // 本地缓存失效：acquire 重读最慢消费者位置后再算一次可容纳帧数。
        producer.cached_consumer_pos = channel_->minimum_consumer_position(producer);
        fit = hybrid_detail::batch_fit(channel_->effective_capacity(producer.ring.capacity()),
                                       producer.producer_pos, producer.cached_consumer_pos, per,
                                       max_frames);
        if (fit == 0) {
          ++producer.backpressure_count;
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      const std::uint64_t base_sequence = [&producer, fit, this] {
        if constexpr (Ordering == Order::Ordered) {
          return channel_->control_->global_seq.fetch_add(fit, std::memory_order_relaxed);
        }
        return producer.total_written;
      }();
      const std::uint64_t position = producer.producer_pos;
      const std::uint64_t span_bytes = static_cast<std::uint64_t>(per) * fit;
      producer.producer_pos += span_bytes;
      producer.total_written += fit;

      // 为批量内每帧写未提交帧头(序列号从 base_sequence 起递增)。
      for (std::uint32_t i = 0; i < fit; ++i) {
        const std::uint64_t frame_position = position + static_cast<std::uint64_t>(per) * i;
        channel_->write_uncommitted_header(producer, frame_position, payload_len,
                                           base_sequence + i);
      }
      producer.visible_producer_pos.store(producer.producer_pos, std::memory_order_release);

      return flow::BatchClaim{
          .region = producer.ring.slice_mut(position, static_cast<std::size_t>(span_bytes)),
          .start_pos = position,
          .frame_len = static_cast<std::uint32_t>(per),
          .frame_count = fit,
          .base_sequence = base_sequence,
          .producer_id = producer_id_,
      };
    }

    /**
     * @brief 提交单帧：以 release 重写帧头置 COMMITTED，使帧对消费者可见，
     *        并按需唤醒等待的消费者。
     * @param claim 此前 claim() 返回的预留描述，payload 必须已写完。
     * @note 内存序：release 写帧头与消费者 acquire load 配对，保证 payload
     *       先于 COMMITTED 可见。唤醒：仅当等待策略 needs_wake() 时才
     *       fetch_add wait_word 并 wake，避免无阻塞消费者时的开销。
     */
    void commit(const flow::Claim& claim) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_)) {
        return;
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      const std::uint32_t committed_meta = claim.meta | frame::FLAG_COMMITTED;
      hybrid_detail::store_header(producer.ring, claim.start_pos, claim.payload_len, committed_meta,
                                  std::memory_order_release);
      if (channel_->wait_.needs_wake()) {
        std::atomic_ref<std::uint32_t>(channel_->control_->wait_word)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(&channel_->control_->wait_word);
      }
    }

    /**
     * @brief 批量提交：逐帧以 release 写 COMMITTED 帧头，按需唤醒消费者。
     * @param batch 此前 claim_batch() 返回的批量预留描述，payload 须已写完。
     * @note 每帧 payload_len = frame_len - kHeaderSize(从定长 frame_len 反推)。
     *       唤醒在所有帧提交后统一执行一次，减少唤醒次数。
     */
    void commit_batch(const flow::BatchClaim& batch) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_)) {
        return;
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      for (std::uint32_t i = 0; i < batch.frame_count; ++i) {
        const std::uint64_t position =
            batch.start_pos + static_cast<std::uint64_t>(batch.frame_len) * i;
        const std::uint32_t committed_meta =
            hybrid_detail::meta_for_sequence(batch.base_sequence + i, true);
        hybrid_detail::store_header(producer.ring, position, batch.frame_len - frame::kHeaderSize,
                                    committed_meta,
                                    std::memory_order_release);
      }
      if (channel_->wait_.needs_wake()) {
        std::atomic_ref<std::uint32_t>(channel_->control_->wait_word)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(&channel_->control_->wait_word);
      }
    }

    /**
     * @brief 便捷封装：claim→memcpy→commit 一步完成，发布一条完整消息。
     * @param payload 待发布的负载数据。
     * @retval true 发布成功。
     * @retval FlowError claim 失败(背压或过大)时透传错误。
     */
    OfferResult offer(std::span<const std::byte> payload) noexcept {
      auto claim_result = claim(static_cast<std::uint32_t>(payload.size()));
      if (!claim_result) {
        return std::unexpected(claim_result.error());
      }
      std::memcpy(claim_result.value().payload.data(), payload.data(), payload.size());
      commit(claim_result.value());
      return true;
    }

   private:
    HybridMpscChannel* channel_;  ///< 非持有的 channel 指针
    std::uint32_t producer_id_;   ///< 本句柄绑定的 producer 索引
  };

  /**
   * @brief 消费端句柄：单 consumer 独占，轮询所有 producer 环消费已提交帧。
   * @details 非持有：仅保存 channel 指针与 consumer_id。轮询采用 last_hit_ring_
   *          起始的循环偏移，局部性优先访问上次命中环。为减少跨核读，
   *          每个环维护 cached_visible_pos_：仅当本地读位置追上缓存可见位时
   *          才 acquire 刷新，一次刷新覆盖一整批消费。线程安全：单消费，
   *          非线程安全。Ordered 模式以全局 expected_sequence_ 跨环排序消费；
   *          Fifo 模式各环独立 next_sequences_ 计数。批量消费路径
   *          (try_recv_run + flush_progress)将进度写共享槽推迟到批末，降低乒乓。
   */
  class Rx {
   public:
    /// @brief 绑定 channel 与 consumer_id，按 producer 数初始化各环的本地状态。
    explicit Rx(HybridMpscChannel& channel, std::uint32_t consumer_id) noexcept
        : channel_(&channel),
          consumer_id_(consumer_id),
          read_positions_(channel.producer_rings_.size(), 0),
          next_sequences_(channel.producer_rings_.size(), 0),
          cached_visible_pos_(channel.producer_rings_.size(), 0),
          flushed_positions_(channel.producer_rings_.size(), 0),
          expected_sequence_(
              channel.has_consumer(consumer_id)
                  ? channel.consumer_sequences_[consumer_id].load(std::memory_order_acquire)
                  : 0) {}

    /**
     * @brief 非阻塞读取一条已提交帧，从上次命中环轮询所有 producer 环。
     * @return 可读则返回 Message；无可消费帧或未绑定则返回 std::nullopt。
     * @note 轮询顺序以 last_hit_ring_ 为起点循环偏移，提升缓存局部性。
     *       target_sequence 在 Ordered 模式取全局期望序，Fifo 模式取各环独立序。
     */
    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          channel_->producer_rings_.empty()) {
        return std::nullopt;
      }

      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t scanned = 0; scanned < count; ++scanned) {
        const std::uint32_t producer_id =
            static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);

        // 只有本地读位置追平缓存可见位时才跨核 acquire-load 刷新，
        // 一次刷新覆盖一整批。
        if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
          cached_visible_pos_[producer_id] =
              channel_->producer_rings_[producer_id].visible_producer_pos.load(
                  std::memory_order_acquire);
          if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
            continue;
          }
        }

        const std::uint64_t target_sequence =
            Ordering == Order::Ordered ? expected_sequence_ : next_sequences_[producer_id];
        auto message =
            channel_->try_read_from_ring(producer_id, read_positions_[producer_id], target_sequence,
                                         cached_visible_pos_[producer_id]);
        if (message.has_value()) {
          // 记录命中环的下一环为下次轮询起点，提升局部性。
          last_hit_ring_ = static_cast<std::uint32_t>((producer_id + 1) % count);
          return message;
        }
      }
      return std::nullopt;
    }

    /**
     * @brief 批量接收：选中一个可读环后在其内紧循环排空，最多填 cap 条。
     * @param out 输出消息数组，调用方分配，须至少 cap 个元素。
     * @param cap 最多接收的消息条数。
     * @return 实际接收的条数(0 表示无可用帧)。
     * @note 每条只更新本地读位置(consume 语义)，不写共享槽——进度由
     *       调用方批末统一 flush_progress()。紧循环内预取下一帧帧头。
     */
    // 批量接收：选中一个可读环后在同环内紧循环排空，填 cap 条到 out[]。
    // 每条只更新本地读位置(consume 语义)，不写共享槽；批末 flush_progress()。
    std::uint32_t try_recv_run(flow::Message* out, std::uint32_t cap) noexcept {
      if (out == nullptr || cap == 0 || channel_ == nullptr ||
          !channel_->has_consumer(consumer_id_) || channel_->producer_rings_.empty()) {
        return 0;
      }

      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t scanned = 0; scanned < count; ++scanned) {
        const std::uint32_t producer_id =
            static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);

        if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
          cached_visible_pos_[producer_id] =
              channel_->producer_rings_[producer_id].visible_producer_pos.load(
                  std::memory_order_acquire);
          if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
            continue;
          }
        }

        std::uint32_t produced = 0;
        while (produced < cap) {
          const std::uint64_t target_sequence =
              Ordering == Order::Ordered ? expected_sequence_ : next_sequences_[producer_id];
          auto message = channel_->try_read_from_ring(producer_id, read_positions_[producer_id],
                                                      target_sequence,
                                                      cached_visible_pos_[producer_id]);
          if (!message.has_value()) {
            break;
          }
          if (message->next_position < cached_visible_pos_[producer_id]) {
            // 下一帧仍在可见范围内则预取其帧头，隐藏后续读的缓存延迟。
            hybrid_detail::prefetch_header(channel_->producer_rings_[producer_id].ring,
                                           message->next_position);
          }
          out[produced++] = *message;
          consume(*message);
        }

        if (produced > 0) {
          last_hit_ring_ = static_cast<std::uint32_t>((producer_id + 1) % count);
          return produced;
        }
      }
      return 0;
    }

    /**
     * @brief 阻塞接收：循环 try_recv，无消息时在 wait_word 上阻塞等待被唤醒。
     * @return 收到的消息。
     * @note 经典 wait/notify 模式：先读 expected，再 wait；若 wait_word 在读后
     *       变化则 wait 立即返回，避免丢失唤醒。wait 策略由模板注入
     *       futex/eventfd 则阻塞)。
     */
    flow::Message recv() noexcept {
      for (;;) {
        if (auto message = try_recv(); message.has_value()) {
          return *message;
        }
        const auto expected = std::atomic_ref<std::uint32_t>(channel_->control_->wait_word)
                                  .load(std::memory_order_acquire);
        channel_->wait_.wait(&channel_->control_->wait_word, expected);
      }
    }

    /**
     * @brief 单条消费确认：更新本地读位置并 release 发布进度给生产者。
     * @param message 此前 try_recv/recv 返回且已处理完毕的消息。
     * @note Ordered 模式校验 message.sequence == expected_sequence_ 防止乱序确认。
     *       release store consumer_positions 与生产者 claim 的 acquire load 配对，
     *       归还环形空间。同时刷新 flushed_positions_ 与期望序列，供后续
     *       flush_progress 判定增量。
     */
    void release(const flow::Message& message) noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          message.producer_id >= channel_->producer_rings_.size() ||
          (Ordering == Order::Ordered && message.sequence != expected_sequence_)) {
        return;
      }

      auto& producer = channel_->producer_rings_[message.producer_id];
      read_positions_[message.producer_id] = message.next_position;
      producer.consumer_positions[consumer_id_].store(message.next_position,
                                                      std::memory_order_release);
      flushed_positions_[message.producer_id] = message.next_position;
      if constexpr (Ordering == Order::Ordered) {
        ++expected_sequence_;
        channel_->consumer_sequences_[consumer_id_].store(expected_sequence_,
                                                          std::memory_order_release);
      } else {
        next_sequences_[message.producer_id] = message.sequence + 1;
      }
    }

    /**
     * @brief 只更新本地消费进度，不写共享槽；配合 flush_progress() 批量。
     * @param message 已处理的消息，据此推进本地读位置与期望序列。
     * @note 与 release() 的区别：不 release store consumer_positions，避免每条
     *       都跨核写共享槽；进度统一由批末 flush_progress() 写回。
     */
    // 只更新本地消费进度，不写共享槽；配合 flush_progress() 用于批量消费。
    void consume(const flow::Message& message) noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          message.producer_id >= channel_->producer_rings_.size() ||
          (Ordering == Order::Ordered && message.sequence != expected_sequence_)) {
        return;
      }
      read_positions_[message.producer_id] = message.next_position;
      if constexpr (Ordering == Order::Ordered) {
        ++expected_sequence_;
      } else {
        next_sequences_[message.producer_id] = message.sequence + 1;
      }
    }

    /**
     * @brief 把本地消费进度发布到生产者可见的原子槽，仅写有推进的环。
     * @note 与 flushed_positions_ 比对，跳过未推进的环，避免无谓跨核原子写。
     *       release store 与生产者 claim 的 acquire load 配对，使生产者可见
     *       已释放空间。
     *       Ordered 模式额外发布全局期望序列(用于断点续消费)。
     */
    // 把本地消费进度发布到生产者可见的原子槽，仅对有推进的环写入。
    void flush_progress() noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_)) {
        return;
      }
      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t producer_id = 0; producer_id < count; ++producer_id) {
        if (read_positions_[producer_id] == flushed_positions_[producer_id]) {
          continue;
        }
        channel_->producer_rings_[producer_id].consumer_positions[consumer_id_].store(
            read_positions_[producer_id], std::memory_order_release);
        flushed_positions_[producer_id] = read_positions_[producer_id];
      }
      if constexpr (Ordering == Order::Ordered) {
        channel_->consumer_sequences_[consumer_id_].store(expected_sequence_,
                                                          std::memory_order_release);
      }
    }

    /**
     * @brief 批量消费循环：分块(每块 32 条)接收回调，每块末 flush_progress，
     *        至多处理 limit 条。
     * @tparam Handler 回调类型：接受 payload(span<const std::byte>) 或 Message。
     * @param handler 每条消息的处理回调，编译期根据可调用签名静态派发。
     * @param limit 最多处理的消息条数。
     * @return 实际处理的消息条数。
     * @note 批量边界对齐：每块处理完统一 flush_progress 一次，平衡 cache 乒乓
     *       与进度可见粒度。
     */
    template <class Handler>
    std::uint32_t poll(Handler&& handler, std::uint32_t limit) noexcept {
      constexpr std::uint32_t kChunk = 32;
      std::array<flow::Message, kChunk> buffer;
      std::uint32_t processed = 0;
      while (processed < limit) {
        const std::uint32_t want = std::min(kChunk, limit - processed);
        const std::uint32_t got = try_recv_run(buffer.data(), want);
        if (got == 0) {
          break;
        }
        for (std::uint32_t i = 0; i < got; ++i) {
          // 编译期判定回调签名：优先传 payload span，否则传整条 Message。
          if constexpr (std::is_invocable_v<Handler&, std::span<const std::byte>>) {
            std::invoke(handler, buffer[i].payload);
          } else {
            std::invoke(handler, buffer[i]);
          }
        }
        flush_progress();
        processed += got;
      }
      return processed;
    }

   private:
    HybridMpscChannel* channel_;          ///< 非持有的 channel 指针
    std::uint32_t consumer_id_ = 0;       ///< 本句柄绑定的 consumer 索引
    std::vector<std::uint64_t> read_positions_;  ///< 每 producer 环的本地读位置(字节)
    std::vector<std::uint64_t> next_sequences_;  ///< 每环期望的下一序列号(Fifo 模式)
    // 每环可见位的本地缓存(减少跨核 acquire 读)。
    std::vector<std::uint64_t> cached_visible_pos_;
    // 每环已 flush 到共享槽的位置(flush_progress 增量判定)。
    std::vector<std::uint64_t> flushed_positions_;
    std::uint64_t expected_sequence_ = 0;  ///< Ordered 模式下的全局期望序列号
    std::uint32_t last_hit_ring_ = 0;  ///< 上次命中环索引(轮询起点，提升局部性)
  };
};

}  // namespace salias::channel

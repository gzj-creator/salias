/**
 * @file src/core/channel/shared_hybrid_mpsc.hpp
 * @brief 跨进程共享内存版的混合 MPSC channel：每生产者独立 ring，共享内存中的原子位置字。
 * @details 位于 L5 channel 层，是进程内 HybridMpscChannel 的跨进程对应物。控制字与
 *          ring 缓冲均映射于 MAP_SHARED 共享内存(L0 平台层 mmap/memfd_create/大页)，
 *          经 std::atomic_ref 以 acquire/release 内存序跨进程读写，实现无锁通信。
 *          每个 producer 拥有独立的 MagicRing(L1 ring 双映射环形寻址)与独立的
 *          visible_producer_pos；consumer 轮询所有 ring。帧编解码由 L2 frame 层
 *          定义，claim/commit 与消费进度模型由 L3 flow 层提供，等待策略(L4)经模板注入。
 *
 *          核心不变式：consumer 只读到已 commit(release-store)的帧；序列号低位环绕
 *          在建表时静态校验；流控窗口(publication_window)限制生产者领先消费者的在途
 *          字节数实现背压。线程/进程模型：每个 producer 独占一个 ring 与其 Tx；
 *          consumer 独占 Rx；热点位置字各居独立缓存行(128B 对齐)避免 false sharing。
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "core/channel/hybrid_mpsc.hpp"
#include "core/flow/consumer.hpp"
#include "core/flow/error.hpp"
#include "core/flow/producer.hpp"
#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"
#include "core/frame/sequence.hpp"
#include "core/ring/magic_ring.hpp"
#include "core/wait/spin_pause.hpp"
#include "core/wait/wait_strategy.hpp"

/// channel 层：在 ring 与 flow 之上封装生产/消费端句柄与等待策略。
namespace salias::channel {

/**
 * @brief 跨进程共享内存混合 MPSC channel。
 * @details 模板参数 Ordering 选择 Ordered(全局单调)或 Fifo(每环内 FIFO)序；
 *          Wait 注入等待策略(默认 SpinPause)。内存布局由外部建表方提供，本类仅持有
 *          指向共享内存各字段的裸指针与 per-producer MagicRing。线程安全：Tx 与 Rx
 *          分别由各自线程独占使用，不可跨线程共享；底层位置字以原子操作保证可见性。
 * @tparam Ordering 投递顺序策略，默认 Ordered。
 * @tparam Wait 等待策略类型，需满足 wait::WaitStrategy 概念，默认 SpinPause。
 */
template <Order Ordering = Order::Ordered, wait::WaitStrategy Wait = wait::SpinPause>
class SharedHybridMpscChannel {
 public:
  /**
   * @brief 指向共享内存中全局控制字的指针集合(非拥有)。
   * @details 所有指针由建表/连接方从命名共享内存控制块复制得到，指向 MAP_SHARED 区域。
   *          global_seq 与 consumer_sequences 仅 Ordered 模式使用；wait_word 供等待策略。
   *          publication_window 为常量配置(创建时确定)，运行期只读。
   */
  struct SharedControl {
    std::uint64_t* global_seq = nullptr;  ///< 全局序列号(Ordered 模式共享)，共享内存中原子字。
    std::vector<std::uint64_t*> consumer_sequences;  ///< 各 consumer 的期望序列号(Ordered)，共享内存原子字。
    std::uint32_t* wait_word = nullptr;  ///< 等待策略唤醒事件计数字。
    std::uint32_t* num_producers = nullptr;  ///< producer 总数(只读)。
    std::uint32_t* num_consumers = nullptr;  ///< consumer 总数(只读)。
    // 发布流控窗口（字节，0 表示满环）。常量配置，创建/连接时从命名控制块复制得到。
    std::uint64_t publication_window = 0;
  };

  /**
   * @brief 单个 consumer 在共享内存中的进度字(非拥有指针)。
   * @details position 是消费者已消费到的字节位置(用于生产者回收空间)；
   *          sequence 是消费者期望的下一序列号。两者均以 release-store 更新。
   */
  struct ConsumerSharedState {
    std::uint64_t* position = nullptr;  ///< consumer 消费进度(字节位置，单调递增)。
    std::uint64_t* sequence = nullptr;  ///< consumer 期望的下一序列号。
  };

  /**
   * @brief 单个 producer 在共享内存中的进度字与其各 consumer 视图(非拥有指针)。
   * @details visible_producer_pos 是该 producer 已发布(可见)的尾位置，consumer 以
   *          acquire 观察它。local_sequence 为 Fifo 模式下该 producer 的本地序列号。
   *          consumers 向量列出所有 consumer 对该 producer ring 的进度字，用于背压计算。
   */
  struct ProducerSharedState {
    std::uint64_t* visible_producer_pos = nullptr;  ///< producer 已发布尾位置(release 更新)。
    std::uint64_t* local_sequence = nullptr;  ///< Fifo 模式下 producer 本地序列号。
    std::vector<ConsumerSharedState> consumers;  ///< 所有 consumer 对本 ring 的进度字。
  };

  /// create() 的返回类型：成功返回 channel，失败返回 ChannelError。
  using CreateResult = std::expected<SharedHybridMpscChannel, ChannelError>;
  /// offer() 的返回类型：成功返回 true，失败返回 flow::FlowError。
  using OfferResult = std::expected<bool, flow::FlowError>;

  class Tx;
  class Rx;

  /**
   * @brief 以外部拥有的共享内存控制字与每 producer 一个 MagicRing 构造 channel。
   * @param rings 每个 producer 对应的 MagicRing(已映射好共享内存双映射环形缓冲)。
   * @param producer_states 每个 producer 的共享进度字指针集合(非拥有)。
   * @param control 全局共享控制字指针集合(非拥有)。
   * @param wait 等待策略实例，用于 consumer 阻塞/唤醒。
   * @note 构造期从共享内存 acquire-load 各 producer 的 local_sequence 以初始化本地缓存，
   *        保证 reconnect 后本地序列号与共享内存一致。noexcept：不做可能抛异常的操作。
   */
  // Wrap externally owned MAP_SHARED control words and one magic ring per producer.
  SharedHybridMpscChannel(std::vector<ring::MagicRing> rings,
                          std::vector<ProducerSharedState> producer_states, SharedControl control,
                          Wait wait = Wait{}) noexcept
      : producer_rings_(), control_(control), wait_(std::move(wait)) {
    producer_rings_.reserve(rings.size());
    for (std::size_t i = 0; i < rings.size(); ++i) {
      producer_rings_.push_back(ProducerRing{
          .ring = std::move(rings[i]),
          .shared = producer_states[i],
          // Fifo 模式下从共享内存 acquire-load 本地序列号，与发布方保持一致。
          .local_sequence = producer_states[i].local_sequence == nullptr
                                ? 0
                                : std::atomic_ref<std::uint64_t>(*producer_states[i].local_sequence)
                                      .load(std::memory_order_acquire),
      });
    }
  }

  /// 移动构造(noexcept，默认实现)。
  SharedHybridMpscChannel(SharedHybridMpscChannel&&) noexcept = default;
  /// 移动赋值(noexcept，默认实现)。
  SharedHybridMpscChannel& operator=(SharedHybridMpscChannel&&) noexcept = default;
  /// 禁止拷贝：持有共享内存裸指针与 MagicRing，拷贝会导致双重所有权。
  SharedHybridMpscChannel(const SharedHybridMpscChannel&) = delete;
  /// 禁止拷贝赋值。
  SharedHybridMpscChannel& operator=(const SharedHybridMpscChannel&) = delete;

  /**
   * @brief 返回当前 producer 总数。
   * @return producer 数量；control_.num_producers 为空时返回 0。
   * @note 以 acquire 读取共享内存中的计数，确保看到建表方的写入。
   */
  std::uint32_t producer_count() const noexcept {
    if (control_.num_producers == nullptr) {
      return 0;
    }
    return std::atomic_ref<std::uint32_t>(*control_.num_producers).load(std::memory_order_acquire);
  }

  /**
   * @brief 返回当前 consumer 总数。
   * @return consumer 数量；control_.num_consumers 为空时返回 0。
   * @note 以 acquire 读取共享内存中的计数，确保看到建表方的写入。
   */
  std::uint32_t consumer_count() const noexcept {
    if (control_.num_consumers == nullptr) {
      return 0;
    }
    return std::atomic_ref<std::uint32_t>(*control_.num_consumers).load(std::memory_order_acquire);
  }

  /// 创建指定 producer 的发送句柄 Tx(轻量，不持有资源)。
  Tx tx(std::uint32_t producer_id) noexcept { return Tx(*this, producer_id); }
  /// 创建指定 consumer 的接收句柄 Rx(默认 consumer_id 为 0)。
  Rx rx(std::uint32_t consumer_id = 0) noexcept { return Rx(*this, consumer_id); }

 private:
  /**
   * @brief 单个 producer 的 ring、共享进度字与本地序列号缓存。
   * @details 把共享内存指针(ProducerSharedState)与本地拷贝(local_sequence)聚合，
   *          避免热路径反复跨核读共享内存。ring 为已映射的双映射环形缓冲。
   */
  struct ProducerRing {
    ring::MagicRing ring;  ///< 该 producer 的双映射环形缓冲(L1 ring 层)。
    ProducerSharedState shared;  ///< 指向共享内存中该 producer 进度字的指针集合。
    std::uint64_t local_sequence = 0;  ///< Fifo 模式下的本地序列号缓存(单调递增)。
  };

  /// 判定 producer_id 是否有效(在 producer_rings_ 范围内)。
  bool has_producer(std::uint32_t producer_id) const noexcept {
    return producer_id < producer_rings_.size();
  }

  /// 判定 consumer_id 是否有效(小于共享内存中的 consumer 总数)。
  bool has_consumer(std::uint32_t consumer_id) const noexcept {
    return consumer_id < consumer_count();
  }

  // 返回流控闸门用的有效容量：窗口为 0 时即环容量，否则取 min(环容量, 窗口)。
  // 只限制生产者领先消费者的在途字节数，不改变环的寻址与回绕（那仍用真实 capacity）。
  std::size_t effective_capacity(std::size_t ring_capacity) const noexcept {
    if (control_.publication_window == 0) {
      return ring_capacity;
    }
    const auto window = static_cast<std::size_t>(control_.publication_window);
    return window < ring_capacity ? window : ring_capacity;
  }

  /**
   * @brief 计算该 producer 所有 consumer 中最小的消费位置(字节位置)。
   * @param producer 待查询的 ProducerRing。
   * @return 最小消费位置；无 consumer 或任一 position 为空时返回 0。
   * @note 该值代表"最慢消费者"的进度，用于 claim 时计算可用空间(背压闸门)。
   *       以 acquire 读取各 consumer 的共享位置字，看到其 release-store 的最新值。
   */
  static std::uint64_t minimum_consumer_position(const ProducerRing& producer) noexcept {
    if (producer.shared.consumers.empty() ||
        producer.shared.consumers.front().position == nullptr) {
      return 0;
    }
    std::uint64_t minimum =
        std::atomic_ref<std::uint64_t>(*producer.shared.consumers.front().position)
            .load(std::memory_order_acquire);
    for (std::size_t index = 1; index < producer.shared.consumers.size(); ++index) {
      if (producer.shared.consumers[index].position == nullptr) {
        return 0;
      }
      const std::uint64_t position =
          std::atomic_ref<std::uint64_t>(*producer.shared.consumers[index].position)
              .load(std::memory_order_acquire);
      if (position < minimum) {
        minimum = position;
      }
    }
    return minimum;
  }

  /**
   * @brief 向 ring 写入未提交(未置 committed 标志)的帧头，供 claim 占位后由 commit 置位。
   * @param producer 目标 ProducerRing。
   * @param position 帧起始字节位置(单调递增，非取模)。
   * @param payload_len 负载字节数。
   * @param sequence 该帧的全局/本地序列号。
   * @note 用 relaxed 内存序：此时帧对 consumer 尚不可见，可见性由 commit 的 release 保证。
   */
  void write_uncommitted_header(ProducerRing& producer, std::uint64_t position,
                                std::uint32_t payload_len, std::uint64_t sequence) noexcept {
    const std::uint32_t meta = hybrid_detail::meta_for_sequence(sequence, false);
    hybrid_detail::store_header(producer.ring, position, payload_len, meta,
                                std::memory_order_relaxed);
  }

  /**
   * @brief 尝试从指定 producer ring 的位置读取一条已提交且序列号匹配的消息。
   * @param producer_id 目标 producer 索引。
   * @param position 当前读位置(字节位置，单调递增)。
   * @param target_sequence 期望的序列号(用于校验帧序与防止读到回绕的旧帧)。
   * @param visible_bound 该 producer 的可见上界(visible_producer_pos)。
   * @return 解析成功的消息；无可读帧时返回 nullopt。
   * @note 校验链：位置未越界 → 帧已 committed → 序列号低位匹配(防回绕) →
   *       帧长合法且不越过可见上界。任一失败均返回 nullopt。
   *       帧头以 acquire 读取，与生产者 commit 的 release 配对，保证 payload 可见。
   */
  std::optional<flow::Message> try_read_from_ring(std::uint32_t producer_id, std::uint64_t position,
                                                  std::uint64_t target_sequence,
                                                  std::uint64_t visible_bound) noexcept {
    auto& producer = producer_rings_[producer_id];
    // 读位置已达可见上界，无可读帧。
    if (position >= visible_bound) {
      return std::nullopt;
    }

    // acquire 读帧头，与生产者 commit 的 release 配对，确保 payload 已写入可见。
    const std::uint64_t header = hybrid_detail::load_header_acquire(producer.ring, position);
    const std::uint32_t payload_len = static_cast<std::uint32_t>(header);
    const std::uint32_t meta = static_cast<std::uint32_t>(header >> 32u);
    const std::uint32_t flags = meta & 0xFFu;
    // 未置 committed 位，说明生产者尚未完成提交，跳过。
    if ((flags & frame::FLAG_COMMITTED) == 0) {
      return std::nullopt;
    }
    // 序列号低位不匹配：该位置上的帧是上一圈回绕留下的旧帧，跳过。
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

  std::vector<ProducerRing> producer_rings_;  ///< 所有 producer 的 ring 与共享进度字。
  SharedControl control_;  ///< 全局共享控制字指针集合(非拥有)。
  Wait wait_;  ///< 等待策略实例，供 consumer 阻塞/唤醒。

 public:
  /**
   * @brief producer 发送端句柄，持有对 channel 与 producer_id 的引用。
   * @details 线程模型：一个 Tx 实例由单个 producer 线程独占使用。提供单帧 claim/commit
   *          与批量 claim_batch/commit_batch 两套两阶段接口，以及一步式 offer。
   *          producer_pos_ 缓存本 producer 的预留尾位置，published_pos_ 缓存已发布
   *          的最高尾位置；二者构造时从共享位置恢复，之后由
   *          单写者本地递增，避免每条 claim 都 acquire-load 消费者正在观察的
   *          visible_producer_pos cache line。cached_consumer_pos_ 缓存最慢消费者
   *          位置，仅在容量快判失败时跨核刷新。
   */
  class Tx {
   public:
    /// claim_batch 的返回类型：成功返回 BatchClaim，失败返回 FlowError。
    using BatchClaimResult = std::expected<flow::BatchClaim, flow::FlowError>;

    /**
     * @brief 构造发送端句柄。
     * @param channel 所属 channel 引用。
     * @param producer_id 本句柄对应的 producer 索引。
     */
    explicit Tx(SharedHybridMpscChannel& channel, std::uint32_t producer_id) noexcept
        : channel_(&channel), producer_id_(producer_id) {
      if (!channel.has_producer(producer_id)) {
        return;
      }
      auto* visible_pos = channel.producer_rings_[producer_id].shared.visible_producer_pos;
      if (visible_pos != nullptr) {
        // 端点创建/重连时只需同步一次共享尾位置；此后 Tx 由单线程独占，
        // producer_pos_ 可作为权威本地写游标，发布时再 release-store 给 consumer。
        producer_pos_ =
            std::atomic_ref<std::uint64_t>(*visible_pos).load(std::memory_order_acquire);
        published_pos_ = producer_pos_;
      }
    }

    /**
     * @brief 申请一帧的可写负载空间(claim 阶段，不提交)。
     * @param payload_len 负载字节数。
     * @return 成功返回 Claim(含可写 slice、起始位置、序列号等)；失败返回 FlowError。
     * @retval BackPressured 流控窗口不足，需稍后重试。
     * @retval MessageTooLarge 配置非法或帧长超过环容量。
     * @note 流控：先用缓存的消费者位置判断空间，缓存过期时才跨核刷新最小消费者位置。
     *       claim 后帧头已写入但未置 committed；可见性由后续 commit 的 release 保证。
     */
    flow::Producer::ClaimResult claim(std::uint32_t payload_len) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_) ||
          (Ordering == Order::Ordered && channel_->control_.global_seq == nullptr)) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      if (producer.shared.visible_producer_pos == nullptr || producer.shared.consumers.empty() ||
          (Ordering == Order::Fifo && producer.shared.local_sequence == nullptr)) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::size_t need = frame::frame_len(payload_len);
      if (need > producer.ring.capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      // 单写者本地游标是当前可写入起点；无需每条消息重读共享 visible position。
      const std::uint64_t tail = producer_pos_;
      const std::size_t effective_cap = channel_->effective_capacity(producer.ring.capacity());
      // 先用缓存的消费者位置判空间(快路径，避免跨核 load)。
      if (!hybrid_detail::has_capacity(effective_cap, tail, cached_consumer_pos_, need)) {
        // 缓存过期，跨核刷新最小消费者位置后再判一次(慢路径)。
        cached_consumer_pos_ = channel_->minimum_consumer_position(producer);
        if (!hybrid_detail::has_capacity(effective_cap, tail, cached_consumer_pos_, need)) {
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      // 取序列号：Ordered 用全局原子 fetch_add(relaxed，序号单调性由 fetch_add 保证)；
      // Fifo 用本地序列号并 store 回共享内存(relaxed，可见性由下方 release 保证)。
      std::uint64_t sequence = 0;
      if constexpr (Ordering == Order::Ordered) {
        sequence = std::atomic_ref<std::uint64_t>(*channel_->control_.global_seq)
                       .fetch_add(1, std::memory_order_relaxed);
      } else {
        sequence = producer.local_sequence++;
        std::atomic_ref<std::uint64_t>(*producer.shared.local_sequence)
            .store(producer.local_sequence, std::memory_order_relaxed);
      }
      // 写未提交帧头占位(此时帧对 consumer 不可见)。
      channel_->write_uncommitted_header(producer, tail, payload_len, sequence);
      producer_pos_ = tail + need;
      return flow::Claim{
          .payload = producer.ring.slice_mut(tail + frame::kHeaderSize, payload_len),
          .start_pos = tail,
          .payload_len = payload_len,
          .meta = hybrid_detail::meta_for_sequence(sequence, false),
          .sequence = sequence,
          .producer_id = producer_id_,
      };
    }

    /**
     * @brief 批量申请等长帧的可写区域(claim 阶段)。
     * @param payload_len 每帧负载字节数(所有帧等长，便于整除对齐)。
     * @param max_frames 调用方期望的最大帧数上限。
     * @return 成功返回 BatchClaim(含连续可写 region、帧数、基序列号等)；失败返回 FlowError。
     * @retval BackPressured 流控窗口不足以放下任何一帧。
     * @note 批量边界对齐：所有帧等长，整批在 ring 中连续排布，消费端可按定长步进解码。
     *       序列号一次性 fetch_add(fit)(Ordered)或本地累加 fit(Fifo)。
     */
    BatchClaimResult claim_batch(std::uint32_t payload_len, std::uint32_t max_frames) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_) ||
          (Ordering == Order::Ordered && channel_->control_.global_seq == nullptr) ||
          max_frames == 0) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      auto& producer = channel_->producer_rings_[producer_id_];
      if (producer.shared.visible_producer_pos == nullptr || producer.shared.consumers.empty() ||
          (Ordering == Order::Fifo && producer.shared.local_sequence == nullptr)) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::size_t per = frame::frame_len(payload_len);
      if (per > producer.ring.capacity()) {
        return std::unexpected(flow::FlowError::MessageTooLarge);
      }

      const std::uint64_t tail = producer_pos_;
      const std::size_t effective_cap = channel_->effective_capacity(producer.ring.capacity());
      // 快路径：用缓存消费者位置计算可容纳帧数。
      std::uint32_t fit =
          hybrid_detail::batch_fit(effective_cap, tail, cached_consumer_pos_, per, max_frames);
      if (fit == 0) {
        // 慢路径：跨核刷新最小消费者位置后重算。
        cached_consumer_pos_ = channel_->minimum_consumer_position(producer);
        fit = hybrid_detail::batch_fit(effective_cap, tail, cached_consumer_pos_, per, max_frames);
        if (fit == 0) {
          return std::unexpected(flow::FlowError::BackPressured);
        }
      }

      // 一次性为整批获取连续序列号(Ordered)或本地累加 fit(Fifo)。
      std::uint64_t base_sequence = 0;
      if constexpr (Ordering == Order::Ordered) {
        base_sequence = std::atomic_ref<std::uint64_t>(*channel_->control_.global_seq)
                            .fetch_add(fit, std::memory_order_relaxed);
      } else {
        base_sequence = producer.local_sequence;
        producer.local_sequence += fit;
        std::atomic_ref<std::uint64_t>(*producer.shared.local_sequence)
            .store(producer.local_sequence, std::memory_order_relaxed);
      }
      const std::uint64_t span_bytes = static_cast<std::uint64_t>(per) * fit;
      // 为批内每帧写未提交帧头，序列号连续递增。
      for (std::uint32_t i = 0; i < fit; ++i) {
        const std::uint64_t frame_position = tail + static_cast<std::uint64_t>(per) * i;
        channel_->write_uncommitted_header(producer, frame_position, payload_len,
                                           base_sequence + i);
      }
      producer_pos_ = tail + span_bytes;
      return flow::BatchClaim{
          .region = producer.ring.slice_mut(tail, static_cast<std::size_t>(span_bytes)),
          .start_pos = tail,
          .frame_len = static_cast<std::uint32_t>(per),
          .frame_count = fit,
          .base_sequence = base_sequence,
          .producer_id = producer_id_,
      };
    }

    /**
     * @brief 提交单帧(commit 阶段)：置 committed 位并 release-store 帧头。
     * @param claim claim() 返回的 Claim(含起始位置、负载长度、meta)。
     * @note release 内存序保证 payload 写入先于 committed 位对 consumer 可见。
     *       若等待策略需要唤醒，则 fetch_add wait_word 并唤醒阻塞的 consumer。
     */
    void commit(const flow::Claim& claim) noexcept {
      if (channel_ == nullptr || !channel_->has_producer(producer_id_)) {
        return;
      }
      auto& producer = channel_->producer_rings_[producer_id_];
      // 置 committed 位并以 release 写帧头：payload 先于 committed 可见。
      hybrid_detail::store_header(producer.ring, claim.start_pos, claim.payload_len,
                                  claim.meta | frame::FLAG_COMMITTED,
                                  std::memory_order_release);
      const std::uint64_t committed_end =
          claim.start_pos + frame::frame_len(claim.payload_len);
      if (committed_end > published_pos_) {
        // 先发布 payload/header，再推进共享可见尾；正常顺序 commit 时 consumer
        // 不再提前撞到未提交帧。乱序 commit 仍允许可见尾跨过空洞，consumer 继续
        // 依赖 COMMITTED 位停在最早未完成帧，后续较早帧提交时不回退可见位置。
        published_pos_ = committed_end;
        std::atomic_ref<std::uint64_t>(*producer.shared.visible_producer_pos)
            .store(published_pos_, std::memory_order_release);
      }
      // 唤醒可能阻塞在等待策略上的 consumer。
      if (channel_->control_.wait_word != nullptr && channel_->wait_.needs_wake()) {
        std::atomic_ref<std::uint32_t>(*channel_->control_.wait_word)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(channel_->control_.wait_word);
      }
    }

    /**
     * @brief 提交整批帧(commit 阶段)：逐帧置 committed 并 release-store。
     * @param batch claim_batch() 返回的 BatchClaim。
     * @note 逐帧写 committed 帧头(均以 release)，确保整批 payload 先于 committed 可见。
     *       最后统一唤醒 consumer。批量提交摊薄了唤醒开销。
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
      const std::uint64_t committed_end =
          batch.start_pos + static_cast<std::uint64_t>(batch.frame_len) * batch.frame_count;
      if (committed_end > published_pos_) {
        published_pos_ = committed_end;
        std::atomic_ref<std::uint64_t>(*producer.shared.visible_producer_pos)
            .store(published_pos_, std::memory_order_release);
      }
      // 整批提交后统一唤醒一次 consumer，摊薄唤醒开销。
      if (channel_->control_.wait_word != nullptr && channel_->wait_.needs_wake()) {
        std::atomic_ref<std::uint32_t>(*channel_->control_.wait_word)
            .fetch_add(1, std::memory_order_release);
        channel_->wait_.wake(channel_->control_.wait_word);
      }
    }

    /**
     * @brief 一步式发送：claim → memcpy → commit。
     * @param payload 待发送的负载字节。
     * @return 成功返回 true；失败(ClaimError)原样透传 FlowError。
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
    SharedHybridMpscChannel* channel_;  ///< 所属 channel(非拥有)。
    std::uint32_t producer_id_;  ///< 本句柄的 producer 索引。
    /// 单写者本地发布位置；构造时从共享 visible position 恢复，claim 后递增。
    std::uint64_t producer_pos_ = 0;
    /// 已提交的最高发布位置；commit 只前进不回退，支持同一 Tx 的乱序 commit。
    std::uint64_t published_pos_ = 0;
    /// 缓存的最慢消费者位置；claim 快路径用它判空间，避免每次跨核 load。
    std::uint64_t cached_consumer_pos_ = 0;
  };

  /**
   * @brief consumer 接收端句柄，持有对 channel 与 consumer_id 的引用及本地进度缓存。
   * @details 线程模型：一个 Rx 实例由单个 consumer 线程独占使用。轮询所有 producer ring，
   *          以缓存的可见位置减少跨核 load。提供单条 try_recv/recv 与批量 try_recv_run
   *          两套接口。批量消费时进度暂存本地，由 flush_progress() 统一回写共享内存，
   *          将"每消息 2 次共享 release-store"收敛为"每批次每环 2 次"。
   */
  class Rx {
   public:
    /**
     * @brief 构造接收端句柄，从共享内存 acquire-load 各进度字初始化本地缓存。
     * @param channel 所属 channel 引用。
     * @param consumer_id 本句柄对应的 consumer 索引。
     * @note 初始化时读取共享内存中的 consumer 位置与序列号，实现 reconnect 后续传。
     *       flushed_positions_ 初值等于 read_positions_，表示初始无未刷新进度。
     */
    explicit Rx(SharedHybridMpscChannel& channel, std::uint32_t consumer_id) noexcept
        : channel_(&channel),
          consumer_id_(consumer_id),
          read_positions_(channel.producer_rings_.size(), 0),
          next_sequences_(channel.producer_rings_.size(), 0),
          cached_visible_pos_(channel.producer_rings_.size(), 0),
          flushed_positions_(channel.producer_rings_.size(), 0) {
      if (!channel.has_consumer(consumer_id)) {
        return;
      }
      // Ordered 模式：从全局 consumer_sequences 取回期望序列号。
      if constexpr (Ordering == Order::Ordered) {
        if (consumer_id < channel.control_.consumer_sequences.size() &&
            channel.control_.consumer_sequences[consumer_id] != nullptr) {
          expected_sequence_ =
              std::atomic_ref<std::uint64_t>(*channel.control_.consumer_sequences[consumer_id])
                  .load(std::memory_order_acquire);
        }
      }
      // 逐 producer 取回本地读位置与序列号缓存，与共享内存对齐。
      for (std::size_t producer_id = 0; producer_id < channel.producer_rings_.size();
           ++producer_id) {
        const auto& consumers = channel.producer_rings_[producer_id].shared.consumers;
        if (consumer_id >= consumers.size()) {
          continue;
        }
        if (consumers[consumer_id].position != nullptr) {
          read_positions_[producer_id] =
              std::atomic_ref<std::uint64_t>(*consumers[consumer_id].position)
                  .load(std::memory_order_acquire);
        }
        if (consumers[consumer_id].sequence != nullptr) {
          next_sequences_[producer_id] =
              std::atomic_ref<std::uint64_t>(*consumers[consumer_id].sequence)
                  .load(std::memory_order_acquire);
        }
      }
      flushed_positions_ = read_positions_;
    }

    /**
     * @brief 尝试非阻塞接收一条消息(轮询所有 producer ring)。
     * @return 可读消息；无消息时返回 nullopt。
     * @note 轮询从上次命中的 ring 的下一个开始(轮转公平)，避免饥饿。
     *       可见位置缓存：仅当本地读位置追平缓存可见位时才跨核刷新，稳态批量消费时
     *       把跨核 load 从"每消息"降到"每批次每环一次"。
     */
    std::optional<flow::Message> try_recv() noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          channel_->producer_rings_.empty() ||
          (Ordering == Order::Ordered &&
           (consumer_id_ >= channel_->control_.consumer_sequences.size() ||
            channel_->control_.consumer_sequences[consumer_id_] == nullptr))) {
        return std::nullopt;
      }

      const std::size_t count = channel_->producer_rings_.size();
      // 从上次命中环的下一个开始轮转扫描，保证各 producer 公平。
      for (std::size_t scanned = 0; scanned < count; ++scanned) {
        const std::uint32_t producer_id =
            static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);

        // 只有当本地读位置追平缓存的可见位时，才跨核 acquire-load 生产者位置刷新缓存。
        // 稳态批量消费时，一次刷新可覆盖一整批，跨核 load 从 “每消息” 降到 “每批次每环一次”。
        if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
          auto* visible_pos = channel_->producer_rings_[producer_id].shared.visible_producer_pos;
          if (visible_pos == nullptr) {
            continue;
          }
          cached_visible_pos_[producer_id] =
              std::atomic_ref<std::uint64_t>(*visible_pos).load(std::memory_order_acquire);
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
          // 记录命中环的下一个，下次从这里开始扫描(轮转)。
          last_hit_ring_ = static_cast<std::uint32_t>((producer_id + 1) % count);
          return message;
        }
      }
      return std::nullopt;
    }

    // 批量接收：选中一个可读环后在同环内紧循环连续排空，最多填 cap 条到 out[]。
    // 每条消息只更新本地读位置（consume 语义），不写共享槽——进度由调用方在批末统一 flush_progress()。
    // 返回本次收集的条数（0 表示当前无可读消息）。
    /**
     * @brief 批量接收：选中一个可读环后在同环内紧循环连续排空。
     * @param out 输出数组，调用方分配，容量至少 cap。
     * @param cap 最多收集的消息条数。
     * @return 本次收集的条数(0 表示当前无可读消息)。
     * @note 每条消息只更新本地读位置(consume 语义)，不写共享槽——进度由调用方在
     *       批末统一 flush_progress()。批内对下一帧帧头做 prefetch 以隐藏解码延迟。
     */
    std::uint32_t try_recv_run(flow::Message* out, std::uint32_t cap) noexcept {
      if (out == nullptr || cap == 0 || channel_ == nullptr ||
          !channel_->has_consumer(consumer_id_) || channel_->producer_rings_.empty() ||
          (Ordering == Order::Ordered &&
           (consumer_id_ >= channel_->control_.consumer_sequences.size() ||
            channel_->control_.consumer_sequences[consumer_id_] == nullptr))) {
        return 0;
      }

      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t scanned = 0; scanned < count; ++scanned) {
        const std::uint32_t producer_id =
            static_cast<std::uint32_t>((last_hit_ring_ + scanned) % count);

        // 可见位置缓存刷新，同 try_recv 的快/慢路径逻辑。
        if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
          auto* visible_pos = channel_->producer_rings_[producer_id].shared.visible_producer_pos;
          if (visible_pos == nullptr) {
            continue;
          }
          cached_visible_pos_[producer_id] =
              std::atomic_ref<std::uint64_t>(*visible_pos).load(std::memory_order_acquire);
          if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {
            continue;
          }
        }

        // 同环内紧循环连续排空，直到填满 cap 或无可读帧。
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
          // 下一帧仍在可见范围内时 prefetch 其帧头，隐藏后续解码延迟。
          if (message->next_position < cached_visible_pos_[producer_id]) {
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
     * @brief 阻塞接收一条消息：循环 try_recv，空时经等待策略阻塞。
     * @return 收到的消息。
     * @note 无 wait_word 时退化为纯自旋；否则 acquire-load wait_word 作为期望值，
     *       交由等待策略等待(避免虚假唤醒导致的 busy-loop)。
     */
    flow::Message recv() noexcept {
      for (;;) {
        if (auto message = try_recv(); message.has_value()) {
          return *message;
        }
        if (channel_ == nullptr || channel_->control_.wait_word == nullptr) {
          continue;
        }
        // acquire 读 wait_word 作为期望值，与生产者唤醒的 release 配对。
        const auto expected = std::atomic_ref<std::uint32_t>(*channel_->control_.wait_word)
                                  .load(std::memory_order_acquire);
        channel_->wait_.wait(channel_->control_.wait_word, expected);
      }
    }

    /**
     * @brief 确认消费一条消息并立即把进度 release-store 回共享内存。
     * @param message 已消费的消息(含 producer_id、next_position、sequence)。
     * @note release 更新消费者位置与序列号，使生产者能回收环空间。
     *       Ordered 模式额外更新全局 consumer_sequences；Fifo 模式更新各环 sequence。
     *       本方法是"每消息回写"语义，批量场景请用 consume + flush_progress。
     */
    void release(const flow::Message& message) noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          message.producer_id >= channel_->producer_rings_.size()) {
        return;
      }

      auto& producer = channel_->producer_rings_[message.producer_id];
      if (consumer_id_ >= producer.shared.consumers.size()) {
        return;
      }
      auto& consumer = producer.shared.consumers[consumer_id_];
      if (consumer.position == nullptr || consumer.sequence == nullptr) {
        return;
      }

      read_positions_[message.producer_id] = message.next_position;
      // release 回写消费位置，生产者据此回收环空间。
      std::atomic_ref<std::uint64_t>(*consumer.position)
          .store(message.next_position, std::memory_order_release);
      flushed_positions_[message.producer_id] = message.next_position;
      if constexpr (Ordering == Order::Ordered) {
        expected_sequence_ = message.sequence + 1;
        // Ordered 模式额外更新全局 consumer_sequences 与该 consumer 的 sequence。
        std::atomic_ref<std::uint64_t>(*channel_->control_.consumer_sequences[consumer_id_])
            .store(expected_sequence_, std::memory_order_release);
        std::atomic_ref<std::uint64_t>(*consumer.sequence)
            .store(expected_sequence_, std::memory_order_release);
      } else {
        next_sequences_[message.producer_id] = message.sequence + 1;
        std::atomic_ref<std::uint64_t>(*consumer.sequence)
            .store(next_sequences_[message.producer_id], std::memory_order_release);
      }
    }

    // 只更新本地消费进度，不写共享槽；热路径批量消费时配合 flush_progress() 使用。
    // 把 “每消息 2 次共享 release-store” 收敛为 “每批次每环 2 次”。
    /**
     * @brief 只更新本地消费进度，不写共享槽(批量热路径用)。
     * @param message 已消费的消息。
     * @note 配合 flush_progress() 使用，将共享内存回写从"每消息"收敛为"每批次每环"。
     */
    void consume(const flow::Message& message) noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_) ||
          message.producer_id >= channel_->producer_rings_.size()) {
        return;
      }
      read_positions_[message.producer_id] = message.next_position;
      if constexpr (Ordering == Order::Ordered) {
        expected_sequence_ = message.sequence + 1;
      } else {
        next_sequences_[message.producer_id] = message.sequence + 1;
      }
    }

    // 把本地消费进度 release-store 到共享内存，让生产者回收环空间。
    // 仅对自上次 flush 后有推进的环写入，避免重复脏化生产者会读的缓存行。
    /**
     * @brief 把本地消费进度 release-store 到共享内存(批量回写)。
     * @note 仅对自上次 flush 后有推进的环写入(read != flushed)，避免重复脏化生产者
     *       会读的缓存行。Ordered 模式同时回写全局 consumer_sequences 与各 consumer
     *       的 position/sequence；Fifo 模式回写 position 与 sequence。
     */
    void flush_progress() noexcept {
      if (channel_ == nullptr || !channel_->has_consumer(consumer_id_)) {
        return;
      }
      const std::size_t count = channel_->producer_rings_.size();
      for (std::size_t producer_id = 0; producer_id < count; ++producer_id) {
        // 该环自上次 flush 后无推进，跳过以避免无谓的共享内存写。
        if (read_positions_[producer_id] == flushed_positions_[producer_id]) {
          continue;
        }
        auto& producer = channel_->producer_rings_[producer_id];
        if (consumer_id_ >= producer.shared.consumers.size()) {
          continue;
        }
        auto& consumer = producer.shared.consumers[consumer_id_];
        if (consumer.position == nullptr || consumer.sequence == nullptr) {
          continue;
        }
        std::atomic_ref<std::uint64_t>(*consumer.position)
            .store(read_positions_[producer_id], std::memory_order_release);
        if constexpr (Ordering == Order::Ordered) {
          if (consumer_id_ < channel_->control_.consumer_sequences.size() &&
              channel_->control_.consumer_sequences[consumer_id_] != nullptr) {
            std::atomic_ref<std::uint64_t>(*channel_->control_.consumer_sequences[consumer_id_])
                .store(expected_sequence_, std::memory_order_release);
          }
          std::atomic_ref<std::uint64_t>(*consumer.sequence)
              .store(expected_sequence_, std::memory_order_release);
        } else {
          std::atomic_ref<std::uint64_t>(*consumer.sequence)
              .store(next_sequences_[producer_id], std::memory_order_release);
        }
        flushed_positions_[producer_id] = read_positions_[producer_id];
      }
    }

   private:
    SharedHybridMpscChannel* channel_;  ///< 所属 channel(非拥有)。
    std::uint32_t consumer_id_ = 0;  ///< 本句柄的 consumer 索引。
    std::uint32_t last_hit_ring_ = 0;  ///< 上次命中的 producer ring，用于轮转扫描。
    std::vector<std::uint64_t> read_positions_;  ///< 各 producer ring 的本地读位置(字节)。
    std::vector<std::uint64_t> next_sequences_;  ///< 各 producer ring 的期望序列号(Fifo 模式)。
    std::vector<std::uint64_t> cached_visible_pos_;  ///< 各 producer ring 的可见位置缓存。
    std::vector<std::uint64_t> flushed_positions_;  ///< 各环已回写共享内存的位置，用于 flush 去重。
    std::uint64_t expected_sequence_ = 0;  ///< Ordered 模式下全局期望的下一序列号。
  };
};

}  // namespace salias::channel

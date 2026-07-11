/**
 * @file src/core/flow/producer.hpp
 * @brief 声明 SPSC 生产端：在 ring 中预留可写区域并发布已提交帧。
 * @details 位于 L3 flow 子层，基于 L1 ring(MagicRing) 与 L3 position 位置单元工作。
 *          生产者先 claim() 预留 payload 区域(此时帧头尚未写入、对消费者不可见)，
 *          写完 payload 后由 commit() 写入帧头并以 release 语义推进 tail，使帧可见。
 *          此"claim→写 payload→commit"两阶段避免了消费者读到半写帧。
 *          线程模型：假定恰好一个写入者独占使用；非线程安全。调用方负责 ring 与
 *          Positions 的生命周期。背压机制：claim 发现空间不足时返回 BackPressured。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "core/flow/error.hpp"
#include "core/flow/position.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias::flow {  // flow 层：环形缓冲之上的生产/消费位置与流控逻辑

/**
 * @brief 一次 claim 的结果，描述预留的可写 payload 区域。
 * @details payload 为指向 ring 内部内存的可写 span，归当前生产者独占使用直至
 *          commit()。start_pos 为帧在 ring 中的起始字节偏移；payload_len 为负载
 *          字节数；meta 为预计算的帧头元数据(含 generation 与 flags)。
 *          生命周期：payload 仅在对应 commit() 调用前有效，commit 后空间归 ring 管理。
 *          所有权：本结构不拥有内存，仅持有视图。
 */
struct Claim {
  std::span<std::byte> payload;  ///< 预留的可写负载区域(指向 ring 内存)
  std::uint64_t start_pos = 0;    ///< 帧起始字节偏移
  std::uint32_t payload_len = 0;  ///< 负载字节数(不含帧头)
  std::uint32_t meta = 0;         ///< 预计算的帧头元数据(generation、flags)
  std::uint64_t sequence = 0;     ///< 帧序列号(由上层填充，flow 层透传)
  std::uint32_t producer_id = 0;  ///< 生产者标识
};

/**
 * @brief 一次批量 claim 的结果，描述连续预留的多帧区域。
 * @details region 为 ring 中连续可写的内存区，承载 frame_count 个等长帧，
 *          每帧长度为 frame_len(含帧头+payload+对齐 padding)。start_pos 为首帧
 *          起始偏移；base_sequence 为首帧序列号，后续帧依次递增。
 *          批量边界对齐：所有帧等长，便于消费端按定长步进解码。
 *          生命周期与所有权同 Claim：commit 前独占，本结构不持有内存。
 */
struct BatchClaim {
  std::span<std::byte> region;    ///< 批量预留的连续可写区(含全部帧的帧头与 payload)
  std::uint64_t start_pos = 0;    ///< 首帧起始字节偏移
  std::uint32_t frame_len = 0;    ///< 单帧长度(帧头+payload+对齐，定长)
  std::uint32_t frame_count = 0;  ///< 帧数量
  std::uint64_t base_sequence = 0;  ///< 首帧序列号，后续帧依次 +1
  std::uint32_t producer_id = 0;  ///< 生产者标识
};

/**
 * @brief SPSC 生产端；它只借用 MagicRing 和位置单元。
 * @details 调用方必须保证 ring 与 Positions 存活。该类假定恰好一个写入者
 *          拥有 producer。线程安全：单写者独占，非线程安全。
 *          不变式：claim 与 commit 必须由同一生产者线程串行调用。
 */
class Producer {
 public:
  /// @brief claim 的结果类型：成功返回 Claim，失败返回 FlowError。
  using ClaimResult = std::expected<Claim, FlowError>;

  /**
   * @brief 绑定非持有的 ring 与生产者/消费者位置单元。
   * @param ring 已完成初始化的可写 MagicRing 引用，须由调用方保证存活。
   * @param positions 生产者/消费者位置单元指针集合，cap>0 方可正常 claim。
   * @note noexcept：仅拷贝指针，不分配内存。
   */
  Producer(ring::MagicRing& ring, Positions positions) noexcept;

  /**
   * @brief 预留可写 payload 区域但不发布。
   * @param payload_len 期望的负载字节数(不含帧头)。
   * @retval Claim  预留成功，payload 指向 ring 内独占可写区域，meta 已预计算。
   * @retval BackPressured  当前 ring 剩余空间不足，需等待消费者 advance 或重试。
   * @retval MessageTooLarge  请求的帧长超过 ring 总容量，不可恢复。
   * @note 内存序：当本地缓存的消费者 head 失效时，以 acquire load 重新读取，
   *       与消费者 advance() 的 release store 配对，确保复用空间前旧数据已被读取。
   *       返回区域在 commit() 前仅归当前生产者使用。
   */
  ClaimResult claim(std::uint32_t payload_len) noexcept;

  /**
   * @brief 写入帧头并发布已预留的帧。
   * @param claim 此前 claim() 返回的预留描述，payload 必须已经写完。
   * @note 调用前 payload 必须已经写完。内存序：先写帧头，再以 release store
   *       推进 tail，与消费者 poll() 的 acquire load 配对，形成 happens-before 边，
   *       保证消费者读到帧时帧头与 payload 均已完整可见。
   */
  void commit(const Claim& claim) noexcept;

 private:
  ring::MagicRing* ring_;          ///< 非持有的 ring 指针(不拥有)
  Positions positions_;            ///< 生产者/消费者位置单元(非持有)
  std::uint64_t cached_head_ = 0;  ///< 消费者 head 的本地缓存，减少原子读的缓存行乒乓
};

}  // namespace salias::flow

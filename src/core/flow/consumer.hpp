/**
 * @file src/core/flow/consumer.hpp
 * @brief 声明 SPSC 消费端：从 ring 读取已提交帧并推进消费位置。
 * @details 位于 L3 flow 子层，基于 L1 ring(MagicRing) 与 L3 position 位置单元工作。
 *          消费者以 acquire 语义观察生产者 tail，确保读到完整的帧头与 payload；
 *          通过 release 语义推进消费者 head，把环形空间归还给生产者复用。
 *          不拥有 ring 与位置存储，调用方(channel 层)负责其生命周期。
 *          线程模型：单个消费者线程/进程独占使用，poll/advance 不可并发调用。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

#include "core/flow/position.hpp"
#include "core/ring/magic_ring.hpp"

namespace salias::flow {  // flow 层：环形缓冲之上的生产/消费位置与流控逻辑

/**
 * @brief 消费者从 ring 读出的一条帧的只读视图。
 * @details payload 为指向 ring 内存的 span，其生命周期不独立——调用方必须在
 *          advance() 推进消费者位置、或生产者写入新帧之前完成对 payload 的使用，
 *          否则数据可能被覆盖。position 为本帧起始字节偏移，next_position 为下一帧
 *          起始偏移(已含帧对齐)。meta 携带帧头元数据(generation/flags)。
 *          所有字段为值类型，可安全拷贝；但 payload 指向的内存不归本结构所有。
 */
struct Message {
  std::span<const std::byte> payload;  ///< 帧负载只读视图(指向 ring 内部内存)
  std::uint64_t position = 0;           ///< 本帧在 ring 中的起始字节偏移
  std::uint64_t next_position = 0;      ///< 下一帧的起始字节偏移(含帧对齐)
  std::uint32_t meta = 0;               ///< 帧头元数据(generation、标志位)
  std::uint64_t sequence = 0;           ///< 帧序列号(由上层填充，flow 层透传)
  std::uint32_t producer_id = 0;        ///< 生产者标识(多生产场景下区分来源)
};

/**
 * @brief SPSC 消费端；它不拥有 ring 或位置存储。
 * @details poll() 以 acquire 语义观察生产者位置，advance() 将消费者进度
 *          release 给生产者。线程安全：单个消费者独占使用，非线程安全。
 *          不变式：ring_ 与 positions_ 在构造后保持有效直至对象销毁。
 */
class Consumer {
 public:
  /**
   * @brief 绑定非持有的 ring 与生产者/消费者位置单元。
   * @param ring 已完成初始化的只读 MagicRing 引用，须由调用方保证存活。
   * @param positions 生产者/消费者位置单元指针集合，不可为 nullptr(否则 poll 返回空)。
   * @note noexcept：仅拷贝指针，不分配内存。
   */
  Consumer(const ring::MagicRing& ring, Positions positions) noexcept;

  /**
   * @brief 非阻塞读取下一条已提交帧，但不推进消费者位置。
   * @return 若有可用帧返回 Message；无帧、未绑定或未就绪时返回 std::nullopt。
   * @retval Message  调用方应在使用完 payload 后调用 advance() 释放空间。
   * @retval std::nullopt  当前无可消费帧(生产者尚未提交或未绑定)。
   * @note 内存序：内部以 acquire load 读取生产者位置，与生产者 commit() 的
   *       release store 配对，形成跨线程/进程的 happens-before 边。
   */
  std::optional<Message> poll() noexcept;

  /**
   * @brief 发布消费者进度，使生产者可以复用已释放的环形空间。
   * @param new_head 新的消费者 head 位置(通常为上次 poll() 返回的 next_position)。
   * @note 内存序：以 release store 写入，与生产者 claim() 中的 acquire load 配对，
   *       确保此前对帧的读取对生产者可见后才允许其覆盖该区域。
   */
  void advance(std::uint64_t new_head) noexcept;

 private:
  const ring::MagicRing* ring_;  ///< 非持有的 ring 指针(不拥有)
  Positions positions_;          ///< 生产者/消费者位置单元(非持有)
  std::uint64_t cached_tail_ = 0;  ///< 生产者 tail 的本地缓存，减少原子读的缓存行乒乓
};

}  // namespace salias::flow

/**
 * @file src/core/channel/hybrid_control.hpp
 * @brief 跨进程共享内存中 hybrid MPSC channel 的序列号与控制块布局。
 * @details 位于 L5 channel 层，定义被映射到 MAP_SHARED 共享内存(mmap/memfd_create)
 *          的控制结构。该结构由多个 producer 进程与(单个)consumer 进程共同读写，
 *          因此每个热点字段独占一条 128 字节缓存行(alignas(128))，避免 false sharing。
 *          依赖 L0 平台层保证内存可见性与缓存一致性；内存序由读写方各自以
 *          acquire/release 约束(本结构本身不强制内存序)。该结构是 SharedHybridMpscChannel
 *          的 SharedControl 所指向的底层共享字段的物理宿主。
 */
#pragma once

#include <atomic>
#include <cstdint>

/// channel 层：在 ring 与 flow 之上封装生产/消费端句柄与等待策略。
namespace salias::channel {

/**
 * @brief 消息投递顺序策略。
 * @details Ordered 表示全局单调序列号——所有 producer 共享同一个 global_seq，
 *          consumer 按序列号严格有序消费；Fifo 表示每个 producer 的 ring 内部
 *          各自 FIFO，环间无先后保证。两种序影响 claim/commit 时序列号的取法。
 */
enum class Order : std::uint8_t {
  Fifo,     ///< 每环内 FIFO：各 producer 独立 local_sequence，环间无序。
  Ordered,  ///< 全局有序：所有 producer 共享 global_seq，consumer 按序消费。
};

/**
 * @brief 跨进程共享的 hybrid MPSC 控制块，映射于共享内存。
 * @details 每个 cache line(128 字节)对齐的热点字段独立成行，确保多 producer 与
 *          consumer 在不同 CPU 核上推进各自进度时不发生 false sharing。
 *          线程/进程模型：global_seq 被 Order::Ordered 模式下所有 producer 原子
 *          fetch_add；consumer_seq 被 consumer release-store；wait_word 经
 *          等待策略(L4)用作唤醒事件计数；num_producers 在创建/连接时写入。
 *          不变式：字段值单调递增(序列号与 wait_word)，环绕由上层序列号低位窗口保证。
 * @note 所有字段在共享内存中以 atomic_ref 访问；结构本身仅定义布局与初值。
 */
// Shared sequencing state for the hybrid MPSC design. Each hot field sits on its own
// 128-byte line so producers and the single consumer do not false-share progress words.
struct alignas(128) HybridSharedControl {
  /// 全局序列号(Ordered 模式下所有 producer 共享)，独占缓存行避免多写者 false sharing。
  alignas(128) std::atomic<std::uint64_t> global_seq{0};
  /// consumer 当前期望的下一个序列号(Ordered 模式)，release-store 后对 producer 可见。
  alignas(128) std::atomic<std::uint64_t> consumer_seq{0};
  /// 等待策略唤醒事件计数字，consumer 阻塞前 acquire-load、producer 唤醒时 release 自增。
  alignas(128) std::uint32_t wait_word = 0;
  /// producer 总数，创建/连接共享内存时由建表方写入，运行期视为只读。
  std::uint32_t num_producers = 0;
  // 发布流控窗口（字节，0 表示满环）；限制生产者领先消费者的在途字节数。
  std::uint64_t publication_window = 0;
};

/// 编译期断言：控制块整体按 128 字节对齐，保证共享内存映射首地址落在缓存行边界。
static_assert(alignof(HybridSharedControl) == 128);

}  // namespace salias::channel

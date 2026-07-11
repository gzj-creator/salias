/**
 * @file src/core/flow/position.hpp
 * @brief 定义 SPSC 生产/消费位置单元的非持有引用。
 * @details 位于 L3 flow 子层，将"位置指针"与"ring 存储"解耦：本结构只持有裸指针，
 *          不负责分配或释放。在测试场景下指针指向进程本地内存；在真实 IPC 通道中
 *          指针指向经 mmap 映射的共享内存区域。
 *          不变式：producer/consumer 指向的 64 位整数必须按缓存行对齐，且其访问
 *          统一经 atomic_ref 以 acquire/release 语义进行，避免撕裂读与重排序。
 *          线程模型：producer 单写、consumer 单写(各自由不同线程/进程持有)。
 */
#pragma once

#include <cstddef>
#include <cstdint>

namespace salias::flow {  // flow 层：环形缓冲之上的生产/消费位置与流控逻辑

/**
 * @brief 指向 SPSC 位置单元的非持有指针集合。
 * @details 这些单元在测试中可位于进程本地内存，在真实通道中可位于共享内存；
 *          L3 跨越生产者/消费者所有权边界时通过 atomic_ref helper 访问。
 *          所有权：本结构不拥有所指内存，调用方(通道层)必须保证其存活期覆盖
 *          Producer/Consumer 的全部使用。cap 为 ring 容量，用于容量判定与
 *          sequence 低位环绕(wraparound)计算。任何指针为 nullptr 表示未绑定。
 */
struct Positions {
  std::uint64_t* producer = nullptr;  ///< 生产者 tail：下一个可写入位置(单调递增)
  std::uint64_t* consumer = nullptr;  ///< 消费者 head：下一个可读取位置(单调递增)
  std::size_t cap = 0;                ///< ring 容量(字节)，0 表示未初始化
};

}  // namespace salias::flow

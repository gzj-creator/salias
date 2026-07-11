/**
 * @file src/core/frame/header.hpp
 * @brief frame 帧头常量、标志位与布局定义。
 * @details 本文件位于 L2 frame 层，定义 ring 中每条消息固定 8 字节帧头(frame header)
 *          的内存布局：前 4 字节为 payload 长度(len)，后 4 字节为 metadata
 *          （低 8 位 flags + 高 24 位 sequence 低位）。
 *          关键不变式：kFrameAlign=8 保证帧头与 payload 起始均按 8 字节对齐，
 *          消费者可用对齐读且避免跨缓存行(cache line)拆分。salias 仅面向同主机
 *          Linux IPC，字段按主机小端写入，不做字节序转换（异构传输不在 L2 范围）。
 *          线程/进程模型：帧头由 producer 写入、consumer 读取，可见性由上层 ring
 *          的 atomic release/acquire 保证，本结构体本身无同步原语。
 */
#pragma once

#include <cstddef>
#include <cstdint>

namespace salias::frame {
/// frame 帧头布局与标志位命名空间；定义 ring 内每帧的固定 8 字节元数据。

/// 帧头固定字节数；所有帧在 ring 中至少占用 kHeaderSize 字节。
inline constexpr std::size_t kHeaderSize = 8;
/// 帧对齐粒度（2 的幂）；payload 与帧头均按此对齐，便于掩码运算与缓存行访问。
inline constexpr std::size_t kFrameAlign = 8;

/**
 * @brief 帧 metadata 的标志位定义。
 * @details 这些标志位占据 metadata 的低 8 位，高 24 位留给 sequence 低位
 *          （见 sequence.hpp）。标准单帧消息组合使用 BEGIN | END | COMMITTED；
 *          PADDING 用于 ring 尾部不足以容纳一帧时的填充槽，consumer 据此跳过。
 *          多帧(batch)消息用 BEGIN 标记首帧、END 标记末帧，中间帧两者皆无。
 */
// 帧 metadata 标志；标准单帧消息使用 BEGIN | END | COMMITTED。
enum Flags : std::uint32_t {
  FLAG_BEGIN = 1u << 0,     ///< 多帧消息的首帧标志。
  FLAG_END = 1u << 1,       ///< 多帧消息的末帧标志。
  FLAG_PADDING = 1u << 2,   ///< ring 尾部填充槽标志，consumer 应跳过。
  FLAG_COMMITTED = 1u << 3, ///< 帧已发布提交标志，producer 写完后置位。
};

/**
 * @brief ring 内固定 8 字节帧头(frame header)。
 * @details
 * - 内存布局：len(4B) + meta(4B)，共 8 字节，static_assert 校验；
 * - len：payload 字节数（不含帧头本身），consumer 据此读取后续数据；
 * - meta：低 8 位为 flags，高 24 位为 sequence 低位，编码见 sequence.hpp；
 * - 所有权/生命周期：帧头驻留在 ring 的共享内存(shared memory)中，无独立所有权，
 *   生命周期由上层 ring 管理；
 * - 线程安全：结构体本身非线程安全，跨线程可见性由上层 ring 的内存序保证；
 * - 字节序：按主机小端写入，仅限同主机 IPC。
 */
// ring 内固定 8 字节帧头。
// salias 只面向同主机 Linux IPC，因此字段按主机小端布局写入；异构字节序传输不属于 L2 范围。
struct FrameHeader {
  std::uint32_t len;  ///< payload 长度（字节数，不含帧头）。
  std::uint32_t meta; ///< metadata：低 8 位 flags + 高 24 位 sequence 低位。
};

/// 编译期断言帧头恰好为 kHeaderSize 字节，防止布局漂移破坏跨进程兼容。
static_assert(sizeof(FrameHeader) == kHeaderSize);

}  // namespace salias::frame

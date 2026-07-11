/** @file test/frame/codec_test.cpp
 * @brief salias L2 frame 层帧编解码（codec）与对齐计算的单元测试。
 * @details 本文件位于 L2 frame（帧编解码）层，验证固定 8 字节帧头（frame
 *   header）的 encode/decode 往返一致性、payload 按 8 字节对齐后帧总长
 *   （frame_len）的数学计算，以及固定槽位（fixed_slot）取整逻辑。关键不变式：
 *   帧头大小 kHeaderSize 与结构体 FrameHeader 均为 8 字节，帧对齐 kFrameAlign
 *   为 8；frame_len = kHeaderSize + align_up(payload_len)，保证每帧在 ring 中
 *   起始与结束均按缓存行友好的 8 字节边界对齐。
 */

#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>

namespace {  // 匿名命名空间：帧编解码测试用例，仅本编译单元可见。

/// @brief 验证固定 8 字节帧头字节布局可通过 encode/decode 往返还原。
/// @details 同时以 static_assert 锁定帧头大小为 8 字节、帧对齐为 8 字节，
///   防止后续重构无意改动内存布局而破坏 ring 中的帧定位。
// 验证固定帧头字节布局可通过 encode/decode 往返。
TEST(FrameCodecTest, EncodesAndDecodesEightByteHeader) {
  static_assert(sizeof(salias::frame::FrameHeader) == salias::frame::kHeaderSize);
  static_assert(salias::frame::kHeaderSize == 8);
  static_assert(salias::frame::kFrameAlign == 8);

  std::array<std::byte, salias::frame::kHeaderSize> bytes{};
  // meta 高位承载序列号（seq），低位承载标志位（flags）。
  const std::uint32_t meta =
      salias::frame::FLAG_BEGIN | salias::frame::FLAG_END | salias::frame::FLAG_COMMITTED |
      (0x00A5A5u << 8);

  salias::frame::encode_header(bytes, 1234, meta);
  const salias::frame::FrameHeader decoded = salias::frame::decode_header(bytes);

  EXPECT_EQ(decoded.len, 1234u);
  EXPECT_EQ(decoded.meta, meta);
  EXPECT_EQ(salias::frame::flags(decoded),
            salias::frame::FLAG_BEGIN | salias::frame::FLAG_END |
                salias::frame::FLAG_COMMITTED);
  EXPECT_EQ(salias::frame::seq(decoded), 0x00A5A5u);
}

/// @brief 验证 payload 按 8 字节向上对齐及帧总长计算的边界用例。
/// @details align_up 将 payload 长度取整到 8 的倍数；frame_len 在其基础上
///   加上 8 字节帧头；fixed_slot 为去除帧头后的可用固定槽位大小。
// 验证 payload 对齐和总帧大小计算。
TEST(FrameCodecTest, AlignsPayloadAndComputesFrameLength) {
  EXPECT_EQ(salias::frame::align_up(0), 0u);
  EXPECT_EQ(salias::frame::align_up(1), 8u);
  EXPECT_EQ(salias::frame::align_up(8), 8u);
  EXPECT_EQ(salias::frame::align_up(9), 16u);

  EXPECT_EQ(salias::frame::frame_len(0), 8u);
  EXPECT_EQ(salias::frame::frame_len(8), 16u);
  EXPECT_EQ(salias::frame::frame_len(9), 24u);
  EXPECT_EQ(salias::frame::fixed_slot(9), 16u);
}

/// @brief 验证常见 8 字节 payload（恰好无需额外填充）的帧总长为 16 字节。
// 验证常见 8 字节 payload 的帧大小。
TEST(FrameCodecTest, EightBytePayloadUsesSixteenBytesTotal) {
  EXPECT_EQ(salias::frame::frame_len(8), 16u);
}

}  // namespace

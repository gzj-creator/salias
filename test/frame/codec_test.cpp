#include "core/frame/codec.hpp"
#include "core/frame/header.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>

namespace {

TEST(FrameCodecTest, EncodesAndDecodesEightByteHeader) {
  static_assert(sizeof(salias::frame::FrameHeader) == salias::frame::kHeaderSize);
  static_assert(salias::frame::kHeaderSize == 8);
  static_assert(salias::frame::kFrameAlign == 8);

  std::array<std::byte, salias::frame::kHeaderSize> bytes{};
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

TEST(FrameCodecTest, EightBytePayloadUsesSixteenBytesTotal) {
  EXPECT_EQ(salias::frame::frame_len(8), 16u);
}

}  // namespace

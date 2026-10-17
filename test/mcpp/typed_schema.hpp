#pragma once

#include <cstdint>

namespace salias_mcpp {

struct TypedPayload {
  std::uint64_t id = 0;
  std::uint32_t sequence = 0;
  bool operator==(const TypedPayload&) const = default;
};

struct TypedPayloadSchema {
  using message_type = TypedPayload;
  static constexpr std::uint64_t schema_id = 0x53414C4941534D43ull;
  static constexpr std::uint32_t version = 1;
};

}  // namespace salias_mcpp

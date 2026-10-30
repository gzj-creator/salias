#pragma once

#include <cstdint>

namespace salias_mcpp {

struct TypedPayload {
  std::uint64_t id = 0;
  std::uint32_t sequence = 0;
  bool operator==(const TypedPayload&) const = default;
};

using TypedPayloadSchema = salias::Schema<TypedPayload, 0x53414C4941534D43ull, 1>;

}  // namespace salias_mcpp

#pragma once

#include <cstddef>
#include <cstdint>

namespace salias::flow {

// Non-owning pointers to SPSC position cells. The pointed-to cells may live in process-local memory
// during tests or in shared memory in real channels; L3 accesses them with atomic_ref helpers when
// crossing producer/consumer ownership boundaries.
struct Positions {
  std::uint64_t* producer = nullptr;
  std::uint64_t* consumer = nullptr;
  std::size_t cap = 0;
};

}  // namespace salias::flow

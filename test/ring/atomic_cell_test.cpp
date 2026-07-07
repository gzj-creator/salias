#include "core/ring/atomic_cell.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace {

// 验证 release store 能被 acquire load 观察到。
TEST(AtomicCellTest, StoreReleaseIsObservedByLoadAcquire) {
  std::uint64_t cell = 0;

  salias::ring::store_release(cell, 42);

  EXPECT_EQ(salias::ring::load_acquire(cell), 42u);
}

}  // namespace

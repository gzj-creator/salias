#pragma once

#include <cstddef>
#include <cstdint>

namespace salias::flow {

// 指向 SPSC 位置单元的非持有指针。
// 这些单元在测试中可位于进程本地内存，在真实通道中可位于共享内存；
// L3 跨越生产者/消费者所有权边界时通过 atomic_ref helper 访问。
struct Positions {
  std::uint64_t* producer = nullptr;
  std::uint64_t* consumer = nullptr;
  std::size_t cap = 0;
};

}  // namespace salias::flow

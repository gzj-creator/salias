/**
 * @file poll_buffer.hpp
 * @brief 单次 poll 独占的未初始化描述符存储，不改变公开 Message 的默认值。
 */
#pragma once

#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>

#include "salias/message.hpp"

namespace salias::detail {

/**
 * @brief 为批量接收提供栈上存储，避免空轮询先清零整批 Message。
 * @note std::byte 数组开始生命周期时可隐式创建 Message 数组（C++20 隐式生命周期规则）。
 *       fetch_batch 必须完整赋值前 got 个元素后才能读取；未返回的元素禁止读取。
 *       每次调用独占存储，不与端点或递归调用共享；不改变借用和批末 flush 时机。
 */
template <std::size_t Count>
class PollMessageBuffer {
  static_assert(Count > 0);
  static_assert(std::is_aggregate_v<Message> && std::is_trivially_copyable_v<Message> &&
                std::is_trivially_copy_constructible_v<Message> &&
                std::is_trivially_destructible_v<Message>);

 public:
  Message* data() noexcept { return std::launder(reinterpret_cast<Message*>(storage_)); }

 private:
  alignas(Message) std::byte storage_[sizeof(Message) * Count];
};

}  // namespace salias::detail

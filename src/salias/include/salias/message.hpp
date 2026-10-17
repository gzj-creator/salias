/**
 * @file src/salias/include/salias/message.hpp
 * @brief salias 公共 API 中的消息类型别名。
 * @details 位于 L7 公共外观层，将 L3 flow 层的 flow::Message 暴露为 salias::Message。
 *          Message 是已接收消息的零拷贝借用视图，其 payload 指针指向所属 Channel
 *          的 ring 内存；该内存在消费者 release 或被覆盖前保持有效。进程间共享，
 *          线程安全语义由上层协议（MPSC/MPMC）保证。
 */
#pragma once

#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

#include "core/flow/consumer.hpp"

/// salias 公共 API 命名空间，聚合 L7 用户可见的通道、发布者、订阅者与消息类型。
namespace salias {

// 已接收消息的借用视图。
// payload 指向所属 Channel 的 ring，按通道协议在释放或覆盖前保持有效。
using Message = flow::Message;

/// @brief Marker schema used by the legacy variable-length byte channel API.
struct RawPayloadSchema final {};

namespace detail {

/// @brief 识别 std::span，避免把进程内指针/长度描述符误当成跨进程消息发送。
template <class T>
struct IsSpan : std::false_type {};

template <class Element, std::size_t Extent>
struct IsSpan<std::span<Element, Extent>> : std::true_type {};

template <class T>
struct IsStringView : std::false_type {};

template <class Character, class Traits>
struct IsStringView<std::basic_string_view<Character, Traits>> : std::true_type {};

/**
 * @brief 约束可直接按对象表示传输的固定大小消息类型。
 * @details 该约束只验证顶层类型；结构体成员不得包含进程内指针，布局兼容性仍由应用协议保证。
 *          对象 padding 也会按原样传输；不适用于需要字段级序列化或跨不可信边界的场景。
 */
template <class T>
concept WireMessage =
    std::is_object_v<T> && std::is_same_v<T, std::remove_cv_t<T>> &&
    std::is_trivially_copyable_v<T> && std::is_trivially_copy_constructible_v<T> &&
    !std::is_array_v<T> && !std::is_pointer_v<T> && !std::is_member_pointer_v<T> &&
    !IsSpan<T>::value && !IsStringView<T>::value;

/**
 * @brief Application-declared, fixed-message wire schema for a typed Channel.
 * @details The schema ID and version must be stable across processes. Bump the version whenever
 *          the field layout or meaning changes, including same-size layout changes.
 */
template <class Schema>
concept WireSchema = requires {
  typename Schema::message_type;
  requires WireMessage<typename Schema::message_type>;
  { Schema::schema_id } -> std::convertible_to<std::uint64_t>;
  requires std::is_integral_v<std::remove_cv_t<decltype(Schema::schema_id)>>;
  { Schema::version } -> std::convertible_to<std::uint32_t>;
  requires std::is_integral_v<std::remove_cv_t<decltype(Schema::version)>>;
};

struct SchemaDescriptor {
  std::uint64_t schema_id = 0;
  std::uint32_t version = 0;
  std::uint32_t payload_size = 0;
  bool typed = false;
};

template <WireSchema Schema>
consteval SchemaDescriptor schema_descriptor() {
  using T = typename Schema::message_type;
  static_assert(Schema::schema_id != 0, "schema_id zero is reserved for raw channels");
  static_assert(Schema::version != 0, "wire schema version must be explicit and non-zero");
  static_assert(std::in_range<std::uint64_t>(Schema::schema_id), "schema_id must fit in uint64_t");
  static_assert(std::in_range<std::uint32_t>(Schema::version),
                "wire schema version must fit in uint32_t");
  static_assert(sizeof(T) <= std::numeric_limits<std::uint32_t>::max(),
                "wire message exceeds the supported frame payload size");
  return {.schema_id = static_cast<std::uint64_t>(Schema::schema_id),
          .version = static_cast<std::uint32_t>(Schema::version),
          .payload_size = static_cast<std::uint32_t>(sizeof(T)),
          .typed = true};
}

inline constexpr SchemaDescriptor kRawPayloadSchema{};

/// @brief 获取一个固定类型值的只读字节视图，不分配内存。
template <WireMessage T>
std::span<const std::byte> as_bytes(const T& value) noexcept {
  return std::as_bytes(std::span<const T>{std::addressof(value), 1});
}

/**
 * @brief 从共享内存 payload 的对象表示构造本地值。
 * @note 调用方必须保证 payload 来自与 T 兼容的协议；非法对象表示不属于支持范围。
 */
template <WireMessage T>
T copy_from_bytes(std::span<const std::byte> bytes) noexcept {
  // memcpy overwrites every byte before bit_cast; avoid zero-initializing large payloads.
  std::array<std::byte, sizeof(T)> object_bytes;
  std::memcpy(object_bytes.data(), bytes.data(), sizeof(T));
  return std::bit_cast<T>(object_bytes);
}

}  // namespace detail

}  // namespace salias

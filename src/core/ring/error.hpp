/** @file src/core/ring/error.hpp
 * @brief 定义 L1 ring 层的容量与参数错误码。
 * @details 本文件位于 L1 ring 环形缓冲层，向上为 MagicRing 提供容量校验失败时的
 *  统一错误类型。下层依赖 L0 平台层（mmap/memfd_create/huge page）成功映射后的
 *  Mapping；本文件本身不涉及内存映射，只描述 ring 创建阶段对容量（必须为 2 的幂、
 *  非零、不超过上限）的约束。错误码以 std::expected 的 E 类型传递，遵循 salias 的
 *  “值语义返回 + 错误即枚举”约定，不抛异常。
 */
#pragma once

namespace salias::ring {
/// L1 ring 层错误码命名空间；所有 ring 创建/校验失败均以 RingError 形式返回。

/**
 * @brief ring 环形缓冲创建与参数校验的错误码。
 * @details 这些值通过 std::expected<MagicRing, RingError> 返回；调用者据此区分
 *  映射长度为零、非 2 的幂或超过单段容量上限等失败原因，便于在 L3 flow 层与
 *  L5 channel 层做差异化处理。零值 Ok 表示成功，便于隐式布尔判断。
 */
enum class RingError {
  Ok = 0,           ///< 成功；映射有效且容量合法。
  NotPowerOfTwo,    ///< 映射长度非 2 的幂，无法用位掩码做环绕索引。
  TooLarge,         ///< 映射长度超过单段容量上限。
  ZeroLen           ///< 映射指针为空或长度为零。
};

}  // namespace salias::ring

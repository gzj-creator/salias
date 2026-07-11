/**
 * @file src/core/platform/error.hpp
 * @brief 平台层错误码定义。
 * @details 本文件位于 salias 分层架构的 L0 平台层，集中定义 mmap/memfd_create/
 * huge page 等系统调用失败时返回的强类型错误码。上层（L1 ring、L5 channel 等）
 * 通过 std::expected<Mapping, PlatformError> 携带这些错误码，从而避免异常开销，
 * 保持无锁热路径上的确定性。错误码为 scoped enum（enum class），零值表示成功。
 */
#pragma once

namespace salias::platform {

/// L0 平台层错误码命名空间，仅用于平台资源（内存映射、文件描述符等）相关错误。

/**
 * @brief 平台层系统调用失败的强类型错误码。
 * @details 使用 enum class 防止隐式转换与命名污染。Ok = 0 表示成功；
 * 其余枚举值分别对应 memfd 创建、ftruncate、虚拟地址预留、MAP_FIXED 映射、
 * 解除映射、大页（huge page）可用性、NUMA 绑定与尺寸校验等失败场景。
 * 值不对外稳定承诺，调用方应始终通过枚举名而非数值比较。
 */
enum class PlatformError {
  Ok = 0,                  ///< 操作成功。
  MemfdCreateFailed,       ///< memfd_create 系统调用失败。
  FtruncateFailed,         ///< ftruncate 设置 backing store 大小失败。
  ReserveFailed,           ///< 预留连续虚拟地址区间失败。
  MapFixedFailed,          ///< MAP_FIXED 双映射替换预留区间失败。
  UnmapFailed,             ///< munmap 解除映射失败（当前未直接对外暴露）。
  HugePageUnavailable,     ///< 显式请求大页（huge page）时资源不足或不可用。
  NumaUnavailable,         ///< NUMA 内存绑定当前实现不支持。
  InvalidSize,             ///< 请求的容量非 2 的幂、未页对齐或越界。
};

}  // namespace salias::platform

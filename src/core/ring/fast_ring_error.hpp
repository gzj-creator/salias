/**
 * @file src/core/ring/fast_ring_error.hpp
 * @brief 固定槽快速 ring 的参数、共享布局与操作错误码。
 * @details Fast Ring 使用固定大小 slot，不扫描变长帧头。错误码覆盖共享内存视图
 *          创建/连接校验、单 producer/consumer 未完成操作以及可靠背压。
 */
#pragma once

namespace salias::ring {

/// 固定槽快速 ring 的错误码；零值表示成功。
enum class FastRingError {
  Ok = 0,
  InvalidArgument,
  UnalignedStorage,
  StorageTooSmall,
  BadMagic,
  IncompatibleLayout,
  AlreadyInitialized,
  BackPressured,
  Empty,
  ClaimOutstanding,
  ReadOutstanding,
  NoOutstandingClaim,
  NoOutstandingRead,
  PayloadSizeMismatch,
};

}  // namespace salias::ring

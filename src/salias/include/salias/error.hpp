#pragma once

#include <expected>

namespace salias {

enum class Error {
  Ok = 0,
  NotFound,
  VersionMismatch,
  BackPressured,
  MessageTooLarge,
  Lagged,
  PlatformFail,
  BadConfig,
};

// 公共 API 使用的显式结果类型；保留 salias::Result<T> 名称，底层采用 C++23 std::expected。
template <class T>
using Result = std::expected<T, Error>;

}  // namespace salias

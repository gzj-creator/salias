/**
 * @file example/quote.hpp
 * @brief 示例共享的固定消息类型、schema 与小工具。
 * @details Fast*Ring 示例都通过公开入口 `salias::FixedChannel` 使用固定槽 ring，
 *          `FixedChannel` 会按拓扑选择底层类型：
 *          - Spsc -> `FastSpscRing`
 *          - Mpsc -> `FastMpscRing`
 *          - Spmc -> `FastSpmcRing`
 *          - Mpmc -> `FastMpmcRing`
 *          本文件只放示例共用的 `Quote` 消息、schema 和错误处理，不包含任何
 *          通道状态，便于每个示例独立阅读；`simple/latest_mpmc.cpp` 也复用同一
 *          消息类型演示 Latest 语义。
 */
#pragma once

#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <salias/salias.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "common.hpp"

/// 示例专用命名空间，避免污染库的 `salias` 命名空间。
namespace salias_demo {

/// @brief 行情快照：固定大小、平凡可复制，可直接按对象表示跨进程传输。
struct Quote {
  std::uint64_t symbol;    ///< 标的代码，用于区分不同 producer 的消息流。
  std::int64_t price;      ///< 最新价。
  std::uint32_t quantity;  ///< 数量。
  std::uint32_t sequence;  ///< 每个 producer 内的递增序号，用于验证 lane 内 FIFO。
};

/// @brief 示例 schema：ID 与版本由应用显式维护，连接双方必须完全一致。
using QuoteSchema = salias::Schema<Quote, 0x53414C4941534651ull, 1>;

/// @brief 生成带 PID 的通道名，避免同一台机器上重复运行示例时名称冲突。
inline std::string unique_name(std::string_view prefix) {
  return std::string(prefix) + "-" + std::to_string(::getpid());
}

/// @brief 打印固定通道错误；错误码名字复用 `example/common.hpp` 的映射。
inline void print_error(const char* stage, salias::Error error) {
  std::cerr << stage << " failed: " << salias_example::error_name(error) << '\n';
}

///
/// @brief 从 `Result<T>` 取值的示例便捷函数；失败时打印阶段并退出。
/// @details 示例为了保持主流程易读使用退出式错误处理；生产代码应按业务决定重试、
///          降级或向上返回错误。
///
template <class T>
T unwrap(salias::Result<T> result, const char* stage) {
  if (!result) {
    print_error(stage, result.error());
    std::exit(1);
  }
  return std::move(*result);
}

/// @brief 检查通道操作是否成功；适用于 `Result<void>` 和返回布尔值的发送接口。
template <class T>
void check(salias::Result<T> result, const char* stage) {
  if (!result) {
    print_error(stage, result.error());
    std::exit(1);
  }
}

/// @brief 打印一条消息，前缀用于区分 producer 或 consumer。
inline void print_quote(const char* label, const Quote& quote) {
  std::cout << label << " symbol=" << quote.symbol << " seq=" << quote.sequence
            << " price=" << quote.price << " qty=" << quote.quantity << '\n';
}

}  // namespace salias_demo

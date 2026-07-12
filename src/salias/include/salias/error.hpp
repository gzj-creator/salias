/**
 * @file src/salias/include/salias/error.hpp
 * @brief salias 公共 API 的错误码枚举与结果类型别名。
 * @details 位于 L7 公共外观层，定义所有公共接口返回的统一错误语义。
 *          Result<T> 基于 C++23 std::expected 实现，以显式结果类型替代异常，
 *          适配无锁 IPC 热路径中禁止抛异常的约束（核心引擎全程 noexcept）。
 *          所有错误码值需保持稳定，以支持跨进程 ABI 兼容。
 */
#pragma once

#include <expected>

/// salias 公共 API 命名空间，聚合 L7 用户可见的通道、发布者、订阅者与消息类型。
namespace salias {

/**
 * @brief salias 公共接口的错误码枚举。
 * @details 枚举值必须保持稳定，因为共享内存命名控制块中可能持久化版本/兼容性标记。
 *          Ok 表示成功；其余值对应各类可恢复失败场景。
 */
enum class Error {
  Ok = 0,             ///< 成功，无错误。
  NotFound,           ///< 找不到指定名称的通道（connect 时通道尚未创建）。
  VersionMismatch,    ///< 共享元数据版本不匹配，创建者与连接者协议版本不一致。
  BackPressured,      ///< 生产者遭遇背压：环形缓冲已满或流控窗口在途字节数达上限。
  MessageTooLarge,    ///< 单条消息负载超过环容量限制。
  Lagged,             ///< 消费者落后过多，目标消息已被生产者覆盖（仅 fanout 协议）。
  PlatformFail,       ///< 平台层失败（mmap/memfd_create/huge page 等系统调用出错）。
  BadConfig,          ///< 配置非法（name 为空、容量或端点数量不合法等）。
  OutOfMemory,        ///< 创建或连接通道时的动态内存分配失败。
};

// 公共 API 使用的显式结果类型；保留 salias::Result<T> 名称，底层采用 C++23 std::expected。
template <class T>
using Result = std::expected<T, Error>;

}  // namespace salias

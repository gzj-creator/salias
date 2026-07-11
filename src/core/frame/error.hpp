/**
 * @file src/core/frame/error.hpp
 * @brief frame 帧编解码层的错误码定义。
 * @details 本文件位于 L2 frame 层，定义帧校验过程中可返回的轻量错误码。
 *          frame 层不抛异常，所有失败以返回值形式上抛；上层 channel/wait 层据此
 *          决定重试、丢弃或断言。该错误码为编译期枚举，无运行时开销，跨进程共享
 *          内存时不需要序列化（仅用于本进程内的编解码校验）。
 */
#pragma once

namespace salias::frame {
/// frame 帧编解码错误码命名空间；所有错误为值语义、零开销。

/**
 * @brief 帧编解码/校验错误码。
 * @details Ok=0 保证未初始化默认值即"成功"，避免误判；
 *          LenTooLarge 用于 payload 超出 ring 单帧上限；
 *          BadFlags 用于 metadata 标志位非法组合；
 *          Truncated 用于可读字节不足以构成完整帧头或 payload。
 *          枚举值为强类型 enum class，防止与整数隐式转换。
 */
enum class FrameError {
  Ok = 0,        ///< 成功；默认零值即代表无错误。
  LenTooLarge,   ///< payload 长度超出单帧允许上限。
  BadFlags,      ///< metadata 标志位组合非法。
  Truncated      ///< 数据被截断，不足以构成完整帧。
};

}  // namespace salias::frame

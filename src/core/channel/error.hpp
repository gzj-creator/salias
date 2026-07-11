/**
 * @file src/core/channel/error.hpp
 * @brief 定义 channel 通道层的错误码枚举。
 * @details 位于 L5 channel 层，为 HybridMpscChannel / SharedHybridMpscChannel 等
 *          通道类型在创建(create)与连接(attach)阶段提供统一的错误分类。错误码经
 *          std::expected<Channel, ChannelError> 返回，避免异常开销，契合无锁热路径
 *          的 noexcept 风格。所有值均为编译期常量，可在共享内存与跨进程场景下安全传递。
 */
#pragma once

/// channel 层：在 ring 与 flow 之上封装生产/消费端句柄与等待策略。
namespace salias::channel {

/**
 * @brief channel 创建/连接失败时的错误码。
 * @details 各枚举值语义：Ok 表示成功；BadConfig 表示配置非法(如容量为 0、帧过大、
 *          序列号低位窗口不足以容纳环回绕)；PlatformFail 表示 L0 平台层调用失败
 *          (如 mmap/memfd_create/大页 申请失败)；RingFail 表示 L1 ring 层初始化失败
 *          (如双映射建立失败、对齐不满足)。
 */
enum class ChannelError { Ok = 0, BadConfig, PlatformFail, RingFail };

}  // namespace salias::channel

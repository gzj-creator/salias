/**
 * @file src/core/flow/error.hpp
 * @brief 定义 flow 层(生产/消费)的错误码枚举。
 * @details 位于 L3 flow 子层，供 Producer::claim 等接口以 std::expected 返回失败原因。
 *          错误码刻意保持简洁(POD 枚举)，避免跨进程/共享内存边界引入复杂对象。
 *          线程安全：枚举值为只读常量，可在任意线程/进程间共享。
 */
#pragma once

namespace salias::flow {  // flow 层：环形缓冲之上的生产/消费位置与流控逻辑

/**
 * @brief flow 层操作结果错误码。
 * @details Ok 表示成功；BackPressured 表示消费者尚未腾出足够环形缓冲空间
 *          (背压，需等待或重试)；MessageTooLarge 表示单帧所需容量超过 ring
 *          总容量，属不可恢复错误。枚举值稳定，可作为序列化/IPC 协议字段。
 */
enum class FlowError { Ok = 0, BackPressured, MessageTooLarge };

}  // namespace salias::flow

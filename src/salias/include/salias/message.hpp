/**
 * @file src/salias/include/salias/message.hpp
 * @brief salias 公共 API 中的消息类型别名。
 * @details 位于 L7 公共外观层，将 L3 flow 层的 flow::Message 暴露为 salias::Message。
 *          Message 是已接收消息的零拷贝借用视图，其 payload 指针指向所属 Channel
 *          的 ring 内存；该内存在消费者 release 或被覆盖前保持有效。进程间共享，
 *          线程安全语义由上层协议（MPSC/MPMC）保证。
 */
#pragma once

#include "core/flow/consumer.hpp"

/// salias 公共 API 命名空间，聚合 L7 用户可见的通道、发布者、订阅者与消息类型。
namespace salias {

// 已接收消息的借用视图。
// payload 指向所属 Channel 的 ring，按通道协议在释放或覆盖前保持有效。
using Message = flow::Message;

}  // namespace salias

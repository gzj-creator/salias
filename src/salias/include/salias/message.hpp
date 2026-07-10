#pragma once

#include "core/flow/consumer.hpp"

namespace salias {

// 已接收消息的借用视图。
// payload 指向所属 Channel 的 ring，按通道协议在释放或覆盖前保持有效。
using Message = flow::Message;

}  // namespace salias

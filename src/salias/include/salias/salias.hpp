/**
 * @file src/salias/include/salias/salias.hpp
 * @brief salias 公共 API 的统一伞头文件。
 * @details 仅聚合 L7 公共外观层所需的全部头文件：通道(channel)、配置(config)、
 *          错误与结果类型(error)、消息(message)、发布者(publisher)与订阅者(subscriber)。
 *          用户只需 #include "salias/salias.hpp" 即可获得完整的 IPC 通信接口；
 *          下层(L0 mmap/memfd_create/huge page、L1 ring、L2 frame、L3 flow、
 *          L4 wait strategy)均为实现细节，不对公共 API 暴露。
 */
#pragma once

#include "salias/channel.hpp"
#include "salias/config.hpp"
#include "salias/error.hpp"
#include "salias/message.hpp"
#include "salias/publisher.hpp"
#include "salias/subscriber.hpp"

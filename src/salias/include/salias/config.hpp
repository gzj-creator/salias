/**
 * @file src/salias/include/salias/config.hpp
 * @brief salias 公共 API 的通道配置与协议/大页枚举。
 * @details 位于 L7 公共外观层，定义用户创建 IPC 通道所需的全部可调参数。
 *          Config 由 Channel::create() 消费，并据此初始化 L0 平台层（mmap/memfd_create、
 *          huge page 选择）、L1 ring（capacity 决定环形缓冲几何）、L3 flow（num_producers/
 *          num_consumers 决定 per-producer 双模引擎布局与流控窗口）。通道一经创建，配置即
 *          持久化于共享内存命名控制块，连接者据此校验版本一致性。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

/// salias 公共 API 命名空间，聚合 L7 用户可见的通道、发布者、订阅者与消息类型。
namespace salias {

/**
 * @brief 通道传输协议模式枚举。
 * @details 四种模式按两个正交维度组合：投递语义（FIFO 先进先出 / Ordered 保序）与
 *          拓扑（MPSC 多生产者单消费者 / Fanout 多消费者扇出）。
 *          - FIFO：生产者侧按 claim 顺序提交，适合日志类场景。
 *          - Ordered：进一步保证跨生产者的全局有序，适合事件溯源。
 *          模式一经创建不可变更。
 */
enum class Mode { FifoMpsc, FifoFanout, OrderedMpsc, OrderedFanout };

/**
 * @brief 透明大页（huge page）配置枚举。
 * @details 大页可降低 TLB miss，在低延迟热路径上显著减少地址翻译开销。
 *          None 使用普通 4KB 页；Size2MB/Size1GB 分别请求 2MB/1GB 透明大页。
 *          大页选择影响 L0 平台层 mmap 的长度与对齐要求。
 */
enum class HugePage { None, Size2MB, Size1GB };

// Channel<Mode>::create() 使用的公共 IPC 配置。
// name 必须非空；模板参数 Mode 决定协议，mode 字段仅保留给内部命名控制块兼容路径。
struct Config {
  std::string name;                  ///< 通道名称，作为共享内存命名控制块的键，必须非空。
  Mode mode = Mode::FifoMpsc;        ///< 通道协议；实际生效值由 Channel<Mode> 模板参数覆盖。
  std::size_t capacity = 1u << 20;   ///< 环形缓冲容量（字节），默认 1MiB；须为 2 的幂且为系统页大小的整数倍。
  std::uint32_t num_producers = 1;   ///< 生产者数量，用于预分配 per-producer 提交槽位。
  std::uint32_t num_consumers = 1;   ///< 消费者数量；MPMC fanout 模式下决定扇出订阅索引数。
  HugePage huge = HugePage::None;    ///< 大页策略；显式大页要求 capacity 同时为所选大页大小的整数倍。
  // 发布流控窗口（字节）。0 表示不限制，生产者可领先消费者直至写满整个环。
  // 非 0 时把 “生产者领先消费者的在途字节数” 限制在 min(capacity, publication_window) 内，
  // 用于把稳态排队延迟从 “满环” 压到 “窗口大小”；不改变环的实际几何与回绕。
  // 低延迟推荐 64KiB–256KiB。64B 消息下满 1MiB 环可排队约 1.4 万条；128KiB 窗口
  // 实测 p99 排队从满环约 5.7ms 降到约 350us，吞吐下降约一成。窗口再小会让
  // cached_consumer_pos 更频繁失效，拐点需要按负载复测。
  std::size_t publication_window = 0;
};

}  // namespace salias

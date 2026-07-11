/**
 * @file src/core/platform/map_options.hpp
 * @brief 内存映射（mmap）与大页（huge page）的配置参数。
 * @details 本文件位于 L0 平台层，为 ring 后端存储的创建提供参数载体。
 * MapOptions 描述了共享内存段的容量、大页类型与 NUMA 绑定意图，
 * 被 Mapping::create() 与 Mapping::map_shared_fd() 消费。
 * 该结构体为纯数据聚合（POD-like），无所有权与生命周期语义，可按值传递。
 */
#pragma once

#include <cstddef>

namespace salias::platform {

/// L0 平台层配置命名空间，承载内存映射选项与平台错误码。

/**
 * @brief 显式大页（huge page）尺寸选择。
 * @details Linux 支持多种透明与显式大页，本枚举仅覆盖显式大页的两种常见规格。
 * 选择大页可减少 TLB miss，提升大容量 ring 的吞吐（throughput）与延迟（latency）。
 * 选择非 None 时，映射容量必须按对应大页大小对齐。
 */
enum class HugePage {
  None,      ///< 不使用显式大页，采用常规 4 KiB 页。
  Size2MB,   ///< 使用 2 MiB 显式大页。
  Size1GB   ///< 使用 1 GiB 显式大页（需内核与硬件支持）。
};

/**
 * @brief 内存映射创建参数聚合。
 * @details 作为 Mapping::create() / map_shared_fd() 的输入，描述后端存储需求。
 * 默认值表示零大小、无大页、无 NUMA 绑定的最小配置；调用前应自行设置 size。
 * 纯数据类型，可安全拷贝与按值传递。
 */
struct MapOptions {
  std::size_t size = 0;          ///< 映射逻辑容量（字节），须为 2 的幂且页对齐。
  HugePage huge = HugePage::None;  ///< 显式大页类型，None 表示使用常规页。
  int numa_node = -1;            ///< NUMA 绑定节点，-1 表示不绑定（当前实现不支持绑定）。
};

}  // namespace salias::platform

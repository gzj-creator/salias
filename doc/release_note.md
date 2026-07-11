# Release Notes

本文件按时间顺序记录每个发版版本的说明，与 `CHANGELOG.md` 的对应版本节对齐。
每个版本的 git tag 注解内容即取自本文件对应版本节。

## v1.0.0 - 2026-07-10

- 版本级别：主版本（major）
- Git 提交消息：chore: 发版 v1.0.0
- 对应 git tag：v1.0.0
- 基线版本：无历史 tag，以 v0.0.0 为基线首次发版

### 本次变更摘要（自基线以来的主干）

salias 首个发版版本。它是一个 Linux-only 的 C++23 低延迟具名 IPC 消息通道，
收敛为单个 per-producer magic-ring 引擎，通过排序模式与消费者拓扑提供四种公共通道。

- **通道架构**：实现 per-producer hybrid magic-ring 引擎（hybrid_control /
  hybrid_mpsc / shared_hybrid_mpsc），每个生产者独占 ring，消费者按 global_seq
  归并；frame/sequence.hpp 承载 24 位帧头序号 + 高位旁路编码。删除早期的
  broadcast / bulk / mpsc / spsc / shared_* 单环实现、L6 metrics 模块、平台 futex
  原语及 busy_spin / yielding / futex_wait 等待策略，wait 层收敛为 SpinPause。
- **公共 API**：按 Mode 模板化的四通道 FifoMpsc / FifoFanout / OrderedMpsc /
  OrderedFanout；FIFO 生产者热路径无共享 RMW，ordered 模式用单个
  global_seq.fetch_add 建立跨生产者全序。Publisher 提供 try_claim/commit 零拷贝发布，
  Subscriber 提供 poll 批量消费。
- **大页与具名 IPC**：Config 支持 HugePage(None/Size2MB/Size1GB)，进程内走
  memfd_create(MFD_HUGETLB)，具名 IPC 走 POSIX shm（普通页）/ hugetlbfs（大页），
  含版本与不可信元数据校验。
- **结果类型**：迁移到 C++23 std::expected，移除自定义 result 封装与 tl-expected 依赖。
- **工具与基准**：salias vs Aeron C++ IPC 多进程对照工具（MPSC/MPMC），新增采样
  p50/p99 单向延迟（latency_histogram.hpp 头库 + 测试），run_release_compare.sh
  支持 normal/huge2m/huge1g 页选择；channel_stress 基准；example/ 下 MPSC/MPMC
  发布订阅示例及 CTest smoke。
- **安全修复**：修复 position 回绕后容量检查，避免重建 publisher 状态经无符号下溢绕过背压。
- **文档**：完整 doc/ 设计文档集（分层架构、平台、flow、wait、channel、API、项目布局）、
  doc/plans/ 下 hybrid ring / 双模引擎 / 大页 / 具名大页 IPC / 吞吐与 CAS backoff 优化 /
  示例 / 延迟采样等设计与执行方案，以及 Tencent CVM Aeron 对照报告。

## v1.1.0 - 2026-07-11

- 版本级别：次版本（minor）
- Git 提交消息：feat: 通道层与构建系统适配 macOS 平台
- 对应 git tag：v1.1.0
- 基线版本：v1.0.0

### 本次变更摘要（自 v1.0.0 以来的主干）

在 v1.0.0 的 Linux-only per-producer hybrid 引擎基础上，本次发版新增 macOS 平台
支持，并继续打磨通道热路径与文档。

- **macOS 平台适配（本版主线）**：magic-ring 双映射后端扩展为 Linux + macOS 双平台。
  macOS 用 `mkstemp` + 立即 `unlink` 的临时文件作为匿名共享内存后端（Linux 仍走
  `memfd_create`）；`MAP_POPULATE` 经 `kPopulateFlag` 兜底，仅在平台定义时启用；
  `linux/memfd.h`、`sys/syscall.h` 等 Linux 专属头文件以 `__linux__` 守卫。显式大页
  后端仍仅 Linux 可用。构建系统从 Linux-only 放宽为 Linux + macOS，新增 `BUILD_TESTING`
  选项、`SALIAS_BUILD_BENCHMARKS` 默认 OFF，Google Benchmark / GTest 改为可选（`QUIET`），
  `salias_ipc_compare` 限定 Linux；`SharedHybridMpscChannel` 序列号取值由立即调用 lambda
  重构为普通 if/else（单帧与批量 claim 两条路径，行为不变）。
- **流控窗口与批量接收**：新增 `Config::publication_window` 发布流控窗口，将稳态排队延迟
  从「满环」压到「窗口大小」；消费端 `try_recv_run()` 单环连续排空，`consume()` /
  `flush_progress()` 把每消息 release-store 收敛为每批每环一次，缓存 `visible_producer_pos`
  把跨核 acquire 从每消息降到每批每环；L7 `Subscriber::poll` 内联 handler 批量缓冲。
  具名 IPC 协议 `kNamedVersion` 3→4（二进制不兼容）。
- **文档**：新增 `doc/13-optimization-retrospective.md` 完整优化复盘（架构 → 共享状态降频 →
  消费热路径 → 帧头微优化）与最终推荐配置；Tencent 4 vCPU 对照追加「64 位帧头发布优化
  最终复验」（FIFO batch=1 达 Aeron 中位 115.0%、Ordered batch=8 达 93.4%）及 batch1/8/16 ×
  20 轮原始日志；全树补全中文 Doxygen 注释与最小示例。

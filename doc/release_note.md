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

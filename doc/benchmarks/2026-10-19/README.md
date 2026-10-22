# 2026-10-19 性能与诊断证据范围

本目录收录报告所需的精简证据，而不是完整实验工作目录。日期、命令、源码与二进制哈希
描述测量发生时的输入；其中绝对路径和本地 commit 状态不是发布后的安装要求。
当前结论统一见[最终性能测试与优化交接报告](../../performance-report.md)。

## 入库内容

- `spmc-aeron/`：v3.0.0 同组单发布双订阅 Salias/Aeron 主实验。
- `spmc-salias/`：较早 Salias-only 单发布单/双订阅辅助实验，CPU 分配与主实验不同，不得拼接。
  两者各含 `summary.json`、`environment.json`、72 条 `results.jsonl`、72 份 `raw/*.stdout`
  与原样冻结的 `frozen-runner.py`。正式轮次均为 60 次，其余为预热；没有新跑测试。
- [evidence-manifest.json](evidence-manifest.json)：上述 152 个文件的源/目标路径、SHA-256 与字节数。
  JSONL 保留原值与绝对路径；按原 basename 在各自 `raw/` 中定位输出，并核验 `stdout_sha256`。
  runner 必须恢复到原目录深度后使用，命令与 CPU/资源边界见主报告。
- `aeron-latency-summary.json`：独立轮次分位数及资源摘要，不是池化直方图。
- 三个设计实验目录的 `summary.json` 与逐轮 JSONL：保留全部已导出轮次，包括不利结果、
  预热与 profile 标签；统计正式时延时排除预热和 profile，不混合负载、调度或不安全协议。
- 报告直接引用的 8 组真实 profile：SVG、folded stacks、top/report、采样 stdout/stderr；
  poll-path 的 3 组另含反汇编注释。采样日志用于检查丢样，不作为未插桩性能数据。
- 分段与调度关联 JSON、清零反汇编片段、调度策略快照、7 个实现的精简 API 核验 JSON。

## 仅本地保留

`build/benchmarks/aeron-latency-20261018/` 的 Aeron 依赖及旧对照继续保留；新主实验和辅助实验原目录分别为
`build/benchmarks/spmc-aeron-v3.0.0-20261019-170804/`、
`build/benchmarks/spmc-v3.0.0-20261019-165821/`。生成的产品源码副本、编译对象、二进制、
`perf.data`、未明确列入上节的 bulk stdout/stderr、driver 日志、完整 GTest 输出及早期实验不纳入精简证据。
导出工具可能生成较大的本地审计包；不能直接将其整个输出目录加入 Git。

本轮将原有未跟踪草稿、重复 bulk 输出及生成源码副本共 1,069 个文件（2,705,527 字节）
原样移到本机 `build/document-cleanup-20261019/`，映射和 SHA-256 记录在其
`relocation-manifest.json`。此步骤不改已跟踪证据；未提交或打 tag。

后续按清理要求，将旧 `design-diagnosis-20261019`、`design-implementation-20261019`、
`poll-path-20261019`、`publisher-tail-20261018` 四组本地产物逐文件核验后无损压缩，
存于 `build/benchmark-cleanup-20261019/<原目录名>.tar.gz`，原散文件目录已清理。
本地 `manifest.json` 含逐文件及归档 SHA-256；原证据路径需恢复后访问，步骤见
[最终报告第 8.2 节](../../performance-report.md)。三个未跟踪 fast-path/guard 实验骨架也移至
该目录的 `drafts/`，未混入正式工具或性能证据；受维护工具和本目录精简数据保持不变。

精简 JSONL 内的原始输出路径和哈希是溯源记录，不保证在其他机器上可访问。
仅凭本目录不能重新核验未入库的二进制、完整日志或重算池化分位数；需要保留原机器的
本地产物或重新运行工具。原始证据内容不因发布重新测量或改写。

## 结论边界

实际采用的产品优化仅包括调用独占 poll 描述符存储等已提交安全实现。
六个后续轮询候选没有晋升为默认实现；跳过可见水位的帧头原型不安全，FIFO 调度曾触发
约 50 ms RT 限流。当前共享 VM 不提供有效硬件 PMU，这些结果不能证明已找全约 100 ns
差距的因果来源，也不构成长时 SLA、完整 payload 或 2P2S 性能验收。

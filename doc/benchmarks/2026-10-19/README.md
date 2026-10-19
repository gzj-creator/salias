# 2026-10-19 延迟诊断证据范围

本目录收录报告所需的精简证据，而不是完整实验工作目录。日期、命令、源码与二进制哈希
描述测量发生时的输入；其中绝对路径和本地 commit 状态不是发布后的安装要求。

## 入库内容

- `aeron-latency-summary.json`：独立轮次分位数及资源摘要，不是池化直方图。
- 三个设计实验目录的 `summary.json` 与逐轮 JSONL：保留全部已导出轮次，包括不利结果、
  预热与 profile 标签；统计正式时延时排除预热和 profile，不混合负载、调度或不安全协议。
- 报告直接引用的 8 组真实 profile：SVG、folded stacks、top/report、采样 stdout/stderr；
  poll-path 的 3 组另含反汇编注释。采样日志用于检查丢样，不作为未插桩性能数据。
- 分段与调度关联 JSON、清零反汇编片段、调度策略快照、7 个实现的精简 API 核验 JSON。

## 仅本地保留

完整产物分别在 `build/benchmarks/aeron-latency-20261018/`、
`build/benchmarks/design-diagnosis-20261019/`、
`build/benchmarks/design-implementation-20261019/` 和
`build/benchmarks/poll-path-20261019/`。生成的产品源码副本、编译对象、二进制、
`perf.data`、完整 stdout/stderr、完整 GTest JSON/日志及早期实验不会随 tag 分发。
导出工具可能生成较大的本地审计包；不能直接将其整个输出目录加入 Git。

精简 JSONL 内的原始输出路径和哈希是溯源记录，不保证在其他机器上可访问。
仅凭本目录不能重新核验未入库的二进制、完整日志或重算池化分位数；需要保留原机器的
本地产物或重新运行工具。原始证据内容不因发布重新测量或改写。

## 结论边界

实际采用的产品优化仅包括调用独占 poll 描述符存储等已提交安全实现。
六个后续轮询候选没有晋升为默认实现；跳过可见水位的帧头原型不安全，FIFO 调度曾触发
约 50 ms RT 限流。当前共享 VM 不提供有效硬件 PMU，这些结果不能证明已找全约 100 ns
差距的因果来源，也不构成长时 SLA、完整 payload 或 2P2S 性能验收。

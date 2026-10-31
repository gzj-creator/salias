# Channel API 收敛前后 ABBA 证据

日期：2026-10-31（Asia/Shanghai）

本目录验证 Channel API 收敛是否改变固定槽与通用 Channel 的热路径。旧臂为 `v4.1.0`
（`c60ae34950d335b7afcfe9acb73b2431a76a5fbf`），新臂为 Phase 4 源码删除后的
`c013a9214dc759751a92682237bb87e5f23ccabf`。

## 结论

- 两个臂使用相同的 benchmark 源构建。`salias_fixed_channel_poll_compare` 在两臂均为
  `c723db1b...acd4945`，`salias_ipc_compare` 在两臂均为 `bf285620...4055949c`，逐字节相同。
  因此本轮数值差异只能来自调度与噪声，不能归因于 API 收敛、include 拆分或链接边界。
- 5 组 A/B/B/A、每 case 每臂 10 次正式运行全部退出 0。固定槽吞吐中位数为
  **40.04 Mmsg/s -> 40.18 Mmsg/s**，配对比值中位数 **0.998x**；通用 Channel 饱和吞吐为
  **7.165 Mmsg/s -> 7.204 Mmsg/s**，配对比值 **1.010x**。
- 1 Mmsg/s planned 负载下，固定槽 p50 为 **254 -> 244 ns**，通用 Channel p50 为
  **459 -> 457 ns**。固定槽 p99 中位数为 **2,959 -> 3,295 ns**，通用 Channel 为
  **4,287 -> 4,543 ns**；p99 的组间范围明显宽于中位数差，且两臂运行相同二进制，不据此
  宣称任何延迟改善或退化。
- 80 次正式运行共发布、接收并全量校验 **5.2 亿**条 64 B 消息。wire protocol 版本仍为 15，
  未修改共享内存协议、通道版本、固定槽布局或任一数据面源文件。

该结果只关闭“收敛前后同机 ABBA 与二进制一致性”门禁，不替代负责人发布签署、主版本
release note 或 `5.0.0` tag 决定。

## 条件

| 项目 | 设置 |
|---|---|
| 主机 | 4 vCPU KVM，Intel Xeon Platinum 8255C，Linux 6.8 |
| 构建 | CMake Release preset：`-O3 -DNDEBUG -flto -march=native` |
| 固定槽 | SPSC `FixedChannel`，4096 slots，64 B payload，`poll(64)` |
| 通用通道 | `Channel<Mode::FifoMpsc>`，1 producer / 1 consumer，64 B payload，capacity 1,048,576 |
| CPU | 固定槽 producer CPU2 / consumer CPU3；通用通道 CPU1 / CPU2 |
| 顺序 | 每组 `before, after, after, before`；先对两个臂各做一次不计入汇总的预热 |
| 正式样本 | 4 个 case x 5 组 x 4 次 = 80 次运行；每个 case 每个臂 10 次 |
| 正确性 | 通用通道逐条 `--verify full`；固定槽逐字段与 64 B body 校验；消息数和采样数逐轮断言 |
| 聚合 | 每个指标保留全部样本；报告两臂 median/min/max 与 5 个组内 ABBA 配对比值 |

## 汇总

| Case | 指标 | before 中位数 | after 中位数 | after/before 配对比中位数 |
|---|---|---:|---:|---:|
| fixed-throughput | 吞吐 (Mmsg/s) | 40.0385 | 40.1771 | 0.998x |
| fixed-planned-latency | p50 (ns) | 254 | 244 | 0.988x |
| fixed-planned-latency | p99 (ns) | 2,959 | 3,295 | 0.781x |
| generic-throughput | 吞吐 (msg/s) | 7,164,750 | 7,203,595 | 1.010x |
| generic-planned-latency | p50 (ns) | 459 | 457 | 0.996x |
| generic-planned-latency | p99 (ns) | 4,287 | 4,543 | 1.119x |

planned 吞吐由限速决定，两臂均在 0.9948 Mmsg/s 附近；完整范围和配对分布见
[summary.json](summary.json)。

## 复现

在同一台机器分别检出两臂并构建相同目标：

```bash
git worktree add --detach /tmp/salias-v4.1.0-abba v4.1.0
(cd /tmp/salias-v4.1.0-abba && cmake --preset release && \
  cmake --build --preset release --target \
    salias_fixed_channel_poll_compare salias_ipc_compare -j 4)
cmake --preset release && cmake --build --preset release --target \
  salias_fixed_channel_poll_compare salias_ipc_compare -j 4
python3 doc/benchmarks/2026-10-31-channel-api-abba/run_abba.py \
  --before-root /tmp/salias-v4.1.0-abba --after-root "$PWD" --groups 5
```

## 证据

- [results.jsonl](results.jsonl)：80 次正式运行的命令、解析结果、前后环境快照和原始文件路径。
- [raw/](raw/)：80 次正式运行及 8 次预热的 stdout/stderr，共 176 个文件。
- [summary.json](summary.json)：每臂统计和组内 ABBA 配对比值。
- [environment.txt](environment.txt)：主机、工具链、CPU、内存和负载。
- [source-state.txt](source-state.txt)：两臂 revision、benchmark 源与二进制 SHA-256、wire 版本。
- [SHA256SUMS](SHA256SUMS)：本目录全部证据文件的 SHA-256。

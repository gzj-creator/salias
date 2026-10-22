# 2026-10-18 当前 FIFO Publisher 单向时延

> 历史归档：仅记录当时版本、配置与结论，不代表当前性能或验收状态。当前汇总及下一轮任务见[最终性能测试与优化交接报告](../../performance-report.md)。

> 早期基线源码 `5cfcbc9` 的 Release 构建实测，不包含后续 Reliable 存活探测候选补丁。
> 最新结果见[性能总结](performance-summary-2026-10-18.md)。代码包含 2026-09-25 FIFO 接收批量热路径优化；
> 本次只测优化后的版本，没有在相同 harness 下重测旧版本，因此不把差值归因于单项优化。

## 测量范围

- 主机：Tencent CVM，4 vCPU，Intel Xeon Platinum 8255C，KVM；Ubuntu 24.04.4，Linux 6.8.0。
- 构建：GCC 13.3.0，Release `-O3 -DNDEBUG`。
- 模式：FIFO，64 B payload，单条 `try_claim()` + `commit()`；4 MiB capacity，poll limit 64。
- 拓扑：2P1S（消费者 CPU0、生产者 CPU1/2）；2P2S（消费者 CPU0/1、生产者 CPU2/3），均固定 CPU。
- 每个生产者每轮发布 2,000,000 条消息；每种拓扑和窗口先预热 1 轮，再测 5 轮。
- 四种正式配置共 20 轮：正式测量发布 8,000 万条（64 B payload 共 5.12 GB），fanout 计入重复交付后共消费 1.2 亿条（7.68 GB 逻辑 payload）。另有 4 轮预热，发布 1,600 万条、交付 2,400 万条，不计入下表。
- 延迟每 64 条采样一次；单向时间从成功 claim 后、写 payload/commit 前的时间戳，到 Subscriber poll 回调读取时间戳为止。
  因此包含排队、提交和调度时间，但不包含 Publisher 等待背压解除的时间。
- 表中百分位先由每轮直方图计算，再取 5 轮百分位的中位数；数值是直方图桶上界估计，不是把 5 轮样本合并后的百分位。

## 结果

单位均为微秒。`window=0` 表示不限制在途数据；128 KiB 是显式 `publication_window`。

| 拓扑 | 发布窗口 | p50 | p99 | p99.9 | 单轮 max 的中位数 | 发布吞吐中位数 |
|---|---:|---:|---:|---:|---:|---:|
| 2P1S | 0 | 1.471 | 32.255 | 71.679 | 92.310 | 37.660 Mmsg/s |
| 2P1S | 128 KiB | 1.407 | 16.639 | 59.391 | 73.022 | 32.878 Mmsg/s |
| 2P2S | 0 | 1.087 | 991.231 | 1,179.647 | 1,211.420 | 28.066 Mmsg/s |
| 2P2S | 128 KiB | 0.767 | 176.127 | 1,851.391 | 3,182.814 | 17.741 Mmsg/s |

5 轮中 p99 的最小至最大值：2P1S 完整环为 19.199–41.471 us、128 KiB 为 15.359–20.223 us；
2P2S 完整环为 876.543–3,637.247 us、128 KiB 为 102.399–655.359 us。
2P2S 中较小窗口明显压低了本次的 p99 中位数，但 p99.9 和 max 仍受 KVM 调度尖峰影响，不能据此声称
尾部时延稳定改善。窗口还伴随吞吐变化：本次中位吞吐在 2P1S 下降约 12.7%，2P2S 下降约 36.8%。

这组结果反映当前 4 vCPU KVM 上的持续发布负载，不是空载 ping-pong，也不代表独占物理机的 SLA。
若目标是尾延迟验收，应在部署机器上重复测量并按实际生产者/消费者拓扑配置窗口。

## 原始数据与复现

逐轮临时输出已按要求清理；本页保留四配置最终结果及测量边界，原始文件 SHA256
保留在[精简归档](../../benchmarks/2026-10-18/performance-results.json)，不再提供原始输出链接。
测试使用 `tools/aeron_compare/salias_ipc_compare`，每个条件丢弃 1 轮预热后连续运行 5 轮，命令形式如下：

```bash
build/release/bench/salias_ipc_compare \
  --scenario fifo --messages 2000000 --producers 2 --consumers 2 \
  --payload 64 --capacity 4194304 --batch-size 1 \
  --publication-window 131072 --poll-limit 64 \
  --cpu-base 0 --cpu-stride 1 --latency-sample-rate 64
```

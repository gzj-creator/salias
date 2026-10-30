# FixedChannel 批量 consumer cursor 发布对比

日期：2026-10-30（Asia/Singapore）

本目录比较同一个 `FixedChannel` SPSC 产品 API 下的两条消费路径：

- `copy`：逐条调用 `try_recv_copy()`，每条消息立即发布 consumer cursor。
- `poll`：调用 `poll(64, handler)`，消息复制并交给 handler 后，在批次结束或 ring
  暂时为空时调用一次 `flush_release()`。

这条改动只影响批量 `poll()`；单条 `try_recv_copy()` 仍保持立即发布 cursor 的语义。

## 结论

- SPSC 64 B 饱和吞吐从 **8.20 Mmsg/s** 提升到 **40.03 Mmsg/s**，20 次对 20 次的中位数
  为 **4.88x**。10 个 ABBA 组的配对比中位数为 **4.84x**，范围 **4.26x 至 5.06x**，
  10/10 组同向。
- 延迟没有同步改善。1 Mmsg/s attempt 负载的 p50 为 **198 ns 对 192 ns**，配对比中位数
  **1.06x**；p99 为 **371 ns 对 369 ns**，配对比中位数 **1.03x**；p99.9 配对比中位数
  **1.00x**。即吞吐优化没有证明 p50/p99 下降，只能判定为本轮调度噪声下的小幅持平。
- planned 负载同样不支持延迟改善结论：p50 配对比中位数 **1.06x**，p99 **1.27x**，
  p99.9 **1.05x**。其中一轮 `poll` 出现 **2.589 ms** p99 和 **15.224 ms** max，已保留
  在原始记录中，没有删除不利轮次。
- 因批末发布减少跨核 cursor 写回，饱和吞吐收益很大；但当前 4 vCPU 共享 KVM 的调度
  波动足以覆盖约 6 ns 的 p50 差异，不能宣称尾延迟改善。

该结果只覆盖 `FixedChannel` SPSC、64 B payload。它不是四拓扑产品 API 矩阵，也不能替代
需要 6 组 x 20 轮 ABBA 和至少 5 个独占 CPU 的最终重命名门禁。

## 条件

| 项目 | 设置 |
|---|---|
| 主机 | 4 vCPU KVM，Intel Xeon Platinum 8255C |
| 通道 | 具名共享内存 SPSC `FixedChannel`，4096 slots |
| payload | 固定 64 B，逐字段和完整 body 校验 |
| 线程 | producer CPU1，consumer CPU2，`taskset -c 1,2` |
| 吞吐 | 每轮 2000 万条，不限速，10 组 ABBA，每模式 20 次 |
| 延迟 | 总计 1 Mmsg/s x 2 s，每 16 条采样一次，attempt 与 planned 各 10 组 ABBA |
| 聚合 | 保留全部 120 次运行；逐组计算 paired ratio，报告 20 个同模式样本的中位数、范围和组内配对中位数 |

## 延迟中位数

单位 ns。最大值为每轮 histogram 的最大观测值，再取 20 轮中位数。

| 起点 | 指标 | copy | poll | poll/copy 配对中位数 |
|---|---:|---:|---:|---:|
| attempt | p50 | 192 | 198 | 1.06x |
| attempt | p99 | 369 | 371 | 1.03x |
| attempt | p99.9 | 3,903 | 3,711 | 1.00x |
| attempt | max | 53,390 | 46,280 | 0.89x |
| planned | p50 | 246 | 251 | 1.06x |
| planned | p99 | 3,071 | 2,495 | 1.27x |
| planned | p99.9 | 10,879 | 11,199 | 1.05x |
| planned | max | 62,743 | 67,190 | 1.19x |

planned 的跨轮范围明显长于同轮 ABBA 差异，因而组内配对比较优先于直接比较两个全样本
中位数。机器可读汇总见 [summary.json](summary.json)。

## 复现

吞吐 ABBA：

```bash
for group in $(seq 1 10); do
  for mode in copy poll poll copy; do
    taskset -c 1,2 ./build/release/bench/salias_fixed_channel_poll_compare \
      --mode "$mode" --workload throughput --messages 20000000 \
      --producer-cpu 1 --consumer-cpu 2 --poll-batch 64
  done
done
```

attempt 延迟：

```bash
for group in $(seq 1 10); do
  for mode in copy poll poll copy; do
    taskset -c 1,2 ./build/release/bench/salias_fixed_channel_poll_compare \
      --mode "$mode" --workload latency --origin attempt \
      --rate 1000000 --seconds 2 --sample-rate 16 \
      --producer-cpu 1 --consumer-cpu 2 --poll-batch 64
  done
done
```

planned 延迟只需把 `--origin attempt` 改为 `--origin planned`。完整输入、环境和二进制身份
记录在 [SHA256SUMS](SHA256SUMS)、[environment.txt](environment.txt) 和
[source-state.txt](source-state.txt)。

## 证据

- [throughput/results.txt](throughput/results.txt)：40 次正式吞吐运行。
- [latency-attempt/results.txt](latency-attempt/results.txt)：40 次 attempt 延迟运行。
- [latency-planned/results.txt](latency-planned/results.txt)：40 次 planned 延迟运行。
- [summary.json](summary.json)：中位数、范围、配对比和样本数。

每轮 benchmark 在退出前验证消息数、producer 身份、序号及 64 B payload 全内容。本次三套
矩阵共发布并校验 **9.6 亿**条消息。

回归证据位于 [validation/](validation/)：Release CTest **22/22**、ASan/UBSan
**21/21**、TSan **21/21**、Aeron 工具定向单测 **28/28**；`FixedChannelTest` 另重复
20 轮，每轮 8/8 通过。

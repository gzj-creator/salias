# Salias v6.0.0 四拓扑 Salias / Aeron 对比（2026-11-02）

本目录记录 `v6.0.0`（提交 `feb722bd6674a7d7af7fd4fa341666c2df2cf1b9`）与
Aeron 1.52.0 IPC exclusive publication 的同机对比。测量沿用
[2026-10-29 Fast Ring 开发验收](../2026-10-29-fast-ring/README.md)的协议：
SPSC、MPSC、SPMC、MPMC，64/256 B，1 轮预热 + 5 轮正式 ABBA，所有性能轮
`--verify full`，吞吐每 producer 1000 万条，延迟总计 100 万 msg/s、2 秒、每 16 条采样一次。

`v6.0.0` 只移除 `salias::extras`，通用 `Channel`、`FixedChannel` 和 `Fast*Ring` 数据面
没有修改；本目录是当前版本在同一 4 vCPU 主机上的复跑，不是与 2026-10-29 的代码性能
A/B，也不能把两轮差异归因于 v6 的 API/打包删除。`environment.json` 的 tag 为
`v6.0.0-dirty`，原因是测量时工作区含文档修改；产品输入由其中的二进制 SHA-256 固定。

## 结论

- **8/8 门禁通过。** 判据为 Fast/Aeron 吞吐 ≥1.1x、Fast/FIFO ≥1.25x、attempt p50
  ≤ Aeron、attempt p99 ≤1.1x Aeron；实测 Fast/Aeron 为 **1.43x 至 5.38x**，
  Fast/FIFO 为 **1.41x 至 16.37x**，attempt p50/p99 在全部 8 个拓扑/payload 单元均
  不高于 Aeron。机器可读结论见
  [verification-full-5-round.json](verification-full-5-round.json)。
- **Fast 的 8 个吞吐单元全部领先。** 最低为 MPSC 256 B 的 1.43x，最高为 SPMC 64 B 的
  5.38x；两套矩阵各 240 次正式运行全部退出 0。
- **通用 FIFO 的差距仍分拓扑。** 64 B 时 SPSC/MPSC/SPMC 的 Salias/Aeron 分别为
  **0.25/0.74/0.28**，MPMC 为 **1.91**；256 B 时分别为 **0.92/0.77/1.02/3.21**。
  MPMC 的 Aeron driver 与 subscriber0 共用 CPU0，领先不能外推为无核竞争结论。
- **attempt p50 仍由 Aeron 更低，但差距小于吞吐差异。** FIFO 的 p50 为 403–519 ns，
  Aeron 为 221–28,159 ns；Fast 的 p50 为 161–307 ns，Aeron 为 221–11,775 ns。
  Aeron 在 MPMC 256 B 的较高中位数反映共享核上的发送排期积压，不是稳定的固有延迟。
- **尾分位没有全面领先。** Fast MPMC attempt p99.9 为 64 B **2.720 ms** 对 Aeron
  **1.327 ms**、256 B **1.622 ms** 对 **1.606 ms**；其余 6 个单元 p99.9 低于 Aeron。
  planned 同样只能按单元解释，不能把 Aeron 在 MPMC 的百毫秒级 planned 分位数当作
  其固有性能。

## 吞吐

单位为所有 producer 合计发布 Mmsg/s；SPMC/MPMC 会向每个订阅者 fanout，交付吞吐是发布
吞吐的两倍。数值为 5 轮正式结果中位数，比值使用同一轮配对数据计算。

| 拓扑 | Payload | Fast / Aeron | Fast/Aeron | FIFO / Aeron | FIFO/Aeron | Fast/FIFO |
|---|---:|---:|---:|---:|---:|---:|
| SPSC | 64 B | 79.14 / 26.72 | 2.96x | 7.37 / 29.43 | 0.25x | 10.74x |
| SPSC | 256 B | 34.48 / 18.54 | 1.86x | 16.73 / 18.12 | 0.92x | 2.06x |
| MPSC | 64 B | 72.40 / 44.71 | 1.62x | 34.11 / 46.08 | 0.74x | 2.12x |
| MPSC | 256 B | 28.75 / 20.18 | 1.43x | 16.23 / 21.21 | 0.77x | 1.77x |
| SPMC | 64 B | 91.30 / 16.98 | 5.38x | 5.58 / 20.18 | 0.28x | 16.37x |
| SPMC | 256 B | 32.83 / 16.61 | 1.98x | 17.68 / 17.26 | 1.02x | 1.86x |
| MPMC | 64 B | 58.14 / 12.17 | 4.78x | 21.10 / 11.03 | 1.91x | 2.76x |
| MPMC | 256 B | 25.67 / 5.79 | 4.43x | 18.21 / 5.68 | 3.21x | 1.41x |

## Attempt 延迟

单位 ns。attempt 从首次发布尝试前开始，包含背压重试、payload 写入、commit 和投递；
每轮先取各 subscriber 的最差分位数，再取 5 轮中位数。

**Fast Ring**

| 拓扑 | Payload | Fast p50 / p99 | Aeron p50 / p99 | Fast p99.9 / Aeron p99.9 |
|---|---:|---:|---:|---:|
| SPSC | 64 B | 173 / 379 | 221 / 451 | 4,671 / 8,575 |
| SPSC | 256 B | 307 / 535 | 307 / 567 | 6,911 / 11,519 |
| MPSC | 64 B | 161 / 423 | 237 / 4,159 | 11,007 / 909,311 |
| MPSC | 256 B | 245 / 2,975 | 307 / 45,055 | 113,663 / 1,310,719 |
| SPMC | 64 B | 191 / 467 | 239 / 18,687 | 202,751 / 950,271 |
| SPMC | 256 B | 263 / 687 | 391 / 72,703 | 167,935 / 1,376,255 |
| MPMC | 64 B | 181 / 112,639 | 259 / 737,279 | 2,719,743 / 1,327,103 |
| MPMC | 256 B | 259 / 81,919 | 11,775 / 737,279 | 1,622,015 / 1,605,631 |

**通用 FIFO**

| 拓扑 | Payload | FIFO p50 / p99 | Aeron p50 / p99 |
|---|---:|---:|---:|
| SPSC | 64 B | 403 / 527 | 221 / 419 |
| SPSC | 256 B | 471 / 671 | 307 / 591 |
| MPSC | 64 B | 407 / 703 | 237 / 7,743 |
| MPSC | 256 B | 495 / 3,775 | 299 / 4,223 |
| SPMC | 64 B | 427 / 631 | 237 / 5,247 |
| SPMC | 256 B | 499 / 823 | 403 / 17,407 |
| MPMC | 64 B | 427 / 507,903 | 263 / 589,823 |
| MPMC | 256 B | 519 / 77,823 | 28,159 / 557,055 |

## Planned 延迟

planned 从原始开环计划到达时间开始，额外包含 producer 抢占和调度积压，不能与 attempt
相减解释某阶段成本。Fast 的 p50 为 **235–339 ns**，Aeron 为 **287 ns–28.927 us**；
Fast p99 为 **3.775 us–1.032 ms**，Aeron 为 **3.647 us–186.647 ms**。MPMC 64 B 的
planned p99/p99.9 为 Fast **1.032/2.556 ms** 对 Aeron **104.858/113.246 ms**，该格
Aeron 的百毫秒级结果来自 4 vCPU 上 driver 与 subscriber0 共用 CPU0 的发送积压，
不能解释成无竞争环境下的固有延迟。逐轮数据见两套 `summary.json`。

## 校验与证据

- 两套矩阵各 288 次案例（48 次预热、240 次正式）全部退出 0；各发布 **15.2 亿**条、
  交付并逐字节校验 **22.8 亿**次、采集 **3000 万**个延迟样本，MemAvailable 最低约
  1.51 GiB，cgroup 的 high/max/OOM/OOM-kill 计数均为 0。
- 全文校验覆盖每个 payload 字节、producer 身份、每 producer 连续序号、顺序和重复；
  runner 还逐轮核对发布数、交付数、采样数和 consumer 编号。
- Fast 证据：[summary.json](formal-full-fast-5-round/summary.json)、
  [environment.json](formal-full-fast-5-round/environment.json)、
  [results.jsonl](formal-full-fast-5-round/results.jsonl)、
  [SHA256SUMS](formal-full-fast-5-round/SHA256SUMS)。
- FIFO 证据：[summary.json](formal-full-fifo-5-round/summary.json)、
  [environment.json](formal-full-fifo-5-round/environment.json)、
  [results.jsonl](formal-full-fifo-5-round/results.jsonl)、
  [SHA256SUMS](formal-full-fifo-5-round/SHA256SUMS)。
- 门禁：[verification-full-5-round.json](verification-full-5-round.json)。固定 runner
  副本分别保存在两套目录的 `frozen-runner.py`，逐文件哈希由各自 `SHA256SUMS` 固定。
- 顶层 [SHA256SUMS](SHA256SUMS) 固定本 README、门禁结论与两套运行的清单文件。

产品输入 SHA-256（摘自 `environment.json`）：

| 文件 | SHA-256 |
|---|---|
| `build/release/bench/salias_ipc_compare` | `053bfb02a78b4b7dfc9a7756d82777ed6423f11a5da4e8994233744d15277e4a` |
| `build/release/bench/salias_fast_ring_compare` | `1f085d6f9943e30886322c496c50eaee18d5577cfb81870ffb76c67314afb99b` |
| `build/release/bench/salias_fast_ring_latency` | `6befa6de7146ec4d59394b96329d935e5ae5574a3676244081a97c94e3bb8b57` |
| `build/benchmarks/verification-clients/aeron_ipc_compare` | `b6ac1c433b23bd36b7c4ca8ed5854fffc8fe7899acc1d96e533308814d676e53` |
| `build/benchmarks/topology-v4.0.1-20261029-115020/bin/aeronmd` | `9bd356315b87e6f7b87ce7dca454ee79637c635bda04d85d01384e543d955d72` |

## 边界

本机为共享 4 vCPU KVM，不是独占裸机或生产 SLA 基线；Aeron 另有 media driver，且其
multi-term 日志与本组固定槽/1 MiB ring 的总映射容量不同。MPMC 2P2S 已占满四个应用
worker，Aeron driver 与 subscriber0 共用 CPU0。该矩阵是 1 组 x 5 轮的开发验收，不是
最终 6 组 x 20 轮 ABBA 矩阵，也没有覆盖独占核、长时持续负载或 HFT 长尾 SLA。64/256 B
是逻辑长度，虽然本轮逐字节校验了完整 payload，仍不能把结果外推到其他 payload、CPU
拓扑或生产负载。

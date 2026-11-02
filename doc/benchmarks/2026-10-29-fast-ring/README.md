# Salias 固定槽 Ring 四拓扑开发验收：全文校验版（2026-10-29）

> 本文是上一轮开发验收记录。当前 `v6.0.0` 复跑结果见
> [2026-11-02 四拓扑对比](../2026-11-02-v6-aeron/README.md)。

本目录记录 `FastSpscRing`、`FastSpmcRing`、`FastMpscRing`、`FastMpmcRing` 与
Aeron 1.52.0 IPC、v4.0.1 通用 FIFO 的同机对比。本版在 **所有 240 次正式性能轮**
启用逐字节、producer 身份、每 producer 序号、顺序和重复校验。结论是：**8/8 开发
门禁通过，最终重命名门禁仍未通过**。缺少的是 6 组 x 20 轮 ABBA 正式矩阵，以及
MPMC 2P2S 不与 Aeron driver 共享应用 worker 的 CPU 环境；不能用本目录结果触发
Phase 7 改名。

## 结论

- 8/8 拓扑/Payload 单元同时通过当前 v4.0.1 吞吐提升、相对 Aeron 吞吐、attempt p50
  和 attempt p99 门禁；三条路径使用相同 ABBA 轮次与完整 payload 校验。
- Fast Ring 对 Aeron 的发布吞吐为 **1.31x 至 5.02x**；对 v4.0.1 通用 FIFO 为
  **1.48x 至 16.31x**。
- attempt p50 为 **161 至 287 ns**，Aeron 为 **223 ns 至 13.311 us**；attempt p99
  为 **347 ns 至 299.007 us**，Aeron 为 **415 ns 至 557.055 us**。8/8 单元更低。
- MPMC attempt p99.9 仍未领先：64 B 为 **3.178 ms 对 1.671 ms**，256 B 为
  **1.589 ms 对 1.163 ms**。计划门禁只覆盖 p50/p99，不能把结论扩大为所有尾分位。
- 8/8 单元另有一组无计时压力矩阵，每个 producer 1000 万条；本版 240 次正式性能轮本身
  也已完成 **22.8 亿次**逐字节校验，二者不再存在“性能轮只计数”的缺口。

机器可读门禁结论见
[verification-full-5-round.json](formal-full-fast-5-round/verification-full-5-round.json)。

## 对比条件

| 项目 | 设置 |
|---|---|
| 主机 | 4 vCPU KVM，Intel Xeon Platinum 8255C，Linux 6.8.0-101-generic |
| 引擎 | Salias Fast Ring、Aeron 1.52.0 IPC exclusive publication、v4.0.1 通用 FIFO |
| 拓扑 | SPSC 1P1S、MPSC 2P1S、SPMC 1P2S、MPMC 2P2S |
| Payload | 64 B、256 B |
| 吞吐 | 每 producer 1000 万条，不限速、不采样 |
| 延迟 | 总计 100 万 msg/s、2 秒、每 16 条采样一次 |
| 聚合 | 1 轮预热、5 轮正式；逐轮取得分位数后取五轮中位数 |
| 校验 | 全部轮次 `--verify full`；每个 payload 全字节、producer 身份、每 producer 序号、顺序和重复 |
| 容量 | SPSC/SPMC 每 producer 1 MiB payload；MPSC/MPMC 每 producer 256 KiB；Aeron 使用 1 MiB term |
| 资源 | 704 MiB cgroup，零 swap，Aeron SHARED/backoff driver |

SPSC、MPSC、SPMC 的应用 worker 位于 CPU1-3，Aeron driver 与 coordinator 位于 CPU0，
应用 worker 不与其共享核。MPMC 2P2S 使用 CPU0-3，Aeron driver 与 subscriber0 共用
CPU0，因此 MPMC 结果包含真实的 4 vCPU 部署约束，但不是独占核证书。
Aeron 使用多 term 日志，总映射容量与本组固定槽容量不同；该差异未被解释成库本身的差异。

性能客户端直接驱动 `Fast*Ring`，所以下面的四拓扑表是 ring 热路径结果。
`FixedSubscriber::poll()` 后续已接入批量 `release(false)` + `flush_release()`，并用
独立 SPSC 64 B 产品 API ABBA 对比验证：饱和吞吐由 8.20 Mmsg/s 提升到 40.03 Mmsg/s，
配对中位数 4.84x；attempt p50/p99 没有证明改善。该产品 API 证据见
[FixedChannel 批量 cursor 对比](fixed-channel-poll-batch-20m-abba-10-group/README.md)，
但它仍不能替代四拓扑产品 API 矩阵或最终 SLA。

## 吞吐

单位为所有 producer 合计 Mmsg/s。SPMC/MPMC 会向每个订阅者 fanout，所以交付吞吐是发布
吞吐的两倍；比较使用发布吞吐，不把交付次数冒充发布次数。

| 单元 | Fast | Aeron | Fast / Aeron | v4.0.1 FIFO（全文） | Fast / FIFO |
|---|---:|---:|---:|---:|---:|
| SPSC 64 B | 75.55 | 30.08 | 2.51x | 7.22 | 10.47x |
| SPSC 256 B | 34.56 | 17.74 | 1.95x | 16.85 | 2.05x |
| MPSC 64 B | 77.44 | 45.55 | 1.70x | 30.68 | 2.52x |
| MPSC 256 B | 28.32 | 21.54 | 1.31x | 16.46 | 1.72x |
| SPMC 64 B | 93.29 | 18.58 | 5.02x | 5.72 | 16.31x |
| SPMC 256 B | 31.68 | 18.29 | 1.73x | 17.68 | 1.79x |
| MPMC 64 B | 62.68 | 12.53 | 5.00x | 19.97 | 3.14x |
| MPMC 256 B | 27.16 | 6.26 | 4.34x | 18.32 | 1.48x |

## Attempt 延迟

单位 ns。attempt 从首次发布尝试前开始，包含背压重试、payload 写入、commit 和投递。
每轮先取各 subscriber 的最差分位数，再取五轮中位数。

| 单元 | Fast p50 | Aeron p50 | Fast p99 | Aeron p99 |
|---|---:|---:|---:|---:|
| SPSC 64 B | 171 | 223 | 347 | 415 |
| SPSC 256 B | 275 | 315 | 507 | 607 |
| MPSC 64 B | 161 | 237 | 379 | 1,743 |
| MPSC 256 B | 259 | 311 | 567 | 7,295 |
| SPMC 64 B | 193 | 239 | 451 | 12,543 |
| SPMC 256 B | 287 | 399 | 6,399 | 8,447 |
| MPMC 64 B | 189 | 259 | 299,007 | 516,095 |
| MPMC 256 B | 263 | 13,311 | 50,175 | 557,055 |

MPMC p50/p99 仍受 driver、coordinator 和 subscriber0 共核影响。p99.9 的方向不成立：
64 B 为 Fast **3.178 ms**、Aeron **1.671 ms**；256 B 为 Fast **1.589 ms**、Aeron
**1.163 ms**。其他 6 个单元 p99.9 低于 Aeron。计划门禁只要求 p99，不能通过只展示
p99 而隐去 MPMC 的尾分位差距；下一步必须在独占核环境复验。

## Planned 延迟

单位 ns。planned 从开环计划到达时间开始，额外包含 producer 抢占和调度积压；它不能与
attempt 相减解释某个阶段成本。

| 单元 | Fast p50 | Aeron p50 | Fast p99 | Aeron p99 |
|---|---:|---:|---:|---:|
| SPSC 64 B | 247 | 287 | 3,039 | 4,735 |
| SPSC 256 B | 327 | 379 | 3,807 | 5,375 |
| MPSC 64 B | 235 | 303 | 5,887 | 8,319 |
| MPSC 256 B | 311 | 379 | 5,247 | 45,567 |
| SPMC 64 B | 267 | 299 | 8,959 | 8,703 |
| SPMC 256 B | 339 | 423 | 7,167 | 29,439 |
| MPMC 64 B | 255 | 335 | 48,127 | 128,974,847 |
| MPMC 256 B | 343 | 23,295 | 376,831 | 209,715,199 |

MPMC planned 中 Aeron 的百万纳秒级分位数反映共享核上的发送排期积压，不能外推为 Aeron
固有延迟；同样，它也不能证明 Fast Ring 在独占环境下具有相同尾延迟优势。另需注意 SPMC
64 B 的 planned p99/p99.9 为 Fast **8.959/811.007 us** 对 Aeron **8.703/294.911 us**，
该单元没有领先，不能把“attempt p99 门禁通过”扩大为所有 planned 尾分位。

## 证据

- 1 轮预热 + 5 轮正式、全文校验的 Fast 原始运行：
  [formal-full-fast-5-round/](formal-full-fast-5-round/)
- 同协议 FIFO 基线：[formal-full-fifo-5-round/](formal-full-fifo-5-round/)
- 聚合数据：[Fast summary.json](formal-full-fast-5-round/summary.json)、
  [FIFO summary.json](formal-full-fifo-5-round/summary.json)
- 环境、CPU、二进制和输入 SHA-256：
  [Fast environment.json](formal-full-fast-5-round/environment.json)
- 冻结 runner：[frozen-runner.py](formal-full-fast-5-round/frozen-runner.py)
- 原运行文件清单：[Fast SHA256SUMS](formal-full-fast-5-round/SHA256SUMS)、
  [FIFO SHA256SUMS](formal-full-fifo-5-round/SHA256SUMS)
- 全文校验开发门禁：[verification-full-5-round.json](formal-full-fast-5-round/verification-full-5-round.json)
- 8 单元完整正确性矩阵：[correctness-full-10m/](correctness-full-10m/)
  及其 [SHA256SUMS](correctness-full-10m/SHA256SUMS)
- Release、ASan/UBSan、TSan 与门禁脚本日志：[validation/](validation/)

Fast 与 FIFO 各执行 288 次案例：各 48 次预热、240 次正式。每个正式矩阵发布
**15.2 亿**条、交付并逐字节校验 **22.8 亿**次、采集 **3000 万**个延迟样本，所有
案例退出 0。Fast 最低 MemAvailable 约 **1.63 GiB**；两套目录的 high/max/OOM/
OOM-kill 计数均为 0。

完整正确性矩阵共发布 **1.20 亿**条、交付 **1.80 亿**次。本版 240 次正式性能轮则
另完成 22.8 亿次全文校验；`environment.json` 的 `payload_limit` 明确记录该口径。

候选回归为 Release **22/22**、ASan/UBSan **21/21**、TSan **21/21**；安装后下游
consumer 已实际创建 `FixedChannel` 并收发消息。拓扑/Aeron/门禁相关 Python 测试为
**28/28**，其中固定 Ring 门禁单测 **8/8**。Ring 和 API 测试另各重复 20 轮无失败。

## 复现

性能门禁：

```bash
python3 tools/aeron_compare/verify_fixed_ring_gates.py \
  --fast-summary doc/benchmarks/2026-10-29-fast-ring/formal-full-fast-5-round/summary.json \
  --baseline-summary doc/benchmarks/2026-10-29-fast-ring/formal-full-fifo-5-round/summary.json \
  --minimum-rounds 5 --minimum-groups 1 \
  --require-abba --require-full-verification --require-matched-protocol \
  --output doc/benchmarks/2026-10-29-fast-ring/formal-full-fast-5-round/verification-full-5-round.json
```

完整正确性矩阵：

```bash
for topology in spsc mpsc spmc mpmc; do
  for payload in 64 256; do
    cpu_base=1
    [[ "$topology" == "mpmc" ]] && cpu_base=0
    build/release/bench/salias_fast_ring_compare \
      --topology "$topology" --messages 10000000 --payload "$payload" --verify full \
      --cpu-base "$cpu_base" --cpu-stride 1
  done
done
```

原运行文件可在两个 `formal-full-*-5-round/` 目录分别执行 `sha256sum -c SHA256SUMS`。
旧版 [formal-development-5-round/](formal-development-5-round/) 保留为 count-only 历史证据，
不与本版表格混用。

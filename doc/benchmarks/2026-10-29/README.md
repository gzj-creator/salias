# v4.0.1 四拓扑 Salias / Aeron 对比

> 测量日期：2026-10-29，Asia/Singapore。代码为 `v4.0.1`，提交
> `064281a77d8b`。本目录是**旧 count-only v4.0.1 通用 FIFO 与 Aeron 基线**，性能轮只
> 校验计数和采样数，不校验完整 payload、序号或重复；固定槽候选和各拓扑的全文校验结果见
> [Fast Ring 全文校验开发验收](../2026-10-29-fast-ring/README.md)。本目录不与新的全文
> FIFO/Fast 矩阵拼接，也不与更早的拓扑数据拼接。
>
> 产品二进制由该提交的 v4.0.1 构建产生；测量时工作区含文档和 runner 修改，
> 因此 `environment.json` 的 git 描述为 `v4.0.1-dirty`。精确输入以其中的二进制
> SHA-256 为准。本目录采用修正 Aeron driver 日志哈希采集时点后的正式重跑结果。

## 结论

- **吞吐不是“四拓扑全面提升”**。64 B 发布吞吐相对 Aeron 的 Salias/Aeron 比值为：
  SPSC **0.242**、MPSC **0.773**、SPMC **0.241**、MPMC **1.414**。
  SPSC/SPMC 仍有明显差距；MPSC 的轮间波动较大，MPMC 只在本次 4 vCPU 绑核边界内领先。
- **256 B 差距缩小但仍分拓扑**。SPSC/MPSC/SPMC/MPMC 的比值分别为 **0.600 / 0.955 /
  0.646 / 1.665**。MPSC 在 256 B 已接近持平；MPMC 的 Aeron media driver 与
  subscriber0 共享 CPU0，因此 **1.665 倍不能解释成无核竞争的通用提升**。
- **attempt p50 在全部八个拓扑/payload 单元中都由 Aeron 更低**。Salias FIFO 为
  **0.359-0.431 us**，Aeron 为 **0.189-0.267 us**；差值 **+160 至 +212 ns**。
- **尾延迟没有一致赢家**。例如 SPSC/64 B 的 attempt p99 Salias/Aeron 为
  **67.583/46.591 us**，而 SPMC/256 B 为 **110.591/157.695 us**。两端都出现毫秒级
  p99.9，planned 中还出现数十毫秒尖峰，本组短测不能证明 SLA 或稳定尾部优势。
- 本机不是独占裸机。正式时段有其他 Web/开发服务运行，所有案例使用 `Nice=5`；Aeron
  还有独立 media driver。结果应作为当前共享 4 vCPU 环境的可复现对照，不是硬件上限。

## 测试矩阵

| 项目 | 设置 |
|---|---|
| 主机 | Tencent 4 vCPU KVM，Intel Xeon Platinum 8255C，Linux 6.8.0-101-generic |
| 工具链 | GCC 14.2；Salias 与 Aeron C++ 比较器 Release、`O3/native/LTO`；Aeron 原生库仍含原构建的 GCC 13.3 对象 |
| 引擎 | Salias `FifoMpsc` / `FifoMpmc`，Aeron 1.52.0 IPC exclusive publication |
| 拓扑 | SPSC=1P1S，MPSC=2P1S，SPMC=1P2S，MPMC=2P2S |
| Payload | 64 B、256 B |
| 容量 | Salias 每 producer 1 MiB ring；Aeron 1 MiB term。Aeron 为多 term 日志，总映射容量不相同 |
| 发布与消费 | batch=1；poll/fragment limit=64；普通页 |
| 吞吐 | 每 producer 1000 万条，不计时采样 |
| 延迟 | 总计 100 万 msg/s、2 秒、每 16 条采样一条；attempt 与 planned 分轮独立测量 |
| 轮次 | 每格 1 轮预热、5 轮正式；每轮重建成交通道和新进程，引擎顺序交替 |
| 聚合 | 每轮先取对应分位数，再取五轮中位数；延迟取同轮各订阅者中的最差分位数。没有合并直方图 |
| 资源边界 | cgroup `MemoryHigh=MemoryMax=704 MiB`、零 swap、`TasksMax=64`、`Nice=5` |

应用 worker 在每个案例内固定绑核：

| 拓扑 | Salias/Aeron 应用 worker | Aeron driver |
|---|---|---|
| SPSC | subscriber CPU1，publisher CPU2 | CPU0 |
| MPSC | subscriber CPU1，publishers CPU2/3 | CPU0 |
| SPMC | subscribers CPU1/2，publisher CPU3 | CPU0 |
| MPMC | subscribers CPU0/1，publishers CPU2/3 | CPU0，与 subscriber0 共享 |

协调进程固定在 CPU0。MPMC 在 4 vCPU 上共有四个应用 worker，Aeron 没有第五个独占核，
这是 Aeron 在此部署下的真实成本，也是本组 MPMC 比值的主要解释边界。

## 吞吐

单位 Mmsg/s。发布吞吐为各 producer 的总和；SPMC/MPMC 每个订阅者都接收全部消息，
所以交付吞吐是发布吞吐的两倍。“范围”为五轮正式结果的最小值到最大值。

| 拓扑 | Payload | Salias 中位（范围） | Aeron 中位（范围） | Salias/Aeron | Salias 胜轮 |
|---|---:|---:|---:|---:|---:|
| SPSC | 64 B | 8.883（8.484-9.003） | 36.717（18.772-45.795） | 0.242 | 0/5 |
| SPSC | 256 B | 8.629（8.229-9.643） | 14.389（6.679-17.270） | 0.600 | 1/5 |
| MPSC | 64 B | 33.700（18.606-35.539） | 43.618（13.757-57.835） | 0.773 | 1/5 |
| MPSC | 256 B | 14.907（10.824-15.088） | 15.603（4.708-16.707） | 0.955 | 1/5 |
| SPMC | 64 B | 6.863（6.301-7.893） | 28.440（7.696-33.875） | 0.241 | 0/5 |
| SPMC | 256 B | 6.484（3.397-7.293） | 10.035（2.153-11.068） | 0.646 | 2/5 |
| MPMC | 64 B | 16.895（11.982-22.535） | 11.948（6.650-22.256） | 1.414 | 3/5 |
| MPMC | 256 B | 12.032（5.590-14.366） | 7.227（2.589-8.181） | 1.665 | 5/5 |

吞吐轮间范围较宽，尤其 Aeron 在共享宿主上的最小值明显偏离中位数。因此文档不把
单轮最高值当作稳定能力；MPSC 的 64/256 B 都只有 1/5 轮 Salias 更快，不能宣称
接近持平等于稳定追平。MPMC/64 B 的 3/5 胜轮同样不足以证明稳定领先。

## 延迟

单位 us。每格是五轮“本轮对应分位数”的中位数，每轮先取两个订阅者中较差的对应分位数。
`attempt` 从首次发布尝试前开始，包含背压重试、payload 写入、commit 和投递。

| 拓扑 | Payload | Salias p50 / p99 / p99.9 | Aeron p50 / p99 / p99.9 | p50 差值 |
|---|---:|---:|---:|---:|
| SPSC | 64 B | 0.375 / 67.583 / 876.543 | 0.191 / 46.591 / 655.359 | +184 ns |
| SPSC | 256 B | 0.359 / 37.375 / 1097.727 | 0.189 / 48.639 / 1064.959 | +170 ns |
| MPSC | 64 B | 0.403 / 364.543 / 1458.175 | 0.197 / 327.679 / 2031.615 | +206 ns |
| MPSC | 256 B | 0.383 / 339.967 / 1900.543 | 0.199 / 172.031 / 1130.495 | +184 ns |
| SPMC | 64 B | 0.415 / 229.375 / 3538.943 | 0.205 / 241.663 / 2047.999 | +210 ns |
| SPMC | 256 B | 0.415 / 110.591 / 1146.879 | 0.203 / 157.695 / 1490.943 | +212 ns |
| MPMC | 64 B | 0.431 / 278.527 / 1409.023 | 0.221 / 266.239 / 1490.943 | +210 ns |
| MPMC | 256 B | 0.427 / 737.279 / 3342.335 | 0.267 / 1589.247 / 3833.855 | +160 ns |

`planned` 延迟从原始开环计划到达时间开始，包含发布者在尝试前的调度积压。它同样保持
五轮分位数中位数：

| 拓扑 | Payload | Salias p50 / p99 / p99.9 | Aeron p50 / p99 / p99.9 | p50 差值 |
|---|---:|---:|---:|---:|
| SPSC | 64 B | 0.463 / 598.015 / 3211.263 | 0.255 / 24.319 / 1179.647 | +208 ns |
| SPSC | 256 B | 0.423 / 22.783 / 1196.031 | 0.263 / 966.655 / 2326.527 | +160 ns |
| MPSC | 64 B | 0.451 / 274.431 / 1916.927 | 0.271 / 495.615 / 2621.439 | +180 ns |
| MPSC | 256 B | 0.455 / 1245.183 / 2752.511 | 0.287 / 6225.919 / 11927.551 | +168 ns |
| SPMC | 64 B | 0.487 / 221.183 / 1982.463 | 0.275 / 5767.167 / 10485.759 | +212 ns |
| SPMC | 256 B | 0.459 / 401.407 / 2228.223 | 0.267 / 1802.239 / 3342.335 | +192 ns |
| MPMC | 64 B | 0.519 / 7536.639 / 13238.271 | 0.335 / 487.423 / 8519.679 | +184 ns |
| MPMC | 256 B | 0.903 / 9699.327 / 14811.135 | 3.199 / 39321.599 / 51904.511 | -2296 ns |

MPMC planned p50 中位数看似由 Salias 领先，但五轮 p50 范围仍有明显波动：
64 B 的 Salias/Aeron 为 0.503-0.567 / 0.307-2.399 us，256 B 为
0.527-13.439 / 0.343-11.775 us。256 B 的 p99 中位数又是 Salias 9.699 us、
Aeron 39.322 us。这个结果只说明当前共享核调度下计划积压和轮间波动很大，不能外推为
Aeron 在其他 CPU 预算下的固有延迟。

## 校验与限制

- 288 次案例运行全部退出 0：48 次预热、240 次正式；正式发布 **15.2 亿**条、交付
  **22.8 亿**次、采集 **3000 万**个延迟样本。
- 每个案例核对 published/delivered/latency sample 计数和 consumer 编号；所有原始
  stdout、stderr、driver log 与聚合文件的 SHA-256 见 [SHA256SUMS](SHA256SUMS)。
  清单使用相对本目录的路径，共 726 个文件，可在本目录执行 `sha256sum -c SHA256SUMS`。
- cgroup 峰值约 **586.7 MiB**，`memory.events` 的 high/max/oom/oom_kill 均为 0；
  主机 MemAvailable 最低观测约 **886 MiB**。
- 64/256 B 是逻辑负载。延迟只写 16 B，吞吐只写 8 B；没有填充和校验完整 payload，
  也没有逐条校验内容、重复或顺序。
- 本组不是独占真机、长时间持续负载或生产 SLA 验收。Aeron 额外 driver、总映射容量、
  native 库工具链和 MPMC 核共享均保留为差异，不应合并成单一“库本身”结论。

## 证据与复现

- 机器可读聚合：[summary.json](summary.json)
- 逐案例记录：[results.jsonl](results.jsonl)
- 环境、输入哈希与 cgroup：[environment.json](environment.json)
- 完整文件校验：[SHA256SUMS](SHA256SUMS)
- 本地构建输入哈希：[binary-sha256.txt](binary-sha256.txt)
- 原始输出：[raw/](raw/)
- 冻结 runner：[frozen-runner.py](frozen-runner.py)
- 通用 runner：[run_topology_suite.py](../../../tools/aeron_compare/run_topology_suite.py)

构建并使用当前客户端后，在 4 vCPU Linux cgroup-v2 主机上运行：

```bash
OUT="$PWD/build/benchmarks/topology-suite-$(date +%Y%m%d-%H%M%S)"
systemd-run --user --wait --pipe --collect \
  -p "WorkingDirectory=$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
  -p MemoryHigh=704M -p MemoryMax=704M -p MemorySwapMax=0 \
  -p TasksMax=64 -p RuntimeMaxSec=2400s -p KillMode=control-group -p OOMPolicy=stop \
  --setenv=SALIAS_RESOURCE_CAPPED=1 \
  python3 tools/aeron_compare/run_topology_suite.py \
  --output "$OUT" \
  --salias-client build/release/bench/salias_ipc_compare \
  --aeron-client build/benchmarks/<aeron-build>/aeron_ipc_compare \
  --aeron-driver build/benchmarks/<aeron-build>/binaries/aeronmd
```

输入二进制哈希、Aeron 源码包哈希和冻结 runner 哈希见 `environment.json`；
安装包中不保留本地生成二进制。

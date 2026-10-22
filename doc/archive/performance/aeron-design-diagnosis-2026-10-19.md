# Salias / Aeron：100 ns 差距与长尾的设计级诊断

> 历史归档：仅记录当时版本、配置与结论，不代表当前性能或验收状态。当前汇总及下一轮任务见[最终性能测试与优化交接报告](../../performance-report.md)。

## 结论先行

本次实际完成 perf 采样、火焰图、同消息分段计时、调度事件关联、缓存布局和消费者轮询 A/B。
**没有修改产品原子内存序，也没有把未验收的设计原型合入 `src/`。**

1. **约 100 ns 是典型路径差距，不是 p99.9 的同义词。** 同轮串行复测的
   1P1S / 64 B / 1M msg/s，Salias p50=283 ns、Aeron=187 ns，差 96 ns；
   p99=443/263 ns。但 p99.9=4.799/11.263 us，Salias 此指标反而较低。
   两者都出现过长尾，不能称 Salias 在所有尾分位上远差于 Aeron。
2. **已确认的接收端浪费：空 poll 初始化 2 KiB 描述符数组。** Release 汇编确实出现
   `memset(..., 0, 0x800)`；199 Hz perf 采样中占消费者 CPU 的约 15%，不是猜测。
3. **长尾最大的已证实因素是失去 CPU，而不是一条原子指令。** 干净、无其他本次矩阵
   并发的调度跟踪中，293.026 us 延迟与发布者 278 us 的被抢占区间重合；
   169.356 us 与发布者 157 us 被抢占重合。减少几条指令无法消除这些停顿。
4. **缓存对齐不是默认收益，轮询重写也不是自动变快。** 隔离 visible cursor 只有
   4 ns 的 p50 差异；帧步长补齐至 64 B、首条直达、逐条接收均明显变慢。
   持久描述符原型仅在 1M/s 改善约 12–16 ns，低速和 p99.9 不稳定，不满足合入门槛。
5. 因此本次的优化结果是：保留可复现诊断工具，排除负收益方案，明确下一步优先级；
   **没有依据宣称已经追回 100 ns，也没有依据宣称已经解决最差尾延迟。**

## 环境、口径与证据

- 日期：2026-10-19；仓库基线 `ffbf6e10fb`，保留原有未提交工具/文档改动。
- 4 vCPU Xeon Platinum 8255C KVM；GCC 14.2、O3/native/LTO；Aeron 1.52.0。
- CPU1 消费、CPU2 发布、CPU3 Aeron SHARED/backoff driver；CPU0 不分配给测试。
  绑核不是隔离，不能把该共享 VM 结果当作裸机 HFT SLA。
- 串行运行、nice=5、每例超时、MemoryMax=704 MiB、MemorySwapMax=0、主机余量保护。
  保留证据中最低观测 MemAvailable=1510.7 MiB，memory high/max/oom/oom_kill 均为零。
- 每个正式组先独立预跑一轮，再 5 次独立测量、每次 2 s；轮换变体顺序。
  轮询初筛为 3 次正式测量。下表是**各轮分位数的中位数**，不是合并直方图分位数。
- `attempt` 从首次 claim 前计时；`planned` 从原始开环到达计划计时，落后后不重置。
  采样率 1/16，直方图约 1.56% 桶上界。固定周期采样有相位偏差风险。
- 逻辑 payload 为 64/256 B，常规基准只写时间戳和 marker 共 16 B；分段探针写 32 B。
  这不是完整 payload 构建/校验基准。投递数和样本数全部核对，但不是去重/内容验收。
- 证据汇总覆盖 196 次 measured runs、3.44 亿消息、2150 万延迟样本；其中包含
  单独的分段计时和 trace 运行，**不能把全部运行混为一个正式延迟总体**。

持久证据：[summary.json](../../benchmarks/2026-10-19/design-diagnosis/summary.json)、
[逐轮正式结果](../../benchmarks/2026-10-19/design-diagnosis/clean-final.jsonl)、
[分段计时](../../benchmarks/2026-10-19/design-diagnosis/stages.json)、
[调度关联](../../benchmarks/2026-10-19/design-diagnosis/switch-analysis.json)。
大型 `perf.data` 和完整反汇编留在 `build/benchmarks/design-diagnosis-20261019/`。

## 一、100 ns 差距的诊断

### 1.1 先拆开典型延迟和尾部

同一 `clean-final` 矩阵，1P1S、64 B、1M/s：

| 口径 | 实现 | p50 ns | p99 ns | p99.9 us | 各轮 p99.9 范围 us | sampled max us |
|---|---|---:|---:|---:|---:|---:|
| attempt | Salias 基线 | 283 | 443 | 4.799 | 4.063–11.263 | 66.207 |
| attempt | Aeron | 187 | 263 | 11.263 | 6.143–247.807 | 2130.385 |
| planned | Salias 基线 | 323 | 3423 | 12.543 | 9.727–27.135 | 1090.071 |
| planned | Aeron | 247 | 4415 | 14.719 | 12.415–119.807 | 1689.746 |

planned p99 包含排期落后，不能解释成单次库调用耗时。不同口径是独立运行，禁止直接相减
作为 claim、调度等阶段的精确耗时。最差轮次没有删除；单独确认矩阵还记录到 Salias
1M/s/64 B sampled max=1052.526 us，进一步说明 66.207 us 不是稳定的上界。

### 1.2 perf 与火焰图：明确发现了什么

普通用户 `perf_event_paranoid=4` 无法采样；通过既有 `sudo -n` 权限采集，未更改 sysctl。
KVM 未暴露 cycles/instructions/cache-misses PMU：这些事件返回 `not supported`。
因此**没有 cache-miss/HITM、IPC 或精确 stall-cycle 结论**，不能伪造 perf c2c 结果。

使用 `cpu-clock`、DWARF 调用栈。最终采用 199 Hz 和更大缓冲，报告引用轮次的原始 stderr
与 folded stacks 一并入库；完整日志仅保留于本地 build 目录。早期 997 Hz 采样存在丢样，
只作排查，不用于最终占比结论。提交证据范围见[证据说明](../../benchmarks/2026-10-19/README.md)。

正式诊断轮次消费者样本（基线 598、scratch 599、Aeron 601，频率低，非精密计数）：

| 实现 | 消费者主要 on-CPU 位置 | 自身样本占比 |
|---|---|---:|
| Salias 基线 | `Rx::try_recv_run` | 45.32% |
| Salias 基线 | libc `memset` | 14.88% |
| Salias 基线 | vDSO/无法细分地址 | 27.42% |
| 持久 scratch 原型 | `Rx::try_recv_run` | 50.42% |
| 持久 scratch 原型 | libc `memset` | 0%（该轮无命中） |
| Aeron | `aeron_image_poll` | 23.46% |
| Aeron | `aeron_subscription_poll` | 8.82% |

基线 `Subscriber::poll` 在 **fetch 之前**声明 `Message buffer[32]`。
`sizeof(Message)=64`，含默认成员初始化；汇编对应每次 poll 的 `memset` 2048 B，
无数据也执行。火焰图与反汇编互相印证，说明这是确实值得评估的设计问题。
但它占 CPU 15% 不等于端到端延迟一定能减 15%，更不等于能追回 100 ns。

可直接打开实际生成的火焰图：
[Salias 基线](../../benchmarks/2026-10-19/design-diagnosis/profiles/008-baseline-attempt-1000000-64-r1.svg)、
[持久 scratch 原型](../../benchmarks/2026-10-19/design-diagnosis/profiles/006-scratch-attempt-1000000-64-r1.svg)、
[Aeron](../../benchmarks/2026-10-19/design-diagnosis/profiles/007-aeron-attempt-1000000-64-r1.svg)。
图宽是 on-CPU 样本，不是消息延迟，也看不到 off-CPU 停顿的全部时间。

### 1.3 同一消息的阶段探针

另编译单独探针，在同一采样消息内记录 `attempt → claim end → precommit → callback`。
前三段区间覆盖端到端路径；precommit 时间戳位于最终写入该时间戳之前，所以 handoff
还包括该次写入、commit 与回调前路径，**不是纯跨核传输时间**。

| 阶段 | Salias p50 ns | Aeron p50 ns |
|---|---:|---:|
| 首次 attempt 到 claim 成功并取时钟 | 110 | 37 |
| claim 后取时钟到 precommit 取时钟 | 31 | 32 |
| precommit 到消费回调取时钟 | 255 | 114 |

这是 3 次独立运行的分位数中位数。额外时钟和写入会改变流水线，探针 Salias 端到端
p50 约 403 ns，不能把 73 ns claim 差和 141 ns handoff 差直接套回无探针的 96 ns。
**可支持的结论**：差距同时存在于发布准入和接收交接，不是 payload 填充；
不能支持“某两条原子恰好解释全部 100 ns”。

源码还给出了更重要的设计区别：Aeron `BufferClaim::commit()` release 发布帧长，
`aeron_image_poll()` acquire 读取同一帧长；Salias 先写帧头，再 release 推进独立的
`visible_producer_pos`，消费者先观察 cursor，再读帧头和 payload。后者需要更多依赖链和
共享状态，并保留乱序 commit 空洞、动态注册、TTL 等语义。**这是下一步协议级原型的
候选方向，不是本次已经通过 PMU 定量证明的 100 ns 根因。**

## 二、设计级 A/B：收益与负收益都保留

以下均使用原基准循环，不在每次 poll 中增加字符串分派或取模。
设计改动仅生成到 `build/` 的源码副本，不污染产品 ABI。

| 方案 | attempt p50 ns | attempt p99 ns | attempt p99.9 us | planned p99.9 us | 判断 |
|---|---:|---:|---:|---:|---|
| 基线 | 283 | 443 | 4.799 | 12.543 | 对照 |
| visible cursor 与 reservation 分隔 64 B | 279 | 443 | 4.863 | 13.183 | 4 ns 差不足以认定高收益 |
| 每帧总步长补齐至 64 B | 423 | 759 | 4.159 | 14.463 | p50 +140 ns，拒绝 |
| 首条立即回调，余下继续批量 | 387 | 451 | 4.415 | 12.031 | p50 +104 ns，拒绝 |
| `try_recv + release` 逐条模式 | 383 | 459 | 4.287 | 13.567 | p50 +100 ns，拒绝 |
| 空 poll 一次 PAUSE | 279 | 463 | 5.439 | 10.751 | 无稳定全面收益，不改默认 |
| 端点持久 `Message[32]` scratch | 267 | 447 | 8.959 | 12.927 | p50 -16 ns，尾部不稳，不合入 |

缓存布局补充：原 `NamedProducerSlot` 已按 128 B 对齐，但 visible/local sequence/
reservation 同处前 64 B。实验利用原 padding 分开它们，**槽位总大小不变但字段偏移变了**；
若正式采用仍应升级共享协议版本，不能因 sizeof 相同就宣称 ABI 兼容。
64 B 帧补齐使 64 B payload 的帧从 72 B 变 128 B，空间开销约 +77.8%，
并改变环绕和流控，不能把结果简单解释成单一 cache-line 指标。

轮询初筛还测了空轮询 `yield`、每 256 轮检查 watchdog、chunk=1 和可见性预探测。
yield 的 attempt p50 从 287 ns 升至 459 ns；watchdog 改频的中位收益仅约 20 ns，
且 planned 尾部更差；chunk=1 的典型延迟略低但失去原有批量回写优势。
可见性预探测也未追回差距（另一组基线 p50=283 ns，预探测=339 ns）。
这些都不是值得直接替换默认实现的高收益方案。

### 跨负载确认：不能只挑 scratch 的有利轮次

以下 5 轮独立确认只比较 scratch 与基线，口径为 attempt：

| 速率 / payload | 基线 p50 ns | scratch p50 ns | 基线 p99.9 us | scratch p99.9 us |
|---|---:|---:|---:|---:|
| 100k/s / 64 B | 303 | 307 | 2.847 | 4.287 |
| 100k/s / 256 B | 303 | 307 | 2.943 | 3.551 |
| 1M/s / 64 B | 287 | 275 | 6.655 | 4.223 |
| 1M/s / 256 B | 283 | 271 | 4.351 | 5.375 |

同一方案 1M/s 有 12 ns 的 p50 收益，100k/s 没有，且 256 B 的 p99.9 恶化。
端点 scratch 还改变同一 subscriber 递归 poll 的缓冲隔离，不应未经契约测试就合入。
这也是本次没有为了交付“优化百分比”而修改产品源码的原因。

## 三、尾部的真实高收益因素

### 3.1 调度事件与最慢消息直接关联

使用 `perf record -e dummy --clockid mono --switch-events`，记录同一消息的原始
planned/receive 时间，再与 switch-out/in 区间求交。解析使用整数纳秒，perf 文本时间
精度约 1 us；这是区间关联，不声称每纳秒都精确。

| trace 轮次 | 最慢采样延迟 us | 对应线程/核 | 重合的被抢占 off-CPU us | 占延迟 |
|---|---:|---|---:|---:|
| 预跑 | 200.797 | consumer / CPU1 | 164 | 81.7% |
| 正式 1 | 293.026 | producer / CPU2 | 278 | 94.9% |
| 正式 2 | 169.356 | producer / CPU2 | 157 | 92.7% |

这证明了这几次最差事件的大部分时间不在库里运行。不能据此推广到所有尾样本，也不能
仅凭 switch 事件区分宿主机 steal、来宾调度竞争、IRQ 或其他系统活动的全部原因。

早期曾让诊断与另一矩阵短暂重叠，发生 512 ms 的发布者被抢占，几乎完整解释该条
512.462 ms 延迟；**该批次不作为正式性能证据**。已补进程级排他锁，重新采集上述干净
trace；亦重跑完整 `clean-final`，该矩阵期间没有本次构建或另一测试并发。
早期 screen/design-ab/frame-ab/formal/fixed-formal/ready-formal 的结果不计入最终验收表。
`polling-formal` 是初筛，不作为合入承诺；跨负载确认和 clean-final 才用于最终判断。

### 3.2 下一轮优先级与验收门槛

| 优先级 | 方向 | 已有依据 | 合入/验收要求 |
|---|---|---|---|
| P0 | 测试独占、稳定 CPU 配额与宿主资源 | 最慢消息 82%–95% 与 off-CPU 重合 | 独占裸机/受控 VM，trace 与时延样本同步，保留最差轮 |
| P1 | 缩短接收空轮询/描述符路径 | 每次 2 KiB memset，消费者约 15% CPU | 不新增 heap 热分配；保留批量/TTL/借用/重入契约；多负载 p99.9 不退化 |
| P1 | 发布与可见性协议级原型 | claim 分段差、Aeron 帧内发布 vs 独立 cursor | 先测原型；覆盖动态注册、乱序 commit、回绕、fanout，不能靠删除 guard 破坏语义 |
| P2 | 拆 visible/reservation 缓存行 | 本机只有 4 ns p50 差、长尾不稳定 | 有 PMU 的裸机上 perf c2c/HITM + 多轮收益，再决定是否承担布局升级 |
| 暂停 | 通用 64 B padding、逐条接收、首条直达 | 本机 p50 明确回退 | 不推广为默认优化 |

**本次未进行**：独占裸机 CPU 隔离/IRQ 迁移、修改宿主调度或频率、2P2S 矩阵、长时 SLA
验收、HITM/LLC PMU 归因。绑核/排他锁已落实到工具，但不能宣称已经消除所有系统抖动。

## 可复现入口

见 [诊断工具说明](../../../tools/aeron_compare/README.md#design-diagnostics)。新增构建器生成
独立源码副本；所有运行必须在受限 cgroup 内串行执行。代码入口：

- `build_design_variants.py`：布局、首条直达、轮询和 scratch 设计原型。
- `diagnose_design.py`：有资源门禁和排他锁的交错矩阵，核对投递/采样数、记录哈希。
- `build_stage_probe.py`：同一消息 claim/precommit/handoff 探针。
- `build_tail_probe.py`、`analyze_switches.py`：尾消息时间与真实 switch 事件求交。
- `render_profiles.py`：由真实 `perf script` 经上游 FlameGraph 工具生成图，不合成栈。
- `export_design_evidence.py`：核验并保存标量、哈希、火焰图、调度和分段证据。

已有 `OPTIMIZATION_PLAN.md` / `OPTIMIZATION_ANALYSIS.md` 中固定估计的原子成本与
“Phase 1 已完成”不能代替本次测量。当前 `ClaimReservationGuard` 仍为 seq_cst，
没有 `SkipRegistrationGuard` 产品选项；本次未对该正确性协议降级。

## 交付验证

- Release 正常比较器、独立 baseline 和尾部探针重新构建成功；分段探针两端也重新构建成功。
- `ctest --test-dir build/release --output-on-failure -j1`：20/20 通过。
- `python3 -m unittest discover -s tools/aeron_compare -p 'test*.py'`：13/13 通过；
  覆盖独立轮次聚合、profile 排除、整数时钟关联、变体锚点与特权采样进程组清理。
- 分段探针在解析参数时拒绝 payload <32 B、非 attempt 口径和关闭采样；
  两个生成二进制对这三种错误配置的实际拒绝测试均通过，避免越界写入或错误阶段解释。
- 已验证持久数据集 SHA-256、逐轮数量和报告本地链接；`git diff --check` 通过。
- 本节为诊断结束时的历史状态：当时新增/修改范围为诊断工具、证据与说明，产品 `src/`
  保持不变，未创建 commit 或 tag；后续产品优化及发布状态分别以实作报告和 release note 为准。

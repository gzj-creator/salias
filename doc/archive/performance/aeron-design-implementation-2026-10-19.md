# Salias / Aeron：设计级优化实作与验收

> 历史归档：仅记录当时版本、配置与结论，不代表当前性能或验收状态。当前汇总及下一轮任务见[最终性能测试与优化交接报告](../../performance-report.md)。

## 本轮交付

在[上一轮真实诊断](aeron-design-diagnosis-2026-10-19.md)基础上，本轮实际修改接收路径，
测量调度策略，并实现两种帧内发布原型。**没有修改产品原子内存序、注册 guard、
共享内存布局或帧格式；没有启用系统级实时调度或修改 sysctl。**

- 产品两种 `Subscriber::poll` 重载改为调用独占的未初始化描述符存储，去掉每次空 poll
  对 32 个 `Message` 的 2 KiB 清零；仍然一次 fetch、逐条回调、批末 flush。
- 高速 64 B 确认组 attempt p50 **287 → 267 ns**，p99 **475 → 435 ns**，
  p99.9 **7.999 → 4.159 us**；低速典型延迟基本不变。另一个独立组 p50 仅改善 4 ns，
  **不能宣称稳定追回 20 ns，更没有追回全部约 100 ns。**
- `SCHED_FIFO` 实测产生约 **49–50 ms** off-CPU，planned p99.9 升至 **47.710 ms**；
  拒绝作为默认优化。只绑核或提高优先级，不等于获得独占 CPU。
- 跳过可见水位的帧内发布原型虽然更快，但**被变长回绕反例证伪**；保留水位的原型
  没有稳定高收益。两者都只保留在诊断工具和 `build/` 副本中，不进入产品。

## 口径和运行纪律

环境仍是 4 vCPU Xeon Platinum 8255C KVM，GCC 14.2、O3/native/LTO、Aeron 1.52.0。
消费者 CPU1、发布者 CPU2、Aeron SHARED/backoff driver CPU3；CPU0 不分配给本次测试。
沿用每例资源门禁、704 MiB 运行上限、零 swap、超时及进程组清理；构建与测量不并发。
没有迁移其他用户任务，没有调整 IRQ、宿主机资源、频率或全局 RT runtime。

常规组为 1P1S，每例独立预跑一次后 5 次、每次 2 s；planned 二次确认是 7 次。
表中均是**独立轮次分位数的中位数，不是合并直方图分位数**。采样 1/16，直方图约
1.56% 桶精度。固定采样周期可能存在相位偏差；常规逻辑 payload 只写时间戳和 marker
共 16 B，不代表完整 payload 构建/校验。投递数和采样数逐例核验，未据此宣称内容去重验收。

`attempt` 从首次 claim 前开始；`planned` 从原始开环到达计划开始，不重置落后排期。
不同口径独立运行，不可相减为精确阶段成本。共享 VM 中保留最差轮和异常值，不删除
不利结果；2 s 短轮次和当前矩阵不能代替长时 SLA、裸机 PMU 或 2P2S 验收。

原始目录：`build/benchmarks/design-implementation-20261019/`。
持久[汇总](../../benchmarks/2026-10-19/design-implementation/summary.json)包含逐轮数据、
命令、二进制/输出哈希及资源计数；unsafe `frame` 明确标记，不能作为产品性能结果。

## 一、去掉空轮询初始化，而不共享 scratch

### 实现和契约

新增 `salias/detail/poll_buffer.hpp`：对齐的 `std::byte` 栈存储，根据 C++20 隐式生命周期
规则承载隐式生命周期的 `Message` 数组；编译期约束 aggregate、平凡复制/复制构造/析构。
`fetch_batch` 的各返回分支必须完整赋值前 `got` 个元素；回调只读取这些元素。
未填充槽不读，不使用 heap，不调用 `Message` 默认构造以清零整批。

与上一轮 endpoint scratch 不同：每次 poll 和递归 poll 均有独立栈存储；不覆盖外层
描述符，不更改公开 `Message message;` 的默认零值、ABI、批处理大小、TTL 和 flush 时机。
这不新增“递归 flush 后外层 payload 仍被 pin”的承诺，原借用契约继续适用。

反汇编对照确认旧二进制有四处 2048 B `memset` 调用点，新二进制为零；
正式 CPU profile 各消费者约 400 个样本，旧版 libc `memset` **14.0%**，新版无命中。
profile 量小、存在未知地址，不把样本占比折算为端到端收益；有 profile 的延迟不计入下表。

实际火焰图：[旧版](../../benchmarks/2026-10-19/design-implementation/profiles/004-baseline-normal-attempt-1000000-64-r1.svg)、
[新版](../../benchmarks/2026-10-19/design-implementation/profiles/003-stack-normal-attempt-1000000-64-r1.svg)。

### 跨负载结果

`confirm` 组，5 次独立测量，attempt：

| 速率 / payload | 旧 p50 ns | 新 p50 ns | 旧 p99 ns | 新 p99 ns | 旧 p99.9 us | 新 p99.9 us |
|---|---:|---:|---:|---:|---:|---:|
| 100k/s / 64 B | 299 | 303 | 519 | 551 | 3.391 | 2.463 |
| 100k/s / 256 B | 311 | 311 | 559 | 559 | 4.607 | 3.839 |
| 1M/s / 64 B | 287 | 267 | 475 | 435 | 7.999 | 4.159 |
| 1M/s / 256 B | 295 | 283 | 495 | 459 | 4.927 | 4.415 |

低速 64 B 的 p50/p99 有小幅回退，不能把它描述成所有指标全面提升。高速 256 B
新版最差轮 p99.9=13.823 us，旧版=6.847 us，说明中位数改善不等于最差轮保证。
同组 Aeron 1M/s / 64 B 为 p50=187 ns、p99=263 ns；新版 Salias 仍有 80 ns p50 差。

`checked-timing` 是另一个独立的 64 B / 1M/s 组：旧/新 p50=283/279 ns，
p99=467/431 ns，p99.9=5.119/3.999 us；收益大小受运行相位/系统噪声影响，不能只挑
267 ns 的有利结果。该组 planned p99.9=9.983/11.903 us，反而回退，故追加下列确认。

`planned-confirm`，7 次独立测量，1M/s：

| payload | 旧 p50 ns | 新 p50 ns | 旧 p99 ns | 新 p99 ns | 旧 p99.9 us | 新 p99.9 us |
|---|---:|---:|---:|---:|---:|---:|
| 64 B | 327 | 331 | 2687 | 2367 | 12.031 | 9.983 |
| 256 B | 339 | 331 | 3551 | 3103 | 15.743 | 11.775 |

该确认组支持减掉已确认的空轮询工作，但不支持“每一轮尾部不退化”。64 B 新版
最差轮 p99.9=18.943 us，旧版=13.183 us。保留此局限，不称已经解决共享 VM 长尾。

## 二、调度不是把优先级拉满

实现独立 worker scheduler 构建器：controller 在 fork 前恢复 `SCHED_OTHER`，
只让已经绑核且完成建连的热 worker 进入 FIFO priority=1；Aeron conductor 保持普通调度。
两种调度用**同一二进制**轮换测量，最多 2 s，每例受 cgroup 和系统 RT 限流保护。

新增调度代码改变了 LTO 布局，Salias 普通调度 p50 也升至约 387–391 ns。因此这一组
只用于调度内部 A/B，**不能用它与普通基准的 287/267 ns 相减来算产品收益**。

64 B / 1M/s，5 次 measured runs：

| 实现 | 普通 attempt p99.9 us | FIFO attempt p99.9 us | 普通 planned p99.9 us | FIFO planned p99.9 ms |
|---|---:|---:|---:|---:|
| 旧 Salias | 4.063 | 4.223 | 13.695 | 47.710 |
| 新 Salias | 4.095 | 4.223 | 11.775 | 47.710 |
| Aeron | 6.719 | 7.935 | 16.127 | 47.710 |

系统原设置为 `sched_rt_period_us=1000000`、`sched_rt_runtime_us=950000`，没有修改。
持续 busy-spin 的 FIFO worker 用完 RT 预算会被限流，这与约 50 ms 的停顿相符。
单独的 `perf --switch-events` trace 直接观测到：

| FIFO 运行 | 最慢 planned 样本 | consumer off-CPU | producer off-CPU |
|---|---:|---:|---:|
| 预跑 | 50.166506 ms | 50.142 ms | 50.148 ms |
| 正式 1 | 50.167023 ms | 50.150 ms | 49.167 ms |
| 正式 2 | 49.180662 ms | 49.162 ms | 49.173 ms |

同条消息各线程停顿并发重叠，不能相加。RT runtime 设置和同步 switch 区间强烈支持
RT 限流解释；没有声称通过 `sched:rt_throttled` tracepoint 精确标记内核原因。
普通 trace 中还保留了一次 2.278 ms 尾样本没有 switch 重合，原因未定；不能说所有
VM 尖峰都能由来宾调度解释。

结论：**不改产品默认调度，不关闭 RT 限流，不以 root busy-spin 作为一般部署建议。**
下一步环境验收仍需要独占裸机/受控宿主配额、IRQ/steal/频率观测，而非单纯 chrt。

## 三、协议原型：更快但不正确不算收益

### 跳过 cursor 的原型被反例拒绝

独立副本把帧头改为 release/acquire 发布；只在连续前缀补齐时开放 COMMITTED，
保留注册 guard、乱序 commit 空洞与旧 visible cursor 的更新，FIFO 接收直接观察帧头。
在普通内容测试中，64 B / 1M/s p50=225 ns；**这不是可合入的优化成绩**。

原有 95 个 API 测试和乱序提交测试都曾通过，但新增对抗用例证明它仍会误交付：

1. 先发布一条占满 4096 B ring 的大帧，在任意 payload 中放置看似合法的帧头字节。
2. 消费释放后回绕，发布一条 16 B 的短帧。
3. 新的下一帧位置落到旧 payload 中；伪造字节恰好有 COMMITTED 和期望的序列号。
4. 没有 authoritative visible bound 的接收端把旧 payload 当成未来的新消息。

`StalePayloadCannotImpersonateFutureHeaderAfterVariableWrap` 在产品实现通过，在该
原型中 `try_recv()` 和 `poll()` 都失败。仅靠 24 位序号/committed 位不是完整的防旧帧
协议；动态变长帧下必须有可证明的槽位代际/预清理/额外发布元数据等设计及其成本。
**该原型已显式标记 unsafe，构建需单独 opt-in，未合入产品。**

### 保留 cursor 的修正版无稳定额外收益

`frame-checked` 先看帧头，但在真正读取 payload 前仍验证原可见水位；96 个 API 测试通过。
64 B / 1M/s：旧基线、栈存储、checked p50 分别 **283 / 279 / 283 ns**，
p99 分别 **467 / 431 / 427 ns**。planned p99.9 分别 **9.983 / 11.903 / 11.775 us**。
没有在栈存储基础上获得稳定高收益，保留为实验。

此外，在可见水位之前提前读取可能曾是 payload 的字节，其与变长写入并发的原子对象
生命周期和重叠访问仍需完整证明；**通过这些功能测试不等于协议已经可发布**。

## 回归与可复现性

- 产品 Release CTest **21/21** 通过，包含新独立存储契约；API 测试包含新增的调用独占、
  默认值和伪头回绕反例，共 **96** 项。
- 独立 `poll_storage_contract_test` 对全部 5 种模式、两种 poll 重载、部分批次、空批次
  和多次回绕，在 **ASan + UBSan** 下通过。
- 完整 API sanitizer 构建在 600 MiB memory-high 下进入回收停顿；第一次超时、第二次
  主动停止后改为小型契约目标。**没有声称完整 sanitizer API 套件跑完。**
- Python 工具测试 **16/16**，验证聚合口径、调度分组、worker 构建锚点及原型安全门禁；
  `git diff --check` 通过。产品没有新增 heap 热分配，也没有移除正确性同步。
- 预变更 baseline 的源码/include/object/二进制快照保留于 `build/`；每轮记录实际命令
  和哈希。该次实作结束时尚未 commit、tag，未替换其他已有未提交改动；随后产品优化
  单独保存为 `f9ea5c2`，发布状态以 `docs/release_note.md` 为准。

工具入口和风险见[比较工具说明](../../../tools/aeron_compare/README.md#follow-up-implementation)。
本轮落地的是有回归保障的接收路径减负；调度和发布协议都按证据拒绝直接推广。

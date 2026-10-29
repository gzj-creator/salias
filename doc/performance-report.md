# Salias 最终性能测试与优化交接报告

更新日期：2026-10-29，Asia/Singapore。**本页是当前性能结论与下一轮优化的唯一阅读入口。**
“最终”表示本轮结果已经汇总，不表示性能或生产 SLA 已验收。更早的完整实验和旧吞吐表保留在
[性能归档](archive/performance/README.md)；不要拼接不同日期、版本、CPU 分配或计时口径的数据。

## 1. 给下一个 agent 的结论

### 1.1 v4.0.1 当前四拓扑结果（2026-10-29）

本组在 `v4.0.1`、提交 `064281a77d8b` 上重新执行 SPSC、MPSC、SPMC、MPMC 四拓扑，
同一轮内配对 Salias FIFO 与 Aeron 1.52.0 IPC exclusive publication，同时覆盖吞吐、
attempt 和 planned 延迟。完整表格、逐轮原始记录和环境见
[2026-10-29 四拓扑对比](benchmarks/2026-10-29/README.md)。该组不与任何早期拓扑数据拼接。

吞吐为每 producer 1000 万条、五轮正式结果中位数，单位为 Mmsg/s：

| 拓扑 | 64 B Salias / Aeron | Salias/Aeron | 256 B Salias / Aeron | Salias/Aeron |
|---|---:|---:|---:|---:|
| SPSC | 8.883 / 36.717 | 0.242 | 8.629 / 14.389 | 0.600 |
| MPSC | 33.700 / 43.618 | 0.773 | 14.907 / 15.603 | 0.955 |
| SPMC | 6.863 / 28.440 | 0.241 | 6.484 / 10.035 | 0.646 |
| MPMC | 16.895 / 11.948 | 1.414 | 12.032 / 7.227 | 1.665 |

- **MPSC 的 256 B 已接近持平，64 B 尚未稳定追平。** 比值分别为 0.955/0.773；两种
  payload 都只有 1/5 轮 Salias 更快，轮间范围也宽，不能据此宣称稳定领先。
- **SPSC/SPMC 的 64 B 吞吐仍是明确短板。** 比值分别为 0.242/0.241；256 B 分别回升到
  0.600/0.646，说明差距不能只按 payload 大小归一。
- **MPMC 的本组领先有核共享边界。** 2P2S 已占满四个应用 worker，Aeron driver 与
  subscriber0 共用 CPU0；64 B 仅 3/5 轮 Salias 胜出，256 B 虽 5/5 胜出，但 1.665
  不能解释成无核竞争条件下的通用性能结论。

延迟负载为总计 100 万 msg/s、2 秒、每 16 条采样一次；每轮先取各订阅者最差分位数，
再取五轮中位数。下表为 attempt p50（Salias / Aeron）：

| 拓扑 | 64 B | 256 B |
|---|---:|---:|
| SPSC | 375 / 191 ns | 359 / 189 ns |
| MPSC | 403 / 197 ns | 383 / 199 ns |
| SPMC | 415 / 205 ns | 415 / 203 ns |
| MPMC | 431 / 221 ns | 427 / 267 ns |

- **Aeron 在全部八个 attempt p50 单元中更低。** Salias 为 359–431 ns，Aeron 为
  189–267 ns，中位数差为 **160–212 ns**；不能把这个差值外推到其他 CPU、批处理
  或生产负载。
- **尾延迟没有一致赢家。** 例如 SPSC/64 B 的 attempt p99 为 Salias 67.583 us、Aeron
  46.591 us；SPMC/256 B 则为 Salias 110.591 us、Aeron 157.695 us。两端均出现毫秒级
  p99.9，planned 中还有数十毫秒尖峰，本组短测不能证明 SLA 或稳定尾部优势。
- **planned p50 不能替代 attempt。** MPMC planned p50 的描述性中位数由 Salias 更低，
  但 Aeron 在该格出现更大的计划积压和轮间波动，且 p99 方向并不一致；两者是独立运行，
  不能相减推断某个阶段成本。

本机是共享 4 vCPU KVM，不是独占裸机或生产 SLA 基线。Aeron 额外 media driver、
两端总映射容量不同、Aeron native 依赖工具链不同均保留为差异；64/256 B 是逻辑长度，
延迟只写 16 B、吞吐只写 8 B，未逐条校验完整 payload、重复或顺序。正式 240 次测量全部
完成计数核验，但不能据此宣称四拓扑全面领先、延迟非劣或长尾已解决。测量时工作区含
文档和 runner 修改，`environment.json` 的 git 描述为 `v4.0.1-dirty`；产品输入由其中
记录的二进制 SHA-256 固定。

### 1.2 P3-1 历史验收状态（2026-10-21）

发布路径非对称屏障已随 v4.0.0 发布，具名通道协议为 v15。验证证据，以及未通过的
G4 长尾门禁，均保留在下方结果中。
正式结果与证据入口为 [P3-1 验收报告](plans/phase3-p31-acceptance-report.md)；
以下结果不与后文 2026-10-19 的 Aeron 测量拼接。

- **吞吐 G1/G2 通过**：64 B、FIFO 1P2S、每臂 120 轮，B/S/A 发布中位数分别为
  **6.492160 / 6.506805 / 9.061755 Mmsg/s**。同候选二进制 A/S 为 **1.392658**，
  组级精确置换检验 p=0.0021645、6/6 组方向一致；A/B 为 **1.395800**。
- **192 B 非劣 G3 通过**：A/S 中位数比 1.048841，高模式率 69.17%→87.50%，
  但 CV 仍高，不能声称双模态或稳定性问题已解决。
- **G4 FAIL**：独立 1P1S、1M msg/s、每臂每格 10 轮，planned p50 的 A−S 为
  −24/−12 ns（64/256 B），收发完成率均为 100%，短测部分通过。HFT 96 轮全部退出 0，
  但 S 臂有 4/48 轮原始输入未完成；按采集前固定的 p99/p99.9 5% 非劣规则，
  仅 7/24 个组/配置单元通过，50 µs 预算 A 也仅 2/24 通过。因此长尾非劣和完成度均未通过。
- **正确性 G5 通过**：Release 22/22、ASan/UBSan 21/21、TSan 21/21；
  membarrier 正确性主要依据跨进程压力、故障注入与握手论证，不依赖 TSan 对系统调用的建模。
- **注册成本 G6 已记录**：独立直接调用 acquire_consumer 的探针，S/A 为
  **12.2065/15.3725 µs**，增加约 **3.166 µs**，高于立项时 +0.6 µs 预估。
  此项只报告，不设阈值；不代表并发 claim 下的最坏注册耗时。

**P3-1 整体验收未通过，根因是 G4。** 后续若继续该方向，应另立实验定位 2P2S 长尾和
S 臂输入完成度，先固定新假设与门槛；不重跑本轮以追求通过，不改 5% 界限，不跨组池化，
不拼接 E1 nofence 诊断客户端作为产品收益。

### 1.3 Claim API v4 历史产品 A/B（2026-10-23）

claim API v4 的代码、契约测试、示例和文档迁移已完成：`PublishClaim` 不再持有
`shared_ptr<PublisherEndpoint>`，改由 `Publisher::commit(std::move(claim))` 提交并校验
归属。Release 22/22、ASan/UBSan 21/21 通过。

**已完成三次完整正式产品 A/B。** 每次都是 36 个吞吐 block、4 个延迟 case；
每轮原始 RESULT 与 stdout、组级 `SHA256SUMS`、客户端二进制及 fence 环境均通过独立
分析器复核。B=v3.0.0，S=v4 且 `SALIAS_DISABLE_ASYMMETRIC_FENCE=1`，A=v4 默认。
S/B 单独归因 claim API，A/S 单独归因屏障，A/B 才是组合效果。第三次于
2026-10-22 23:29 开始、2026-10-23 01:16 完成。

| 运行 | 证据目录 | 64 B S/B | 256 B S/B | 64 B 判定 |
|---|---|---:|---:|---|
| 第一次 | `claim-api-v4-20261022-181707` | 1.258804 (+25.88%) | 1.281718 (+28.17%) | GO |
| 第二次 | `claim-api-v4-release-20261022-204139` | 1.238393 (+23.84%) | 1.290653 (+29.07%) | INCONCLUSIVE |
| 第三次 | `claim-api-v4-stress3-20261022-232932` | 1.245886 (+24.59%) | 1.320935 (+32.09%) | INCONCLUSIVE |

第三次运行的完整吞吐矩阵：

| Payload | 对比 | 候选 Mmsg/s | 基线 Mmsg/s | 相对变化 | 组方向 | 精确置换 p |
|---|---:|---:|---:|---:|---:|---:|
| 64 B | S/B claim API | 6.206 | 4.981 | **+24.59%** | 6/6 | 0.002165 |
| 64 B | A/S fence | 7.092 | 6.206 | +14.28% | 6/6 | 0.002165 |
| 64 B | A/B combined | 7.092 | 4.981 | **+42.37%** | 6/6 | 0.002165 |
| 256 B | S/B claim API | 6.348 | 4.806 | **+32.09%** | 6/6 | 0.002165 |
| 256 B | A/S fence | 7.092 | 6.348 | +11.72% | 6/6 | 0.002165 |
| 256 B | A/B combined | 7.092 | 4.806 | **+47.58%** | 6/6 | 0.002165 |

**v4.0.0 整体吞吐提升已复现，但 claim API 单独的 64 B GO 未复现。** 第三次是
独立、同二进制复跑：v4 默认路径在 64 B 达到 **7.092 Mmsg/s**，v3.0.0 为
**4.981 Mmsg/s**，组合提升 **+42.37%**；256 B 为 7.092 对 4.806 Mmsg/s，
提升 **+47.58%**。两项都是 6/6 组同向、精确置换 p=0.002165。三次 A/B 组合比值
分别为 1.443618 / 1.420986 / 1.423746（64 B）和 1.424392 / 1.443184 / 1.475764
（256 B），因此在本实验拓扑和计数消费口径下，v4 的吞吐点估计提升是稳定的。
需要保留的统计边界是：组级置换检验只有 6+6 个簇，最小双侧 p 为 0.002165；
本次 A/B 的组簇 bootstrap 区间跨过 1，不能把它写成高置信度的效应幅度。

但预注册的 claim API 单独门槛要求 64 B S/B 中位数比 ≥1.25。三次分别为
**1.258804 / 1.238393 / 1.245886**，只有第一次越过门槛，第二、三次均判为
**INCONCLUSIVE**，尽管各自 p=0.002165、6/6 组同向。稳定结论是 claim API 单独带来
约 **24%–26%** 的 64 B 吞吐提升，而不是稳定超过 25%。预注册指标是 120 轮池化中位数；
不能改用组中位数比、合并三次运行或挑选第一次结果把它改判为 GO，也没有登记
“不确定后扩样”分支。256 B 只作描述性报告，不套用 64 B 门槛。

第三次 1P1S、1M msg/s 的短延迟中位数（各轮独立分位数取中位数，不合并直方图）显示：
64 B S/B 的 attempt p50/p99/p99.9 为 381/475/4543 ns 对 383/475/4319 ns，planned 为
437/3695/11455 ns 对 433/4175/11519 ns；A/B 组合的 attempt p99.9 为 4159 对 4319 ns，
planned p99 为 3599 对 4175 ns。256 B A/B 的 attempt p99/p99.9 为 479/4159 ns 对
529/4671 ns，planned p99/p99.9 为 3439/10879 ns 对 4319/13887 ns。短测 p50/p99 与
基线接近或更好，但轮间仍有毫秒级尖峰，且**没有重跑 HFT 长尾门禁**；因此不能宣称
延迟非劣、长尾已解决或 v4.0.0 整体发布验收通过。完整表格、轮间范围与重算命令见
[迁移计划](plans/phase3-claim-api-v4-migration-plan.md) §4.8，精简证据见
[2026-10-22 证据说明](benchmarks/2026-10-22/README.md)。

### 1.4 v3.0.0 / Aeron 历史对照（2026-10-19）

1. **最明确的瓶颈是 64 B、单发布双订阅广播吞吐。** 同组五轮发布中位数：
   Salias **6.341 Mmsg/s**，Aeron **42.441 Mmsg/s**，Aeron 约为 **6.69 倍**。
   五轮范围分别为 6.110–6.653 / 37.169–42.827 Mmsg/s，短测中差距稳定存在。
2. **256 B 吞吐大致相当，不能把 64 B 的差距推广为所有消息大小。**
   Salias/Aeron 为 14.962/15.019 Mmsg/s，中位数差约 0.38%，小于轮次波动。
   Salias 的 256 B 反而比 64 B 快，这是待诊断现象，不是大消息天然更快的结论。
3. **典型延迟仍落后。** 同组 1P2S、100 万条/s，attempt p50 为
   363/199 ns（64 B）与 379/203 ns（256 B），Salias 多 164/176 ns。
   较早 1P1S 确认组仍有约 80–100 ns 差距，两者不是同一拓扑。
4. **长尾没有解决，也不存在一方全面胜出的结论。** 两端 1P2S 均出现毫秒级尾部；
   Salias 在本组 attempt p99.9 较低，但 planned 64 B 反而比 Aeron 更高。
   共享 VM、短轮次和不同计时起点不能证明生产 SLA 或稳定尾部收益。
5. **已经采用的安全减负是移除空 poll 的 2 KiB 描述符清零。** 后续六个轮询/布局候选
   没有一致收益，均未晋升。不要继续基于固定“原子指令纳秒成本”盲目削弱同步。
6. 下一轮先**复现并剖析 64 B 广播吞吐断点**，同时用同步调度证据解释长尾；
   不要从不安全帧头原型、默认 FIFO 调度或挑选最佳轮次重新开始。

## 2. 产品版本、语义和状态

- 第 3–6 节和第 10 节的历史测量产品：`v3.0.0`，提交 `c5e6aa7df4397df6f47a4e00cf5f72ed5ae778bd`。
  2026-10-19 整理只修改文档与精简证据；P3-1 与 claim API v4 已随 `v4.0.0` 发布，
  对应结果分别单列于 §1.2 与 §1.3。当前四拓扑数据见 §1.1；这些结果不能反向当作
  第 3–8 节 v3 测量的重跑，也不能互相拼接。
- 已采用 poll 优化：`f9ea5c2`；Reliable 发布慢路径减少冗余存活探测：`ffbf6e1`。
- 本页 1P2S 是 **Reliable FIFO 广播**：每个订阅者接收每条消息，不是两个消费者分摊消息。
  Salias 使用 `FifoMpmc`；1P1S 对照使用 `FifoMpsc`，因此拓扑对照也包含产品模式选择差异。
- Aeron 1.52.0：IPC exclusive publication，每个进程独立 client，另有 media driver。
- 历史测量使用具名通道协议 v14，LatestByKey 布局 v2。当前 P3-1 工作区候选协议为 v15；
  跨进程参与者必须同步升级，旧共享通道须重建。

## 3. 历史主实验（v3.0.0）：1P2S Salias / Aeron 同组对比

### 3.1 条件与公平性边界

| 项目 | 设置 |
|---|---|
| 时间 | 2026-10-19 17:08:04 起，运行约 2 分 35 秒 |
| 主机 | Tencent 4 vCPU KVM，Intel Xeon Platinum 8255C，Linux 6.8.0-101-generic |
| 产品工具链 | Salias 与 Aeron C++ 比较器为 GCC 14.2，Release，O3/native/LTO；Aeron 原生依赖含 GCC 13.3 产物，见下方边界 |
| 应用绑核 | subscriber0 CPU1、subscriber1 CPU2、publisher CPU3，两端相同 |
| 附加进程 | coordinator CPU0；Aeron SHARED/backoff driver 也在 CPU0 |
| 缓冲与消费 | Salias 1 MiB ring、普通页、batch=1、poll-limit=64；Aeron 1 MiB term、fragment-limit=64 |
| 轮次 | 每格 1 轮预热、5 轮正式；引擎顺序交错，所有案例串行，无同时编译 |
| 资源约束 | 704 MiB cgroup，MemoryHigh=640 MiB，零 swap，Nice=5，超时和进程组清理 |
| 延迟负载 | 计划 1M msg/s，每例 200 万条，约 2 秒，固定 1/16 采样 |
| 吞吐负载 | 不限速、不采样，每例 1000 万条；实际轮次约 0.23–1.64 秒 |

**不是相同总 CPU 成本或独占机器对比**：Aeron 有额外 driver，客户端内部线程继承其
应用进程亲和性；CPU0 上的其他任务不受本实验控制。两端应用角色有各自固定核，
driver 不与这些应用角色共享核。Aeron 为三段 term 日志，总映射容量不是 Salias 的 1 MiB。

**不是整栈同编译器构建**：文档复核对原哈希匹配二进制执行 `readelf -p .comment`，
Salias 显示 GCC 14.2，Aeron 比较器同时含 GCC 14.2/13.3 标记，`aeronmd` 和 `libaeron.so`
显示 GCC 13.3。元数据没有冻结完整构建命令，不能由标签推断所有依赖使用相同编译器/flags；
下一轮应记录完整构建输入，必要时同工具链重建作为独立对照，不能声称现有差距已排除此因素。

**不是完整 payload 或完整正确性基准**：64/256 B 是逻辑长度，延迟路径只写 16 B
时间戳与 marker，吞吐只写 8 B marker；消费者主要计数/采样，没有逐条序号验重或完整内容校验。
吞吐是计数消费下的 IPC 结果，不能直接解释成业务可处理带宽。

### 3.2 吞吐：64 B 是优先排查项

单位 Mmsg/s。广播交付按订阅者计数，1 条发布对应 2 次交付；不把交付吞吐冒充发布吞吐。

| 逻辑长度 | 引擎 | 发布中位数 | 五轮发布 min–max | 总交付中位数 |
|---|---|---:|---:|---:|
| 64 B | Salias | **6.341** | 6.110–6.653 | 12.682 |
| 64 B | Aeron | **42.441** | 37.169–42.827 | 84.881 |
| 256 B | Salias | **14.962** | 9.190–15.454 | 29.924 |
| 256 B | Aeron | **15.019** | 11.638–16.809 | 30.038 |

这是短轮次观察，不是持续容量保证。64 B 差距应先复现，再区分发布、消费、进度回写、
环回绕与背压各项成本；目前没有数据证明某一项就是 6.69 倍差距的根因。

### 3.3 attempt 延迟：首次尝试前到接收

包含 claim 重试、payload 写入、commit 和期间停顿，不是单个库函数 CPU 开销。
先取每轮两个订阅者中更大的对应分位数，再取五轮中位数；**不是合并直方图分位数**。
`latency_histogram.hpp` 报告的是命中桶上界：64 ns 以下精确、每个二次幂区间再分 64 桶；
本表保留原输出精度，不表示纳秒级测量置信度。`max` 也只是采样消息的最大值，不是全量最大值。

| 逻辑长度 | 引擎 | p50 ns | p99 us | p99.9 us | 五轮 p99.9 min–max us |
|---|---|---:|---:|---:|---:|
| 64 B | Salias | 363 | 3.039 | 1605.631 | 266.239–1867.775 |
| 64 B | Aeron | 199 | 5.311 | 2326.527 | 51.711–4063.231 |
| 256 B | Salias | 379 | 4.671 | 614.399 | 68.607–2949.119 |
| 256 B | Aeron | 203 | 78.847 | 3014.655 | 120.831–3964.927 |

### 3.4 planned 延迟：原始到达排期到接收

包含进入发送函数之前的排期积压，落后时不重置到达计划。
attempt/planned 是独立运行，不能将其分位数相减作为阶段成本。

| 逻辑长度 | 引擎 | p50 ns | p99 us | p99.9 us | 五轮 p99.9 min–max us |
|---|---|---:|---:|---:|---:|
| 64 B | Salias | 379 | 137.215 | 2326.527 | 109.567–3932.159 |
| 64 B | Aeron | 267 | 92.159 | 2080.767 | 1687.551–3571.711 |
| 256 B | Salias | 367 | 14.079 | 1540.095 | 1048.575–4063.231 |
| 256 B | Aeron | 263 | 1228.799 | 3473.407 | 2752.511–4915.199 |

程序平均发布速率约 0.995 Mmsg/s：总计时包含约 10 ms 发令等待及结束开销。
固定消息数最终发完不等于严格 2 秒截止完成性通过。没有丢弃不利轮次，没有测 p99.99。

### 3.5 核验结果

共 12 次预热、60 次正式测量；正式发布 **280,000,000** 条、广播交付 **560,000,000** 次、
采样交付 **10,000,000** 次。每例收发计数、两个订阅者采样数、原始输出 SHA-256、
二进制/依赖哈希和五轮分组聚合均复核通过。
`memory.events` 的 high/max/oom/oom_kill 均为零；cgroup 峰值约 578.3 MiB，
最低观测 MemAvailable 约 1377.5 MiB。此峰值不是单独库内存占用基准。

## 4. 辅助实验：1P1S 到 1P2S 的变化

这是 **16:58:21 起的较早 Salias-only 实验**，不是上节同组 Aeron 对照。
CPU1/2 仍是订阅者、CPU3 发布者，但当时禁用 CPU0，coordinator 在 CPU1–3，
会竞争应用角色的核。共享 VM、绑核安排和运行时间不同，不可拼接或解释为代码优化前后。

100 万条/s，五轮中位数；同样先逐轮取最慢订阅者分位数。attempt：

| 拓扑 | 逻辑长度 | p50 ns | p99 us | p99.9 us | 最差轮 p99.9 us |
|---|---:|---:|---:|---:|---:|
| 1P1S | 64 B | 287 | 0.463 | 4.479 | 479.231 |
| 1P2S | 64 B | 303 | 0.679 | 208.895 | 1163.263 |
| 1P1S | 256 B | 283 | 0.487 | 5.759 | 11.775 |
| 1P2S | 256 B | 303 | 7.039 | 1081.343 | 2260.991 |

同实验吞吐（1P1S / 1P2S 发布中位数）：64 B **9.315 / 6.323 Mmsg/s**，
256 B **14.906 / 15.594 Mmsg/s**。1P2S 的 256 B 轮次范围 6.636–15.745 Mmsg/s，
不能据此声称增加订阅者提升吞吐。该实验只支持多订阅长尾值得单独研究的观察。
共 60 次正式运行、2.8 亿发布、4.2 亿交付、750 万时延样本，计数核验通过。

## 5. 已做优化、诊断证据和失败路线

### 5.1 已合入的 poll 存储优化

两种 `Subscriber::poll` 重载使用调用独占栈存储，去掉每次空轮询 32 个 `Message`
共 2 KiB 清零；只有前 `got` 个完整赋值的描述符可读。保留公开默认值、递归调用隔离、
批量边界、借用/flush 和 TTL 契约，不使用端点共享 scratch，不改原子内存序。

较早 1P1S、64 B、1M/s 五轮确认：旧/新 attempt p50 **287/267 ns**，p99 **475/435 ns**，
p99.9 **7.999/4.159 us**；同组 Aeron p50=187 ns。另一独立组 p50 仅改善 **4 ns**；
低速有小回退，新版部分最差轮更差。因此只确认移除了无用工作，不承诺稳定追回 20 ns。

反汇编旧版四处 2048 B memset，新版零处；小样本 profile 中旧消费者 memset 占 14%，
新版无命中。不能把该占比折算为端到端收益。

### 5.2 perf 和调度究竟证明了什么

- 本 VM 硬件 cycles/instructions/cache-misses 事件不支持，没有 HITM/perf-c2c/IPC 结论。
  `perf_event_paranoid=4`；既有受控权限下曾采集 cpu-clock/DWARF 与 switch-events，未改 sysctl。
- cpu-clock 199 Hz、约 400–600 个消费者样本的 profile 指向接收轮询热路径；存在未解析地址、
  样本少等限制。它支持定位调查，不支持完整归因 100 ns 或证明缓存行是唯一瓶颈。
- 较早干净 trace 的三个最慢事件中，off-CPU 重合约占 **81.7% / 94.9% / 92.7%**。
  只证明这些具体事件大部分时间不在库内执行，不能推广到所有长尾或直接归咎宿主机。
- 一次普通调度 2.278 ms 尖峰没有 switch 重合，原因未定。**本页主实验没有同步 trace**，
  不可把其他实验的解释直接套给当前毫秒尖峰。

较早 1P1S 的同消息阶段探针提供了进一步方向（三次独立运行 p50 中位数）：

| 探针区间 | Salias ns | Aeron ns |
|---|---:|---:|
| 首次 attempt 到 claim 成功并取时钟 | 110 | 37 |
| claim 后取时钟到 precommit 取时钟 | 31 | 32 |
| precommit 到消费回调取时钟 | 255 | 114 |

数据来自[stages.json](benchmarks/2026-10-19/design-diagnosis/stages.json)。探针使 Salias
端到端 p50 升到约 403 ns；handoff 包含最后时间戳写入、commit 与回调前处理，不是纯缓存传输。
这些阶段分位数不能相加还原无探针延迟，也不能把 73/141 ns 差直接套回本轮 1P2S。
它只支持“发布准入与接收交接都需剖析，payload 填充不是该探针里的主要差异”。

源码层面的设计差异是：Aeron commit release 发布帧长、poll acquire 观察帧长；Salias 写帧头
后 release 推进独立的 `visible_producer_pos`，接收者先观察水位再读取帧头。后者还承担乱序
commit 空洞、动态注册与 TTL 等语义。这是需要证明的依赖链候选，不是可直接删掉的“多余同步”。

### 5.3 不要重复推广的候选

| 方案 | 实测/反例 | 当前决策 |
|---|---|---|
| visible/reservation 分隔 64 B | 较早 1P1S attempt p50 283→279 ns，仅 4 ns；planned p99.9 12.543→13.183 us | 无高收益证据，不合入 |
| 每帧步长补齐 64 B | 64 B payload 的帧步长 72→128 B，空间 +77.8%；p50 283→423 ns | 拒绝通用 padding |
| 首条立即回调 / 逐条 try_recv+release | 同一初筛 p50 283→387/383 ns，分别慢 104/100 ns | 不替换默认批量路径 |
| 空 poll PAUSE / yield | PAUSE 初筛仅好 4 ns、尾部不稳；另一组 yield p50 287→459 ns | 不改默认；需按负载重新验证 |
| 分支回绕代替取模 | 初筛 p50 好 24 ns，跨负载仅好 4 ns，另两格退化 52 ns | 不合入 |
| 五组本地游标聚合 | 64 B/1M 初筛 p50 反而差 92 ns | 不合入 |
| 缓存注册定位、布局组合 | 256 B 某格好 20 ns，64 B p50/p99/p99.9 同时退化 | 不合入 |
| 延后 pending-progress、控制流组合 | 没有稳定整体收益 | 不合入 |
| 端点共享 scratch | 破坏递归调用描述符隔离；产品改用调用独占栈存储 | 不用 |
| 跳过 authoritative visible bound 的帧头发布原型 | 变长回绕可读到陈旧 payload，被反例否决 | 不安全，禁止产品化 |
| 保留可见水位的修正版 | 没有稳定额外高收益 | 仅诊断 |
| 默认 SCHED_FIFO busy-spin | 观测约 49–50 ms off-CPU，planned p99.9 约 47.710 ms | 不启用，不关闭 RT 限流 |

六个后续轮询候选共 165 次非 profile 正式运行、2.58 亿消息、1612.5 万样本；
没有新候选晋升，不等于证明这些操作没有成本。
前四行来自较早[设计诊断](archive/performance/aeron-design-diagnosis-2026-10-19.md)，
不是六候选矩阵，也不是本轮 1P2S。现有 `NamedProducerSlot` 已按 128 B 对齐，不能把
“结构体已经对齐”和“热字段已隔离”混为一谈；字段偏移变化即使 sizeof 不变也须评估协议升级。

### 5.4 Publisher/HFT 验收没有通过

2026-10-18 的八配置 30 分钟总长测，p99.9 50 us 门禁 **0/8**；这是产品早期实现的历史验收，
不是 v3.0.0 全部重新验收。后续 `ffbf6e1` 基线上的新候选完成三组 ABBA、96 个正式阶段、
总计 48 分钟；三组均未通过“重点尾部至少改善 20%、其他配置退化不超过 5%”的预设门禁。
2P2S/256 B 突发端到端 p99.9 分别退化 **115.4% / 735.7% / 418.9%**，新候选已撤销。

该 ABBA 正式计划输入 3,312,000,000 条，实际发布 3,311,999,463 条；已发布消息完整性通过，
但两个基线阶段合计 537 条计划输入未发送，仍算完成性失败。不能混淆“未发送输入”和传输丢失。
有条件的后续 30 分钟长测没有运行。不得把本页短测或测试套件通过当成长期 SLA 通过。

## 6. 历史吞吐为什么与新结果不同

2026-07-11 的旧版本、独立可执行程序、2P1S/2P2S 测试曾记录 Salias FIFO 发布
44.867/30.158 Mmsg/s，部分配置超过 Aeron。它不是当前 v3.0.0、1P2S、fork harness、
GCC 14.2 的重测，**不能用来否定当前 64 B 的差距，也不能当成新优化前的同组基线**。
完整旧表移至 [2026-07-11 历史吞吐](archive/performance/throughput-2026-07-11.md)。
Ordered 提供不同的跨发布者全序语义，不用其吞吐直接替代 FIFO/Aeron 的可比结论。

## 7. 下一轮优化任务：先归因，再改变设计

### P0：优先归因 SPSC/SPMC 的小 payload 吞吐差距

1. 在受控裸机或能保证独占 vCPU 的主机上复跑当前四拓扑，优先使用更长时间窗口和
   相邻 payload（32/48/64/80/96/128/256 B）；保留全范围、输入完成性和每订阅者结果，
   不只报最好一轮。
2. 对 SPSC/SPMC 分别检查单发布者通用路径、订阅者 poll、进度回写、环回绕和广播扇出。
   当前 SPSC/64 B 比值为 0.242，而 SPMC/256 B 为 0.646，说明不能只按 payload 或
   多订阅开销做单一解释。
3. MPSC 当前 64/256 B 比值为 0.773/0.955。可把 256 B 作为接近持平的对照，但 64 B
   仍有差距，两种 payload 都只有 1/5 轮 Salias 更快，不能把单次改善称为稳定超越。
   MPMC 的 2P2S 数据只能在记录 driver/worker 核共享后比较；若目标是通用扩展性，
   另做无核共享的公平实验。
4. 在独立诊断运行里测 publisher 的成功/失败 claim、commit、背压重试，以及每订阅者
   poll 空转/成功批次、每批消息数、共享进度回写频率、消费滞后。插桩数据不进入正式收益表。
5. 同时检查比较器差异：Salias 背压 yield 与 Aeron busy-spin、FifoMpsc/FifoMpmc 分派、
   单发布者通用路径开销、Aeron triple-term/window、原生依赖工具链。要做公平诊断，
   不默认删除产品可靠性语义。

### P1：同步解释尾部，不靠提高线程优先级掩盖问题

同条消息记录 planned/attempt/commit/receive 时间，与调度 off-CPU 区间关联，保留单个未解释
尖峰。按 publisher、两个 subscriber、driver 角色区分停顿、背压传播与纯 on-CPU 工作。
需要硬件缓存/流水线结论时迁移到具备 PMU 的受控机器；本机不要伪造 cache-miss/HITM 数字。
独占核、IRQ/频率或宿主配额实验必须单独报告，不能与普通共享 VM 混为同组代码收益。

### P2：只推进经证据支持的设计变化

候选方向是进度发布/回收、广播批量路径和单发布者路径；缓存行对齐与预取也必须有布局或
访问证据支持。每次只改一个候选，先正确性再交错 A/B，至少覆盖 64/256 B 和当前
SPSC/MPSC/SPMC/MPMC 四拓扑。2P2S 结果必须同时记录应用 worker 与 Aeron driver 的
CPU 归属；有资源再扩展到 1P3S/1P4S 或独占核扩展性实验。

**不可牺牲的契约**：注册 gate/代际互斥、PID 启动身份和死亡回收、最慢可靠订阅者背压、
payload 借用期、批量安全前缀、TTL 过期推进、递归 poll 描述符隔离、raw/schema 分帧和变长回绕。
不得用丢消息、减少计划输入、缩短计时、跳过可见水位或直接改 seq_cst 来换漂亮数字。

### 下一轮完成条件

交付“根因证据 → 单一候选 → 正确性回归 → 无插桩同组收益 → 退化检查”的完整链条；
保留失败路线和最差轮。正确性至少覆盖发布版本的 Release CTest 21 项及工具回归，
涉及内存/同步时加定向 sanitizer、跨进程故障与借用/注册测试。历史通过数不是无需重跑的证明。
当前未定义新的吞吐百分比验收阈值，不自行把某个短测改善称为 SLA；HFT 长测需复用其显式门禁。

## 8. 证据位置与复现入口

### 8.1 仓库内冻结证据

| 数据集 | 位置 | 内容 |
|---|---|---|
| **v4.0.1 四拓扑（当前主结论）** | [2026-10-29](benchmarks/2026-10-29/README.md) | 吞吐与 attempt/planned 延迟、288 个案例、逐案例 JSONL、环境、冻结 runner、路径感知 SHA-256 |
| v3.0.0 1P2S 同组对比（历史） | [spmc-aeron](benchmarks/2026-10-19/spmc-aeron/summary.json) | summary、环境、72 条 JSONL、72 份小 stdout、冻结 runner |
| 较早 Salias-only（辅助） | [spmc-salias](benchmarks/2026-10-19/spmc-salias/summary.json) | summary、环境、72 条 JSONL、72 份小 stdout、冻结 runner |
| 新证据完整性 | [evidence-manifest.json](benchmarks/2026-10-19/evidence-manifest.json) | 源路径、目标路径、每文件 SHA-256/字节数 |
| 既有 1P1S 实作确认 | [design-implementation](benchmarks/2026-10-19/design-implementation/summary.json) | 安全 poll 优化、调度与协议原型，分组聚合 |
| 六候选失败实验 | [poll-path-followup](benchmarks/2026-10-19/poll-path-followup/summary.json) | 全部格子、范围及七实现的精简核验 |
| perf/调度证据 | [design-diagnosis](benchmarks/2026-10-19/design-diagnosis/summary.json) | 真实 profile、stage/switch 关联，详见历史报告 |
| HFT 较早长测精简归档 | [performance-results.json](benchmarks/2026-10-18/performance-results.json) | 标量和来源哈希；不含可重算历史分位数的完整桶 |
| Claim API v4 三次正式 A/B | [2026-10-22 证据说明](benchmarks/2026-10-22/README.md) | run1–run3 分析摘要、实验账本和文件 SHA-256 |

新证据保持原始数值和历史绝对路径；`results.jsonl` 的 stdout 原位置可能只在本机存在，
其小文件已按原 basename 冻结到对应 `raw/`，可据 stdout_sha256 核验。
旧二进制、perf.data、完整日志没有入库，哈希不是这些文件的备份。

### 8.2 本机原始目录与源码定位

历史 v3.0.0 主实验：`build/benchmarks/spmc-aeron-v3.0.0-20261019-170804/`；辅助实验：
`build/benchmarks/spmc-v3.0.0-20261019-165821/`。两组目录、冻结二进制、runner 与 Aeron 依赖
继续保留原位，主实验复跑命令不变。

按后续清理要求，四组旧压测目录已无损压缩到 Git 忽略的
`build/benchmark-cleanup-20261019/`，验证后移除散文件：

| 原 `build/benchmarks/` 子目录 | 本地归档文件 | 原文件数 |
|---|---|---:|
| `poll-path-20261019/` | `poll-path-20261019.tar.gz` | 567 |
| `design-diagnosis-20261019/` | `design-diagnosis-20261019.tar.gz` | 1799 |
| `design-implementation-20261019/` | `design-implementation-20261019.tar.gz` | 719 |
| `publisher-tail-20261018/` | `publisher-tail-20261018.tar.gz` | 725 |

每个归档均逐文件解流核验原 SHA-256 后才清理原目录；3,810 个文件从 890.9 MiB
压为 217.6 MiB，按文件字节差节省约 673.3 MiB。失败结果、perf.data、原始日志、冻结源码
及二进制均在包内，未因不利结论被丢弃。`manifest.json` 记录映射、逐文件和归档哈希。
这些包仅在本机保留，不属于仓库交付；历史 JSONL 中的旧路径需要先恢复才能访问。

要复核某组完整证据，先确认无测试正在运行且目标目录不存在，在仓库根解压对应包，例如：

```bash
test ! -e build/benchmarks/design-diagnosis-20261019 && \
  tar --no-same-owner -xzf build/benchmark-cleanup-20261019/design-diagnosis-20261019.tar.gz -C "$PWD"
```

常规分析仍优先使用仓库内精简证据，无须先恢复全部旧产物。

| 组件 | 路径与定位标识 |
|---|---|
| 对比入口/重试策略 | `tools/aeron_compare/salias_ipc_compare.cpp`、`aeron_ipc_compare.cpp` |
| 开环计时 | `tools/aeron_compare/latency_pacing.hpp`：`planned_arrival_ns` |
| 发布/注册/消费主路径 | `src/core/channel/shared_hybrid_mpsc.hpp`：`ClaimReservationGuard`、`Tx::claim`、`Rx::try_recv_run`、`flush_progress` |
| 共享布局/公开端点 | `src/salias/src/channel.cpp`：`NamedProducerSlot`、`Publisher::try_claim`、`Subscriber::fetch_batch/poll` |
| 描述符存储 | `src/salias/include/salias/detail/poll_buffer.hpp` |
| 帧步进/基础容量缓存 | `src/core/frame/codec.hpp`、`src/core/flow/producer.cpp`：`cached_head_` |
| 设计原型/trace | `tools/aeron_compare/build_*`、`diagnose_design.py`、`analyze_switches.py` |
| 正式 HFT 门禁 | `tools/hft_acceptance/run_tail_suite.sh`、`summarize_tail_ab.py` |

先使用 `rg` 定位当前函数，不依赖文档行号。工具能力、构建与通用安全条件见
[Aeron 工具说明](../tools/aeron_compare/README.md)、[HFT 工具说明](../tools/hft_acceptance/README.md)。

### 8.3 在同一主机复跑历史 v3.0.0 1P2S 实验

冻结 runner 按原目录深度推导仓库根；先恢复到以下路径，不要直接在 doc 目录运行。
命令只适用于原有 Linux/systemd/cgroup-v2 主机；先检查没有其他测量/编译并发、二进制存在、
源版本一致且 MemAvailable 至少 1 GiB。不满足条件就重新构建并记录新输入，不能称原二进制复跑。

```bash
mkdir -p build/spmc-check-20261019
cp doc/benchmarks/2026-10-19/spmc-aeron/frozen-runner.py build/spmc-check-20261019/compare_aeron.py
python3 -m py_compile build/spmc-check-20261019/compare_aeron.py
OUT="$PWD/build/benchmarks/spmc-aeron-repeat-$(date +%Y%m%d-%H%M%S)"
systemd-run --user --wait --pipe --collect \
  -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p 'CPUAffinity=0 1 2 3' -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 -p TasksMax=48 \
  -p RuntimeMaxSec=600s -p KillMode=control-group -p OOMPolicy=stop \
  /usr/bin/env SALIAS_RESOURCE_CAPPED=1 PYTHONDONTWRITEBYTECODE=1 \
  python3 "$PWD/build/spmc-check-20261019/compare_aeron.py" --output "$OUT"
```

runner 中主实验允许 CPU0 是刻意的拓扑设置，不要直接套用只允许 CPU1–3 的旧单订阅 runner。
冻结 runner 目前固定 2M/10M 消息短测；延长吞吐和增加诊断需要另存新实验并标注工具变化。
若不在精确 tag HEAD，先审查版本和脚本的 `git describe --exact-match` 检查，不通过伪造 tag 绕过。
当前 v4.0.1 四拓扑的通用命令和输入要求见
[2026-10-29 证据说明](benchmarks/2026-10-29/README.md#证据与复现)。

## 9. 文档整理结果和禁止误读

- 本页作为最终性能汇总；日期报告保留完整证据，旧吞吐表移入 `doc/archive/performance/`。
  README 和工具入口统一指向本页，当前四拓扑明细从本页 §1.1 链接。
- `docs/release_note.md`、`doc/release_note.md` 是历史发布记录，不改写已发布版本声明。
  CHANGELOG 只记录本次整理，不改产品版本或已存在的 v3.0.0 tag。
- 未入库的旧优化估算草稿、生成源码副本及重复 bulk 日志移到本机
  `build/document-cleanup-20261019/`，共 1,069 个文件、2,705,527 字节，映射及哈希保存在
  `relocation-manifest.json`，避免下一位 agent 当成已实现事实。
  不删除源码、测试、失败证据或其他未提交实验来制造干净状态。
- 2026-10-29 的 v4.0.1 四拓扑正式矩阵见 §1.1：240 次测量、48 次预热全部退出 0，
  计数和 720 份 stdout/stderr/driver log 的哈希已核验。该组仍是共享 4 vCPU 短测，
  MPMC 存在 driver/subscriber0 核共享；尚无独占裸机、长时持续负载、完整 payload、
  1P3S/1P4S 或无核共享 2P2S 的验收。
- 2026-10-19 的 §3–8 历史整理未重新采样 perf、未运行新性能矩阵，只复核并冻结既有测量；
  2026-10-22 claim API v4 的独立正式 A/B 见 §1.3。各版本、日期和拓扑数据不得拼接。
- 整理核验：152 个新证据文件的源/目标哈希一致，144 份 stdout 已重解析、24 个五轮分组
  已重算，报告主表与辅助表对齐；历史已跟踪证据及两处 release note 保持原字节。
  该次整理为文档改动，未重跑 CTest；历史通过记录不作为新的测试结果。
- 后续清理将三个未跟踪实验文件 `tools/aeron_compare/salias_fast_path_compare.cpp`、
  `tools/aeron_compare/verify_fast_path.sh`、`tools/microbench/measure_guard_cost.cpp` 移至本地
  `build/benchmark-cleanup-20261019/drafts/`，按原相对路径保留并记录哈希，工具目录不再暴露骨架。
  fast-path 提示性程序/脚本、guard 微测都不是完整产品 A/B；不能把其输出、注释或
  `SkipRegistrationGuard` 设想当成已实现功能或性能证据。
- 保留所有受维护的压测/诊断/回归工具及 Release、ASan/UBSan、TSan 构建目录；未删除产品或测试。
  四组旧实验归档哈希与单文件恢复抽查通过；Aeron 工具回归 23 项、HFT 汇总回归 36 项全部通过。
  无损归档详情见第 8.2 节；该次整理未提交、打 tag 或运行新性能测试。

## 10. Phase 2 的冻结基线与已知 benchmark 限制

Phase 2 已采纳“接受当前状态、基于 v3.0.0 快照继续”的路线。正式性能 A/B 固定使用
SHA-256 为 `3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da`
的本地冻结客户端，不再把扩展后的诊断 benchmark 当成同期性能基线。

2026-10-20 的 192 B、1P2S、50M 消息复验中，冻结客户端为 **18/20** 高模式、中位数
**16.7988 Mmsg/s**；当前扩展 benchmark 为 **0/20**、中位数 **4.90339 Mmsg/s**。
将 v3.0.0 benchmark 源码链接当前产品库后，客户端仍与冻结 v3 二进制逐字节相同，因此差异
属于 benchmark 编译单元/LTO 布局，不是 claim 产品实现回归。

P2-A 又用同一冻结客户端完成 100 轮复核：高模式 **69/100（69.0%）**，未通过“85% 以上”
推荐门槛；全体中位数 16.4806 Mmsg/s，仅高模式轮中位数 16.6859 Mmsg/s。与历史 192 B
400 轮基线的 288/400（72.0%）相比为 -3.0 个百分点（双侧两比例检验 `p=0.5527`），
差异不显著。因此 192 B 仍是已观测的高吞吐区域，但**不标记为稳定推荐配置或 SLA**。
400 轮基线全体轮次中位数为 16.6187 Mmsg/s、仅高模式轮为 16.8356 Mmsg/s。
后续 P2-C、P2-B 的条件、客户端身份、交错 A/B
和 perf 边界统一见 [Phase 2 执行基线](plans/phase2-execution-baseline.md)；
P2-A 的完整结果与失败样本见
[192 B 100 轮验证报告](plans/phase2-a-192b-validation-report.md)。

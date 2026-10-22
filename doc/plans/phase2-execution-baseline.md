# Phase 2 执行基线：冻结 v3.0.0 Benchmark

**决策日期**：2026-10-20（Asia/Singapore）  
**状态**：已采纳选项 2；P2-A 100 轮验证未通过；P2-C 候选 1/2 均未通过，P2-C 已收口  
**性能基线**：`v3.0.0` / `c5e6aa7df4397df6f47a4e00cf5f72ed5ae778bd`

## 1. 决策

Phase 2 的所有正式吞吐与延迟比较统一使用冻结的 v3.0.0 客户端：

```text
build/benchmarks/baseline-extended-20261019-183822/salias_ipc_compare.uninstrumented
SHA-256 3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da
```

不再要求扩展后的 `build/release/bench/salias_ipc_compare` 同时承担正式性能基线和诊断功能。
需要诊断时另建独立实验工具或独立二进制，并把它视为新工具；不得与冻结基线拼接为同一组 A/B。

采纳理由：

- 冻结客户端在同一主机、同一 runner 与资源门禁下复现 192 B、1P2S 高吞吐
  **18/20**，中位数 **16.7988 Mmsg/s**，范围 7.423–17.491 Mmsg/s。
- 当前 release 客户端在同一条件下为 **0/20**，中位数 **4.90339 Mmsg/s**。
- 将 v3.0.0 benchmark 源码链接当前产品库，生成的客户端仍与冻结 v3 二进制逐字节相同，
  因此当前扩展 benchmark 的 `0/20` 不是 claim 产品实现的性能回归。
- benchmark 增加诊断字段、选项和消费者条件后，LTO 会改变完整可执行文件的布局；只恢复 claim
  循环或调用形状不足以恢复高模式。

## 2. 事实边界

1. 192 B 的高吞吐属于已观测配置，不是所有 payload 或所有拓扑的稳定 SLA。
2. 高模式本身脆弱：已有 400 轮基线为 288/400（72.0%）。全部轮次中位数和仅高模式轮次
   中位数分别为 16.6187 / 16.8356 Mmsg/s；两个数字不可互换。
3. 冻结客户端没有 `--diagnostic` 与 `--publish-path`，Phase 2 不得假设这些参数存在。
4. 产品候选必须链接到与基线相同的 benchmark 源码，并从头生成干净客户端；不能用当前扩展
   客户端与否决旧二进制做跨布局比较。
5. 修改 benchmark 源码会改变二进制身份，必须另立客户端哈希、实验目录和工具版本。

## 3. Phase 2 工作包

| 工作包 | 目标 | 正式验证 |
|---|---|---|
| P2-A 192 B 推荐配置 | 文档化 192 B、1P2S、1 MiB、batch 1、poll-limit 64 的观测能力与限制 | 不新增优化声明；引用冻结基线证据 |
| P2-B 延迟优化 | 降低 attempt p50/p99 与 planned 尾延迟 | v3 源码重建的 baseline/candidate 同组交错 A/B；HFT 门禁 |
| P2-C 高模式进入率 | 若可安全改善，将 72% 基线提高至少 10 个百分点 | 至少 218 轮/臂；400 轮/臂作为当前推荐规模 |

P2-A 已用冻结客户端完成 100 轮复核，但只得到 69/100（69.0%）高模式，未达到 85% 门槛；
不把 16.8 Mmsg/s 写成生产 SLA，也不把 192 B 标记为已通过推荐配置。P2-C 候选 2 已证明
4 MiB ring 相对 1 MiB 无显著提升；候选 1 的 `-falign-functions=64` 在 64 B 上同样为
0/100 高模式，最高仅 8.73222 Mmsg/s，低于基线最大值。P2-C 不再继续尝试布局或编译
参数搜索，后续转入 P2-B 延迟优化或已证明更稳定的拓扑/负载。

## 4. 客户端身份与构建规则

每个候选按下述流程产生唯一的干净客户端：

1. 从固定源码版本取出 `tools/aeron_compare/salias_ipc_compare.cpp`。候选不得修改该文件。
2. 只在产品库或产品头文件中实现候选差异。
3. 使用 Release、GCC 14.2、Ninja、`-O3`、`-DNDEBUG`、`-march=native` 和 LTO 构建。
4. 将 baseline 与 candidate benchmark 源码存档到各自实验目录。
5. 记录产品 commit、dirty 状态、编译器版本、完整命令、客户端 SHA-256 和文件大小。
6. 只有 baseline 与 candidate 的 benchmark 源码一致时，结果才能作为产品 A/B。

若候选确实修改了产品头文件，而 instrumentation 风格转换使 header-line 发生变化，也必须
在实验报告中单列，因为这时它属于产品候选代码与代码布局联合效应，不能宣称纯算法收益。

冻结基线可从 tag 重建验证：

```bash
git show v3.0.0:tools/aeron_compare/salias_ipc_compare.cpp > /tmp/salias-v3-benchmark.cpp
c++ -std=c++23 -O3 -DNDEBUG -flto -march=native \
  -I src/salias/include -I src -I tools/aeron_compare \
  /tmp/salias-v3-benchmark.cpp \
  build/release/src/salias/libsalias.a build/release/src/core/libsalias_core.a \
  -pthread -o /tmp/salias-v3-benchmark
sha256sum /tmp/salias-v3-benchmark
```

在当前 v3.0.0 产品源码上，产物必须为
`3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da`。
若重建产品包含候选改动，哈希会变化；此时哈希用于证明输入身份，不能拿旧哈希冒充候选，
也不能把“哈希不同”本身当作性能收益。

## 5. 吞吐 A/B 协议

### 5.1 固定条件

| 项目 | 固定值 |
|---|---|
| payload / ring | 192 B / 1 MiB |
| 拓扑 | 1 发布者、2 订阅者（Reliable FIFO broadcast） |
| 消息数 | 每组每轮 50M |
| 发布路径 | 冻结客户端内置 claim；不得改用 offer 替代 |
| 批量 / 轮询 | batch 1 / poll-limit 64 |
| CPU | coordinator CPU0；consumer0 CPU1；consumer1 CPU2；publisher CPU3 |
| 资源门禁 | CPUs 0-3、Nice 5、MemoryHigh 640M、MemoryMax 704M、swap 0、TasksMax 48 |
| 高模式门槛 | publish `>=15.000 Mmsg/s` |

runner 命令示例（仅适用于该 Linux/systemd/cgroup-v2 主机）：

```bash
systemd-run --user --wait --pipe --collect \
  -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  --setenv=SALIAS_RESOURCE_CAPPED=1 \
  python3 doc/plans/runners/high-frequency-stability-runner.py \
    --client build/benchmarks/baseline-extended-20261019-183822/salias_ipc_compare.uninstrumented \
    --output build/benchmarks/phase2-baseline \
    --payload 192 --capacity 1048576 --interval-seconds 0
```

候选使用同一命令，只替换 `--client` 和全新 `--output`。

### 5.2 交错采集

正式对比按 `B,C,C,B` 顺序分块执行，避免先跑完一只臂再跑另一只臂。每块使用独立 systemd
unit，完成一块后再启动下一块。基线与候选均采集完整 20 组时，分析器才可给出正式结论。

### 5.3 验收

P2-C 必须同时满足：

- 高模式率相对同条件基线提升至少 **10 个百分点**；
- 双侧合并两比例检验 `p < 0.05`，方向为候选更高；
- 候选高模式中位数 **>=15 Mmsg/s**；
- Fisher 与 group-cluster 敏感性检验方向一致；
- 候选低模式中位数不得比同组基线进一步恶化；
- 所有 20 组、每组 20 轮完整，无 timeout、OOM、内存 high/max 事件。

现有 `compare_high_mode_rates.py` 已实现完整性、比率、Fisher、cluster 与功效检查：

```bash
python3 tools/aeron_compare/compare_high_mode_rates.py \
  build/benchmarks/phase2-baseline \
  build/benchmarks/phase2-candidate \
  --output build/benchmarks/phase2-comparison.json --require-complete
```

## 6. 延迟 A/B 协议

延迟优化使用 `tools/aeron_compare/run_latency_compare.py`，至少分别采集 `attempt` 与
`planned`。条件继续保持 1P1S 时 CPU1 subscriber、CPU2 publisher、CPU3 Aeron driver；
不得把不同拓扑、采样率或计时起点拼进同一张比较表。

正式流程使用结构相同的 baseline/candidate 客户端，交错运行相同轮数。每个实验保存逐轮
stdout/stderr、`results.jsonl`、`summary.json` 与 SHA-256。主验收看 planned-arrival
worst-subscriber 的 p50/p99/p99.9 与吞吐完成度；first-attempt 只作辅助。

不许只报告 p50 改善而隐藏 p99/p99.9 或吞吐退化。长尾必须用 HFT 门禁做独立确认，
短评分位数不能替代长测 SLA。

## 7. perf 与机制证据

当需要解释候选收益或瓶颈时，使用 perf 采集同一客户端、同一负载下的调用栈、cycles、
instructions、cache misses 与上下文切换。perf 结果只用于找机制和提出下一个候选，
不能替代吞吐/延迟 A/B。

注意：

- 共享 VM 缺少完整硬件 PMU 时，不能把推算的 IPC/cache 差异写成硬件事实；
- `perf record` 会改变调度和代码布局，不能把插桩运行直接与正式性能结果相减；
- 采集前后都要记录客户端 SHA-256、负载参数、CPU 分配与 cgroup 状态；
- 若 perf 运行无法复现高模式，只把它作为负面机制证据，不得挑选有利轮次。

## 8. 文档与发布边界

P2-A 可在性能报告和 README 中写：

> 在冻结 v3.0.0、192 B、1P2S、单组双订阅广播、1 MiB ring 条件下，已观测高模式中位数
> 约 16.7 Mmsg/s；400 轮历史基线的高模式率约 72%，本次 100 轮复核为 69.0%，
> 因此不是稳定 SLA，也未通过 85% 推荐门槛。

不得写：

- “Salias 所有 payload 都达到 16.8 Mmsg/s”；
- “192 B 已稳定 85% 以上”；
- 用当前扩展 benchmark 的 0/20 证明产品 claim 回归；
- 用 diagnostic/offer 客户端的 16–17 Mmsg/s 替代零拷贝 claim 结论；
- 在任何新版本上沿用 v3.0.0 的哈希或性能数字而不重新验证。

## 9. 当前证据

| 证据 | 位置 | 说明 |
|---|---|---|
| 冻结客户端 | `build/benchmarks/baseline-extended-20261019-183822/salias_ipc_compare.uninstrumented` | SHA `3f8fc695...`；基线身份 |
| 本次 v3 复跑 | `build/benchmarks/v3-reference-verify-20261020-173837/` | 18/20，中位数 16.7988 Mmsg/s |
| 当前布局复跑 | `build/benchmarks/fixed-claim-verify-20261020-173147/` | 0/20，中位数 4.90339 Mmsg/s |
| 400 轮基线 | `build/benchmarks/high-frequency-p0-20261020-140227/` | 400/400 完整；288/400 高模式；高模式中位数 16.8356 Mmsg/s |
| P2-A 100 轮复核 | `build/benchmarks/p2a-192b-validation-20261020-191535/` | 完整 100 轮；69/100 高模式；全体中位数 16.4806、高模式中位数 16.6859 Mmsg/s；未通过 |
| P2-A 报告 | [phase2-a-192b-validation-report.md](phase2-a-192b-validation-report.md) | 记录启动门禁中止样本、完整性校验与相对历史 192 B 的检验 |
| P2-C 候选 2 报告 | [phase2-c-ring-4mib-report.md](phase2-c-ring-4mib-report.md) | 1 MiB vs 4 MiB、64 B × 100 轮；无显著差异，不推荐扩容 |
| P2-C 候选 1 报告 | [phase2-c-candidate1-align64-report.md](phase2-c-candidate1-align64-report.md) | `-falign-functions=64` × 100 轮；0/100 高模式，候选淘汰 |
| 布局机制报告 | [phase1-publish-path-layout-finding.md](phase1-publish-path-layout-finding.md) | v3 产品路径与 benchmark 布局分离证据 |
| 相对比较规则 | [phase1-relative-improvement-strategy.md](phase1-relative-improvement-strategy.md) | 样本量、交错采集与统计门禁 |

本轮临时试验已归档到
`build/benchmark-cleanup-20261020/codex-phase1-claim-verification-temp-20261020.tar.gz`，
SHA-256 `dd6eab8b7b857630594e28f63b72554da6134363bd576434f34c9eeec545c054`。
归档内容不是正式证据；不得把其中失败的布局试验当作候选结果。

## 10. 下一步顺序

1. P2-A 已判定未通过；不走“192 B 已观测最佳配置”的推荐发布路径。
2. P2-C 候选 2（4 MiB ring）无显著差异；候选 1（函数对齐）0/100 高模式，均淘汰。
3. P2-C 收口；不再尝试其他对齐值或候选 3 的编译器参数搜索，避免布局彩票。
4. 后续转向 P2-B 延迟优化，或选择已证明更稳定的拓扑/负载。
5. 若未来确实要扩展 benchmark，另建 `salias_bench_v2` 或独立诊断工具，不修改冻结基线。

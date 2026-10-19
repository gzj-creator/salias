# Session 3：诊断运行与分析结果

**执行时间**：2026-10-19 19:57（Asia/Singapore）。
**范围**：接续已完成的 Session 1/2；本轮只完成 Session 3，不重跑基线、不修改产品源码。
**执行提交**：`55e301a4995e8014947a6bddf1006c92a29026e8`，含 Session 2 工作区改动。
源码、二进制与 runner 的执行时哈希及快照保存在诊断目录。

## 运行与验收

- 数据：`build/benchmarks/diagnostic-20261019-195710/diagnostic_summary.json`。
- 只运行 Salias，1P2S，64/256 B，每次发布 10,000,000 条、交付 20,000,000 条。
- 同一二进制的 off/counters/timed 三种模式；每个 payload × 模式各一轮预热、五轮正式。
- 共 36 次运行，30 次正式、6 次预热；36 份独立 stdout 和 stderr，无丢失、timeout、OOM 或内存 high/max 事件。
- 未运行 Aeron、未采集延迟样本；ring=1 MiB、batch=1、poll-limit=64。
- CPU0 为协调器，C0=CPU1、C1=CPU2、P=CPU3；Nice=5，MemoryHigh=640M、MemoryMax=704M、swap=0。
- 全部 40 项 Python 回归与 IPC 冒烟通过，其中新增汇总测试 7 项。

## 关键指标

以下为五轮正式运行中位数。背压、空 poll 和批次来自 counters；耗时来自 timed。
两种模式分别保存在 JSON 中，不能将汇总表视为同一次运行的因果观测。
每轮先合并两个消费者的整数计数再计算总体空 poll 率与平均成功批次，避免简单平均不同分母的比率。

| Payload | backpressure_rate | consumer_empty_rate | claim_avg_ns（timed） | avg_batch |
|---|---:|---:|---:|---:|
| 64 B | 0.000330% | 20.4201% | 186.375 ns | 3.2891 |
| 256 B | 0.001290% | 25.0505% | 185.287 ns | 2.7235 |

| Payload / 模式 | 空 poll 率 C0 / C1 | 平均成功批次 C0 / C1 | 发布吞吐中位数（Mmsg/s） |
|---|---:|---:|---:|
| 64 B / off | 未计数 | 未计数 | 5.341 |
| 64 B / counters | 19.0811% / 21.7028% | 3.2360 / 3.1283 | 5.223 |
| 64 B / timed | 22.8539% / 22.7363% | 1.7491 / 1.7226 | 4.346 |
| 256 B / off | 未计数 | 未计数 | 5.034 |
| 256 B / counters | 25.4565% / 23.4953% | 2.6908 / 2.7571 | 5.116 |
| 256 B / timed | 23.9385% / 19.5708% | 1.8236 / 1.9985 | 4.362 |

## 阈值与归因边界

- **H1（背压过频）未获支持**：64 B 背压率远低于 10%；最高单轮仅 0.019426%。不据插桩运行严格排除未插桩基线中的背压。
- **本 session 的 H2（消费跟不上）未获支持**：总体空 poll 率为 20.42%，低于 50%；即使超过阈值，也可能是发布供给不足，并非消费落后的充分证据。
- **H3（Guard 开销）未确认**：64 B claim 均值中位数 186.375 ns，五轮范围 165.999–196.432 ns，均未超过 200 ns。计时覆盖成功 `try_claim`，不是 Guard 专属计时。
- H2 命名有历史冲突：原计划 H2 指“帧步长非对齐”，交接提示词 H2 指“消费跟不上”；帧步长问题留待本轮 Session 4 独立验证。

同轮、同 payload 配对 off 后，吞吐变化的五轮中位数：

| Payload | counters 相对 off | timed 相对 off |
|---|---:|---:|
| 64 B | −4.67% | −19.53% |
| 256 B | +2.17% | −24.95% |

计数模式的配对中位数在 5% 内，但单轮仍有波动；逐条计时明显超过 5% 扰动门禁。
64 B off/counters 吞吐 CV 分别为 8.44%/7.18%，256 B 为 15.54%/12.37%，不能宣称稳定性门禁通过。

**决策**：三个阈值均未命中，不直接批准 Phase 2A/2B/2C 产品改动。
先顺序完成 Session 4，再由 Session 5 综合基线与帧分析选型；若需保留一个发布侧探索候选，
仅将 **Phase 2B 单发布者快速路径**列为有条件的隔离验证，前提是低扰动计时与拓扑对照，不能称作已确认根因。

## 复跑与解析

从仓库根目录运行；resource gate 要求显式 cgroup 限制，不能直接在未限制的 shell 中运行 runner。

```bash
OUT="$PWD/build/benchmarks/diagnostic-$(date +%Y%m%d-%H%M%S)"
systemd-run --user --wait --pipe --collect \
  -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  --setenv=SALIAS_RESOURCE_CAPPED=1 \
  python3 doc/plans/runners/diagnostic-runner.py --output "$OUT"
python3 tools/aeron_compare/parse_diagnostic.py "$OUT"
cat "$OUT/diagnostic_summary.json"
```

结果摘要 SHA-256：`bd75dbff722dda02efa71301c5f0a1e4030cb16c849aa239119f18221756c8e9`。
旧诊断目录与全阶段报告保留，不混入本轮正式样本。本轮未执行 commit、tag 或 push。

# Salias Phase 1 - S.1 系统状态相关性报告

**执行日期**：2026-10-19  
**执行人**：Codex

## 一、结论

本轮没有发现 CPU 或 IRQ 指标能够稳定预测高/低模式。按全部 45 轮统计：

- 高模式（≥10 Mmsg/s，8 轮）平均 CPU1/2/3 非空闲率分别为
  **82.55% / 82.51% / 82.36%**。
- 低模式（<10 Mmsg/s，37 轮）平均 CPU1/2/3 非空闲率分别为
  **96.89% / 96.86% / 96.80%**。
- 高模式平均 IRQ/s 分别为 **985.84 / 973.39 / 974.47**；低模式为
  **1030.46 / 1030.06 / 1022.67**。
- CPU 差异为 **-14.80% / -14.81% / -14.92%**，IRQ 差异为
  **-4.33% / -5.50% / -4.71%**，均未超过 20% 显著性阈值。

高模式轮全部来自 192 B 预加载；30 个 64 B 测量轮和 5 个 128 B 轮均为低模式。
因此上表的 CPU 差值包含 payload 与运行时长差异，不能解释为“低模式因状态切换而
更多 spin”。在相同 192 B 内部，排除两组首个采样不完整的高模式轮后，剩余
6 个高模式与 2 个低模式的 CPU 差异约为 **-3.1%**，IRQ 差异约
**-1.8% 至 +0.1%**，进一步支持“没有明显系统层指标差异”。

## 二、高/低模式数据

| 模式 | 轮次 | CPU1 | CPU2 | CPU3 | IRQ/s CPU1 | IRQ/s CPU2 | IRQ/s CPU3 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 高模式 ≥10 Mmsg/s | 8 | 82.55% | 82.51% | 82.36% | 985.84 | 973.39 | 974.47 |
| 低模式 <10 Mmsg/s | 37 | 96.89% | 96.86% | 96.80% | 1030.46 | 1030.06 | 1022.67 |
| 相对变化 | - | -14.80% | -14.81% | -14.92% | -4.33% | -5.50% | -4.71% |

CPU 非空闲率按 `%usr + %nice + %sys + %irq + %soft + %steal` 计算。runner 在
`Nice=5` 下运行，而忙等负载主要落在 `%nice`，只统计 `%usr` 会严重低估 CPU 使用率。

loadavg(1m)：高模式 2.561，低模式 2.428，差异 +5.48%。load5、load15 的差异分别为
-5.53%、-6.67%，同样不显著。

## 三、同 payload 补充对比

| payload | 高模式轮 | 低模式轮 | CPU1 差异 | CPU2 差异 | CPU3 差异 | IRQ CPU1 差异 | IRQ CPU2 差异 | IRQ CPU3 差异 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 192 B | 8 | 2 | -14.17% | -14.13% | -14.23% | -6.01% | -6.19% | -4.24% |
| 192 B，排除首轮采样不完整 | 6 | 2 | -3.07% | -3.11% | -3.16% | -1.79% | -0.25% | +0.08% |

两组首个 192 B 高模式轮（编号 11、26）与监控采样启动边界重叠，覆盖的样本数少于
常态轮次，因此会造成较高的 CPU 偏差估计。排除边界轮后，差异更接近测量噪声。

## 四、判定

- **CPU 高模式 > 低模式 +10%：否。** 高模式 CPU 反而更低，未触发
  “低模式可能在 yield/sleep，检查背压重试”的分支。
- **IRQ 高模式 < 低模式 -30%：否。** IRQ 差异只有约 -4% 至 -6%，未触发
  “低模式被中断干扰，尝试 IRQ 绑定”的分支。
- **是否显著差异（>20%）：否。** CPU 与 IRQ 均未超过 20% 阈值。

**S.1 判定：无明显 CPU/IRQ 系统状态指标可预测模式；状态切换在更底层，需要 S.2
或更深入诊断。**

## 五、实现与证据

- 监控脚本：`tools/system/capture_runtime_state.sh`
  - 每秒采集 `mpstat`、`pidstat`、`/proc/interrupts`、`/proc/loadavg`。
  - stdout 返回监控 PID；支持 `TERM` 后回收全部采样进程。
- 分析工具：`tools/system/analyze_system_state.py`
  - 读取 `results.jsonl` 的每轮起止单调时钟时间。
  - 对齐采样区间，输出 `system-correlation.json`。
  - 同时给出全轮与 measured-only 两种判定视图；同 payload 对比位于
    `mode_breakdown.same_payload`，需与全轮视图结合解读。
- runner：`doc/plans/runners/warmup-effect-runner.py`
  - 测试开始前启动监控，结束后 `terminate + wait`。
  - 将监控数据保存至 `system-monitor/`。
  - 为每轮记录 `started_at_ns`、`finished_at_ns`、`started_monotonic_ns`、
    `finished_monotonic_ns`。
- 实验数据：`build/benchmarks/warmup-monitored-20261019-225816/`
- 相关性结果：`build/benchmarks/warmup-monitored-20261019-225816/system-correlation.json`
- 校验清单：`build/benchmarks/warmup-monitored-20261019-225816/SHA256SUMS`
  （105 个文件，包含 `system-monitor/` 原始日志）

## 六、下一步

1. S.2 应优先检查 worker 内的状态切换、内存页状态、调度时钟与 CPU 频率/steal，
   而不是继续扩大 CPU/IRQ 采样。
2. 若要验证背压或 sleep 假设，应在 worker 内增加等待原因、失败重试次数与
   yield/sleep 调用计数，而不是仅依赖进程级 CPU 百分比。
3. 若后续做跨主机对照，应保留同一套监控脚本与 cgroup 资源限制，并使用
   `system-correlation.json` 的同 payload 视图作首要比较。

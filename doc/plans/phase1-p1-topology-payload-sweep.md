# Phase 1 - P1: 拓扑与 Payload 扫描计划

**创建日期**：2026-10-19  
**前置依赖**：Phase 1.5 降扰动复测完成  
**目标**：用未插桩基线确认瓶颈位置（发布侧 vs 订阅者扫描 vs payload 特定）

## 一、背景与动机

### 1.1 Phase 1.5 遗留问题

- 64 B off 模式 CV=10.18%，基线不稳定
- 256 B 短测 CV=14.21%，但长测 CV=5.46%（消息数影响？）
- 三个假设均未确认，需更多维度数据

### 1.2 P1 实验策略

**P1.1 拓扑扫描（优先）**：
- **假设**：若 1P1S 64 B 也慢，说明瓶颈在发布侧，不是订阅者扫描
- **测试**：1P1S / 1P2S / 1P3S，只测 64 B 和 256 B
- **消息数**：50M（折中，比 10M 长、比 100M 快）
- **轮次**：10 轮（与 Phase 1.5 一致）

**P1.2 Payload 扫描（次要）**：
- **假设**：若存在"断点"（如 96 B 接近 Aeron），说明帧步长影响
- **测试**：48, 56, 64, 72, 80, 96, 128, 192, 256 B，只测 1P2S
- **消息数**：50M
- **轮次**：5 轮（payload 多，缩短单轮）

## 二、P1.1 拓扑扫描详细设计

### 2.1 测试矩阵

| 拓扑 | Payload | Messages | 轮次 | 预计耗时/轮 | 总轮次 |
|---|---|---|---|---|---|
| 1P1S | 64 B | 50M | 10 | ~8 s | 10 |
| 1P1S | 256 B | 50M | 10 | ~3.5 s | 10 |
| 1P2S | 64 B | 50M | 10 | ~8 s | 10 |
| 1P2S | 256 B | 50M | 10 | ~3.5 s | 10 |
| 1P3S | 64 B | 50M | 10 | ~10 s | 10 |
| 1P3S | 256 B | 50M | 10 | ~4 s | 10 |

共 60 轮，预计 6–8 分钟（含预热）。

### 2.2 关键分析指标

**绝对吞吐**：
- 1P1S 64 B vs 1P2S 64 B：若 1P1S 也 ~6 Mmsg/s，说明不是订阅者扫描
- 1P2S 256 B：对比长测 15.465 Mmsg/s，验证 50M 是否足够稳定

**相对退化**：
- Δ(1P1S → 1P2S) / 1P1S：预期 < 20%（若 > 30%，确认订阅者扫描开销）
- Δ(1P2S → 1P3S) / 1P2S：线性退化则广播路径需优化

**稳定性**：
- 各拓扑 CV：预期 50M 下 CV < 10%（比 10M 更稳定）

### 2.3 Runner 实现要点

```python
# doc/plans/runners/topology-sweep-runner.py

TOPOLOGIES = [
    {"producers": 1, "consumers": 1, "scenario": "fifo"},  # FifoMpsc
    {"producers": 1, "consumers": 2, "scenario": "fifo"},  # FifoMpmc
    {"producers": 1, "consumers": 3, "scenario": "fifo"},  # FifoMpmc
]

PAYLOADS = [64, 256]
MESSAGES = 50_000_000
ROUNDS = 10

# 使用未插桩二进制（baseline-extended 的 salias_ipc_compare.uninstrumented）
# 只测 Salias，不测 Aeron（加快迭代）
```

### 2.4 判定准则

| 观测结果 | 结论 | 下一步 |
|---|---|---|
| 1P1S 64 B ≈ 6 Mmsg/s | 瓶颈在发布侧 | Phase 2B（单发布者优化）或深入发布路径 |
| 1P1S 64 B > 10 Mmsg/s，1P2S 退化 > 30% | 瓶颈在订阅者扫描 | Phase 2A（批量进度）或优化 consumer 游标查找 |
| 1P2S 256 B < 10 Mmsg/s（远低于长测） | 50M 消息数不够稳定 | 重跑 100M 或增加轮次到 20 |
| 1P3S 退化线性 | 广播路径批量不足 | 优化 poll 批量或订阅者并行处理 |

## 三、P1.2 Payload 扫描详细设计

### 3.1 测试矩阵

| Payload (B) | 帧步长 (B) | Messages | 轮次 | 预计耗时/轮 |
|---|---|---|---|---|
| 48 | 56 | 50M | 5 | ~7 s |
| 56 | 64 | 50M | 5 | ~7.5 s |
| 64 | 72 | 50M | 5 | ~8 s |
| 72 | 80 | 50M | 5 | ~8 s |
| 80 | 88 | 50M | 5 | ~8.5 s |
| 96 | 104 | 50M | 5 | ~9 s |
| 128 | 136 | 50M | 5 | ~4 s |
| 192 | 200 | 50M | 5 | ~3.5 s |
| 256 | 264 | 50M | 5 | ~3.5 s |

共 45 轮（9 payload × 5 rounds），预计 5–7 分钟。

### 3.2 分析指标

**断点检测**：
- 若 96 B > 12 Mmsg/s（接近 256 B 的 15 Mmsg/s），说明 64–96 B 之间有特定因素
- 若 48 B < 6 Mmsg/s，说明小 payload 共性问题

**帧步长对齐性**：
| Payload | 帧步长 | 是否 64 B 倍数 | 是否 8 mod 64 |
|---|---|---|---|
| 48 | 56 | 否 | 是 |
| 56 | 64 | 是 | 是 |
| 64 | 72 | 否 | 是 |
| 72 | 80 | 否 | 否（16 mod 64） |
| 80 | 88 | 否 | 否（24 mod 64） |
| 96 | 104 | 否 | 否（40 mod 64） |

**判定**：若 56 B（步长 64，对齐）显著快于 64 B（步长 72，不对齐），支持 H2。

### 3.3 Runner 实现要点

```python
# doc/plans/runners/payload-sweep-runner.py

PAYLOADS = [48, 56, 64, 72, 80, 96, 128, 192, 256]
TOPOLOGY = {"producers": 1, "consumers": 2, "scenario": "fifo"}
MESSAGES = 50_000_000
ROUNDS = 5

# 只测 Salias 1P2S
```

### 3.4 判定准则

| 观测结果 | 结论 | 下一步 |
|---|---|---|
| 48–80 B 均 < 7 Mmsg/s | 小 payload 共性瓶颈 | 排查帧步进、环周转、背压频率 |
| 56 B（步长64）> 64 B（步长72）> 20% | 帧步长对齐影响确认 | Phase 2C（对齐实验） |
| 96 B > 12 Mmsg/s | 64–96 B 断点存在 | 细化扫描 64/68/72/76/80 B |
| 无明显断点，平滑上升 | Payload 线性影响，无特定因素 | 回到 P1.1 拓扑结论 |

## 四、实施顺序与资源

### 4.1 推荐顺序

**Day 1 上午**：P1.1 拓扑扫描
- 创建 `topology-sweep-runner.py`
- 运行 1P1S/1P2S/1P3S × 64/256 B（60 轮，~10 分钟）
- 分析结果，确认是否需要 P1.2

**Day 1 下午**（可选，依赖 P1.1 结果）：P1.2 Payload 扫描
- 若 P1.1 未确认根因，运行 payload 扫描（45 轮，~7 分钟）
- 若 P1.1 已确认发布侧瓶颈，跳过 P1.2，直接进入 Phase 2B

**Day 2**：分析与报告
- 生成吞吐-拓扑/payload 曲线
- 撰写 P1 报告（phase1-p1-sweep-report.md）
- 更新 session-state.json

### 4.2 资源需求

- CPU：继续使用 CPU0-3（与 Phase 1 一致）
- 内存：640M/704M cgroup
- 机器时间：P1.1 + P1.2 约 20 分钟
- 人工时间：0.5–1 天（含脚本编写、运行、分析）

## 五、交付物

### 5.1 Runner 脚本

- `doc/plans/runners/topology-sweep-runner.py`
- `doc/plans/runners/payload-sweep-runner.py`（可选）

### 5.2 分析工具

- `tools/aeron_compare/analyze_topology_sweep.py`：
  - 计算各拓扑相对退化
  - 生成 topology-吞吐曲线（matplotlib）

- `tools/aeron_compare/analyze_payload_sweep.py`：
  - 检测断点
  - 标注帧步长对齐性
  - 生成 payload-吞吐曲线

### 5.3 报告与证据

- `doc/plans/phase1-p1-sweep-report.md`：
  - P1.1/P1.2 结果汇总
  - 判定三个假设
  - 推荐下一步（Phase 2A/2B/2C 或其他方向）

- `build/benchmarks/topology-sweep-{timestamp}/`（本地保留）
- `build/benchmarks/payload-sweep-{timestamp}/`（本地保留）

## 六、验收标准

### P1.1 拓扑扫描

- [ ] 60 轮全部成功，无 timeout/OOM
- [ ] 1P1S/1P2S/1P3S 各有 10 轮数据
- [ ] 输出包含 summary.json、results.jsonl、topology-analysis.json
- [ ] 明确回答："1P1S 64 B 吞吐是多少？与 1P2S 比例？"

### P1.2 Payload 扫描（可选）

- [ ] 45 轮全部成功
- [ ] 9 个 payload 各有 5 轮数据
- [ ] 输出包含 payload-analysis.json、payload-curve.png
- [ ] 明确回答："是否存在断点？56 B vs 64 B 差异？"

### P1 报告

- [ ] < 2 页（排除表格和图表）
- [ ] 包含明确的"下一步推荐"
- [ ] 图表清晰标注误差棒（Q1/Q3 或 std）

## 七、风险与缓解

| 风险 | 概率 | 缓解 |
|---|---|---|
| 50M 消息数仍不够稳定 | 中 | 回退到 100M（P1.1 耗时加倍到 ~20 分钟） |
| 1P3S 需要 CPU4，宿主只有 4 vCPU | 高 | 跳过 1P3S，只测 1P1S/1P2S |
| P1.1/P1.2 均未确认根因 | 中 | 转向其他方向：Aeron 机制对比、工具链统一 |
| 拓扑扫描发现多个瓶颈（发布+订阅者） | 低 | 优先解决主要矛盾（贡献度更大的） |

---

**计划版本**：v1.0  
**最后更新**：2026-10-19

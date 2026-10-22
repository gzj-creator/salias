# P1 拓扑与 Payload 扫描 - Session 提示词

## Session P1.1: 拓扑扫描（优先执行）

### 提示词

```
你接手 Salias Phase 1 - P1.1 拓扑扫描任务。

**前置条件**：
- Phase 1.5 已完成（降扰动复测）
- 64 B counters 计数扰动通过 <5% 门禁，但 CV=7.05% 仅通过放宽的 8%
- 未确认根因，需更多维度数据

**本次任务**：测试 1P1S/1P2S/1P3S 拓扑，确认瓶颈是否在订阅者扫描

1. 阅读计划文档：doc/plans/phase1-p1-topology-payload-sweep.md § 二

2. 创建 runner（doc/plans/runners/topology-sweep-runner.py）：
   - 基于 baseline-extended-runner.py
   - 只测 Salias（不测 Aeron）
   - 拓扑：1P1S / 1P2S / 1P3S
   - Payload：64 / 256 B
   - Messages：50M（折中稳定性与速度）
   - 轮次：每组 10 轮正式 + 1 轮预热
   - 使用未插桩二进制：
     ```python
     SALIAS_CLIENT = "build/benchmarks/baseline-extended-20261019-183822/salias_ipc_compare.uninstrumented"
     ```

3. 关键实现：
   - 1P1S 使用 `--scenario fifo --producers 1 --consumers 1`（自动选 FifoMpsc）
   - 1P2S/1P3S 使用 `--scenario fifo --producers 1 --consumers 2/3`（自动选 FifoMpmc）
   - CPU 分配：
     * 1P1S: publisher=CPU3, consumer0=CPU1, coordinator=CPU0
     * 1P2S: publisher=CPU3, consumer0=CPU1, consumer1=CPU2, coordinator=CPU0
     * 1P3S: **若宿主只有 4 vCPU，跳过此组**（需要 CPU4）
   - 输出到独立目录（不覆盖历史数据）

4. 运行：
   ```bash
   OUT="$PWD/build/benchmarks/topology-sweep-$(date +%Y%m%d-%H%M%S)"
   systemd-run --user --wait --pipe --collect \
     -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
     -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
     --setenv=SALIAS_RESOURCE_CAPPED=1 \
     python3 doc/plans/runners/topology-sweep-runner.py --output "$OUT"
   ```

5. 创建分析工具（tools/aeron_compare/analyze_topology_sweep.py）：
   - 从 results.jsonl 读取所有轮次
   - 计算各拓扑的中位数、Q1/Q3、CV
   - 计算相对退化：
     * Δ(1P1S→1P2S) = (1P2S - 1P1S) / 1P1S
     * Δ(1P2S→1P3S) = (1P3S - 1P2S) / 1P2S
   - 输出 topology-analysis.json 和控制台表格

6. 运行分析：
   ```bash
   python3 tools/aeron_compare/analyze_topology_sweep.py "$OUT"
   ```

**验收标准**：
- 60 轮（或 40 轮，若跳过 1P3S）全部成功
- 向我汇报：
  * 1P1S 64 B 中位数 = ? Mmsg/s（CV = ?）
  * 1P2S 64 B 中位数 = ? Mmsg/s（CV = ?）
  * Δ(1P1S→1P2S) = ?%（是否 > 30%？）
  * 若 1P1S 也 ~6 Mmsg/s → 瓶颈在发布侧
  * 若 1P1S > 10 Mmsg/s 且退化 > 30% → 瓶颈在订阅者扫描

**输出**：
- doc/plans/runners/topology-sweep-runner.py
- tools/aeron_compare/analyze_topology_sweep.py
- build/benchmarks/topology-sweep-{timestamp}/
```

---

## Session P1.2: Payload 扫描（可选，依赖 P1.1 结果）

### 提示词

```
你接手 Salias Phase 1 - P1.2 Payload 扫描任务。

**前置条件**：
- P1.1 拓扑扫描已完成
- 若 P1.1 已确认发布侧瓶颈，可跳过本 session
- 若 P1.1 未确认根因，需扫描 payload 找断点

**本次任务**：测试 9 个 payload（48–256 B），找性能断点

1. 阅读计划文档：doc/plans/phase1-p1-topology-payload-sweep.md § 三

2. 创建 runner（doc/plans/runners/payload-sweep-runner.py）：
   - 基于 topology-sweep-runner.py
   - 只测 1P2S
   - Payload：48, 56, 64, 72, 80, 96, 128, 192, 256 B
   - Messages：50M
   - 轮次：每组 5 轮正式 + 1 轮预热（payload 多，缩短单轮）

3. 运行：
   ```bash
   OUT="$PWD/build/benchmarks/payload-sweep-$(date +%Y%m%d-%H%M%S)"
   systemd-run --user --wait --pipe --collect \
     -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
     -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
     --setenv=SALIAS_RESOURCE_CAPPED=1 \
     python3 doc/plans/runners/payload-sweep-runner.py --output "$OUT"
   ```

4. 创建分析工具（tools/aeron_compare/analyze_payload_sweep.py）：
   - 从 results.jsonl 读取所有轮次
   - 计算每个 payload 的中位数、Q1/Q3、CV
   - 标注帧步长（从 codec.hpp 计算）
   - 检测断点：相邻 payload 吞吐变化 > 50%
   - 标注对齐性：步长是否 64 B 倍数
   - 输出 payload-analysis.json 和控制台表格

5. 运行分析：
   ```bash
   python3 tools/aeron_compare/analyze_payload_sweep.py "$OUT"
   ```

**验收标准**：
- 45 轮全部成功
- 向我汇报：
  * 48/56/64/72/80/96/128/192/256 B 各中位数
  * 是否存在断点（如 96 B >> 64 B）？
  * 56 B（步长64，对齐）vs 64 B（步长72，不对齐）差异？
  * 若 56 B > 64 B > 20% → 支持 H2（帧步长对齐影响）

**输出**：
- doc/plans/runners/payload-sweep-runner.py
- tools/aeron_compare/analyze_payload_sweep.py
- build/benchmarks/payload-sweep-{timestamp}/
```

---

## Session P1.3: P1 报告撰写

### 提示词

```
你接手 Salias Phase 1 - P1 扫描报告撰写任务。

**前置条件**：
- P1.1 拓扑扫描已完成
- P1.2 Payload 扫描已完成（或基于 P1.1 跳过）

**本次任务**：汇总 P1 结果，撰写报告并推荐下一步

1. 收集输入数据：
   - build/benchmarks/topology-sweep-{timestamp}/topology-analysis.json
   - build/benchmarks/payload-sweep-{timestamp}/payload-analysis.json（若有）

2. 撰写报告（doc/plans/phase1-p1-sweep-report.md）：

**模板结构**：
```markdown
# Salias Phase 1 - P1 拓扑与 Payload 扫描报告

**执行日期**：YYYY-MM-DD  
**执行人**：[你的标识]

## 一、执行摘要

[一段话总结：瓶颈位置确认为发布侧/订阅者扫描/帧步长对齐/未确认]

## 二、P1.1 拓扑扫描结果

### 2.1 绝对吞吐

| 拓扑 | 64 B 中位数 (Mmsg/s) | 64 B CV | 256 B 中位数 (Mmsg/s) | 256 B CV |
|---|---|---|---|---|
| 1P1S | [数据] | [%] | [数据] | [%] |
| 1P2S | [数据] | [%] | [数据] | [%] |
| 1P3S | [数据] | [%] | [数据] | [%] |

### 2.2 相对退化

| 过渡 | 64 B 退化 | 256 B 退化 | 判定 |
|---|---|---|---|
| 1P1S → 1P2S | [%] | [%] | [是否 > 30%？] |
| 1P2S → 1P3S | [%] | [%] | [是否线性？] |

**结论**：
- 若 1P1S 64 B ≈ 6 Mmsg/s：瓶颈在发布侧
- 若 1P1S 64 B > 10 Mmsg/s 且退化 > 30%：瓶颈在订阅者扫描

## 三、P1.2 Payload 扫描结果（若有）

### 3.1 吞吐-Payload 曲线

| Payload (B) | 帧步长 (B) | 中位数 (Mmsg/s) | CV | 对齐性 |
|---|---|---|---|---|
| 48 | 56 | [数据] | [%] | 8 mod 64 |
| 56 | 64 | [数据] | [%] | 64 B 倍数 ✓ |
| 64 | 72 | [数据] | [%] | 8 mod 64 |
| ... | ... | ... | ... | ... |

### 3.2 断点检测

- 是否存在断点（相邻吞吐变化 > 50%）？
- 56 B vs 64 B 差异？

**结论**：
- 若 56 B > 64 B > 20%：支持 H2（帧步长对齐）
- 若无断点：排除 payload 特定因素

## 四、假设验证汇总

| 假设 | Phase 1 | Phase 1.5 | P1.1 | P1.2 | 综合判定 |
|---|---|---|---|---|---|
| H1（背压过频） | 未支持 | 未支持 | - | - | **排除** |
| H2（帧步长对齐） | 未确认 | - | - | [支持/排除] | [待定/确认/排除] |
| H3（Guard 冗余） | 未确认 | - | [支持/排除] | - | [待定/确认/排除] |
| 订阅者扫描开销（新） | - | - | [支持/排除] | - | [待定/确认/排除] |

## 五、推荐下一步

基于以上结果，推荐：

**[选项 A/B/C/其他]**

**理由**：
1. [假设 X] 得到确认/排除
2. 预计收益/风险
3. 实施复杂度

**备选方案**（若主方案失败）：
- [方案 Y]

## 六、附录

### 6.1 实验配置
- CPU: 0-3, Nice=5, MemoryHigh/Max=640/704M
- Messages: 50M
- Rounds: 10 (topology) / 5 (payload)

### 6.2 数据位置
- 拓扑：build/benchmarks/topology-sweep-YYYYMMDD/
- Payload：build/benchmarks/payload-sweep-YYYYMMDD/
```

3. 填充所有 [占位符] 为实际数据

4. 更新 session-state.json：
   ```json
   {
     "current_session": "P1.3",
     "current_session_status": "complete",
     "next_session": "Phase 2[A/B/C] or further investigation",
     "p1_topology": "build/benchmarks/topology-sweep-{timestamp}",
     "p1_payload": "build/benchmarks/payload-sweep-{timestamp}",
     "p1_conclusion": "bottleneck in [publisher/subscriber/frame-alignment/unclear]"
   }
   ```

**验收**：
- 报告 < 2 页（排除表格）
- 有明确的"推荐下一步"结论
- 向我汇报：最终推荐进入哪个方向？

**输出**：
- doc/plans/phase1-p1-sweep-report.md
- 更新的 session-state.json
```

---

## 通用注意事项

### CPU 分配检查

在运行前检查宿主 vCPU 数量：kai
```bash
nproc  # 若返回 4，跳过 1P3S；若 ≥5，可测 1P3S
```

### 稳定性策略

若 50M 消息数仍 CV > 10%：
1. 增加到 100M（runner 参数修改）
2. 或增加轮次到 20（更可靠）
3. 保留所有轮次数据，不丢弃"不利"轮次

### 数据完整性

所有 runner 输出：
- summary.json（聚合）
- results.jsonl（逐轮原始数据）
- environment.json（执行环境快照）
- frozen-runner.py（冻结执行脚本）
- SHA256SUMS（完整性校验）

---

**文档版本**：v1.0  
**最后更新**：2026-10-19

# Phase 1 - S.2 Worker 内部状态诊断计划

**创建日期**：2026-10-19  
**前置依赖**：S.1 发现高模式 CPU 使用率更低（82% vs 97%）  
**目标**：定位低模式空转的根因（背压、poll 空转、内存等待）

## 一、S.1 核心发现

### 反直觉的 CPU 使用率

| 模式 | CPU 使用率 | 吞吐 | 解释 |
|---|---|---|---|
| 高模式 | 82% | 17 Mmsg/s | 高效工作，有真实产出 |
| 低模式 | 97% | 6.5 Mmsg/s | 空转/等待，低效产出 |

**关键洞察**：低模式不是"CPU 不够"，而是"在等待某种事件/状态"。

### 候选根因

1. **背压 spin-wait**（最可能）：publisher 频繁 BackPressured，yield 循环空转
2. **poll 空转**：consumer 没有消息可读，但仍在 spin-wait
3. **内存等待**：TLB miss、page fault、NUMA 远端访问
4. **同步原子开销**：CAS 失败重试、acquire/release 内存屏障

## 二、S.2.1 快速验证（插桩数据回顾）

### 目标
用 Phase 1.5 的插桩版本测 192 B，对比高/低模式的背压和 poll 空转。

### 操作

```bash
# 使用 Phase 1.5 的插桩二进制
INSTRUMENTED="build/benchmarks/diagnostic-stable-20261019-205458/salias_ipc_compare.instrumented"

# 创建简化 runner（只测 192 B × 20 轮，counters 模式）
cat > doc/plans/runners/verify-instrumented-192b.py << 'EOF'
#!/usr/bin/env python3
# 基于 diagnostic-runner.py，但只测 192 B × 20 轮

import subprocess
from pathlib import Path

def run_case(output_dir, payload=192, rounds=20):
    cmd = [
        str(INSTRUMENTED),
        "--scenario", "fifo",
        "--producers", "1",
        "--consumers", "2",
        "--payload", str(payload),
        "--messages", "50000000",
        "--cpu-base", "1",
        "--cpu-stride", "1",
        "--name", f"instrumented-{payload}b",
        "--capacity", "1048576",
        "--batch-size", "1",
        "--poll-limit", "64",
    ]
    
    for r in range(rounds):
        # ... 运行并收集 diagnostic_* 输出
        pass

# 主逻辑
EOF

# 运行
OUT="$PWD/build/benchmarks/instrumented-192b-$(date +%Y%m%d-%H%M%S)"
python3 doc/plans/runners/verify-instrumented-192b.py --output "$OUT"

# 解析
python3 tools/aeron_compare/parse_diagnostic.py "$OUT"
```

### 验收

从 `diagnostic_summary.json` 提取：
- 高模式轮（≥15 Mmsg/s）的 `backpressure_rate`, `consumer_empty_rate`
- 低模式轮（<10 Mmsg/s）的同指标

**判定准则**：
| 观测 | 结论 | 下一步 |
|---|---|---|
| 高模式背压率 < 0.001%，低模式 > 1% | 确认背压 spin-wait | S.2.2A（优化背压重试策略） |
| 高模式 poll 空转 < 10%，低模式 > 50% | 确认 poll 空转 | S.2.2B（优化 poll 等待策略） |
| 两者均无显著差异 | 瓶颈不在背压/poll | S.2.3（内存/同步原子诊断） |

---

## 三、S.2.2A 背压路径深入（若 S.2.1 确认）

### 目标
量化背压重试的开销，找出为什么低模式频繁背压。

### 新增计数器

在 `salias_ipc_compare.cpp` 的 DiagnosticCounters 中添加：

```cpp
// Publisher 背压细节
alignas(64) std::uint64_t backpressure_yield_only = 0;  // yield 后立即成功
alignas(64) std::uint64_t backpressure_multi_yield = 0; // 多次 yield 才成功
alignas(64) std::uint64_t backpressure_total_yield_cycles = 0;  // 累计 yield 次数

// Consumer 进度回写频率
alignas(64) std::array<std::uint64_t, kMaxWorkers> progress_flush_count{};
alignas(64) std::array<std::uint64_t, kMaxWorkers> progress_lag_bytes_sum{};  // 累计滞后
```

### 插桩位置

```cpp
// Publisher 主循环
auto result = tx.claim(payload_size);
if (!result) {
  counters->claim_backpressure++;
  std::uint64_t yield_count = 0;
  
  while (!result && yield_count < 10000) {
    std::this_thread::yield();
    yield_count++;
    result = tx.claim(payload_size);
    if (result) break;
  }
  
  counters->backpressure_total_yield_cycles += yield_count;
  if (yield_count == 1) {
    counters->backpressure_yield_only++;
  } else if (yield_count > 1) {
    counters->backpressure_multi_yield++;
  }
}
```

### 分析指标

- **平均 yield 次数** = `backpressure_total_yield_cycles / claim_backpressure`
  - 若高模式 < 5，低模式 > 100 → 低模式陷入长时间重试
- **首次 yield 成功率** = `backpressure_yield_only / claim_backpressure`
  - 若低模式 < 10% → consumer 严重滞后

---

## 四、S.2.2B Poll 空转路径深入（若 S.2.1 确认）

### 目标
量化 poll 空转的开销，检查是否是 consumer 处理速度问题。

### 新增计数器

```cpp
// Consumer poll 细节
alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_consecutive_empty{};  // 连续空转次数
alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_max_consecutive_empty{};  // 最大连续空转
```

### 插桩位置

```cpp
// Consumer 主循环
std::uint64_t consecutive_empty = 0;

while (received < expected) {
  auto poll_result = subscriber.poll(handler, options.poll_limit);
  
  if (poll_result > 0) {
    counters->poll_success[consumer_id]++;
    consecutive_empty = 0;
  } else {
    counters->poll_empty[consumer_id]++;
    consecutive_empty++;
    
    counters->poll_consecutive_empty[consumer_id] += consecutive_empty;
    if (consecutive_empty > counters->poll_max_consecutive_empty[consumer_id]) {
      counters->poll_max_consecutive_empty[consumer_id] = consecutive_empty;
    }
  }
}
```

### 分析指标

- **平均连续空转** = `poll_consecutive_empty / poll_empty`
  - 若低模式 > 100 → consumer 长时间没有消息可读
- **最大连续空转** = `poll_max_consecutive_empty`
  - 若低模式 > 10000 → 存在严重的消息饥饿时刻

---

## 五、S.2.3 内存与同步诊断（若 S.2.1/2.2 无进展）

### 目标
用 perf 捕获底层硬件事件，确认是否是内存子系统瓶颈。

### 操作

```bash
# 在 benchmark 运行期间采集 perf
perf record -e cycles,instructions,cache-misses,LLC-load-misses,dTLB-load-misses,page-faults \
  -a -C 1,2,3 --output="$OUTPUT_DIR/perf.data" -- sleep 30 &

# 运行 benchmark
python3 doc/plans/runners/warmup-effect-runner.py --output "$OUTPUT_DIR"

# 停止 perf
wait

# 分析
perf report -i "$OUTPUT_DIR/perf.data" --stdio > "$OUTPUT_DIR/perf-report.txt"
```

### 分析指标

- **IPC**（instructions per cycle）：
  - 若高模式 > 1.0，低模式 < 0.5 → 低模式被内存停顿
- **cache-misses / instructions**：
  - 若低模式显著更高 → cache 不友好
- **dTLB-load-misses**：
  - 若低模式 > 1% loads → TLB 压力大

---

## 六、实施优先级

### 立即做（今天）

1. **S.2.1**（10–30 分钟）
   - 用插桩版本测 192 B × 20 轮
   - 查看现有的背压率和 poll 空转率
   - **成本极低，可能直接确认根因**

### 根据 S.2.1 结果

2. **若确认背压问题** → S.2.2A（添加背压细节计数器）
3. **若确认 poll 空转** → S.2.2B（添加 poll 细节计数器）
4. **若两者均无** → S.2.3（perf 硬件事件采集）

### 不推荐现在做

- ❌ CPU 隔离（S.3）← S.1 已证明不是简单的调度竞争
- ❌ 后台服务停止（原 S.2）← CPU 已经 97% 使用，不是服务抢占问题
- ❌ Phase 2 产品优化 ← 根因未确认

---

## 七、预期收益

若 S.2 成功确认根因：

### 情况 1：背压是根因
- **收益**：优化背压重试策略（如 adaptive backoff、batch claim）可能 2–3 倍提升
- **实施**：Phase 2D（新候选）— 背压优化

### 情况 2：poll 空转是根因
- **收益**：优化 consumer 批量处理可能 1.5–2 倍提升
- **实施**：Phase 2A（批量进度）的变体

### 情况 3：内存/同步是根因
- **收益**：优化布局、减少 CAS 可能 20–50% 提升
- **实施**：Phase 2C（布局优化）或 Phase 2B（减少同步）

---

**计划版本**：v1.0  
**最后更新**：2026-10-19

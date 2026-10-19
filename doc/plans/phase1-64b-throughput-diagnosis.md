# Salias 64B 广播吞吐诊断计划（Phase 1）

**创建日期**：2026-10-19  
**目标版本**：基于 v3.0.0 (c5e6aa7)  
**预计周期**：1.5–2 天  
**状态**：2026-10-19 已执行 Step 1–3；报告与产物清单完成，稳定性/扰动门禁未通过。

> 执行结果见 [Phase 1 诊断报告](phase1-diagnosis-report.md)、
> [产物清单](phase1-artifacts.md) 和 `session-state.json`。
> 下文保留原始实验设计，非验收通过声明。实测发现：100M 不保证所有引擎 >8 s；
> 高空 poll 率不等于消费落后；完整 try_claim 耗时不等于 Guard 耗时；
> 72/264 B 步长均为 8 mod 64，MagicRing 双映射跨尾不加 padding。
> 诊断实际使用 `--diagnostic off|counters|timed`，保留同轮冻结二进制对照。

## 一、背景与动机

### 1.1 核心问题

当前 v3.0.0 在 1P2S Reliable FIFO 广播场景下：
- **64 B 吞吐存在 6.69 倍差距**：Salias 6.341 Mmsg/s vs Aeron 42.441 Mmsg/s
- **256 B 吞吐基本持平**：Salias 14.962 vs Aeron 15.019 Mmsg/s（差异 0.38%）
- **Salias 的 64 B 反常慢于 256 B**：6.341 vs 14.962 Mmsg/s（2.36 倍）

这违反"小包更轻量"的直觉，是优先解释的异常。

### 1.2 已知边界

- 既有优化（移除 2 KiB 清零）确认有效但只追回 ~20 ns
- 六个后续轮询候选无稳定收益
- 较早 1P1S 64 B 吞吐为 9.315 Mmsg/s，增加订阅者退化 32%
- 本机无硬件 PMU，无 cache-miss/HITM 数据
- 主实验轮次短（0.23–1.64 秒），可能受启动/结束开销影响

### 1.3 三个高优先级假设

| 假设 | 证据线索 | 验证方式 |
|---|---|---|
| **H1: 背压过频** | 64 B 短帧快速填满环，消费来不及释放；轮次时长 1.50–1.64 s vs 256 B 的 0.64–1.09 s | 插桩记录 `claim_backpressure / claim_success` 比例 |
| **H2: 帧步长非对齐** | 报告 5.3 提到 72→128 B padding 退化 140 ns；64 B 可能跨缓存行 | 分析实际帧步长、环周转次数；扫描相邻 payload |
| **H3: 单发布者冗余同步** | 1P 场景无发布者竞争，`ClaimReservationGuard` 每次 seq_cst CAS 可能冗余 | 对比 1P1S (FifoMpsc) vs 1P2S (FifoMpmc) 吞吐 |

## 二、阶段 1 执行计划

### Step 1: 延长轮次基线复现（0.5 天）

**目标**：消除"短测不稳定"疑虑，确认 64 B 差距在长轮次下仍存在。

**操作**：

1. 复制冻结 runner：
   ```bash
   cd /data/disk1/projects/salias
   mkdir -p doc/plans/runners
   cp doc/benchmarks/2026-10-19/spmc-aeron/frozen-runner.py \
      doc/plans/runners/baseline-extended-runner.py
   ```

2. 修改 `baseline-extended-runner.py`：
   ```python
   # Line ~80-90: 修改吞吐消息数
   # 原始：messages=10_000_000
   # 修改为：messages=100_000_000  # 延长到 ~10 秒轮次
   
   # 保持其他参数不变：
   # - CPU0-3, subscriber0=CPU1, subscriber1=CPU2, publisher=CPU3
   # - 1 MiB ring/term, batch=1, poll-limit=64
   # - 五轮正式 + 一轮预热
   ```

3. 运行：
   ```bash
   OUT="$PWD/build/benchmarks/baseline-extended-$(date +%Y%m%d-%H%M%S)"
   systemd-run --user --wait --pipe --collect \
     -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
     -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
     python3 doc/plans/runners/baseline-extended-runner.py --output "$OUT"
   ```

**验收标准**：
- 五轮吞吐标准差 < 5%
- 64 B Salias/Aeron 比例仍在 1:6–1:7 范围
- 轮次时长 > 8 秒（排除启动开销占比过高）

**输出**：
- `$OUT/summary.json`
- `$OUT/environment.json`
- `$OUT/results.jsonl`（72 行，含 Salias/Aeron 交错）

---

### Step 2: 插桩诊断运行（1 天）

**目标**：量化三个假设，识别瓶颈在发布侧（背压/Guard）还是消费侧（poll 空转）。

#### 2.1 修改 Salias 比较器

**文件**：`tools/aeron_compare/salias_ipc_compare.cpp`

**新增结构**（在 `SharedRunState` 后）：
```cpp
/// 诊断计数器：跨进程共享，按 cache line 对齐避免伪共享。
struct alignas(64) DiagnosticCounters {
  // Publisher 计数
  alignas(64) std::uint64_t claim_success = 0;
  alignas(64) std::uint64_t claim_backpressure = 0;
  alignas(64) std::uint64_t claim_yield_cycles = 0;
  alignas(64) std::uint64_t claim_total_ns = 0;  // 累计 claim 耗时
  
  // Consumer 计数（per-consumer 数组）
  alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_empty{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_success{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_total_messages{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> consumer_lag_samples{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> consumer_lag_sum_bytes{};
};
```

**插桩位置 1**：publisher 主循环（约 line 300–400）
```cpp
// 在 while (published < messages) 循环内
auto claim_start = std::chrono::steady_clock::now();
auto result = tx.claim(payload_size);
auto claim_end = std::chrono::steady_clock::now();

if (result) {
  counters->claim_success++;
  counters->claim_total_ns += 
    std::chrono::duration_cast<std::chrono::nanoseconds>(claim_end - claim_start).count();
  // ... 填充 payload、commit ...
} else {
  counters->claim_backpressure++;
  std::uint64_t yield_start = yield_count;
  while (!result && yield_count < yield_start + 1000) {
    std::this_thread::yield();
    yield_count++;
    result = tx.claim(payload_size);
  }
  counters->claim_yield_cycles += (yield_count - yield_start);
}
```

**插桩位置 2**：consumer poll 循环（约 line 500–600）
```cpp
// 在 while (received < expected) 循环内
auto poll_result = subscriber.poll(
  [&](const salias::Message& msg) {
    received++;
    counters->poll_total_messages[consumer_id]++;
    // ... 延迟采样逻辑 ...
  },
  options.poll_limit
);

if (poll_result > 0) {
  counters->poll_success[consumer_id]++;
} else {
  counters->poll_empty[consumer_id]++;
}

// 每 100K 消息采样一次 lag
if (received % 100'000 == 0) {
  // 读取 visible_producer_pos 和 consumer.position
  // 需要访问 subscriber 内部状态，可能需要添加 debug API
  std::uint64_t lag = get_consumer_lag(subscriber, producer_id);
  counters->consumer_lag_samples[consumer_id]++;
  counters->consumer_lag_sum_bytes[consumer_id] += lag;
}
```

**输出逻辑**（在结果 JSON 之后）：
```cpp
// 在 print_results() 函数末尾
std::cout << "diagnostic_claim_success=" << counters->claim_success << '\n';
std::cout << "diagnostic_claim_backpressure=" << counters->claim_backpressure << '\n';
std::cout << "diagnostic_claim_yield_cycles=" << counters->claim_yield_cycles << '\n';
std::cout << "diagnostic_claim_avg_ns=" 
          << (counters->claim_success > 0 
              ? counters->claim_total_ns / counters->claim_success : 0) << '\n';

for (std::uint32_t i = 0; i < num_consumers; ++i) {
  std::cout << "diagnostic_consumer" << i << "_poll_empty=" 
            << counters->poll_empty[i] << '\n';
  std::cout << "diagnostic_consumer" << i << "_poll_success=" 
            << counters->poll_success[i] << '\n';
  std::cout << "diagnostic_consumer" << i << "_avg_batch=" 
            << (counters->poll_success[i] > 0 
                ? static_cast<double>(counters->poll_total_messages[i]) / 
                  counters->poll_success[i] : 0.0) << '\n';
  std::cout << "diagnostic_consumer" << i << "_avg_lag_bytes=" 
            << (counters->consumer_lag_samples[i] > 0 
                ? counters->consumer_lag_sum_bytes[i] / 
                  counters->consumer_lag_samples[i] : 0) << '\n';
}
```

#### 2.2 构建诊断版本

```bash
cd /data/disk1/projects/salias
cmake --build build/release --target salias_ipc_compare
```

#### 2.3 运行诊断（只测 Salias）

创建 `doc/plans/runners/diagnostic-runner.py`：
```python
# 基于 baseline-extended-runner.py，但：
# 1. 只运行 Salias（跳过 Aeron）
# 2. 只测 1P2S 吞吐（跳过延迟）
# 3. payload 64/256 B，messages=10_000_000（快速迭代）
# 4. 保存 stdout 到单独文件以便提取 diagnostic_* 行
```

运行：
```bash
OUT="$PWD/build/benchmarks/diagnostic-$(date +%Y%m%d-%H%M%S)"
python3 doc/plans/runners/diagnostic-runner.py --output "$OUT"
```

#### 2.4 解析诊断结果

创建 `tools/aeron_compare/parse_diagnostic.py`：
```python
#!/usr/bin/env python3
"""从诊断运行的 stdout 提取并汇总 diagnostic_* 指标。"""
import json
import sys
from pathlib import Path

def parse_stdout(path):
    metrics = {}
    with open(path) as f:
        for line in f:
            if line.startswith("diagnostic_"):
                key, value = line.strip().split("=")
                metrics[key] = float(value) if "." in value else int(value)
    return metrics

def main():
    output_dir = Path(sys.argv[1])
    results = []
    
    for stdout_file in sorted(output_dir.glob("*.stdout")):
        metrics = parse_stdout(stdout_file)
        # 从文件名提取 payload
        if "64b" in stdout_file.name:
            metrics["payload"] = 64
        elif "256b" in stdout_file.name:
            metrics["payload"] = 256
        results.append(metrics)
    
    # 计算关键比率
    for r in results:
        total_claims = r["diagnostic_claim_success"] + r["diagnostic_claim_backpressure"]
        r["backpressure_rate"] = r["diagnostic_claim_backpressure"] / total_claims
        
        for i in range(2):  # 假设 2 个 consumer
            key_empty = f"diagnostic_consumer{i}_poll_empty"
            key_success = f"diagnostic_consumer{i}_poll_success"
            if key_empty in r and key_success in r:
                total_polls = r[key_empty] + r[key_success]
                r[f"consumer{i}_empty_rate"] = r[key_empty] / total_polls
    
    with open(output_dir / "diagnostic_summary.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
```

运行：
```bash
python3 tools/aeron_compare/parse_diagnostic.py "$OUT"
cat "$OUT/diagnostic_summary.json"
```

**验收标准**：

针对 64 B：
- 若 `backpressure_rate > 0.1`（10%），确认 **H1（背压过频）**
- 若 `consumer{0,1}_empty_rate > 0.5`，说明消费跟不上
- 若 `claim_avg_ns` 显著高于预期（如 > 200 ns），检查是否 Guard 开销

针对 256 B：
- 作为对照基线，预期 `backpressure_rate < 0.01`

---

### Step 3: 帧步长与环利用率分析（0.5 天）

**目标**：确认 64 B 帧步长是否导致环快速周转或缓存行非对齐。

#### 3.1 提取帧步进公式

```bash
# 定位帧编码逻辑
rg "frame_stride|aligned_size|padding" src/core/frame/codec.hpp -A 5
```

预期找到类似：
```cpp
constexpr std::size_t frame_stride(std::size_t payload_size) {
  // 帧头 + payload + 对齐
  return align_to(kHeaderSize + payload_size, kAlignment);
}
```

#### 3.2 计算关键参数

创建 `tools/aeron_compare/analyze_frame_layout.py`：
```python
#!/usr/bin/env python3
"""分析帧步长、环容量、周转次数。"""

# 从源码或实测提取
HEADER_SIZE = 8  # 字节，需从 codec.hpp 确认
ALIGNMENT = 8    # 字节，需从 codec.hpp 确认
RING_CAPACITY = 1 * 1024 * 1024  # 1 MiB

def frame_stride(payload):
    raw = HEADER_SIZE + payload
    return (raw + ALIGNMENT - 1) // ALIGNMENT * ALIGNMENT

def analyze(payload, messages, duration_s):
    stride = frame_stride(payload)
    max_inflight_frames = RING_CAPACITY // stride
    total_bytes = messages * stride
    ring_wraps = total_bytes / RING_CAPACITY
    avg_frame_interval_ns = duration_s * 1e9 / messages
    
    print(f"Payload: {payload}B")
    print(f"  Frame stride: {stride}B")
    print(f"  Max in-flight frames: {max_inflight_frames}")
    print(f"  Total bytes for {messages} msgs: {total_bytes / 1e6:.1f} MiB")
    print(f"  Ring wraps: {ring_wraps:.1f}")
    print(f"  Avg frame interval: {avg_frame_interval_ns:.1f} ns")
    print(f"  Frames per cache line (64B): {64 / stride:.2f}")
    print()

# 从 baseline-extended 实测时长
analyze(64, 100_000_000, 15.8)   # 假设 64 B 约 15.8 秒
analyze(256, 100_000_000, 6.7)   # 假设 256 B 约 6.7 秒
```

#### 3.3 验收标准

- 确认 64 B 实际帧步长（预期 72 B）
- 若 72 B 不是 64 B 倍数，标记"可能跨缓存行"
- 计算 64 B 环周转次数是否显著高于 256 B

---

## 三、判定准则与下一步

### 3.1 诊断结果判定

| 指标 | 阈值 | 结论 | 推荐下一步 |
|---|---|---|---|
| `backpressure_rate` (64B) | > 0.1 | H1 确认：背压过频 | 进入 Phase 2A：批量延迟进度回写 |
| `consumer_empty_rate` (64B) | > 0.5 | 消费跟不上 | 检查 poll 批量、订阅者扫描开销 |
| `claim_avg_ns` (64B) | > 200 | 可能 Guard 开销 | 进入 Phase 2B：单发布者快速路径 |
| 64B frame_stride | 非 64B 倍数 | H2 线索：缓存行对齐 | 进入 Phase 2C：帧步长对齐实验 |
| 以上均不满足 | - | 瓶颈在其他位置 | 扩展扫描：1P1S/1P3S、相邻 payload |

### 3.2 Phase 2 候选（待 Phase 1 确认后选 1–2 个实施）

- **Phase 2A**：批量延迟进度回写（针对 H1）
- **Phase 2B**：单发布者快速路径（针对 H3）
- **Phase 2C**：帧步长缓存行对齐（针对 H2）

---

## 四、交付物清单

Phase 1 完成后提交以下文件到 Git：

1. **Runner 脚本**：
   - `doc/plans/runners/baseline-extended-runner.py`
   - `doc/plans/runners/diagnostic-runner.py`

2. **分析工具**：
   - `tools/aeron_compare/parse_diagnostic.py`
   - `tools/aeron_compare/analyze_frame_layout.py`

3. **结果归档**（不入库，但保留本地）：
   - `build/benchmarks/baseline-extended-YYYYMMDD/`
   - `build/benchmarks/diagnostic-YYYYMMDD/`

4. **Phase 1 报告**：
   - `doc/plans/phase1-diagnosis-report.md`：
     - 基线是否复现（延长轮次后差距）
     - 三个假设的量化结果
     - 推荐进入哪个 Phase 2 候选
     - 一页以内，包含关键表格和决策依据

---

## 五、不可牺牲的契约（提醒）

插桩代码只用于诊断，不合并到产品：
- 不修改 `src/` 下的产品代码
- 所有 `DiagnosticCounters` 只在 `tools/aeron_compare/` 内
- Phase 2 实施优化时，必须移除插桩后重新测量收益

保留的语义：
- 最慢订阅者背压（Reliable 模式）
- 注册代际互斥（`registration_gate`）
- payload 借用期（`lease_start/lease_end`）
- 批量安全前缀（poll 只返回已提交连续前缀）

---

## 六、预计时间线

| 任务 | 预计耗时 | 负责人 | 依赖 |
|---|---|---|---|
| Step 1: 延长轮次基线 | 0.5 天 | 实施者 | - |
| Step 2: 插桩诊断 | 1 天 | 实施者 | Step 1 |
| Step 3: 帧步长分析 | 0.5 天 | 实施者 | - |
| 撰写 Phase 1 报告 | 0.5 天 | 实施者 | Step 1–3 |
| **总计** | **2.5 天** | | |

实际执行时，Step 1 和 Step 3 可并行；Step 2 依赖 Step 1 的环境配置。

---

## 七、风险与缓解

| 风险 | 缓解措施 |
|---|---|
| 插桩影响性能，诊断结果不可信 | 只测吞吐（无延迟采样），计数器按 cache line 对齐；对比插桩前后吞吐退化 < 5% |
| 延长轮次后差距消失 | 说明短测不稳定；需在报告中明确说明，调整后续实验设计 |
| 无明确瓶颈（三个假设均不满足阈值） | 进入扩展诊断：1P1S vs 1P2S、相邻 payload 扫描、Aeron 背压机制对比 |
| 共享 VM 干扰导致结果波动 | 多轮重复（至少 5 轮），记录 min–max 范围，标准差作为置信度指标 |

---

**计划版本**：v1.0  
**最后更新**：2026-10-19

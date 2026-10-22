# Instrumentation Level 设计文档

## 一、双层控制模型

### 1.1 编译时级别（INSTRUMENTATION_LEVEL）

控制**代码是否编译进去**：

```cpp
#ifndef INSTRUMENTATION_LEVEL
#define INSTRUMENTATION_LEVEL 0
#endif

// 编译时常量
constexpr bool kEnableInstrumentation = (INSTRUMENTATION_LEVEL > 0);
constexpr bool kEnableBackpressureCount = (INSTRUMENTATION_LEVEL >= 2);
constexpr bool kEnablePollDetail = (INSTRUMENTATION_LEVEL >= 3);
constexpr bool kEnableTiming = (INSTRUMENTATION_LEVEL >= 4);
```

### 1.2 运行时模式（--diagnostic off/counters/timed）

控制**哪个模板被实例化**：

- `off`: 不使用 Diagnostic 模板
- `counters`: 使用 `Diagnostic<true, false>` 模板
- `timed`: 使用 `Diagnostic<true, true>` 模板

### 1.3 交互规则

| 级别 | 编译时常量 | runner 传参 | 实际效果 |
|---|---|---|---|
| L0 | `kEnableInstrumentation = false` | `--diagnostic counters` | 热路径零开销，输出全零 |
| L1 | `kEnableInstrumentation = true` | `--diagnostic counters` | 只计数成功，无分支 |
| L2 | `kEnableBackpressureCount = true` | `--diagnostic counters` | 计数 + 背压分支 |
| L3 | `kEnablePollDetail = true` | `--diagnostic counters` | 完整计数器 |
| L4 | `kEnableTiming = true` | `--diagnostic timed` | 完整计数 + 计时 |

## 二、代码修改

### 2.1 salias_ipc_compare.cpp 顶部

```cpp
// ============================================================================
// Instrumentation Level Control (S.2.2 Minimal Instrumentation Matrix)
// ============================================================================

#ifndef INSTRUMENTATION_LEVEL
#define INSTRUMENTATION_LEVEL 0
#endif

// Compile-time feature flags
namespace instrumentation {
constexpr int kLevel = INSTRUMENTATION_LEVEL;
constexpr bool kEnabled = (kLevel > 0);
constexpr bool kCountSuccess = (kLevel >= 1);
constexpr bool kCountBackpressure = (kLevel >= 2);
constexpr bool kCountPollDetail = (kLevel >= 3);
constexpr bool kEnableTiming = (kLevel >= 4);
}  // namespace instrumentation
```

### 2.2 DiagnosticCounters 结构体

```cpp
struct alignas(64) DiagnosticCounters {
  // L1: Success counters (no branching)
#if INSTRUMENTATION_LEVEL >= 1
  alignas(64) std::uint64_t claim_success = 0;
  alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_success{};
  alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_total_messages{};
#endif

  // L2: Backpressure (adds branching)
#if INSTRUMENTATION_LEVEL >= 2
  alignas(64) std::uint64_t claim_backpressure = 0;
  alignas(64) std::uint64_t claim_yield_cycles = 0;
#endif

  // L3: Poll detail
#if INSTRUMENTATION_LEVEL >= 3
  alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_empty{};
#endif

  // L4: Timing (expensive)
#if INSTRUMENTATION_LEVEL >= 4
  alignas(64) std::uint64_t claim_total_ns = 0;
  alignas(64) std::uint64_t claim_timing_samples = 0;
#endif
};
```

### 2.3 Publisher 热路径插桩

```cpp
// Publisher main loop
while (published < messages) {
  auto result = tx.claim(payload_size);
  
#if INSTRUMENTATION_LEVEL >= 4
  // L4: Timing (sampled every 1000 successful claims)
  if constexpr (instrumentation::kEnableTiming) {
    if (state->diagnostic.claim_success % 1000 == 0) {
      auto claim_start = std::chrono::steady_clock::now();
      result = tx.claim(payload_size);
      auto claim_end = std::chrono::steady_clock::now();
      
      state->diagnostic.claim_total_ns += 
        std::chrono::duration_cast<std::chrono::nanoseconds>(
          claim_end - claim_start).count();
      state->diagnostic.claim_timing_samples++;
    }
  }
#endif

  if (result) {
#if INSTRUMENTATION_LEVEL >= 1
    // L1: Count success (unconditional, no branching)
    if constexpr (instrumentation::kCountSuccess) {
      state->diagnostic.claim_success++;
    }
#endif
    
    // ... fill payload, commit ...
    published++;
    
  } else {
#if INSTRUMENTATION_LEVEL >= 2
    // L2: Count backpressure (adds branching)
    if constexpr (instrumentation::kCountBackpressure) {
      state->diagnostic.claim_backpressure++;
      
      std::uint64_t yield_count = 0;
      while (!result && yield_count < 10000) {
        std::this_thread::yield();
        yield_count++;
        result = tx.claim(payload_size);
      }
      state->diagnostic.claim_yield_cycles += yield_count;
    }
#else
    // Fallback: simple yield without counting
    std::this_thread::yield();
    result = tx.claim(payload_size);
#endif
  }
}
```

### 2.4 Consumer 热路径插桩

```cpp
// Consumer main loop
while (received < expected) {
  auto poll_result = subscriber.poll(
    [&](const salias::Message& msg) {
      received++;
#if INSTRUMENTATION_LEVEL >= 1
      if constexpr (instrumentation::kCountSuccess) {
        state->diagnostic.poll_total_messages[consumer_id]++;
      }
#endif
      // ... process message ...
    },
    options.poll_limit
  );
  
#if INSTRUMENTATION_LEVEL >= 1
  if constexpr (instrumentation::kCountSuccess) {
    if (poll_result > 0) {
      state->diagnostic.poll_success[consumer_id]++;
    }
  }
#endif

#if INSTRUMENTATION_LEVEL >= 3
  if constexpr (instrumentation::kCountPollDetail) {
    if (poll_result == 0) {
      state->diagnostic.poll_empty[consumer_id]++;
    }
  }
#endif
}
```

### 2.5 L0 特殊处理：完全禁用模板

在 `run_case_typed()` 函数中：

```cpp
template <typename... Args>
void run_case_typed(const Options& options, Args&&... args) {
  // L0: 完全禁用 Diagnostic 模板，即使 runtime 传了 --diagnostic
  if constexpr (!instrumentation::kEnabled) {
    // Force off mode for L0
    if (options.diagnostic != "off") {
      std::cerr << "Warning: INSTRUMENTATION_LEVEL=0, forcing diagnostic to off\n";
    }
    // Call with Diagnostic=false
    run_benchmark<false, false>(options, std::forward<Args>(args)...);
    return;
  }
  
  // L1-L4: 根据 runtime 参数选择模板
  if (options.diagnostic == "timed") {
    run_benchmark<true, true>(options, std::forward<Args>(args)...);
  } else if (options.diagnostic == "counters") {
    run_benchmark<true, false>(options, std::forward<Args>(args)...);
  } else {
    run_benchmark<false, false>(options, std::forward<Args>(args)...);
  }
}
```

### 2.6 输出逻辑（条件编译）

```cpp
void print_diagnostic_output(const DiagnosticCounters* counters, 
                             std::uint32_t num_consumers) {
#if INSTRUMENTATION_LEVEL >= 1
  std::cout << "diagnostic_claim_success=" << counters->claim_success << '\n';
  
  for (std::uint32_t i = 0; i < num_consumers; ++i) {
    std::cout << "diagnostic_consumer" << i << "_poll_success="
              << counters->poll_success[i] << '\n';
    std::cout << "diagnostic_consumer" << i << "_poll_total_messages="
              << counters->poll_total_messages[i] << '\n';
  }
#endif

#if INSTRUMENTATION_LEVEL >= 2
  std::cout << "diagnostic_claim_backpressure=" << counters->claim_backpressure << '\n';
  std::cout << "diagnostic_claim_yield_cycles=" << counters->claim_yield_cycles << '\n';
#endif

#if INSTRUMENTATION_LEVEL >= 3
  for (std::uint32_t i = 0; i < num_consumers; ++i) {
    std::cout << "diagnostic_consumer" << i << "_poll_empty="
              << counters->poll_empty[i] << '\n';
    
    std::uint64_t total_polls = counters->poll_success[i] + counters->poll_empty[i];
    double avg_batch = total_polls > 0 
      ? static_cast<double>(counters->poll_total_messages[i]) / counters->poll_success[i]
      : 0.0;
    std::cout << "diagnostic_consumer" << i << "_avg_batch=" << avg_batch << '\n';
  }
#endif

#if INSTRUMENTATION_LEVEL >= 4
  std::cout << "diagnostic_claim_avg_ns=" 
            << (counters->claim_timing_samples > 0 
                ? counters->claim_total_ns / counters->claim_timing_samples : 0) << '\n';
  std::cout << "diagnostic_claim_timing_samples=" << counters->claim_timing_samples << '\n';
#endif

#if INSTRUMENTATION_LEVEL == 0
  // L0: 输出全零占位符（保持格式兼容）
  std::cout << "diagnostic_claim_success=0\n";
  std::cout << "diagnostic_claim_backpressure=0\n";
  for (std::uint32_t i = 0; i < num_consumers; ++i) {
    std::cout << "diagnostic_consumer" << i << "_poll_success=0\n";
    std::cout << "diagnostic_consumer" << i << "_poll_empty=0\n";
  }
#endif
}
```

## 三、Runner 修改

### 3.1 verify-instrumented-192b.py 新增参数

```python
def infer_instrumentation_level(client_path: str) -> int:
    """从二进制文件名推断级别，如 salias_ipc_compare_L2 → 2"""
    import re
    match = re.search(r'_L(\d+)(?:\.|$)', client_path)
    return int(match.group(1)) if match else 0

def get_diagnostic_mode(level: int) -> str:
    """根据级别决定运行时模式"""
    if level == 0:
        return "off"  # L0 强制 off（虽然会被代码覆盖）
    elif level == 4:
        return "timed"
    else:
        return "counters"

# 主函数
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", required=True)
    parser.add_argument("--instrumentation-level", type=int, default=None,
                       help="Override level (0-4), default infers from filename")
    parser.add_argument("--output", required=True)
    parser.add_argument("--rounds", type=int, default=20)
    args = parser.parse_args()
    
    # 推断或使用指定级别
    level = args.instrumentation_level
    if level is None:
        level = infer_instrumentation_level(args.client)
        print(f"Inferred instrumentation level: {level}")
    
    diagnostic_mode = get_diagnostic_mode(level)
    print(f"Using diagnostic mode: {diagnostic_mode}")
    
    # 构建 benchmark 命令
    cmd = [
        args.client,
        "--scenario", "fifo",
        "--producers", "1",
        "--consumers", "2",
        "--payload", "192",
        "--messages", "50000000",
        # ... 其他参数 ...
    ]
    
    # 只在 L1-L4 添加 --diagnostic
    if level > 0:
        cmd.extend(["--diagnostic", diagnostic_mode])
    
    # 运行并收集结果...
```

### 3.2 验证逻辑调整

```python
def validate_diagnostic_output(level: int, stdout: str, 
                               messages: int, consumers: int):
    """根据级别验证输出"""
    lines = {line.split('=')[0]: int(line.split('=')[1]) 
             for line in stdout.split('\n') if line.startswith('diagnostic_')}
    
    if level == 0:
        # L0: 全零或不存在
        for key, val in lines.items():
            assert val == 0, f"L0 should have zero counters, got {key}={val}"
        return
    
    if level >= 1:
        # L1+: 验证 claim_success
        assert lines['diagnostic_claim_success'] == messages, \
            f"claim_success mismatch"
        
        # 验证 poll_total_messages 总和
        total_delivered = sum(
            lines.get(f'diagnostic_consumer{i}_poll_total_messages', 0)
            for i in range(consumers)
        )
        assert total_delivered == messages * consumers, \
            f"poll_total_messages mismatch"
    
    if level >= 2:
        # L2+: 验证背压计数存在（可以为 0）
        assert 'diagnostic_claim_backpressure' in lines
    
    if level >= 3:
        # L3+: 验证 poll_empty 存在
        for i in range(consumers):
            assert f'diagnostic_consumer{i}_poll_empty' in lines
    
    if level >= 4:
        # L4: 验证计时样本数
        assert 'diagnostic_claim_timing_samples' in lines
        assert lines['diagnostic_claim_timing_samples'] > 0
```

## 四、构建脚本

### 4.1 build-instrumentation-levels.sh

```bash
#!/bin/bash
# Build all 5 instrumentation levels

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

OUTPUT_DIR="$PROJECT_ROOT/build/instrumentation-levels"
mkdir -p "$OUTPUT_DIR"

for level in 0 1 2 3 4; do
  echo "=== Building INSTRUMENTATION_LEVEL=$level ==="
  
  BUILD_DIR="$PROJECT_ROOT/build/release-L$level"
  
  cmake -S "$PROJECT_ROOT" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-DINSTRUMENTATION_LEVEL=$level"
  
  cmake --build "$BUILD_DIR" --target salias_ipc_compare --parallel
  
  # 复制并重命名
  cp "$BUILD_DIR/bench/salias_ipc_compare" \
     "$OUTPUT_DIR/salias_ipc_compare_L$level"
  
  # 记录构建信息
  echo "INSTRUMENTATION_LEVEL=$level" > "$OUTPUT_DIR/salias_ipc_compare_L$level.build-info"
  "$OUTPUT_DIR/salias_ipc_compare_L$level" --help 2>&1 | head -5 >> \
    "$OUTPUT_DIR/salias_ipc_compare_L$level.build-info"
done

echo "=== All levels built ==="
ls -lh "$OUTPUT_DIR"/salias_ipc_compare_L*
```

## 五、测试矩阵脚本

### 5.1 run-instrumentation-matrix.sh

```bash
#!/bin/bash
# Run all 5 levels and compare

set -e

LEVELS_DIR="build/instrumentation-levels"
OUTPUT_BASE="build/benchmarks"

for level in 0 1 2 3 4; do
  CLIENT="$LEVELS_DIR/salias_ipc_compare_L$level"
  OUTPUT="$OUTPUT_BASE/instrumentation-L${level}-$(date +%Y%m%d-%H%M%S)"
  
  echo "=== Testing L$level ==="
  
  python3 doc/plans/runners/verify-instrumented-192b.py \
    --client "$CLIENT" \
    --output "$OUTPUT" \
    --rounds 20
  
  echo "L$level completed: $OUTPUT"
done

echo "=== Comparing all levels ==="
python3 tools/aeron_compare/compare_instrumentation_levels.py \
  "$OUTPUT_BASE"/instrumentation-L*-202610*
```

## 六、验收标准

### 6.1 编译验证

```bash
# 所有 5 个级别成功编译
ls -lh build/instrumentation-levels/salias_ipc_compare_L*
# 应输出 5 个文件

# 检查二进制差异（L0 应该最小）
size build/instrumentation-levels/salias_ipc_compare_L*
```

### 6.2 运行验证

| 级别 | 预期高模式轮数 | 验证点 |
|---|---|---|
| L0 | 15–17/20 | 输出全零，热路径无开销 |
| L1 | ?/20 | claim_success = messages |
| L2 | ?/20 | backpressure 字段存在 |
| L3 | ?/20 | poll_empty 字段存在 |
| L4 | 0–2/20 | claim_timing_samples > 0 |

### 6.3 关键判定

- 若 L0 → L1 高模式显著降低 → 任何额外指令都破坏状态
- 若 L1 → L2 高模式显著降低 → 分支预测是关键
- 若 L2 → L3 高模式显著降低 → poll 路径内存访问破坏 cache
- 若 L3 → L4 高模式显著降低 → 计时开销（已知）

---

**文档版本**：v1.0  
**最后更新**：2026-10-20

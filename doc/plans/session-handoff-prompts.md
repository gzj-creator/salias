# Session 切换提示词

本文档包含 Phase 1 诊断计划的完整实施提示词，用于启动新 session。

---

## Session 1: 延长轮次基线复现

### 提示词

```
你接手 Salias 性能优化项目的 Phase 1 诊断任务。

**当前状态**：
- 仓库：/data/disk1/projects/salias
- 基准版本：v3.0.0 (c5e6aa7)
- 计划文档：doc/plans/phase1-64b-throughput-diagnosis.md

**本次任务**：执行 Phase 1 Step 1 — 延长轮次基线复现

1. 阅读计划文档的 Step 1 部分（约 40 行）
2. 复制冻结 runner：
   - 源：doc/benchmarks/2026-10-19/spmc-aeron/frozen-runner.py
   - 目标：doc/plans/runners/baseline-extended-runner.py
3. 修改吞吐消息数从 10M → 100M（在 runner 脚本约 line 80-90）
4. 保持其他参数不变（CPU 分配、ring 容量、batch size 等）
5. 运行基线测试（使用 systemd-run 命令，见计划文档）
6. 验证结果：
   - 五轮吞吐标准差 < 5%
   - 64 B Salias/Aeron 比例仍在 1:6–1:7
   - 轮次时长 > 8 秒
7. 输出到 build/benchmarks/baseline-extended-{timestamp}/

**验收**：
- runner 脚本已创建并修改正确
- 测试成功运行，无 OOM/timeout
- summary.json 中 64 B 差距仍存在
- 向我汇报：五轮吞吐中位数、标准差、轮次时长范围

**限制**：
- 不修改产品源码（src/ 目录）
- 不提交 Git，不打 tag
- 保留完整 stdout/jsonl 用于后续分析

开始执行 Step 1。
```

---

## Session 2: 插桩诊断实施（Part A - 代码修改）

### 提示词

```
你接手 Salias Phase 1 Step 2 诊断插桩任务的代码修改部分。

**前置条件**（由 Session 1 完成）：
- baseline-extended 已运行，64 B 差距确认仍存在

**本次任务**：修改 Salias 比较器添加诊断计数器

1. 阅读计划文档 doc/plans/phase1-64b-throughput-diagnosis.md 的 Step 2.1 节
2. 编辑 tools/aeron_compare/salias_ipc_compare.cpp：
   
   **新增结构**（在 SharedRunState 之后，约 line 110）：
   ```cpp
   /// 诊断计数器：跨进程共享，按 cache line 对齐避免伪共享。
   struct alignas(64) DiagnosticCounters {
     // Publisher 计数
     alignas(64) std::uint64_t claim_success = 0;
     alignas(64) std::uint64_t claim_backpressure = 0;
     alignas(64) std::uint64_t claim_yield_cycles = 0;
     alignas(64) std::uint64_t claim_total_ns = 0;
     
     // Consumer 计数（per-consumer 数组）
     alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_empty{};
     alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_success{};
     alignas(64) std::array<std::uint64_t, kMaxWorkers> poll_total_messages{};
     alignas(64) std::array<std::uint64_t, kMaxWorkers> consumer_lag_samples{};
     alignas(64) std::array<std::uint64_t, kMaxWorkers> consumer_lag_sum_bytes{};
   };
   ```

   **修改 SharedRunState**（约 line 88-108）：
   - 在 mmap 分配时增加 DiagnosticCounters 大小
   - 在 SharedRunState 末尾添加：`DiagnosticCounters diagnostic{};`

   **插桩位置 1** — Publisher 循环（约 line 300-400，搜索 "while (published < messages)"）：
   ```cpp
   auto claim_start = std::chrono::steady_clock::now();
   auto result = tx.claim(payload_size);
   auto claim_end = std::chrono::steady_clock::now();
   
   if (result) {
     shared_state->diagnostic.claim_success++;
     shared_state->diagnostic.claim_total_ns += 
       std::chrono::duration_cast<std::chrono::nanoseconds>(claim_end - claim_start).count();
     // ... 原有 payload 填充、commit 逻辑 ...
   } else {
     shared_state->diagnostic.claim_backpressure++;
     std::uint64_t yield_start = yield_count;
     while (!result && yield_count < yield_start + 1000) {
       std::this_thread::yield();
       yield_count++;
       result = tx.claim(payload_size);
     }
     shared_state->diagnostic.claim_yield_cycles += (yield_count - yield_start);
   }
   ```

   **插桩位置 2** — Consumer 循环（约 line 500-600，搜索 "subscriber.poll"）：
   ```cpp
   auto poll_result = subscriber.poll(
     [&](const salias::Message& msg) {
       received++;
       shared_state->diagnostic.poll_total_messages[consumer_id]++;
       // ... 原有延迟采样逻辑 ...
     },
     options.poll_limit
   );
   
   if (poll_result > 0) {
     shared_state->diagnostic.poll_success[consumer_id]++;
   } else {
     shared_state->diagnostic.poll_empty[consumer_id]++;
   }
   ```

   **输出逻辑**（在 print_results 函数末尾，约 line 800+）：
   ```cpp
   // 在现有输出后追加
   std::cout << "diagnostic_claim_success=" << state->diagnostic.claim_success << '\n';
   std::cout << "diagnostic_claim_backpressure=" << state->diagnostic.claim_backpressure << '\n';
   std::cout << "diagnostic_claim_yield_cycles=" << state->diagnostic.claim_yield_cycles << '\n';
   std::cout << "diagnostic_claim_avg_ns=" 
             << (state->diagnostic.claim_success > 0 
                 ? state->diagnostic.claim_total_ns / state->diagnostic.claim_success : 0) << '\n';
   
   for (std::uint32_t i = 0; i < options.consumers; ++i) {
     std::cout << "diagnostic_consumer" << i << "_poll_empty=" 
               << state->diagnostic.poll_empty[i] << '\n';
     std::cout << "diagnostic_consumer" << i << "_poll_success=" 
               << state->diagnostic.poll_success[i] << '\n';
     std::cout << "diagnostic_consumer" << i << "_avg_batch=" 
               << (state->diagnostic.poll_success[i] > 0 
                   ? static_cast<double>(state->diagnostic.poll_total_messages[i]) / 
                     state->diagnostic.poll_success[i] : 0.0) << '\n';
   }
   ```

3. 编译验证：
   ```bash
   cmake --build build/release --target salias_ipc_compare
   ```

4. 快速冒烟测试（1K 消息，确认不崩溃）：
   ```bash
   build/release/bench/salias_ipc_compare --scenario fifo --name test-diagnostic \
     --messages 1000 --producers 1 --consumers 2 --payload 64
   ```

**验收**：
- 编译通过，无警告
- 冒烟测试输出包含所有 diagnostic_* 行
- 向我汇报：插桩是否完成、冒烟测试结果

**注意**：
- consumer lag 采样需要访问内部状态，暂时跳过（可选特性）
- 如果 SharedRunState 已经在 mmap 中，确保 DiagnosticCounters 也映射为共享内存
```

---

## Session 3: 插桩诊断实施（Part B - 运行与分析）

### 提示词

```
你接手 Salias Phase 1 Step 2 诊断插桩任务的运行与分析部分。

**前置条件**（由 Session 2 完成）：
- salias_ipc_compare 已添加诊断计数器并编译通过

**本次任务**：运行诊断并解析结果

1. 创建诊断 runner（doc/plans/runners/diagnostic-runner.py）：
   - 基于 baseline-extended-runner.py
   - 只运行 Salias（跳过 Aeron 对比）
   - 只测 1P2S 吞吐（跳过延迟测试）
   - payload: 64/256 B
   - messages: 10_000_000（快速迭代）
   - 保存每个 case 的 stdout 到单独文件

2. 运行诊断：
   ```bash
   OUT="$PWD/build/benchmarks/diagnostic-$(date +%Y%m%d-%H%M%S)"
   python3 doc/plans/runners/diagnostic-runner.py --output "$OUT"
   ```

3. 创建解析工具（tools/aeron_compare/parse_diagnostic.py）：
   - 从 stdout 提取所有 diagnostic_* 行
   - 计算关键比率：
     * backpressure_rate = claim_backpressure / (claim_success + claim_backpressure)
     * consumer_empty_rate = poll_empty / (poll_empty + poll_success)
   - 输出 diagnostic_summary.json

4. 运行解析：
   ```bash
   python3 tools/aeron_compare/parse_diagnostic.py "$OUT"
   cat "$OUT/diagnostic_summary.json"
   ```

5. 判定假设（基于计划文档 Section 3.1 判定准则）：
   - H1（背压过频）：64 B 的 backpressure_rate > 0.1
   - H2（消费跟不上）：64 B 的 consumer_empty_rate > 0.5
   - H3（Guard 开销）：64 B 的 claim_avg_ns > 200

**验收**：
- 诊断运行成功，64/256 B 各有 5 轮数据
- diagnostic_summary.json 包含所有关键指标
- 向我汇报：
  * 64 B: backpressure_rate, consumer_empty_rate, claim_avg_ns, avg_batch
  * 256 B: 同上（对照基线）
  * 哪个假设得到确认？推荐进入哪个 Phase 2 候选？

**输出**：
- doc/plans/runners/diagnostic-runner.py
- tools/aeron_compare/parse_diagnostic.py
- build/benchmarks/diagnostic-{timestamp}/diagnostic_summary.json
```

---

## Session 4: 帧步长分析

### 提示词

```
你接手 Salias Phase 1 Step 3 帧步长分析任务。

**本次任务**：确认 64 B 帧步长是否导致环快速周转或缓存行非对齐

1. 提取帧步进公式：
   - 读取 src/core/frame/codec.hpp
   - 搜索 frame_stride, aligned_size, padding 相关函数
   - 确认帧头大小（HEADER_SIZE）、对齐要求（ALIGNMENT）
   - 计算 64 B 和 256 B 的实际帧步长

2. 创建分析工具（tools/aeron_compare/analyze_frame_layout.py）：
   ```python
   #!/usr/bin/env python3
   """分析帧步长、环容量、周转次数。"""
   
   HEADER_SIZE = ?  # 从 codec.hpp 提取
   ALIGNMENT = ?    # 从 codec.hpp 提取
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
       print(f"  Ring wraps: {ring_wraps:.1f}")
       print(f"  Frames per cache line (64B): {64 / stride:.2f}")
       print(f"  Cache line alignment: {'ALIGNED' if stride % 64 == 0 else 'NOT ALIGNED'}")
       print()
   
   # 使用 baseline-extended 的实测时长
   # 从 build/benchmarks/baseline-extended-*/summary.json 读取
   analyze(64, 100_000_000, duration_64b)
   analyze(256, 100_000_000, duration_256b)
   ```

3. 运行分析：
   ```bash
   python3 tools/aeron_compare/analyze_frame_layout.py
   ```

4. 关键检查点：
   - 64 B 帧步长是否为 64 B 的倍数？
   - 64 B 环周转次数是否显著高于 256 B（> 2 倍）？
   - 每个缓存行能容纳多少帧？

**验收**：
- 确认 64 B 和 256 B 的实际帧步长
- 向我汇报：
  * 64 B frame_stride = ?
  * 256 B frame_stride = ?
  * 64 B 是否缓存行对齐？
  * 64 B 环周转次数 vs 256 B
  * 是否支持 H2 假设（帧步长非对齐）？

**输出**：
- tools/aeron_compare/analyze_frame_layout.py
- 控制台分析结果（可截图或粘贴）
```

---

## Session 5: Phase 1 报告撰写

### 提示词

```
你接手 Salias Phase 1 诊断报告撰写任务。

**前置条件**（由 Session 1-4 完成）：
- 延长轮次基线已运行
- 插桩诊断已完成并解析
- 帧步长分析已完成

**本次任务**：汇总 Phase 1 结果，撰写诊断报告

1. 收集输入数据：
   - build/benchmarks/baseline-extended-*/summary.json
   - build/benchmarks/diagnostic-*/diagnostic_summary.json
   - tools/aeron_compare/analyze_frame_layout.py 输出

2. 撰写报告（doc/plans/phase1-diagnosis-report.md）：

**模板结构**：
```markdown
# Salias 64B 广播吞吐 Phase 1 诊断报告

**执行日期**：YYYY-MM-DD  
**版本**：v3.0.0 (c5e6aa7)  
**执行人**：[你的标识]

## 一、执行摘要

[一段话总结：三个假设哪个确认，推荐进入哪个 Phase 2 候选]

## 二、基线复现结果

### 2.1 延长轮次测试

| Payload | Salias (Mmsg/s) | Aeron (Mmsg/s) | 比例 | 轮次时长 (s) |
|---|---|---|---|---|
| 64 B | [中位数] ± [std] | [中位数] ± [std] | 1:[比例] | [min-max] |
| 256 B | [中位数] ± [std] | [中位数] ± [std] | 1:[比例] | [min-max] |

**结论**：64 B 差距在延长轮次后 [仍存在/有变化]。

## 三、插桩诊断结果

### 3.1 关键指标（64 B）

| 指标 | 数值 | 阈值 | 判定 |
|---|---|---|---|
| backpressure_rate | [%] | 10% | [Pass/Fail] |
| consumer0_empty_rate | [%] | 50% | [Pass/Fail] |
| consumer1_empty_rate | [%] | 50% | [Pass/Fail] |
| claim_avg_ns | [ns] | 200 ns | [Pass/Fail] |
| avg_batch (consumer0) | [条/次] | - | - |

### 3.2 对照基线（256 B）

[同样的表格]

### 3.3 假设验证

- **H1（背压过频）**：[确认/排除]，依据：...
- **H2（消费跟不上）**：[确认/排除]，依据：...
- **H3（Guard 开销）**：[确认/排除]，依据：...

## 四、帧步长分析

| Payload | Frame Stride | 缓存行对齐 | 环周转次数 | 每缓存行帧数 |
|---|---|---|---|---|
| 64 B | [B] | [Yes/No] | [次] | [个] |
| 256 B | [B] | [Yes/No] | [次] | [个] |

**H2（帧步长非对齐）**：[确认/排除]，依据：...

## 五、推荐下一步

基于以上结果，推荐进入 **Phase 2[A/B/C]：[名称]**

**理由**：
1. [假设 X] 得到确认，数据显示...
2. 预计收益：[具体机制]
3. 风险：[语义/性能风险]

**备选方案**（若主方案失败）：
- Phase 2[Y]：[名称]，条件：...

## 六、附录

### 6.1 实验配置
- CPU: CPU0-3, subscriber0=CPU1, subscriber1=CPU2, publisher=CPU3
- Ring: 1 MiB, batch=1, poll-limit=64
- Messages: 100M (吞吐), 10M (诊断)

### 6.2 数据位置
- 基线：build/benchmarks/baseline-extended-YYYYMMDD/
- 诊断：build/benchmarks/diagnostic-YYYYMMDD/
```

3. 填充所有 [占位符] 为实际数据

**验收**：
- 报告长度 < 2 页（排除表格）
- 有明确的"推荐下一步"结论
- 数据引用准确，有文件路径
- 向我汇报：最终推荐进入哪个 Phase 2 候选？

**输出**：
- doc/plans/phase1-diagnosis-report.md
```

---

## Session 6: Phase 1 成果提交

### 提示词

```
你接手 Salias Phase 1 诊断成果提交任务。

**前置条件**（由 Session 1-5 完成）：
- 所有脚本、工具、报告已创建
- Phase 1 诊断报告已撰写

**本次任务**：整理并提交 Phase 1 交付物

1. 检查待提交文件：
   ```bash
   git status
   git add -n doc/plans/  # 预览将添加的文件
   ```

   应包含：
   - doc/plans/phase1-64b-throughput-diagnosis.md（计划）
   - doc/plans/phase1-diagnosis-report.md（报告）
   - doc/plans/session-handoff-prompts.md（本文档）
   - doc/plans/runners/baseline-extended-runner.py
   - doc/plans/runners/diagnostic-runner.py
   - tools/aeron_compare/parse_diagnostic.py
   - tools/aeron_compare/analyze_frame_layout.py

2. 检查不应提交的文件：
   - 不提交 build/benchmarks/ 下的实验结果
   - 不提交产品源码修改（插桩只在 tools/）
   - 不提交二进制

3. 提交：
   ```bash
   git add doc/plans/ tools/aeron_compare/parse_diagnostic.py \
           tools/aeron_compare/analyze_frame_layout.py
   git commit -m "docs: 添加 Phase 1 64B 吞吐诊断计划与工具

- 计划文档：phase1-64b-throughput-diagnosis.md
- 执行报告：phase1-diagnosis-report.md
- Session 切换提示词：session-handoff-prompts.md
- Runner 脚本：baseline-extended, diagnostic
- 分析工具：parse_diagnostic.py, analyze_frame_layout.py

Phase 1 确认 [假设名称]，推荐进入 Phase 2[X]。

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
   ```

4. 创建归档清单（doc/plans/phase1-artifacts.md）：
   ```markdown
   # Phase 1 诊断产物清单

   ## 已入库
   - 计划与报告：doc/plans/phase1-*.md
   - 工具脚本：doc/plans/runners/, tools/aeron_compare/
   
   ## 本地保留（未入库）
   - 基线数据：build/benchmarks/baseline-extended-YYYYMMDD/
     * summary.json: SHA256=[哈希]
     * environment.json: SHA256=[哈希]
   - 诊断数据：build/benchmarks/diagnostic-YYYYMMDD/
     * diagnostic_summary.json: SHA256=[哈希]
   
   ## 可复现性
   - 复跑基线：`python3 doc/plans/runners/baseline-extended-runner.py`
   - 复跑诊断：`python3 doc/plans/runners/diagnostic-runner.py`
   ```

**验收**：
- commit message 遵循约定式提交格式
- 所有脚本有执行权限（chmod +x）
- 向我确认：是否已提交？commit SHA？

**输出**：
- Git commit（1 个）
- doc/plans/phase1-artifacts.md（产物清单）
```

---

## 通用注意事项

### 跨 Session 数据传递

如果需要在 session 之间传递数据：
1. 使用 JSON 文件（放在 doc/plans/session-state.json）
2. 在 session 结束时写入，下个 session 开始时读取
3. 包含：完成的步骤、关键数据路径、待办事项

### 错误恢复

如果某个 session 失败：
1. 检查 git status，撤销未完成的修改
2. 阅读计划文档，确认当前应处于哪一步
3. 重新启动对应的 session 提示词
4. 在新 session 开头说明："接续失败的 Session X，从 Step Y 重新开始"

### 时间估算

| Session | 预计耗时 | 可并行 |
|---|---|---|
| Session 1 | 1–2 小时 | 可与 Session 4 并行 |
| Session 2 | 2–3 小时 | - |
| Session 3 | 1–2 小时 | 依赖 Session 2 |
| Session 4 | 0.5–1 小时 | 可与 Session 1 并行 |
| Session 5 | 1–1.5 小时 | 依赖 Session 1-4 |
| Session 6 | 0.5 小时 | 依赖 Session 5 |

**总计**：6–10 小时（实际机器运行时间 + 人工干预）

---

**文档版本**：v1.0  
**最后更新**：2026-10-19

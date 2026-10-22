# Phase 1 - P1 系统状态稳定性与高模式触发条件

**创建日期**：2026-10-20  
**前置依赖**：P0 确认高模式可重复（15/20），但脆弱且不可预测  
**目标**：在 20 组 × 20 轮（400 轮总）中找出进入高模式的条件

## 一、背景与动机

### 1.1 P0 发现

- 高模式稳定性：进入后中位数 16.844 Mmsg/s，范围 15.341–17.178 Mmsg/s，极度稳定
- 但进入概率随机：15/20 轮进入，5 轮随机分散低吞吐
- 不是预热问题：低吞吐轮分散在 5、7、10、14、17 位置，非集中在前几轮

### 1.2 S.2.2 发现

- L0→L1 的 42.2% 性能跳变（6.131 → 3.543 Mmsg/s）
- 所有级别都难以进入高模式（0/20）
- 说明**微架构状态极度敏感**

### 1.3 理论假设

高模式可能由以下条件组合触发：

| 条件 | 机制 | 如何测试 |
|---|---|---|
| CPU 频率 | 高频率 (3+ GHz) 时更容易进入 | 前一轮冷却 vs 热启动 |
| cache 预热 | 热启动后 ring/data 已在 L3 中 | 连续运行 vs 间隔冷却 |
| TLB 状态 | 大页或 TLB 预热后更稳定 | huge page vs normal page |
| 内存页对齐 | 某种对齐模式触发高吞吐 | 重复运行相同地址 |
| 分支预测器 | BTB 预热状态 | 反复同一代码路径 |

---

## 二、P1.1 高频重复测试

### 2.1 实验设计

**目标**：20 组独立运行，每组 20 轮，共 400 轮

```python
# doc/plans/runners/high-frequency-stability-runner.py

def main():
    output_base = Path(args.output) / "high-frequency"
    
    for group_idx in range(20):
        output_dir = output_base / f"group-{group_idx:02d}"
        
        # 每组测试间隔
        if group_idx > 0:
            # 前 5 组：无间隔（连续热启动）
            # 6-10 组：1 秒间隔（轻度冷却）
            # 11-15 组：10 秒间隔（中度冷却）
            # 16-20 组：60 秒间隔（重度冷却）
            
            if group_idx <= 5:
                sleep_time = 0
            elif group_idx <= 10:
                sleep_time = 1
            elif group_idx <= 15:
                sleep_time = 10
            else:
                sleep_time = 60
            
            time.sleep(sleep_time)
        
        # 运行 20 轮
        run_benchmark(
            client=args.client,
            output_dir=output_dir,
            rounds=20
        )
    
    # 分析 400 轮数据
    analyze_high_frequency_data(output_base)
```

### 2.2 关键分析指标

#### 指标 1：高模式率随间隔变化

```python
def analyze_high_mode_by_interval(data):
    """计算各间隔下的高模式进入率"""
    
    intervals = {
        "no_sleep (0s)": (0, 5),      # group 0-5
        "light_sleep (1s)": (6, 10),   # group 6-10
        "medium_sleep (10s)": (11, 15), # group 11-15
        "heavy_sleep (60s)": (16, 20)   # group 16-20
    }
    
    results = {}
    for interval_name, (start, end) in intervals.items():
        high_count = 0
        total_rounds = 0
        
        for group_idx in range(start, end + 1):
            group_data = data[group_idx]
            high = sum(1 for r in group_data['rounds'] if r['publish_mmsg_s'] >= 15)
            high_count += high
            total_rounds += len(group_data['rounds'])
        
        results[interval_name] = {
            'high_mode_rate': high_count / total_rounds,
            'high_count': high_count,
            'total': total_rounds
        }
    
    return results
```

**预期**：
- 若热启动优于冷启动 → cache 预热是关键
- 若无差异 → 状态切换是随机的，不是调度/频率

#### 指标 2：轮内位置偏差

```python
def analyze_round_position_bias(data):
    """分析高/低吞吐是否集中在特定位置"""
    
    high_positions = []
    low_positions = []
    
    for group_data in data.values():
        for round_idx, round_data in enumerate(group_data['rounds']):
            if round_data['publish_mmsg_s'] >= 15:
                high_positions.append(round_idx)
            else:
                low_positions.append(round_idx)
    
    # 计算位置分布
    high_hist = histogram(high_positions, bins=4)  # 5 bin 分组
    low_hist = histogram(low_positions, bins=4)
    
    return {
        'high_position_distribution': high_hist,
        'low_position_distribution': low_hist,
        'high_skew': chi_square(high_hist, uniform()),  # 检验是否均匀分布
        'low_skew': chi_square(low_hist, uniform())
    }
```

**预期**：
- 若高吞吐集中在前几轮 → 预热问题
- 若均匀分布 → 随机状态切换

#### 指标 3：连续性分析

```python
def analyze_state_transitions(data):
    """分析高/低模式的持续长度"""
    
    all_states = []
    for group_data in data.values():
        for round_data in group_data['rounds']:
            state = 'high' if round_data['publish_mmsg_s'] >= 15 else 'low'
            all_states.append(state)
    
    # 找连续段
    runs = []
    current_state = all_states[0]
    current_length = 1
    
    for state in all_states[1:]:
        if state == current_state:
            current_length += 1
        else:
            runs.append((current_state, current_length))
            current_state = state
            current_length = 1
    
    runs.append((current_state, current_length))
    
    high_runs = [r for r in runs if r[0] == 'high']
    low_runs = [r for r in runs if r[0] == 'low']
    
    return {
        'avg_high_run_length': mean([r[1] for r in high_runs]) if high_runs else 0,
        'avg_low_run_length': mean([r[1] for r in low_runs]) if low_runs else 0,
        'total_runs': len(runs),
        'run_transitions': len(runs) - 1
    }
```

**预期**：
- 若高模式容易"粘滞"（长连续段）→ cache/BTB 预热维持效果
- 若高频切换 → 随机干扰（IRQ、调度）

---

## 三、P1.2 控制变量实验

### 3.1 CPU 频率固定（若 P1.1 发现频率相关）

```bash
# 禁用 CPU 频率调节（需要 root）
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# 验证频率
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq

# 在固定频率下重测
python3 doc/plans/runners/high-frequency-stability-runner.py \
  --client build/instrumentation-levels/salias_ipc_compare_L0 \
  --output build/benchmarks/fixed-freq-$(date +%Y%m%d)

# 恢复动态调节
echo powersave | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

### 3.2 CPU 隔离（若 P1.1 发现调度竞争）

```bash
# 编辑 /etc/default/grub，添加 isolcpus=1,2,3
# 重启后，CPU1-3 无任何系统进程

# 在隔离核上重测
taskset -c 1,2,3 \
  python3 doc/plans/runners/high-frequency-stability-runner.py \
    --client build/instrumentation-levels/salias_ipc_compare_L0 \
    --output build/benchmarks/isolated-cpu-$(date +%Y%m%d)
```

### 3.3 TLB/Cache 预热（连续长测）

```bash
# 100 轮连续运行（不间隔），检查是否最终稳定进入高模式
python3 doc/plans/runners/verify-instrumented-192b.py \
  --client build/instrumentation-levels/salias_ipc_compare_L0 \
  --output build/benchmarks/continuous-100rounds-$(date +%Y%m%d) \
  --rounds 100
```

---

## 四、输出与分析

### 4.1 新增分析工具

**tools/aeron_compare/analyze_high_frequency_stability.py**：

```python
def main():
    # 读取 20 组 × 20 轮数据
    data = load_all_groups(args.input_dir)
    
    # 1. 高模式率随间隔变化
    interval_analysis = analyze_high_mode_by_interval(data)
    print_interval_analysis(interval_analysis)
    
    # 2. 轮内位置偏差
    position_analysis = analyze_round_position_bias(data)
    print_position_analysis(position_analysis)
    
    # 3. 连续性分析
    transition_analysis = analyze_state_transitions(data)
    print_transition_analysis(transition_analysis)
    
    # 4. 输出完整统计
    output_json = {
        'total_rounds': 400,
        'high_mode_rate': sum(r['high_count'] for r in interval_analysis.values()) / 400,
        'interval_analysis': interval_analysis,
        'position_analysis': position_analysis,
        'transition_analysis': transition_analysis
    }
    
    save_json(args.output, output_json)
```

### 4.2 决策表

基于 P1.1 的结果：

| 观测 | 判定 | 下一步 |
|---|---|---|
| 热启动 > 冷启动 20% | cache 预热是关键 | P1.2A：连续长测，验证 cache 预热稳定性 |
| 无间隔 > 60s 冷却 30% | CPU 频率/调度是关键 | P1.2B：固定频率或 CPU 隔离实验 |
| 高模式集中在前 5 轮 | 初始化问题 | 检查 ring/channel 初始化代码 |
| 高模式随机分散 | 外部干扰（IRQ、页面） | 接受不可控性，转向相对比较 |
| 高模式粘滞（长连续段） | BTB 预热效应 | 研究代码对齐优化 |

---

## 五、预期收益与局限

### 如果发现强烈模式

- **预热依赖**：可以设计"启动预热序列"，确保性能稳定
- **频率依赖**：可以与功耗管理平衡，或固定频率在性能关键场景
- **调度依赖**：可以通过 CPU 隔离在生产环境中保证

### 如果无明确模式

- 高模式是**不可控的微架构效应**
- 应转向**相对比较**（优化前后）而非绝对值
- 目标改为"让高模式进入率从 75% → 90%"而非"为什么是 75%"

---

## 六、实施成本与时间

| 阶段 | 内容 | 机器时间 | 实际时间 |
|---|---|---|---|
| P1.1 | 20 组 × 20 轮 | ~3 小时 | 3–4 小时 |
| P1.2A | 100 轮连续 | ~30 分钟 | 1 小时 |
| P1.2B（可选） | 固定频率 | ~2 小时 | 2–3 小时 |
| P1.2C（可选） | CPU 隔离 | ~3 小时 | 需要重启 |

**推荐**：先做 P1.1（确定是否有模式），然后根据结果选择 P1.2A 或 P1.2B。

---

**计划版本**：v1.0  
**最后更新**：2026-10-20

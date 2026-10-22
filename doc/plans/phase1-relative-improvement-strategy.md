# Salias Phase 1 - 相对改进验收策略

**创建日期**：2026-10-20  
**状态**：A 策略工具已完成；等待真实候选做交错 A/B 验证

## 一、结论先行

后续不应再以“绝对达到并保持 17 Mmsg/s”作为唯一门槛。现有证据表明，高模式
（发布吞吐 `>= 15 Mmsg/s`）本身较窄，真实问题是进入高模式的概率及其时间稳定性。
因此验收改为相对比较：候选相对同期基线的高模式率提升至少 **10 个百分点**，且统计显著、
高模式中位数不退化，并保留低模式中位数作为恶化检查。

同时，原提案中的 P0 候选“把 192 B 作为默认”不能作为相对改进：P1.1A 的 400 轮
基线已经是 192 B、50M 消息、1 MiB ring，高模式率仍为 288/400（72.0%）。把同一条件
改名为“优化”不会提高该比例。后续候选必须来自发布/消费热路径、ring 或调度等可观测变化。

## 二、统计口径修正

原提案公式只使用双侧两比例检验，并固定使用 baseline 的方差。两组独立样本应使用合并比例：

```text
p = (x_baseline + x_candidate) / (n_baseline + n_candidate)
se = sqrt(p * (1 - p) * (1 / n_baseline + 1 / n_candidate))
z = (p_candidate - p_baseline) / se
```

工具同时输出 Wilson/Newcombe 率差区间、单侧 Fisher 精确检验，以及按 20 轮组为单位的
cluster permutation 检验。原因是单轮之间存在状态粘滞，直接把 400 轮视作 400 个独立
伯努利样本会低估不确定性；cluster 检验只把“组”视作独立单位，作为敏感性检查。

原计划每组 100 轮对 `72% -> 82%` 并不具备足够功效。主验收是“候选优于基线”的单侧方向，
按单侧 `alpha=0.05`、80% 功效估算，每组至少需 **218 轮**；若采用更保守的双侧口径，
每组约需 **277 轮**。实际观测 100 对 100 时，合并双侧 z 检验 `p=0.0929`，不能验收。

## 三、实验设计

正式 A/B 必须固定客户端身份、payload、消息数、ring、CPU 绑定、资源上限和高模式阈值。
基线使用 P0 原始未插桩快照
`baseline-extended-20261019-183822/salias_ipc_compare.uninstrumented`（SHA-256
`3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da`），候选使用
单独构建并记录 SHA-256。不要用不同 L 级别、不同 payload 或不同 ring 的结果拼成一对。

推荐采用交错采集，而不是把基线跑完再跑候选：

1. 每 20 轮为一组，基线与候选按 `B,C,C,B` 或随机顺序交替。
   现有 runner 一次只接受一个客户端，因此基线跑 `0-10`、候选跑 `0-10`、基线跑
   `11-19`、候选跑 `11-19`，每段严格相同轮数；两边最终仍然是完整的 20 组。
2. 每组使用新的 systemd unit；每轮正常创建和销毁 channel/worker。
3. 功效必须结合预期目标率计算。若期望 `72% -> 82%`，单侧 80% 功效约需 218 轮/臂，
   即至少 11 组；100 轮的检验功效不足。更保守口径约需 277 轮/臂，即 14 组。
   建议运行到 20 组/臂（400 轮），与现有基线规模一致，并为集群敏感性检查留出余量。
4. 先运行 `--end-group` 分块，保留逐轮 stdout/stderr、`summary.json` 和 SHA-256；
   任一组不完整时不得进行最终验收。
5. 记录系统负载、CPU 频率/亲和性、内存水位和客户端哈希；不能用不同日期的单次 100 轮
   结果代替同期对照。

## 四、验收门槛

候选必须同时满足：

- 高模式率提升 **>= 10 pp**；
- 双侧合并两比例 z 检验 `p < 0.05`；候选率必须更高；
- 候选高模式中位数 **>= 15 Mmsg/s**；
- 基线、候选的 20 轮组数完整，且实验条件与环境阈值一致；
- 敏感性检查中 Fisher 和 cluster 检验方向一致；若两者明显冲突，应先扩大样本而不是宣布通过。

低模式不要求绝对高于某个固定值，但候选低模式中位数不得比基线进一步恶化；工具会保留该项供报告判断。

## 五、执行命令

高模式稳定性 runner 已支持 payload 与 capacity 参数，默认仍为历史上的 192 B / 1 MiB：

```bash
systemd-run --user --wait --pipe --collect \
  -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  --setenv=SALIAS_RESOURCE_CAPPED=1 \
  python3 doc/plans/runners/high-frequency-stability-runner.py \
    --client build/benchmarks/baseline-extended-20261019-183822/salias_ipc_compare.uninstrumented \
    --output build/benchmarks/relative-baseline \
    --payload 192 --capacity 1048576 --interval-seconds 0
```

候选使用同一命令，只替换 `--client` 与 `--output`。`--interval-seconds 0` 关闭历史
间隔矩阵，避免组号位置与等待时长混淆；正式 A/B 仍应按上文交错顺序启动两个 runner。
两组完成后比较：

```bash
python3 tools/aeron_compare/compare_high_mode_rates.py \
  build/benchmarks/relative-baseline \
  build/benchmarks/relative-candidate \
  --output build/benchmarks/relative-comparison.json
```

对部分实验只能做诊断比较，必须显式加 `--allow-incomplete`，且永远不会判为通过。
CI 或验收脚本可加 `--require-complete`，门槛不满足时返回非零退出码。报告或自动化
可加 `--markdown` 直接得到简明表格；默认输出完整统计 JSON，便于复核。

## 六、现有证据审计

已有 P1.1A 数据位于
`build/benchmarks/high-frequency-p0-20261020-140227/`：

- 400/400 有效轮，整体高模式率 **72.0%**；
- 高模式中位数 **16.838 Mmsg/s**，高模式范围 15.081–17.533 Mmsg/s；
- 前 200 轮 75.5%，后 200 轮 68.5%，双侧 `p=0.119`，未达到时间漂移显著；
- 前/后 200 轮的低模式中位数分别为 9.854/8.254 Mmsg/s，后半段低模式更深，
  但高模式中位数基本不变（16.812/16.849 Mmsg/s），后续比较必须保留低模式项；
- 0/1/10/60 秒间隔组分别为 75%/76%/72%/65%，没有热启动优势。

这组数据说明 192 B 能频繁进入高模式，但不能证明 192 B 本身能把 72% 提升到 95%。
它应作为后续候选的固定基线，而不是候选优化。

## 七、优先级

1. **P0**：建立上述交错 A/B 采集，先跑足够的同期基线；不要重复已有 100 轮 192 B 实验。
2. **P1**：发布热路径减少指令，必须以同二进制条件下的发布路径差异做候选。
3. **P2/P3**：consumer 批量进度回写、ring 扩容均应通过本策略比较；ring 扩容会改变内存和语义，
   不能只凭单轮高吞吐宣布收益。
4. 若候选无法在足够样本下提高高模式率，则接受当前不稳定状态，转向延迟、可靠性或专用硬件，
   不再用“绝对 17 Mmsg/s”作为 SLA。

## 八、工具与测试

- `tools/aeron_compare/compare_high_mode_rates.py`
- `tools/aeron_compare/test_compare_high_mode_rates.py`
- `doc/plans/runners/high-frequency-stability-runner.py`
- `tools/aeron_compare/analyze_high_frequency_stability.py`

相关单元测试覆盖合并两比例检验、功效样本量、阈值/条件不一致拒绝、不完整实验拒绝、
高模式中位数退化和 Fisher 精确检验。工具只读取实验产物，不修改原始证据。

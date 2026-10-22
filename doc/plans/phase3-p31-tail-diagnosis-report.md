# Phase 3 P3-1 HFT 历史尾部诊断报告

日期：2026-10-22，Asia/Singapore。

## 1. 结论与边界

P3-1 正式验收结论保持 **FAIL（G4）**。本报告只复核并拆分既有 HFT 证据，不重跑测量，
不重新判定 G4，也不把诊断结果用于替换、补跑或冲销失败轮次。

可以确认的事实是：3 组 × 8 配置 × 4 轮共 96 轮正式输入、身份和角色结构全部通过复核；
其中 92 轮完成原始输入，4 轮未完成，4 轮全部位于 S 臂；96 轮内容/序号完整性通过，
worker failure 为 0。由此不能得出“背压是根因”“宿主调度是根因”或“缓存竞争是根因”的结论。

## 2. 身份与结构复核

固定输入目录为 `build/benchmarks/p3-p31-acceptance-20261021-155122/hft-tail-formal/`。
独立分析器重新计算并核对了 manifest 中列出的全部身份与 SHA：

| 项目 | 复核结果 |
|---|---:|
| manifest SHA-256 | `1d3ca385d6950f6bbabc6757449d543957220e6a8b61ba2d0685015c78abf7ee` |
| 客户端 SHA-256 | `bfec4dd5af451ad49b5b112ec510eb9f832bff9bd2a4c2bac8fd487a095162c1` |
| manifest 冻结输入 | 50/50 SHA 一致 |
| 正式 JSONL | 96/96 SHA 一致 |
| 正式 stderr | 96/96 SHA 一致 |
| 预热 JSONL/stderr | 16/16 对 SHA 一致 |
| publisher 角色 | 192/192 结构一致，均为 P0/P1 |
| subscriber 角色 | 144/144 结构一致，数量与 2P1S/2P2S 配置一致 |
| 完成度 | 92/96 完成；S 44/48，A 48/48 |
| 完整性 | 96/96 通过；144/144 subscriber 通过 |
| worker failure | 0 |

这里的“正式 JSONL 96/96”指 96 个轮次文件；每个文件包含 1 条 `start`、周期性
`progress` 和 1 条末尾 `result`。文件存在、SHA 一致、publisher/subscriber 角色完整，
与原始输入是否全部发布是两个独立检查。4 个不完整轮次被保留，没有被剔除。

## 3. 时间语义

分析严格区分以下三层观测，禁止用前一层替代后一层：

| 观测层 | 来源 | 能证明什么 | 不能证明什么 |
|---|---|---|---|
| 1 秒进度快照 | `progress` | 该时刻独立原子计数器的近似状态；publisher `sent` 每 256 条更新一次 | 不是精确截止，也不是排空后的最终状态 |
| measurement cutoff | `result.unsent_at_cutoff`、publisher 最终 `sent/unsent`、subscriber `unreceived_at_measurement_cutoff_observed` | publisher 结束时实际未发布数；subscriber 在 30 秒边界后首次轮询观察到的未收数 | subscriber 截止计数不是精确边界瞬间；不能拆出截止到排空的独立耗时 |
| drain/reap 结束 | `delivered`、`unreceived_after_drain`、`wall_seconds_including_drain_and_reap` | 排空和子进程回收结束后的最终交付量 | 只有一个合并 wall time，没有 cutoff 与 drain/reap 分界时间戳 |

对 4 个不完整轮次，最后一个 `progress` 的 `due - sum(sent)` 恰好与最终未发布数相同。
这只是一次已验证的数值重合，不改变其“周期快照”语义；分析仍以 `result` 计数为准。
现有数据无法计算“截止后用了多少时间排空”，因为没有独立的 cutoff 时间戳。

## 4. 不完整轮次

| 组 | 配置 | 轮次/臂 | 计划发布 | 实际发布 | 未发布 | wall 含 drain/reap |
|---:|---|---|---:|---:|---:|---:|
| 1 | 2P2S-256B-steady | 0/S | 30,000,000 | 29,996,460 | 3,540 | 30.0030 s |
| 2 | 2P1S-256B-burst | 0/S | 39,000,000 | 38,999,484 | 516 | 30.0156 s |
| 2 | 2P2S-64B-steady | 3/S | 30,000,000 | 29,999,991 | 9 | 30.0152 s |
| 3 | 2P2S-256B-steady | 0/S | 30,000,000 | 29,999,403 | 597 | 30.0125 s |

4 轮的所有 subscriber 完整性均为 true。subscriber 截止观测与最终未收数如下：

| 组/配置/轮 | Subscriber | cutoff 未收 | drain 后未收 | 完整交付 |
|---|---:|---:|---:|---|
| 1 / 2P2S-256B-steady / 0 | 0 | 4,532 | 3,540 | true |
| 1 / 2P2S-256B-steady / 0 | 1 | 3,540 | 3,540 | true |
| 2 / 2P1S-256B-burst / 0 | 0 | 516 | 516 | true |
| 2 / 2P2S-64B-steady / 3 | 0 | 9 | 9 | true |
| 2 / 2P2S-64B-steady / 3 | 1 | 9 | 9 | true |
| 3 / 2P2S-256B-steady / 0 | 0 | 1,525 | 597 | true |
| 3 / 2P2S-256B-steady / 0 | 1 | 597 | 597 | true |

两个组内 subscriber 的 cutoff 观测不同，说明截止计数本身也受 subscriber 轮询时机影响；
drain 后未收数才与 publisher 最终未发布数一致。

## 5. 逐发布者异常证据

下表仅摘录 4 个不完整轮次的逐发布者计数。全部 96 轮、192 个 publisher 的相同字段由
独立分析器完整输出，不跨组或跨配置合并。`排期积压高水位`是 publisher 每 256 条消息
采样到的 observed highwater，不是逐条精确最大值。

| 组/配置/轮 | P | 未发布 | 重试 | 背压消息 | 背压总时长 ms | 最长背压 ms | 排期积压高水位 | 排期 p999 us | 排期 max us |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 / 2P2S-256B-steady / 0 | 0 | 1,644 | 35,183 | 391 | 296.782 | 10.508 | 5,323 | 4,128.767 | 10,570.274 |
| 1 / 2P2S-256B-steady / 0 | 1 | 1,896 | 35,016 | 407 | 301.713 | 10.538 | 5,327 | 3,735.551 | 10,882.840 |
| 2 / 2P1S-256B-burst / 0 | 0 | 508 | 422 | 12 | 4.163 | 0.908 | 949 | 28.415 | 2,155.428 |
| 2 / 2P1S-256B-burst / 0 | 1 | 8 | 386 | 10 | 4.245 | 0.902 | 1,007 | 14.847 | 2,200.049 |
| 2 / 2P2S-64B-steady / 3 | 0 | 9 | 1,139 | 6 | 12.420 | 6.538 | 3,708 | 2,686.975 | 7,518.255 |
| 2 / 2P2S-64B-steady / 3 | 1 | 0 | 2,105 | 7 | 14.640 | 8.387 | 4,121 | 3,112.959 | 8,223.345 |
| 3 / 2P2S-256B-steady / 0 | 0 | 306 | 43,099 | 430 | 382.944 | 6.481 | 6,925 | 4,325.375 | 14,021.683 |
| 3 / 2P2S-256B-steady / 0 | 1 | 291 | 42,490 | 463 | 364.413 | 6.478 | 3,107 | 3,014.655 | 6,497.540 |

对应 publisher 资源计数如下；上下文中 `cs` 为 context switches：

| 组/配置/轮 | P | user us | system us | vol cs | invol cs | minor faults | major faults |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 / 2P2S-256B-steady / 0 | 0 | 29,350,160 | 236,192 | 0 | 5,941 | 9 | 0 |
| 1 / 2P2S-256B-steady / 0 | 1 | 29,363,442 | 229,794 | 0 | 10,117 | 11 | 0 |
| 2 / 2P1S-256B-burst / 0 | 0 | 30,054,642 | 9,341 | 0 | 740 | 9 | 0 |
| 2 / 2P1S-256B-burst / 0 | 1 | 30,072,763 | 6,526 | 0 | 340 | 10 | 0 |
| 2 / 2P2S-64B-steady / 3 | 0 | 29,130,113 | 67,554 | 0 | 10,534 | 10 | 0 |
| 2 / 2P2S-64B-steady / 3 | 1 | 29,017,783 | 42,491 | 0 | 9,521 | 10 | 0 |
| 3 / 2P2S-256B-steady / 0 | 0 | 29,243,230 | 321,663 | 0 | 8,517 | 10 | 0 |
| 3 / 2P2S-256B-steady / 0 | 1 | 28,880,803 | 294,050 | 0 | 7,498 | 11 | 0 |

四个不完整轮次都有非零背压和排期高水位，但并不存在“计数越高就越不完整”的单调关系。
例如 1/2P2S-256B-steady 的未完成 S 轮最大重试为 35,183，而同组同配置的完整 A 轮达到
98,041；3/2P2S-256B-steady 的未完成 S 轮最大重试为 43,099，而完整 A 轮达到 80,517。
因此背压和排期积压是直接观测到的机制候选，不能单独解释完成度失败。

同样，完整轮次也记录了排期毫秒尾和高 involuntary context switch。例如
2/2P2S-64B-steady 的未完成 S 轮 invol cs 为 10,534/9,521，而同组同配置完整 S 轮为
4,864/12,353。没有同消息的同步 off-CPU/commit/receive trace，不能把宿主调度认定为根因。

## 6. 候选根因排序

排序依据是现有证据的支持强度，不是因果证明；所有候选都保留“未证明”状态。

1. **S 臂运行中的间歇性发布进度损失。** 4/4 不完整轮次在 S，A 为 0/48；这是最强的关联事实。
   但同一组、同一配置下 S 也有完整轮次，因此现有证据不支持“S 在全部条件下必然失败”。
2. **publisher 背压/发布窗口积压。** 4/4 不完整轮次都有非零重试、背压和 backlog 高水位；
   但多轮完整运行的计数更高，说明它可能是共同症状或放大器，尚不能作为充分原因。
3. **接近 30 秒边界的短差或截止竞争。** 2/2P2S-64B-steady/3/S 只差 9 条，符合边界附近
   短差的形态；其余轮次差 516–3,540 条，不能由这一个候选解释。
4. **宿主调度干扰。** 不完整轮次存在毫秒级 planned-to-attempt 尾和 involuntary context switch；
   完整轮次也有同类现象，且没有同步 trace，最多作为待验证候选。

明确未证明：

- 宿主调度器是根因；
- CPU cache 或 cache-line contention 是根因；
- 某个具体产品代码缺陷是根因；
- 4 个 S 轮共享同一个根因；
- 4,662 条未发布会造成已发布消息的正确性失败。实际上 96 轮完整性检查和 144 个 subscriber 均通过。

## 7. 可复现分析

独立分析器只读固定证据目录，不执行 HFT：

```bash
python3 tools/hft_acceptance/analyze_p31_followup.py \
  build/benchmarks/p3-p31-acceptance-20261021-155122/hft-tail-formal
```

本次保留的独立输出为同目录下的 `followup-analysis.json`
（SHA-256 `f74487fbc8e430fb0e58e27c31ceb6f0d3fe352176e9b57f71fca2415399ab63`）
与 `followup-analysis.md`，不修改任何原始 HFT 文件。

输出 JSON 保留每轮的 `progress_observation`、`measurement_cutoff`、`drain_reap_end`、
全部 publisher/subscriber 计数和资源字段；`--format markdown` 输出完整逐 publisher 表。
合成测试覆盖客户端/raw/冻结输入身份损坏、截断 JSONL、缺少 result、原始输入不完整、
publisher/subscriber 角色结构错误、陈旧 progress 不得替代 cutoff，以及根因误归因边界：

```bash
python3 -m unittest tools/hft_acceptance/test_analyze_p31_followup.py
```

本轮没有运行新的 HFT 性能测量，没有提交、打 tag 或推送。

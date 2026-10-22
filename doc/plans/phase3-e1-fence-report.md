# Phase 3 E1：零布局屏障判定实验报告

**日期**：2026-10-21（Asia/Singapore）
**结论**：**GO**。64 B 中位数比 1.4048，精确置换检验 p=0.002165，6/6 块方向一致。
**产品边界**：本次未修改 `src/`、未提交、未打 tag、未推送。P3-1 实现仍须用户批准。

## 1. 实验身份

| 项目 | 值 |
|---|---|
| 基线 B | `build/benchmarks/baseline-extended-20261019-183822/salias_ipc_compare.uninstrumented` |
| B SHA-256 | `3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da` |
| 补丁 C | `build/benchmarks/p3-e1-20261021-091727/salias_ipc_compare.nofence-diag` |
| C SHA-256 | `5efae0b41ed42f9a38d69997ed8a1fafc69d78ae69eea948c79abbdec0f433b6` |
| 补丁清单 | `build/benchmarks/p3-e1-20261021-091727/patch-manifest.json` |
| 正式目录 | `build/benchmarks/p3-e1-formal-20261021-092102` |

补丁只把文件偏移 `0x23259`、`0x23264`、`0x23283`、`0x232c5` 的 `0x87`
改为 `0x89`。`cmp -l` 恰好报告 4 处差异；`objdump -d` 恰好 4 条
`xchg` → `mov`，操作数和指令长度不变，其余指令地址不变。可执行 `LOAD`
段为 `0x3000 → 0x3000`，所以文件偏移等于虚拟地址。

该客户端仅用于诊断。它删除了注册与 claim 之间的 SC 握手；测量有效的前提是注册
在发令前完成、测量期不注册/注销、消费者只 poll。已从 v3.0.0 源码核对：
`run_consumer_child` 先执行 `channel.subscriber()`，再调用
`ready_and_wait_for_start`；测量循环只有 `subscriber.poll`；consumer 与 producer
都在子进程 `_exit` 前不销毁端点。满足上述前提。

## 2. 实现与测试

新增：

- `tools/aeron_compare/make_fence_patch_client.py`：源/补丁 SHA、ELF 段、
  四处字节、`cmp`、反汇编和补丁清单校验；失败时删除不完整产物。
- `tools/aeron_compare/compare_throughput_ab.py`：六组完整性、`SHA256SUMS`、
  原始 RESULT 与 JSON 一致性、组中位数、924 种划分的双侧精确置换检验、
  ABBA 方向、组级 cluster bootstrap 和固定判定规则。
- `doc/plans/runners/p3-e1-throughput.py`：按
  `B0 C0 C1 B1 B2 C2 C3 B3 B4 C4 C5 B5` 执行 12 次独立 runner 调用。
- `tools/aeron_compare/compare_latency_ab.py`：按每条记录实际客户端 SHA 拆分
  B/C，输出每轮分位数的中位数。
- `run_latency_compare.py --alternate-salias-client`：每轮都跑 B/C 两臂，
  奇偶轮反转先后；每条记录写入 `arm` 和 `client_sha256`。

相关单测：

`test_make_fence_patch_client.py`、`test_compare_throughput_ab.py`、
`test_p3_e1_throughput.py`、`test_compare_latency_ab.py` 和
`test_latency_compare.py` 全部通过。补丁单测直接对真实冻结客户端执行并校验
登记哈希。

## 3. 64 B 吞吐（判定实验）

条件：FIFO 1P2S，64 B，1 MiB，batch 1，poll-limit 64，CPU0 coordinator，
50M 消息/轮，每臂 6 组 × 20 轮。每块一次 runner 调用，固定
`--interval-seconds 0 --max-consecutive-low 0`。

| 臂 | 中位数 Mmsg/s | 均值 Mmsg/s | CV | ≥10 Mmsg/s 轮数 |
|---|---:|---:|---:|---:|
| B 冻结 | 6.589805 | 6.907617 | 17.66% | 9/120 |
| C 无 fence 诊断 | 9.257205 | 9.867356 | 29.99% | 14/120 |

组中位数（Mmsg/s）：

| 组 | B | C | C/B |
|---|---:|---:|---:|
| 0 | 6.2405 | 9.1673 | 1.4690 |
| 1 | 6.5097 | 9.5494 | 1.4670 |
| 2 | 6.5437 | 9.3296 | 1.4257 |
| 3 | 6.7403 | 9.1901 | 1.3635 |
| 4 | 6.6649 | 9.1317 | 1.3701 |
| 5 | 6.6781 | 9.2284 | 1.3819 |

- 中位数比 C/B：**1.404777**。
- 精确置换检验（924 种 6/6 划分，双侧）：**p=0.002165**（2/924）。
- ABBA 方向：**6/6** 块 C 高于 B。
- 组级 cluster bootstrap（20,000 次，seed 20261021）中位数差：
  **+2.6647 Mmsg/s，95% 区间 +2.5277 至 +2.8530**。
- 交付完整性：两臂各 120 轮；12 块 return code 全为 0；SHA256SUMS 校验
  分别覆盖 380 个文件；原始 RESULT 均为 `published=50000000`、
  `delivered=100000000`。

判定规则在运行前固定为：GO 需中位数比 ≥1.25、p<0.01、至少 5/6 块 C 更高；
NO-GO 为中位数比 <1.10 或 p≥0.05。本次三项 GO 条件全部满足，故为 **GO**，
不触发扩大样本分支。

## 4. 192 B 吞吐（描述性）

条件同 64 B，只把 payload 改为 192 B。

| 臂 | 中位数 Mmsg/s | 均值 Mmsg/s | CV | ≥15 Mmsg/s 轮数 |
|---|---:|---:|---:|---:|
| B | 16.296050 | 14.796956 | 19.98% | 106/120 |
| C | 17.395950 | 17.068266 | 17.99% | 110/120 |

中位数比 **1.067495**；组中位数 B 为 15.7605、16.1941、16.4302、16.3779、
15.4756、16.3836，C 为 17.4261、17.4696、17.2137、17.3452、17.1302、17.5152。
方向 6/6，置换检验 p=0.002165，bootstrap 中位数差 +0.9135 至 +1.5954。
分析工具对 192 B 明确输出 `decision=DESCRIPTIVE`，不会把 64 B 的 GO/NO-GO
规则套用到这一档。这些数字只作描述，不设门槛。

对照预测 P-c（192 B 高/低双模态减弱）：B 均值明显低于中位数，C 的均值更接近
中位数；高模式轮数 106→110。方向一致，但 120 轮不足以宣称双模态已经消除。

## 5. 延迟（描述性）

条件：FIFO 1P1S，1M msg/s，2 秒/轮，64/256 B，每臂 10 轮；每轮 B/C 都跑，
奇偶轮反转先后。以下是各轮分位数的中位数，不是合并直方图。

| Origin | B | 臂 | p50 ns | p99 ns | p99.9 ns |
|---|---:|---|---:|---:|---:|
| attempt | 64 | B | 269 | 437 | 4607 |
| attempt | 64 | C | 259 | 417 | 3999 |
| attempt | 256 | B | 275 | 455 | 4255 |
| attempt | 256 | C | 255 | 427 | 4479 |
| planned | 64 | B | 331 | 2831 | 10303 |
| planned | 64 | C | 311 | 2575 | 11135 |
| planned | 256 | B | 333 | 2687 | 11711 |
| planned | 256 | C | 315 | 3103 | 12287 |

- 两臂吞吐完成度均为 100%（每轮 `published=delivered=2,000,000`）。
- 所有记录的 cgroup `high/max/oom/oom_kill` 均为 0。
- attempt p50 仅下降 10–20 ns，与 P-b“缓冲区在消息间隔内排空”的预测一致。
- planned p99.9 在 64 B 变差、256 B 变差，在 256 B p99 也变差；尾部不是单调收益。
  本实验对延迟不设正式门槛，因此不能把 p50 的小幅下降写成尾延迟改善。

## 6. 协议偏差与处理

第一次延迟采集把“逐轮交替”误做成每轮只跑一臂，所以每臂只有 5 轮。该数据保留在
`latency/attempt` 与 `latency/planned`，不进入报告结论。修正后每轮执行 B/C
两臂并反转先后，正式数据在 `latency-attempt-10x2` 与 `latency-planned-10x2`。

另有两个 50M 单轮冒烟，不能替代正式矩阵：一次 B 11.43 / C 8.53 Mmsg/s，
另一次 B 4.48 / C 5.17 Mmsg/s（5M 消息）。它们说明单轮测量极不稳定，也正是
预先登记组中位数、20 轮和 6 块重复的原因。冒烟数据不作为产品性能结论。

## 7. 对 P3-1 的含义

E1 只证明“去掉每条 claim 的两条完整屏障”能在零布局变化下显著提高 64 B 稳态
吞吐。它没有证明 P3-1 产品实现同样安全或同样快：

- 产品实现必须保留注册握手，把同步成本移到注册/注销；不能把 seq_cst 无条件改弱。
- 必须用真实的候选构建完成 §6.3 的 G1–G6，包括同二进制
  `SALIAS_DISABLE_ASYMMETRIC_FENCE` 的 C-seq/C-asym 对照。
- C 客户端只能用于机制判定，不得出现在产品性能结论中。
- 本次未改 `src/`。进入 P3-1 实现前仍需要用户明确批准。

## 8. 证据路径

| 证据 | 路径 |
|---|---|
| 补丁客户端与清单 | `build/benchmarks/p3-e1-20261021-091727/` |
| 64 B 两臂原始数据 | `build/benchmarks/p3-e1-formal-20261021-092102/{baseline,candidate}/` |
| 64 B 分析 | `build/benchmarks/p3-e1-formal-20261021-092102/analysis-64b.json` |
| 192 B 两臂原始数据 | `build/benchmarks/p3-e1-formal-20261021-092102/throughput-192b/` |
| 192 B 分析 | `build/benchmarks/p3-e1-formal-20261021-092102/throughput-192b/analysis-192b.json` |
| 延迟原始数据 | `build/benchmarks/p3-e1-formal-20261021-092102/latency-{attempt,planned}-10x2/` |
| 延迟 B/C 汇总 | `build/benchmarks/p3-e1-formal-20261021-092102/latency-ab-10x2.json` |

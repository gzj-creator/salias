# P3-1 正式验收：发布路径非对称屏障

日期：2026-10-21，Asia/Singapore。
依据：[最终优化计划](phase3-final-optimizations.md) §6、§9.3；
实现：[P3-1 实现报告](phase3-p31-implementation-report.md)。

**结论（2026-10-21 18:43）**：正式验收已完成，但 **P3-1 整体验收未通过**。
G1、G2、G3、G5 通过；G4 的短测 p50 与短测完成度通过，HFT 长尾非劣和 HFT 输入完成度未通过；
G6 已记录、不设阈值。不能把`run-u2465.service`退出码 0 或吞吐收益解释为整体验收通过。
本轮未重跑正式 HFT 以追求通过，未改变门槛；不提交、不打 tag、不推送，未改产品源码。

## 0. 门槛总结

| 门槛 | 状态 | 依据 |
|---|---|---|
| G1 屏障效应 | PASS | 64 B A/S=1.392658，p=0.0021645，方向 6/6 |
| G2 产品对外数字 | PASS | 64 B A/B=1.395800 |
| G3 192 B 非劣 | PASS | A/S=1.048841，高模式率 +18.33 pp |
| G4 延迟非劣 | **FAIL** | 短测 planned p50 通过；HFT 仅 7/24 组/配置单元通过，4 个输入不完整 |
| G5 正确性 | PASS | Release/ASan+UBSan/TSan 全绿，跨进程故障注入通过 |
| G6 注册成本 | RECORDED | A−S=+3.166 µs；只报告，不设阈值 |

最终结论必须同时保留两层事实：64 B 发布吞吐有明确、可重复的屏障收益且正确性回归通过；
然而在预先固定的 HFT 完成度与长尾非劣规则下，候选不能通过本轮整体验收。

## 1. 实验身份与构建

统一证据目录：`build/benchmarks/p3-p31-acceptance-20261021-155122/`，以下称 `ROOT`。

| 臂 | 含义 | SHA-256 |
|---|---|---|
| B | 冻结 v3.0.0 客户端 | `3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da` |
| S / C-seq | 候选客户端，`SALIAS_DISABLE_ASYMMETRIC_FENCE=1` | `328b951c36d3458a11f728df52311a9115640f9d0fbdf46f6827d9834dc69a3d` |
| A / C-asym | 与 S 同一文件，清除该环境开关 | 同 S |
| HFT S/A | 同一个 `acceptance.cpp` 候选，环境开关同上 | `bfec4dd5af451ad49b5b112ec510eb9f832bff9bd2a4c2bac8fd487a095162c1` |

v3 benchmark 源码来自 `git show v3.0.0:tools/aeron_compare/salias_ipc_compare.cpp`，
SHA-256 为 `6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e`。
先在 `/tmp/p31-v3-154924/` 重建未修改的 v3.0.0 产品与 benchmark，得到冻结哈希 `3f8fc695...`；
候选使用同一份旧 benchmark，链接当前产品库，而非扩展过的诊断 benchmark。

编译器为 GCC 14.2.0；产品库使用 CMake Release `-O3 -DNDEBUG`，无 sanitizer。
benchmark 使用 `-std=c++23 -O3 -DNDEBUG -flto -march=native`、`-pthread`。
HFT harness 使用 `-std=c++23 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Werror`、`-pthread -lrt`。
两种 harness 的编译参数不同，不能混作同一二进制实验。

候选静态库哈希：

- `libsalias.a`：`165d6adb1ba69b2b48c735a7cd3c470905689d9e51e3101c354c1c10a0545894`。
- `libsalias_core.a`：`ab30beceaa771fe9c596182d69a2293631c08c034f6b6096add4bbed02d8d720`。

S/A 同文件对照排除了重编译布局差异；A/B 是实际产品对照，仍可能含布局效应。
E1 的不安全 nofence 诊断客户端没有用于本报告任何产品数字。

## 2. 吞吐：G1–G3

条件：FIFO 1P2S 广播，1 MiB ring、batch=1、poll-limit=64，50M 消息/轮，
每 payload 每臂 6 组 × 20 轮。块序为 B/S/A、A/S/B 交替；
每块独立调用 runner，`--interval-seconds 0 --max-consecutive-low 0`，不因低轮次停止或筛样。
coordinator CPU0、subscriber CPU1/2、publisher CPU3。

### 2.1 64 B

| 臂 | 中位数 Mmsg/s | 均值 Mmsg/s | CV |
|---|---:|---:|---:|
| B | 6.492160 | 6.825762 | 18.18% |
| S | 6.506805 | 6.816413 | 17.88% |
| A | 9.061755 | 9.512976 | 27.85% |

| 组 | B 中位数 | S 中位数 | A 中位数 |
|---|---:|---:|---:|
| 0 | 6.559930 | 6.519685 | 9.153305 |
| 1 | 6.683855 | 6.332665 | 9.210040 |
| 2 | 6.291080 | 6.556650 | 9.095665 |
| 3 | 6.603770 | 6.476640 | 8.863125 |
| 4 | 6.396795 | 6.513190 | 9.115305 |
| 5 | 6.475230 | 6.552110 | 8.909445 |

- **G1 PASS**：A/S 中位数比 **1.392658**，超过 1.25；组中位数双侧精确置换检验
  **p=0.0021645**（2/924），6/6 组方向一致，超过 5/6 要求。
- 组级 cluster bootstrap（20,000 次，seed 20261021）中位数差 **+2.567375 Mmsg/s**，
  95% 区间 **+2.395260 至 +2.705605 Mmsg/s**。
- **G2 PASS**：A/B 中位数比 **1.395800**，超过 1.20。
  这是此主机、此负载下的候选吞吐观察，不代表整体延迟验收通过或相对 Aeron 的新结论。

### 2.2 192 B

| 臂 | 中位数 Mmsg/s | 均值 Mmsg/s | CV | ≥15 Mmsg/s 轮数 |
|---|---:|---:|---:|---:|
| B | 16.686650 | 15.063865 | 21.17% | 93/120 |
| S | 16.674600 | 14.567747 | 25.21% | 83/120 |
| A | 17.489000 | 16.838721 | 19.83% | 105/120 |

**G3 PASS**：A/S 中位数比 **1.048841**，超过 0.97；高模式率从 **69.17%** 升至 **87.50%**，
变化 **+18.33 pp**，未发生超过 10 pp 的下降。
192 B 的置换检验、bootstrap 是描述性结果，不套用 64 B 的 G1 提升门槛。
高模式率并非 100%，CV 仍高，不声称双模态或稳定性问题已解决。

### 2.3 完整性

两种 payload 各 360 轮，合计 **720 轮**正式吞吐测量；每臂每 payload 校验 380 个
`SHA256SUMS` 文件，36 个块全部正常退出。原始 RESULT 每轮必须为
`published=50,000,000`、`delivered=100,000,000`。
合计正式发布 **36,000,000,000** 条、广播计数交付 **72,000,000,000** 次。
这些是 benchmark 收发计数检查，不替代 HFT 的逐条序号/全 payload 校验。

## 3. G4 短测延迟

FIFO **1P1S**，1M msg/s，2 秒/轮，64/256 B、attempt/planned，每臂每格 10 轮。
每轮三臂全部运行，按轮号轮转先后；每格另有各臂一次预热，不混入统计。
以下是独立轮次分位数的中位数，不是池化直方图，不能与 1P2S 吞吐或 HFT 2P2S 拼接。

| Origin | Payload | 臂 | p50 ns | p99 ns | p99.9 ns |
|---|---:|---|---:|---:|---:|
| attempt | 64 | B | 275 | 443 | 6143 |
| attempt | 64 | S | 271 | 429 | 4159 |
| attempt | 64 | A | 252 | 399 | 4159 |
| attempt | 256 | B | 277 | 447 | 4351 |
| attempt | 256 | S | 273 | 461 | 7903 |
| attempt | 256 | A | 253 | 437 | 6143 |
| planned | 64 | B | 321 | 1903 | 10815 |
| planned | 64 | S | 327 | 1775 | 10047 |
| planned | 64 | A | 303 | 2447 | 9183 |
| planned | 256 | B | 337 | 4831 | 16959 |
| planned | 256 | S | 325 | 3071 | 10367 |
| planned | 256 | A | 313 | 2431 | 11711 |

planned p50 的 A−S 为 **−24 ns / −12 ns**，均满足“不超过 S+10 ns”。
120 轮正式测量每轮 published=delivered=2M，采样数 125,000，完成率均为 100%；
原始 RESULT/LATENCY 与 JSON、stdout/stderr SHA、臂环境与二进制哈希已复核。
所有记录的 cgroup high/max/oom/oom_kill 为零。

短测不能判定尾部非劣：64 B planned p99 上升，256 B planned p99.9 上升，
不能只展示 p50 下降而称“延迟全面改善”。G4 总结须结合下一节 HFT 长尾。

## 4. G4 HFT 长尾

执行细则在第一条 HFT 数据采集之前写入计划 §6.4 与 `experiment.json`：
沿用既有 HFT 非目标配置的 **5% 非劣界限**，不继承历史实验的“burst 必须改善 20%”目标。
该细则不是对既有明确数值门槛的修改，而是将原计划未量化的“HFT 判定非劣”落实为可审计规则。

- 8 配置：2P1S/2P2S × 64/256 B × steady/burst；原 offered rate、采样、计时与完整性检查不变。
- 预热每配置 S/A 各 2 秒，随后 3 个独立 SAAS 组，每轮 30 秒，共 96 轮、2880 秒正式测量。
- 每组每配置按 subscriber 分别合并原始桶，然后分别取最慢 subscriber 的 planned p99、p99.9。
  两者都须 A ≤ S×1.05，24 个组/配置全部满足；不能跨组池化挽救失败组。
- worker、原始负载完成与逐条内容/序号检查必须全部通过；50 µs 预算另报。
- 2P2S 使用 CPU0/1 subscriber、CPU2/3 publisher；2P1S 使用 CPU0 subscriber、CPU1/2 publisher。
  全部 4 vCPU 可用，704 MiB cgroup、MemoryHigh=640 MiB、零 cgroup swap、Nice=5；
  不同时编译或运行其他基准，不使用 SCHED_FIFO，不改 sysctl。

正式服务 `run-u2465.service` 于 17:54:28 启动、18:43:20 结束，96 个正式 worker 全部
退出 0；执行耗时 48 分 52 秒，累计 CPU 时间 2 小时 48 分 12 秒。退出 0 只表示采集和审计执行完成。
完整结果在 `ROOT/hft-tail-formal/hft-noninferiority.{json,md}`。

### 4.1 有效性、完成度与完整性

| 检查 | S / C-seq | A / C-asym |
|---|---:|---:|
| 正式轮次 | 48 | 48 |
| worker 正常退出 | 48/48 | 48/48 |
| 原始负载完成 | 44/48 | 48/48 |
| 内容/序号完整性 | 48/48 | 48/48 |

采集计划发布 **3,312,000,000** 条，实际发布 **3,311,995,338** 条，截止时未发布
**4,662** 条；所有已发布消息的完整性检查通过。4 个不完整轮次均在 S 臂：

| 组 | 配置 | 轮次 | 计划发布 | 实际发布 | 未发布 |
|---:|---|---:|---:|---:|---:|
| 1 | 2P2S-256B-steady | 0/S | 30,000,000 | 29,996,460 | 3,540 |
| 2 | 2P1S-256B-burst | 0/S | 39,000,000 | 38,999,484 | 516 |
| 2 | 2P2S-64B-steady | 3/S | 30,000,000 | 29,999,991 | 9 |
| 3 | 2P2S-256B-steady | 0/S | 30,000,000 | 29,999,403 | 597 |

因此“原始输入必须全部完成”的条件不成立。4 个不完整单元仍按既定规则判失败，没有剔除或补跑。

### 4.2 相对长尾结果

下表单位均为 µs；每个值是该组该配置两轮原始桶按 subscriber 合并后，再取最慢 subscriber
的 planned 分位数。`S有效` 同时要求两轮原始输入完成、worker 正常且内容/序号完整性通过。

| 配置 | 组 | S有效 | S p99 | A p99 | S p99.9 | A p99.9 | 非劣 |
|---|---:|---|---:|---:|---:|---:|---|
| 2P1S-64B-steady | 1 | 是 | 3.647 | 3.551 | 155.647 | 133.119 | PASS |
| 2P1S-64B-burst | 1 | 是 | 5.119 | 3.551 | 761.855 | 29.439 | PASS |
| 2P1S-256B-steady | 1 | 是 | 3.615 | 4.159 | 57.343 | 368.639 | FAIL |
| 2P1S-256B-burst | 1 | 是 | 5.311 | 5.887 | 60.927 | 49.663 | FAIL |
| 2P2S-64B-steady | 1 | 是 | 589.823 | 630.783 | 3276.799 | 2588.671 | FAIL |
| 2P2S-64B-burst | 1 | 是 | 1392.639 | 1703.935 | 2949.119 | 4521.983 | FAIL |
| 2P2S-256B-steady | 1 | 否 | 1474.559 | 1966.079 | 4128.767 | 6488.063 | FAIL |
| 2P2S-256B-burst | 1 | 是 | 1736.703 | 2293.759 | 6750.207 | 7077.887 | FAIL |
| 2P1S-64B-steady | 2 | 是 | 5.119 | 3.935 | 270.335 | 286.719 | FAIL |
| 2P1S-64B-burst | 2 | 是 | 4.223 | 5.823 | 84.991 | 172.031 | FAIL |
| 2P1S-256B-steady | 2 | 是 | 5.311 | 7.167 | 655.359 | 376.831 | FAIL |
| 2P1S-256B-burst | 2 | 否 | 6.975 | 5.823 | 245.759 | 241.663 | FAIL |
| 2P2S-64B-steady | 2 | 否 | 1490.943 | 1474.559 | 3473.407 | 3375.103 | FAIL |
| 2P2S-64B-burst | 2 | 是 | 1736.703 | 1671.167 | 4784.127 | 2916.351 | PASS |
| 2P2S-256B-steady | 2 | 是 | 1933.311 | 1671.167 | 5177.343 | 4456.447 | PASS |
| 2P2S-256B-burst | 2 | 是 | 1736.703 | 1933.311 | 7143.423 | 4980.735 | FAIL |
| 2P1S-64B-steady | 3 | 是 | 5.247 | 4.927 | 499.711 | 364.543 | PASS |
| 2P1S-64B-burst | 3 | 是 | 5.631 | 6.143 | 663.551 | 876.543 | FAIL |
| 2P1S-256B-steady | 3 | 是 | 5.823 | 5.311 | 540.671 | 141.311 | PASS |
| 2P1S-256B-burst | 3 | 是 | 7.423 | 9.855 | 573.439 | 901.119 | FAIL |
| 2P2S-64B-steady | 3 | 是 | 1687.551 | 1916.927 | 3538.943 | 3670.015 | FAIL |
| 2P2S-64B-burst | 3 | 是 | 2293.759 | 2080.767 | 3899.391 | 2949.119 | PASS |
| 2P2S-256B-steady | 3 | 否 | 1572.863 | 1736.703 | 3801.087 | 5373.951 | FAIL |
| 2P2S-256B-burst | 3 | 是 | 2129.919 | 2293.759 | 9568.255 | 6094.847 | FAIL |

- **7/24** 个组/配置单元同时满足 p99 与 p99.9 的 A ≤ S×1.05；**17/24 失败**。
- 其中 4 个单元因 S 原始输入未完成直接失败；其余 13 个有效单元至少一个分位数超限。
- 在 20 个有效单元中，p99 超限 12 个、p99.9 超限 6 个；两个分位数分别判定，互不抵消。
- 2P1S 的 p99 大多为几微秒，2P2S 的 p99 常达数百微秒至约 2.3 ms，p99.9 常达数毫秒；
  不同拓扑、payload 和 pattern 仍有明显方向差异，不能声称长尾整体改善或稳定。

分位数沿用既有 HFT 桶上界定义；尾部比值的大幅百分比可能由少量粗桶边界放大，不能据此
推断宿主调度或硬件事件归因。但这是采集前固定的观测定义，本轮不因结果不理想而重算。

### 4.3 独立 50 µs 预算

50 µs planned p99.9 预算作为独立结果另报：A 仅在 **2/24** 个单元通过，S 为 **0/24**。
该预算不能替代相对非劣，也没有用于挽救任何失败单元。因此 **G4 FAIL**。

服务结束汇总记录内存峰值 **35.5 MiB**、swap 峰值 **0 B**。18:40:07 最后一次直接读取
瞬态 cgroup 的 `memory.events` 时 low/high/max/oom/oom_kill 均为 0；服务结束后 cgroup
已由 systemd 回收，不能把这次中途读数冒充为结束后的最终计数。

## 5. G5 正确性与回归

本轮重新执行而非仅引用实现报告：

| 套件 | 结果 |
|---|---|
| Release CTest | 22/22 通过 |
| ASan/UBSan CTest | 21/21 通过 |
| TSan CTest | 21/21 通过 |
| Aeron Python 工具 | 153 项，3 项跳过，其余通过 |
| HFT Python 工具 | 50/50 通过 |

新增 asymmetric suite 包括两种协议各 10,000 次跨进程 acquire/release、fork 后发布、
heavy fence/发布端注册故障注入、注销 fail closed、v14/未知协议拒绝和协议选择。
因此 **G5 PASS**。TSan 不能建模 membarrier，跨进程压力与协议论证才是该机制的主要正确性证据；
ARM 和非 Linux 的运行结果没有在本 x86_64 Linux 主机验证。

## 6. G6 注册成本

独立 `registration_cost.cpp` 探针直接包含相应版本的 `channel.cpp`，调用原
`NamedChannelState::acquire_consumer`，**不修改 src/**，不把探针链接进产品或吞吐客户端。
registrar CPU1；另一个发布进程 CPU2 构造 Tx 后忙等、不 claim，以便真实覆盖远端已注册进程。
G6 是空闲注册成本，不测 claim 竞争、进程死亡回收或最坏耗时。

B/S/A 各 6 轮，交替正反顺序；每轮预热 1000 次、计时 5000 次。
释放不在计时区间，保留全部 90,000 个正式原始耗时。
下表采用“每轮中位数的中位数”，不挑选最低轮次：

| 臂 | acquire_consumer µs | 6 轮中位数 µs |
|---|---:|---|
| B | 12.59275 | 12.201 / 12.987 / 12.8775 / 16.872 / 12.2585 / 12.308 |
| S | 12.20650 | 12.193 / 16.977 / 12.314 / 12.183 / 12.166 / 12.220 |
| A | 15.37250 | 15.437 / 15.308 / 15.204 / 15.290 / 20.3785 / 15.453 |

A−S 为 **+3.166 µs（约 +25.94%）**，A−B 为 **+2.77975 µs**。
这比计划中的“预计 +0.6 µs”大，必须使用实测，不将早期单 syscall 探针预测当成产品注册成本。
探针确认 A 发布进程 light path=true，B/S=false，注册全部成功。
G6 只要求报告，不设通过阈值，故状态为 **RECORDED**。

## 7. 工具与执行偏差

- 本轮补充 P3-1 HFT runner、原始桶审计器与 14 个单测，以及独立注册探针/runner。
- 原任务已补齐吞吐 `--setenv` 透传和 B/S/A 同文件延迟交错；未重跑已完成的正式吞吐数据。
- HFT 首次启动 `run-u2463.service` 在环境采集阶段因 systemd 服务 PATH 中无 `rg` 退出 127，
  **尚未预热或测量**；保留 `ROOT/hft-tail/experiment.json`，不进入结论。
  诊断复现 `run-u2464.service` 后，正式启动显式透传 PATH，使用新目录 `hft-tail-formal/`。
  runner 已改为先保存环境采集 stdout，再判断退出码，便于后续留存失败信息。
- 注册探针因单独包含实现 TU 有 `-Wsubobject-linkage` 警告，完整构建日志保留；
  该工具不使用 `-Werror`。产品库和 HFT harness 未为此改 flags。
- 共享 VM 无 PMU；不声称已证明 cache-miss、HITM、IPC 或宿主调度归因。

## 8. 证据与复现

| 内容 | 路径 |
|---|---|
| 64 B 三臂原始吞吐 | `build/benchmarks/p3-p31-throughput-64b-20261021-160125/` |
| 192 B 三臂原始吞吐 | `build/benchmarks/p3-p31-throughput-192b-20261021-1646/` |
| 吞吐分析 | `ROOT/throughput-{64b,192b}.json` |
| 1P1S 短测原始数据 | `ROOT/latency-{attempt,planned}/` |
| 短测分析 | `ROOT/latency.json` |
| HFT 规则、原始桶、命令与 SHA | `ROOT/hft-tail-formal/` |
| HFT 服务结束汇总 | `ROOT/hft-tail-formal/service-journal.txt` |
| G6 构建输入与原始耗时 | `ROOT/registration/registration.json`、`ROOT/registration/group-*.json` |

```bash
python3 tools/aeron_compare/compare_p31_throughput.py \
  build/benchmarks/p3-p31-throughput-64b-20261021-160125 --payload 64 \
  --output build/benchmarks/p3-p31-acceptance-20261021-155122/throughput-64b.json
python3 tools/aeron_compare/compare_p31_throughput.py \
  build/benchmarks/p3-p31-throughput-192b-20261021-1646 --payload 192 \
  --output build/benchmarks/p3-p31-acceptance-20261021-155122/throughput-192b.json
python3 tools/aeron_compare/compare_p31_latency.py \
  build/benchmarks/p3-p31-acceptance-20261021-155122/latency-attempt \
  build/benchmarks/p3-p31-acceptance-20261021-155122/latency-planned \
  --output build/benchmarks/p3-p31-acceptance-20261021-155122/latency.json
python3 tools/hft_acceptance/compare_p31_tail.py \
  build/benchmarks/p3-p31-acceptance-20261021-155122/hft-tail-formal
python3 -m unittest discover -s tools/hft_acceptance -p 'test_*.py'
```

原始日志、直方图与二进制留在 Git-ignored `build/benchmarks/`，不塞入文档、不清理失败轮次。

## 9. 总体结论

**P3-1 正式验收 FAIL，原因是 G4。** G1/G2/G3/G5 的通过和 G6 的记录仍然成立：
64 B 发布吞吐的屏障收益有统计与完整性证据，机制实现也通过正确性回归。但 HFT 长尾在执行前
固定的 5% 非劣规则下只有 7/24 个单元通过，且 S 臂有 4/48 轮原始输入未完成，
不满足 G4 的完成度和逐组 p99/p99.9 非劣要求。

因此不得宣称“延迟非劣”“长尾已解决”或“P3-1 全部门槛通过”。24 个组/配置单元中的 17 个失败不是
运行故障造成的假失败：96 个 worker 均正常退出，4,662 条未发布输入和 17 个失败单元均有原始证据。
后续若继续该方向，应另立实验定位 2P2S 长尾和 S 臂完成度问题，明确新假设与新门槛后再采集；
不得用本轮数据重挑轮次、跨组池化或改 5% 界限来追溯通过。工作区改动仍未提交、未打 tag、未推送。

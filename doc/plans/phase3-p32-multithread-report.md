# Phase 3 P3-2 多线程发布进程成本诊断

日期：2026-10-22，Asia/Singapore。
依据：[后续诊断计划](phase3-followup-diagnostics.md) §3。

## 1. 结论

本轮完成了预先固定的 ST/MT ABBA 矩阵，**144/144 个正式轮次成功**，没有超时、OOM、
worker failure、身份错误或输入不完整。ST 与 MT 使用同一二进制、同一 A 屏障模式和同一
命令行，只通过环境变量让 publisher 子进程保持单线程（ST），或增加一个休眠 helper
线程（MT）。

结果是描述性的，不是产品验收门禁：

- 64 B 1P2S 吞吐中位数：ST **8.849060 Mmsg/s**，MT **6.223405 Mmsg/s**；
  按臂中位数计 ST/MT **1.421900**；6/6 组方向一致。
- 256 B 1P2S 吞吐中位数：ST **16.298050 Mmsg/s**，MT **6.246215 Mmsg/s**；
  按臂中位数计 ST/MT **2.609268**；6/6 组方向一致。
- 短延迟没有呈现同样一致、同量级的全面退化：四种配置的 p50 中位数只增加
  **1.3%–5.4%**；p99/p99.9 的方向和幅度随配置变化，且轮间范围很大。

因此可以确认：**当前基准中，多线程 publisher 进程存在稳定的大幅吞吐成本**。
但不能把这整个差额归因于 `shared_ptr` 引用计数，也不能据此删除引用计数或改变 claim
生命周期。隔离实验尚未做，其他 libc/运行时状态和宿主波动仍是混杂因素。

## 2. 身份与矩阵

证据目录：
`build/benchmarks/p3-p32-multithread-20261022-104421/`。

| 项目 | 值 |
|---|---|
| protocol | `p3-p32-publisher-thread-probe-v1` |
| experiment.json SHA-256 | `f25a051188e35a3080e401f1b6a5345b96165997a534e54413b18f48b50eb1c6` |
| probe SHA-256 | `f6367f645710b5cb761c08b79e071f282c67d7873309fd63ca399db3ad7bed38` |
| 未修改 v3 benchmark 源码 | `v3.0.0:tools/aeron_compare/salias_ipc_compare.cpp` |
| v3 源码 SHA-256 | `6a23a77598c6e980f7b6625eb2471cc42a26917469dc77fc51ab9d5b767bab1e` |
| 生成源码 SHA-256 | `59e86a1aa13c81fe5e0e4db4fe02f0ecb1e02066ef568d548ff1a82a41cff462` |
| 产品 `libsalias.a` SHA-256 | `165d6adb1ba69b2b48c735a7cd3c470905689d9e51e3101c354c1c10a0545894` |
| 产品 `libsalias_core.a` SHA-256 | `ab30beceaa771fe9c596182d69a2293631c08c034f6b6096add4bbed02d8d720` |

矩阵在测量前固定为 6 个配置 × 6 组 × 4 轮 = 144 个正式轮次。每组顺序严格为
**ST、MT、MT、ST**；另有 2 个功能 smoke 和 12 个独立预热轮次，均不进入统计。

| 配置 | 拓扑 | payload | 每轮消息 | offered rate |
|---|---|---|---:|---:|
| throughput-64b | 1P2S | 64 B | 50,000,000 | 不限速 |
| throughput-256b | 1P2S | 256 B | 50,000,000 | 不限速 |
| latency-64b-attempt | 1P1S | 64 B | 2,000,000 | 1,000,000 msg/s |
| latency-64b-planned | 1P1S | 64 B | 2,000,000 | 1,000,000 msg/s |
| latency-256b-attempt | 1P1S | 256 B | 2,000,000 | 1,000,000 msg/s |
| latency-256b-planned | 1P1S | 256 B | 2,000,000 | 1,000,000 msg/s |

每轮使用 1 MiB ring、batch=1、poll-limit=64，并独立运行在
`systemd-run --user --wait --pipe --collect` 瞬态 unit 中：
AllowedCPUs=0-3、Nice=5、MemoryHigh=640 MiB、MemoryMax=704 MiB、swap=0。
coordinator 固定 CPU0；worker 从 CPU1 起按拓扑连续绑核。

## 3. 测量有效性

独立分析器重新核对了全部 144 个正式轮次，而不只读取 runner 的汇总：

- 全部记录的状态为 `ok`、退出码为 0，manifest 状态为 `complete`，完成计数 144/144。
- 每轮的 ST/MT 环境覆盖、ABBA 位置、命令、客户端 SHA、v3 输入 SHA、stdout/stderr/cgroup
  文件 SHA 均与记录一致。
- 每轮 cgroup 证据满足固定限制，`memory.events` 的 high/max/oom/oom_kill 均为 0，
  swap 使用为 0；144 轮均无 cgroup 压力失败。
- ST publisher 实际线程数为 **1**，`__libc_single_threaded=1`；
  MT publisher 实际线程数为 **2**，`__libc_single_threaded=0`。
- 所有吞吐轮发布 50,000,000 条、向两个 subscriber 共交付 100,000,000 条；
  所有延迟轮都产生 125,000 个采样，计数与固定采样间隔一致。

这些检查只证明模式切换和采集有效性，不证明 ST/MT 差异的产品根因。

## 4. 吞吐

每个配置每臂 12 轮，统计单位为 Mmsg/s。均值、标准差与 CV 按 12 个独立轮次计算；
组内比值使用该组两轮的中位数。

| 配置 | 臂 | 中位数 | 均值 | CV | 最小–最大 |
|---|---|---:|---:|---:|---:|
| 64 B | ST | 8.849060 | 9.993703 | 34.95% | 8.143160–20.887700 |
| 64 B | MT | 6.223405 | 6.287880 | 7.94% | 5.569630–7.344160 |
| 256 B | ST | 16.298050 | 16.472567 | 6.06% | 15.200200–19.366800 |
| 256 B | MT | 6.246215 | 6.408476 | 10.33% | 5.645440–8.211030 |

| 配置 | 组内 ST/MT 中位数比（组 0…5） | 组比中位数 | 方向 |
|---|---|---:|---:|
| 64 B | 1.465684 / 1.273778 / 2.463347 / 1.312012 / 1.497251 / 1.544898 | 1.481468 | 6/6 ST 更快 |
| 256 B | 2.175118 / 2.766969 / 2.764581 / 2.503102 / 2.833356 / 2.488382 | 2.633841 | 6/6 ST 更快 |

64 B 的 ST 受高模式离群轮影响更大，CV 为 34.95%，中位数8.849060 与均值9.993703
相差明显；MT 反而较稳定。因此 64 B 的组内比值范围较宽，但方向没有反转。
256 B 的 6 个组内比值均超过 2.17，方向更一致。

若把每组的 ST/MT 中位数方向做探索性精确符号检验，两个配置均为 6/6，单侧
`p=0.03125`。这不是预先登记的验收门禁，也不能替代独立隔离实验。

## 5. 短延迟

下表给出每臂 12 轮分位数的**中位数（最小值–最大值）**；不把不同轮次的原始样本池化。

| 配置/起点 | 指标 | ST 中位数（范围） | MT 中位数（范围） | MT/ST（中位数） |
|---|---|---:|---:|---:|
| 64 B attempt | p50 | 257（171–267） | 271（175–279） | 1.054 |
| 64 B attempt | p99 | 413（237–623） | 431（247–3,391） | 1.044 |
| 64 B attempt | p99.9 | 4,127（1,615–327,679） | 7,679（1,151–14,591） | 1.861 |
| 64 B attempt | max | 60,689（45,546–1,485,164） | 60,951（42,950–201,805） | 1.004 |
| 64 B planned | p50 | 307（299–319） | 319（247–335） | 1.039 |
| 64 B planned | p99 | 3,071（1,327–122,879） | 2,847（367–233,471） | 0.927 |
| 64 B planned | p99.9 | 13,311（6,719–1,949,695） | 12,415（6,911–1,196,031） | 0.933 |
| 64 B planned | max | 92,325（51,854–3,168,629） | 83,329（46,282–1,957,239） | 0.902 |
| 256 B attempt | p50 | 250（177–267） | 259（249–275） | 1.036 |
| 256 B attempt | p99 | 419（291–2,591） | 431（415–4,479） | 1.029 |
| 256 B attempt | p99.9 | 3,983（2,111–1,228,799） | 4,079（3,743–13,951） | 1.024 |
| 256 B attempt | max | 53,750（41,445–1,825,523） | 57,273（43,151–1,163,425） | 1.066 |
| 256 B planned | p50 | 311（299–323） | 315（303–327） | 1.013 |
| 256 B planned | p99 | 3,279（1,343–6,719） | 3,951（2,431–6,271） | 1.205 |
| 256 B planned | p99.9 | 11,711（6,655–15,871） | 13,183（9,471–15,231） | 1.126 |
| 256 B planned | max | 71,270（48,317–129,992） | 102,270.5（59,382–337,822） | 1.435 |

轮间尾部波动很大。例如 64 B attempt 的 ST p99.9 范围是 **1,615–327,679 ns**，
MT 是 **1,151–14,591 ns**；64 B planned 的 ST p99.9 最大达到 **1,949,695 ns**。
因此不能根据中位数比值声称 MT 的 p99.9 全面更差或更好。可复现的观察只有：
p50 在四种配置中均小幅高于 ST；尾部没有一致量级的方向。

## 6. 解释边界

ST/MT 是同一生成的 v3 benchmark 二进制，依赖同一当前产品库、同一命令行和同一 A
屏障模式。MT 相对 ST 的唯一主动差异，是 publisher 子进程在 fork 后、发布前创建了一个
休眠 helper，使进程实际变为双线程并使 libc 进入多线程状态。helper 不发布、不轮询、
不忙等，也不与共享对象交互。

该设计能回答“多线程 publisher 进程是否有成本”，但不能单独拆出以下因素：

- `try_claim` 成功时复制 endpoint `shared_ptr`、claim 析构时释放引用所触发的原子计数；
- glibc 在 `__libc_single_threaded=0` 后采用的其他原子/运行时路径；
- 运行中 live helper 线程对内核调度、内存管理或宿主噪声的间接影响；
- 宿主虚拟化与频率状态造成的轮次级波动。

尤其不能把 64 B 与 256 B 的差额比例直接当作每条消息的 `shared_ptr` 纳秒成本。
本机无 PMU，也没有同消息的原子指令、claim 生命周期或 off-CPU 同步 trace。

## 7. 后续建议

值得做的是**独立 E2 隔离实验**，而不是直接改产品：

1. 在诊断客户端中做一个零布局对照，使 claim 不再复制/释放 endpoint `shared_ptr`，
   但保持 publisher 在 claim 生命周期内存活；只比较同一 MT 进程内的“有/无引用计数”。
   该原型涉及生命周期假设，绝不能直接作为产品修复。
2. 增加“helper 创建后立即退出”的第三臂，区分 live helper 线程与 libc 永久多线程状态。
3. 若两者都指向引用计数，再单独设计不损害 claim 生命周期的产品候选，并由用户批准。

在完成这些隔离前，不应删除保活引用、缩短 claim 生命周期或宣称已经找到产品根因。
P3-1 正式验收仍保持 **FAIL（G4）**；本轮没有重跑或改写其结果。

## 8. 复现

```bash
python3 tools/aeron_compare/build_multithread_probe.py
python3 doc/plans/runners/p3-p32-multithread.py \
  --client build/p3-p32-multithread-probe/probe \
  --output build/benchmarks/p3-p32-multithread-20261022-104421
python3 tools/aeron_compare/analyze_p32_multithread.py \
  build/benchmarks/p3-p32-multithread-20261022-104421 --format markdown
```

本次保留的独立汇总位于同一证据目录：
`analysis.json`（SHA-256 `5fea3317006002b9ac1b40d3be5736e8d9fbb9ff2b6d534e03c11cd164b940d6`）
和 `analysis.md`（SHA-256 `bda745db6e82677f525b489b74a6b83806b6b63df0b4313ccaf2dd39bad4ca89`）。

相关测试：

```bash
python3 -m unittest tools/aeron_compare/test_multithread_probe.py
python3 -m unittest tools/aeron_compare/test_analyze_p32_multithread.py
```

本轮未修改 `src/`，未提交、打 tag 或推送。

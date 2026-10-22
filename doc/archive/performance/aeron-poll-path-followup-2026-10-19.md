# Salias / Aeron：保存优化后的轮询路径探索

> 历史归档：仅记录当时版本、配置与结论，不代表当前性能或验收状态。当前汇总及下一轮任务见[最终性能测试与优化交接报告](../../performance-report.md)。

## 结论

该次探索开始前，已将调用独占的未初始化 poll 描述符存储保存为本地提交
`f9ea5c2`：`refactor: 移除空轮询描述符清零并保留调用隔离`。
该提交只包含产品实现、对应测试、API 文档和 Unreleased changelog 共 8 个文件，
当时没有打 tag、没有 push，原有其他工作区改动未纳入提交；后续发布状态以
`docs/release_note.md` 为准。

随后在此基础上构建 **6 个新候选加 1 个对照**，完成独立初筛、跨负载确认和真实 perf
采样。**没有发现可以稳定继续缩小延迟差距、且改善尾部的新默认方案，因此没有再改产品代码。**

- 去掉轮转取模初筛 p50 改善 24 ns，但跨负载确认不能复现，部分负载明显回退。
- 把五组本地游标聚合成逐环结构，64 B / 1M/s 初筛 p50 反而增加 92 ns。
- 缓存固定注册表形状和指针，没有稳定收益；与游标聚合组合后仍是混合结果。
- 延后 pending-progress 扫描及其组合，没有可靠的整体收益，不推进为默认实现。
- perf 继续指向接收轮询路径，但不支持把约 100 ns 全部归因于取模或缓存行。
  原有[调度跟踪](aeron-design-diagnosis-2026-10-19.md#三尾部的真实高收益因素)中，
  最慢事件的大部分时间与 off-CPU 重合；这些局部重写不能解决此类停顿。

## 范围与口径

产品基线为上述已提交优化，**不是清零优化之前的旧版**。候选只生成在
`build/benchmarks/poll-path-20261019/`，没有修改共享段布局、帧格式、原子内存序，
没有删除注册代际检查，也没有使用 FIFO 调度或跳过 authoritative visible bound。

环境沿用 4 vCPU Xeon Platinum 8255C KVM、GCC 14.2、O3/native/LTO、Aeron 1.52.0。
消费者 CPU1、发布者 CPU2、Aeron SHARED/backoff driver CPU3；不使用 CPU0。
构建和基准串行运行，测量进程受 704 MiB cgroup、零 swap、超时、余量检查和进程组清理约束。
逐轮 `memory.events` 的 high/max/oom/oom_kill 均为零，最低观测 MemAvailable 为 1511.4 MiB。

性能矩阵均为 1P1S；每格独立预跑一轮，初筛 3 轮、确认 5 轮，每轮 2 s。
`attempt` 从首次 claim 前开始，`planned` 从原始开环排期开始，落后不重置排期。
表格是**独立轮次分位数的中位数，不是合并直方图分位数**；异常轮次全部保留。
采样 1/16，直方图约 1.56% 桶精度。64/256 B 是逻辑 payload，实际只写时间戳和 marker
共 16 B，不能当作完整 payload 构造/校验基准。固定采样周期仍有相位偏差风险。

非 profile 正式总体共 **165 次运行、2.58 亿消息、1612.5 万延迟样本**。
预跑与 profile 均不进入该总体；不同口径和负载不合并解释。
持久[汇总](../../benchmarks/2026-10-19/poll-path-followup/summary.json)保留所有格子的中位数、
范围、命令、哈希、资源计数和测试记录；各子目录入库逐轮 JSONL，完整 stdout/stderr
仅本地保留。提交证据范围见[证据说明](../../benchmarks/2026-10-19/README.md)。

## 候选实现

| 二进制名 / 数据标签 | 改动 | 保留的关键契约 |
|---|---|---|
| `control` / `baseline` | 当前产品副本，核心头文件字节不变 | 当前产品全部同步与验证 |
| `deferred-progress` / `deferred` | 首个可读环出现时才快照 pending progress | 在任何读取推进前快照，后续环共用该值 |
| `branch-scan` / `branch` | 递增并比较回绕，代替两处动态取模 | 轮转顺序、公平性和批量边界不变 |
| `combined` | 上述两者组合 | 不改变消息借用与 TTL 前缀发布时机 |
| `cursor-pack` / `pack` | 五个 `vector<uint64_t>` 合并为 `vector<RingCursor>` | 只改 Rx 本地状态，五个字段的语义不变 |
| `cached-registration` / `cached` | 缓存固定表形状和 registration-state 指针 | 每次仍读取 consumer_count，并 acquire 核验代际 token |
| `layout-cached` / `layout` | 本地游标聚合加注册定位缓存 | 共享内存布局和发布协议不变 |

## 一、轮询控制流

### 初筛不能当作验收

`screen`，64 B / 1M/s，attempt，3 个正式轮次：

| 实现 | p50 ns | p99 ns | p99.9 us |
|---|---:|---:|---:|
| 当前 Salias | 283 | 459 | 4.991 |
| 延后 pending 扫描 | 279 | 479 | 5.247 |
| 比较回绕代替取模 | 259 | 423 | 4.031 |
| 两者组合 | 267 | 423 | 6.207 |
| Aeron | 187 | 287 | 9.599 |

单看这一组，去取模有希望；组合却没有叠加收益，p99.9 还变差。
这组 Aeron 的 p50/p99 更低，但 p99.9 高于 Salias，不能混称为所有尾部分位都有巨大差距。

### 去取模跨负载确认

`confirm`，每格 5 轮；新实现指 `branch`：

| 口径 | 速率/s | payload B | 对照 p50 ns | 新 p50 ns | 对照 p99.9 us | 新 p99.9 us |
|---|---:|---:|---:|---:|---:|---:|
| attempt | 100k | 64 | 291 | 343 | 3.199 | 4.223 |
| attempt | 100k | 256 | 307 | 315 | 3.167 | 2.623 |
| attempt | 1M | 64 | 267 | 263 | 4.063 | 4.223 |
| attempt | 1M | 256 | 279 | 331 | 4.223 | 4.351 |
| planned | 100k | 64 | 351 | 363 | 9.855 | 4.031 |
| planned | 100k | 256 | 355 | 383 | 8.319 | 7.487 |
| planned | 1M | 64 | 335 | 323 | 11.135 | 11.391 |
| planned | 1M | 256 | 335 | 323 | 17.919 | 14.975 |

初筛的 24 ns 没有稳定复现：确认组高速 64 B 只有 4 ns，低速 64 B 和高速 256 B
p50 分别回退 52 ns。planned 部分 p99.9 改善，不足以推导为通用收益。

最差轮也不能隐藏：attempt 1M/s / 64 B 的新/对照 p99.9 为 137.215/7.743 us，
256 B 为 102.399/4.927 us；反过来，planned 1M/s / 64 B 的对照/新最差轮
为 966.655/12.031 us。没有这些事件的同步 trace，不能断言尖峰由源码改动造成，
也不能只选择对自己有利的一侧。**结论是不合入，而非宣称取模无成本。**

## 二、本地布局与注册定位

### 布局初筛

`layout-screen`，64 B / 1M/s，attempt，3 轮：

| 实现 | p50 ns | p99 ns | p99.9 us |
|---|---:|---:|---:|
| 当前 Salias | 279 | 443 | 3.871 |
| 聚合游标 | 371 | 459 | 4.159 |
| 缓存注册定位 | 279 | 463 | 6.399 |
| 两者组合 | 267 | 435 | 4.223 |
| Aeron | 187 | 315 | 9.727 |

游标聚合减少了分散的本地分配，但没有成为端到端收益。组合出现过单轮 p50=177 ns，
其余两轮为 295/267 ns，**不能挑出 177 ns 宣称已经超越 Aeron**。
planned 初筛也保留在汇总中：对照/聚合/缓存/组合/Aeron p50 为
245/443/335/323/243 ns，p99.9 为 10.239/13.439/10.367/7.935/12.415 us。
不同口径独立测量，不能用这些 p50 相减解释阶段成本。

### 组合确认

`layout-confirm`，1M/s，每格 5 轮；新实现指 `layout`：

| 口径 | payload B | 对照 p50 ns | 新 p50 ns | 对照 p99 ns | 新 p99 ns | 对照 p99.9 us | 新 p99.9 us |
|---|---:|---:|---:|---:|---:|---:|---:|
| attempt | 64 | 275 | 279 | 435 | 447 | 3.999 | 4.863 |
| attempt | 256 | 283 | 263 | 491 | 427 | 4.223 | 4.223 |
| planned | 64 | 335 | 323 | 3679 | 3967 | 11.007 | 13.311 |
| planned | 256 | 335 | 335 | 2623 | 4287 | 14.335 | 12.031 |

256 B attempt 有 20 ns p50 改善，但 64 B attempt 的 p50/p99/p99.9 同时回退，
64 B planned p99.9 也变差；没有一致的尾部改善。attempt 64 B 新/对照最差轮
p99.9 为 12.671/4.287 us；planned 256 B 对照/新最差轮为 511.999/14.975 us。
这些结果全部保留。**不把组合初筛的有利结果推广为产品默认布局。**

## 三、真实 perf 与反汇编

在单独目录对 control、pack、layout 各运行一次预跑和一次正式 profile，每例 3 s，
`cpu-clock` 199 Hz、DWARF、switch events。profile 延迟不进入前述性能表。

| 正式 profile | 消费者样本 | `Rx::try_recv_run` 样本/占比 | 未解析顶层样本占比 | FIFO 函数机器码大小 B |
|---|---:|---:|---:|---:|
| control | 599 | 300 / 50.08% | 37.56% | 2015 |
| pack | 599 | 316 / 52.75% | 37.23% | 2050 |
| layout | 599 | 279 / 46.58% | 44.07% | 1977 |

真实火焰图：[control](../../benchmarks/2026-10-19/poll-path-followup/profiles/006-baseline-normal-attempt-1000000-64-r1.svg)、
[pack](../../benchmarks/2026-10-19/poll-path-followup/profiles/004-pack-normal-attempt-1000000-64-r1.svg)、
[layout](../../benchmarks/2026-10-19/poll-path-followup/profiles/005-layout-normal-attempt-1000000-64-r1.svg)。
同目录另有 `top.json`、`report.txt`、`annotate.txt` 和 folded stacks；原始 `perf.data`
保留在 `build/benchmarks/poll-path-20261019/profiles/`。

反汇编能确认的结构差异：

- 对照仍有动态 `divq`；采样集中在 visible 水位读取附近和取模后的指令附近。
  这说明值得做上述控制流 A/B，不意味着采样比例等于该条指令的精确耗时。
- `RingCursor` 步长为 40 B，需要新的索引地址计算；编译器也改变了寄存器分配、
  合并字段写入和函数布局。FIFO 函数栈预留从 184 B 变成 200 B，机器码并未简单缩短。
- 注册定位缓存确实缩短了入口表遍历，但仍保留动态 consumer_count 和 acquire token
  检查。布局加缓存的源码、代码生成和布局效果不是两个独立收益的简单相加。

这些是可验证的机器码事实，**不是对 pack 增加 92 ns 的唯一因果解释**。
40 B 结构也不保证落在单条缓存行；减少本地分配数量不等于减少跨核一致性流量。
该 VM 未暴露可用硬件 PMU，不能给出 HITM、LLC miss、IPC 或 stall-cycle 定量结论。
约 600 个消费者样本且有较多未解析地址，也不足以精确归因剩余约 100 ns。

## 验证与取舍

- 产品 Release CTest 重新运行 **21/21 通过**；提交后产品源码保持不变。
- 六个候选及对照各自重新链接并通过完整 **96 项 API 测试**，共 7 个独立实现；
  测试覆盖公共 API、TTL、注册、回绕和借用等，不能将其等同于并发协议的形式证明。
- 每个候选构建时的独立存储契约测试通过，覆盖全部 5 种模式和两种 poll 重载。
- 已提交优化的独立 **ASan + UBSan** 存储契约再次通过；没有声称全部候选跑过完整 sanitizer API 套件。
- Python 工具测试 **23/23 通过**，包含本轮 7 项测试，验证改写范围、轮转等价、
  同步护栏、profile 排除、输出/二进制篡改拒绝、投递/采样计数和资源门禁。
- 导出时逐轮核验实际 stdout 与 JSON 结果一致、stdout/二进制哈希一致、投递和采样数正确、
  内存压力事件为零；完整候选源码快照及 API 测试原始记录本地保留，入库精简验证记录和
  源码哈希。`git diff --check` 通过。

本轮性能仍仅覆盖 1P1S、短轮次和共享 VM；未做 2P2S 性能、完整 payload 校验、
长时 SLA 或独占裸机 PMU 验收。下一步最高优先级仍是受控 CPU/宿主资源下的尾事件关联，
其次是在保留注册、乱序 commit 和变长回绕安全性的前提下缩短发布到可见的依赖链。
不继续以盲目 padding、去 guard 或某次有利的 p50 代替高收益证据。

## 复现

从仓库根目录运行；先准备 Release 库和测试对象、已有 Aeron 构建，参照
[受限构建说明](../../../tools/aeron_compare/README.md#bounded-build)。以下所有命令串行执行，
不要在测量窗口编译。使用新目录，避免覆盖已测二进制或向旧结果追加：

```bash
EXP="$PWD/build/benchmarks/poll-path-repro-$(date +%Y%m%d-%H%M%S)"
AERON="$PWD/build/benchmarks/aeron-latency-20261018"
capped_build() {
  systemd-run --user --wait --pipe --working-directory="$PWD" \
    -p 'CPUAffinity=2 3' -p Nice=10 -p MemoryHigh=600M -p MemoryMax=768M \
    -p MemorySwapMax=0 -p TasksMax=32 -p RuntimeMaxSec=600s \
    -p KillMode=control-group "$@"
}
capped_run() {
  systemd-run --user --wait --pipe --working-directory="$PWD" \
    -p 'CPUAffinity=1 2 3' -p Nice=5 -p MemoryHigh=640M -p MemoryMax=704M \
    -p MemorySwapMax=0 -p TasksMax=48 -p RuntimeMaxSec=600s \
    -p KillMode=control-group -p OOMPolicy=stop \
    /usr/bin/env SALIAS_RESOURCE_CAPPED=1 "$@"
}
capped_build cmake --build build/release --target salias_api_tests -j1
capped_build python3 tools/aeron_compare/build_poll_path_variants.py \
  --output "$EXP" --variants control deferred-progress branch-scan combined \
  cursor-pack cached-registration layout-cached
capped_build python3 tools/aeron_compare/build_poll_path_variants.py \
  --output "$EXP" --variants control deferred-progress branch-scan combined \
  cursor-pack cached-registration layout-cached --verify-api-only

capped_run python3 tools/aeron_compare/diagnose_design.py \
  --output "$EXP/confirm" --baseline "$EXP/bin/control" \
  --variant branch="$EXP/bin/branch-scan" --cases baseline branch \
  --aeron-client "$AERON/aeron_ipc_compare" --aeron-build "$AERON/deps/aeron-build" \
  --origins attempt planned --rates 100000 1000000 --payloads 64 256 \
  --seconds 2 --rounds 5
capped_run python3 tools/aeron_compare/diagnose_design.py \
  --output "$EXP/layout-confirm" --baseline "$EXP/bin/control" \
  --variant layout="$EXP/bin/layout-cached" --cases baseline layout \
  --aeron-client "$AERON/aeron_ipc_compare" --aeron-build "$AERON/deps/aeron-build" \
  --origins attempt planned --rates 1000000 --payloads 64 256 --seconds 2 --rounds 5

# 单独 profile，绝不将其分位数混入无采样运行。
capped_run python3 tools/aeron_compare/diagnose_design.py \
  --output "$EXP/profiles" --baseline "$EXP/bin/control" \
  --variant pack="$EXP/bin/cursor-pack" --variant layout="$EXP/bin/layout-cached" \
  --cases baseline pack layout --aeron-client "$AERON/aeron_ipc_compare" \
  --aeron-build "$AERON/deps/aeron-build" --origins attempt \
  --rates 1000000 --payloads 64 --seconds 3 --rounds 1 --profile
python3 tools/aeron_compare/render_profiles.py --directory "$EXP/profiles" \
  --flamegraph build/benchmarks/design-diagnosis-20261019/FlameGraph
python3 tools/aeron_compare/export_poll_path_evidence.py \
  --source "$EXP" --output "$EXP/evidence"
python3 -m unittest discover -s tools/aeron_compare -p 'test*.py'
```

初筛的精确运行命令保存在各数据目录的 `results.jsonl`。复现时需要既有 perf 权限；
不修改 sysctl，不关闭 RT 限流。正式实验源码副本仅保留在本地实验目录，源码哈希见
持久证据的 `summary.json`；完整编译与验证入口为 `build_poll_path_variants.py`。

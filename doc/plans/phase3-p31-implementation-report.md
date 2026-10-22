# P3-1 实现报告：发布路径非对称屏障

**日期**：2026-10-21（Asia/Singapore）
**依据**：`doc/plans/phase3-final-optimizations.md` §5、§9.2；E1 结论 GO（`doc/plans/phase3-e1-fence-report.md`）
**状态**：实现与回归完成，**未提交、未打 tag、未推送**。正式验收（§6 的 G1–G6）尚未开始。

接续说明：上句及 §8 记录的是实现会话结束时的状态。后续已进入正式验收，
最新门槛结果、原始证据与限制以 [P3-1 验收报告](phase3-p31-acceptance-report.md) 为准。

## 1. 结论摘要

- claim 热路径上原有的两条 seq_cst store（x86 上两次完整屏障）在 `claim_fence_protocol=1`
  的通道上被替换为：一次 relaxed store + 编译器屏障 + 一次 release store。
  实测反汇编中该路径**没有** `xchg`/`lock`/`mfence`。
- 完整屏障移到三个冷路径：`acquire_consumer`、`release_consumer`、
  `try_arm_wait_notification`（仅 sticky 通知激活前，每个通道至多一次）。
- 动态注册、代际回收、死亡回收、最慢订阅者背压、TTL、借用期、帧格式与 ring 布局均未改动。
- 协议升级 v14 → v15；控制段 `sizeof(NamedControl)` 与 `global_sequence` 偏移逐字节不变
  （均已加 `static_assert`）。
- Release CTest 22/22、ASan/UBSan 21/21、TSan 21/21、Aeron 工具 126 项（3 跳过）、
  HFT summary 36 项全部通过。

## 2. 改动文件

新增：

| 文件 | 作用 |
|---|---|
| `src/core/platform/asymmetric_fence.hpp` | light/heavy fence、进程级一次性注册、能力查询、运维开关、测试钩子 |
| `test/api/asymmetric_fence_test.cpp` | 跨进程压力、fork、故障注入、协议边界用例 |

修改：

| 文件 | 改动 |
|---|---|
| `src/core/channel/shared_hybrid_mpsc.hpp` | `SharedControl::claim_fence_protocol`、`ClaimReservationGuard` 双路径、`Tx` 进程注册与缓存、`try_arm_wait_notification` heavy fence |
| `src/salias/src/channel.cpp` | `kNamedVersion` 14→15、`NamedControl::claim_fence_protocol`、布局断言、建表写入与连接校验、注册/注销 heavy fence |
| `test/CMakeLists.txt` | 新增 `salias_asymmetric_fence_tests`（含 `SALIAS_TEST_HOOKS=1` 与 `src/` 头路径） |
| `README.md` | 协议说明升至 v15 并补充 fence 协议语义 |
| `CHANGELOG.md` | Unreleased 条目 |
| `doc/plans/session-state.json` | 接续状态更新 |

`src/salias/src/channel.cpp` 中 `PublishClaim` 构造函数原有的纯缩进差异被保留，未做回退。

## 3. 不变式与论证

### 3.1 需要保持的不变式

对任意注册操作 R 与任意 claim C，二者必居其一：

1. C 读到 `gate != 0`，于是写回 `claim_active=0` 并返回 `BackPressured`；或
2. R 在 heavy fence 之后读到 `claim_active == 1`，于是等待 C 结束。

并且 R 看到 `claim_active == 0` 时必须能读到 C 写入的 reservation。
`wait_for_claims()` 的陈旧 `claim_active` 清理（进程已死则 CAS 归零）保持原样，未修改。

### 3.2 非对称路径为什么仍然成立

发布端（light）：`active=1 (relaxed)` → 编译器屏障 → 读 `gate (relaxed)`。
注册端（heavy）：写 `gate` → `membarrier(GLOBAL_EXPEDITED)` → 读 `active (seq_cst/acquire)`。

- `membarrier` 在调用线程上等价于一次完整屏障（内核在发 IPI 前执行 `smp_mb()`），
  因此 R 写下的 `gate` 在 IPI 到达前已全局可见。
- 已注册进程（本实现的每个发布进程）的线程会被 IPI 强制在返回前执行一次核心同步：
  若发布线程的 `gate` 读发生在该同步之前，则它先前的 `active=1` 必然在 `membarrier`
  返回前对 R 可见；若同步先到，则发布线程随后的 `gate` 读必然看到已置位的 `gate` 并放弃本次 claim。
- 两种交错都不可能让“C 读到 `gate==0`”与“R 读到 `active==0`”同时成立。这与两次
  seq_cst store 的 Dekker 握手等价。
- 清除侧：C 用 `release` 写 `active=0`，R 用 acquire（`seq_cst` load）读 `claim_active`，
  因此 `active==0` ⇒ C 的 reservation 写入可见。

### 3.3 依赖与边界

- 前提是发布进程已成功 `MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED`。`Tx` 构造时按进程一次性注册；
  失败或不支持时该进程全部 Publisher 退回原 seq_cst 路径（两条屏障都在），只有性能差异，没有正确性差异。
- 注册端不需要自己注册，只需要调用 `membarrier`；失败时 fail closed（见 §4）。
- fork 语义：注册位随 mm 复制，子进程 `GET_REGISTRATIONS` 仍报告注册（本机实测为 `4`，
  即 `MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED` 位）；`exec` 换 mm 后需要重新注册，
  状态机在子进程里会因 owner pid 变化重新执行。用例 `AsymmetricFenceForkTest.ChildPublisherAfterFork`
  覆盖了 fork + 子进程重新创建 Publisher 的场景。
- 非 Linux 平台不选择该协议，热路径与原实现一致；ARM 上的收益与 membarrier 语义**待验证**
  （本机为 x86_64 TSO）。

## 4. heavy fence 位置（全部）

`grep -n "registration_gate|claim_active"` 复核后，只有以下三处存在“写/持有 gate 后读
`claim_active`”的时序：

| 位置 | 说明 | 失败行为 |
|---|---|---|
| `channel.cpp:1006` `acquire_consumer` | `acquire_registration_gate` 之后、`wait_for_claims` 之前 | 返回 `Error::PlatformFail`，gate 由 `RegistrationGateGuard` 释放 |
| `channel.cpp:1233` `release_consumer` | 同上 | 直接返回，不做任何槽位清理（fail closed，注册保持有效） |
| `shared_hybrid_mpsc.hpp:283` `try_arm_wait_notification` | gate CAS 成功之后、读 `claim_active` 之前 | 释放 gate、返回 `false`，调用方退回有界轮询 |

- 归一化入口：`enter_registration_barrier()`（`channel.cpp:1170`）只在协议为 1 时调用
  `platform::fence::heavy_fence()`；协议为 0 时返回 `true`，与 v14 行为完全一致。
- `try_arm_wait_notification` 只在 `!wait_.needs_wake()`（sticky 激活前）执行，属于冷路径；
  激活后该段整体跳过，稳态阻塞接收不再调用 heavy fence。
- 发布端只有 `ClaimReservationGuard` 一处（`claim` 与 `claim_batch` 共用），两处调用点均已更新。

## 5. light 路径的机器码验证

用 `-O2 -march=native` 编译一个只调用 `Tx::claim` / `Tx::claim_batch` 的翻译单元并反汇编：

- `claim`：`c9: test %r11b,%r11b` → `jne f0`（fence 协议分支）。
  light 分支为 `f0: movl $0x1,(%r9)` → 读 gate → `movl $0x0,(%r9)`；
  seq_cst 分支仍是 `d3: xchg %edx,(%r9)` / `e5: xchg %eax,(%r9)`。
- 析构：`153: test %r11b,%r11b` → `jne 340`；`340: movl $0x0,(%r9)`（release，无屏障）。
- `claim_batch` 同构（`330: movl $0x1,(%r11)`、`34c`/`360: movl $0x0,(%r11)`）。

**实现期间发现的编译器陷阱（已修正）**：若把析构写成
`store(0, asymmetric_ ? release : seq_cst)` 这样的条件表达式，GCC 会把两个分支合并成
单条 `xchg`（用更强的 seq_cst 实现 release 是合法优化），light 路径因此仍会付一次屏障。
最小复现：

```
G g(p, s);   // s==true 分支
20: movl   $0x1,(%rdi)
26: jmp    10
10: xor    %eax,%eax
12: xchg   %eax,(%rdi)     <-- 条件表达式写法
```

改成提前返回（`if (!owns_active_) return; if (asymmetric_) {store(release); return;} store(seq_cst);`）
后，light 分支变为两条普通 `movl`。代码里已加注释说明不能改回三目写法。

## 6. 协议与开关

- `NamedControl::claim_fence_protocol`（`offsetof == 104`，`sizeof(NamedControl) == 14'208`）：
  `0` = seq_cst（与 v14 语义一致），`1` = asymmetric（membarrier）。建表端在 `ready=1` 之前写入。
- 选择条件（仅建表端）：Linux、`MEMBARRIER_CMD_QUERY` 同时支持 `GLOBAL_EXPEDITED` 与
  `REGISTER_GLOBAL_EXPEDITED`、未设置 `SALIAS_DISABLE_ASYMMETRIC_FENCE=1`。
- 连接端：`version != 15` → `VersionMismatch`；`claim_fence_protocol > 1` → `BadConfig`
  （不依赖版本号单独拒绝未知取值）。
- `SALIAS_DISABLE_ASYMMETRIC_FENCE=1` 只影响建表端选择，不改变已建通道的 heavy side 行为，
  避免出现“只拆一侧屏障”的半成品状态。正式 A/B 用同一二进制跑 C-seq 与 C-asym 两臂。
- 故障注入开关（`SALIAS_TEST_HOOKS`）：`set_heavy_fence_failure_for_testing`、
  `set_registration_failure_for_testing`、`reset_registration_state_for_testing`、
  `clear_failure_injection_for_testing`。被检查的原子变量在所有构建中都存在（生产构建恒为
  `false`，只表现为冷路径上一次 relaxed load），只有 setter 受 `SALIAS_TEST_HOOKS` 保护，
  这样库里的 `channel.cpp` 与测试 TU 共享同一份状态。

## 7. 测试结果

### 7.1 新增用例（`salias_asymmetric_fence_tests`，8 项）

| 用例 | 覆盖 | 结果 |
|---|---|---|
| `Protocols/AsymmetricFenceStressTest/Asymmetric` | 1 发布进程持续 claim/commit + 1 进程 10,000 次 acquire/release，每会话校验序号连续 | 通过（279 ms） |
| `Protocols/AsymmetricFenceStressTest/ForcedSeqCst` | 同上，`SALIAS_DISABLE_ASYMMETRIC_FENCE=1` | 通过（218 ms） |
| `AsymmetricFenceForkTest.ChildPublisherAfterFork` | fork 继承注册位 + 子进程重建 Publisher 发布 64 条 | 通过 |
| `AsymmetricFenceFaultTest.HeavyFenceFailureFailsClosed` | heavy fence 失败 → `PlatformFail`；恢复后无消息损坏 | 通过 |
| `AsymmetricFenceFaultTest.RegistrationFailureFallsBackToSeqCst` | 发布端注册失败 → 退回 seq_cst 仍正常收发 | 通过 |
| `AsymmetricFenceFaultTest.ReleaseFailureKeepsRegistration` | 注销路径 fail closed：槽位仍 Active、`NoSubscriberSlots` | 通过 |
| `AsymmetricFenceProtocolTest.RejectsLegacyVersionAndUnknownProtocol` | v14 → `VersionMismatch`；协议值 2 → `BadConfig` | 通过 |
| `AsymmetricFenceProtocolTest.ProtocolSelectionFollowsEnvironment` | 运维开关与能力查询的选择矩阵 | 通过 |

### 7.2 回归

| 套件 | 结果 |
|---|---|
| Release CTest（`build/release`） | 22/22 通过（原 21 项 + 新 1 项） |
| ASan/UBSan（`build/debug-asan-ubsan`） | 21/21 通过 |
| TSan（`build/tsan`） | 21/21 通过 |
| `python3 -m unittest discover -s tools/aeron_compare` | 126 项通过，3 项跳过 |
| `python3 -m unittest discover -s tools/hft_acceptance` | 36 项通过 |

注：计划中写的“CTest 21 项 / Aeron 23 项”是 E1 时的计数；本轮实际计数为 21→22、126 与 36，
以实测为准。

### 7.3 实现过程中出现过的失败（原文保留）

1. `AsymmetricFenceForkTest.ChildPublisherAfterFork` 首轮失败：
   `child exited with code 7`。原因是用例假设 `GET_REGISTRATIONS` 返回
   `MEMBARRIER_CMD_GLOBAL_EXPEDITED`（`2`），实际返回注册命令位
   `MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED`（`4`）；本机探针确认父/子进程均为 `4`。已改用正确位。
2. `registered_commands()` 恒返回 `0`：`MEMBARRIER_CMD_GET_REGISTRATIONS` 是枚举常量而非宏，
   `#if defined(...)` 永远为假，函数落到了 `return 0u` 分支。改为直接使用等值字面量 `1 << 9`。
3. light 路径仍有一条 `xchg`：见 §5 的编译器陷阱，改用提前返回结构后消失。
4. `AsymmetricFenceFaultTest.RegistrationFailureFallsBackToSeqCst` 首轮失败：
   `publisher_light_path_ready()` 期望 `false` 实际 `true`。原因是同一进程内更早的用例已经注册，
   注册状态是进程级的。用例改为先用 `reset_registration_state_for_testing()` 复位用户态状态机再注入失败。

### 7.4 端到端冒烟（不是正式测量）

用 `build/release/bench/salias_ipc_compare`（链接本轮改动后的产品库）跑 1P2S / 64 B /
10M 消息，默认容量 4 MiB，各臂 3 轮交替：

| 轮次 | 默认（asymmetric） | `SALIAS_DISABLE_ASYMMETRIC_FENCE=1` |
|---|---|---|
| 1 | 8.803 Mmsg/s | 5.260 Mmsg/s |
| 2 | 6.990 Mmsg/s | 5.420 Mmsg/s |
| 3 | 7.109 Mmsg/s | 4.883 Mmsg/s |

中位数 7.109 对 5.260（比值约 1.35），方向与 E1 的 1.40 一致；`delivered` 计数每轮均为
20,000,000，无丢失或重复。**这组数字不能用于对外结论**：轮次太少、容量与冻结客户端不同
（4 MiB 对 1 MiB）、未做 ABBA 分块与置换检验。正式判定见 §6。

## 8. 未做与下一步

- 未提交、未打 tag、未推送；工作区其它既存改动未回退。
- 正式验收（`doc/plans/phase3-final-optimizations.md` §6）未开始：需要 B / C-seq / C-asym 三臂、
  `--setenv` 透传、G1–G6 判定，其中 G6（注册耗时）本轮只做了正确性验证，未测耗时。
- TSan 无法建模 `membarrier`，正确性主证据是 §7.1 的跨进程压力与故障注入用例；
  ARM 平台行为待验证。

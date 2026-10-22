# Salias Phase 3 最终优化计划：发布路径非对称屏障（P3-1）

**创建日期**：2026-10-21（Asia/Singapore）
**状态**：用户已批准；E1 GO、P3-1 实现完成，§9.3 正式验收已完成，**整体结论 FAIL（G4）**。
G1/G2/G3/G5 通过，G4 的 HFT 长尾与输入完成度未通过，G6 已记录；完整结果见
[验收报告](phase3-p31-acceptance-report.md)。未提交、未打 tag、未推送。
**基线**：v3.0.0 / `c5e6aa7`，冻结客户端 SHA-256 `3f8fc695...`

## 0. 结论

唯一推荐方案：**用非对称屏障（Linux `membarrier`）替换 `ClaimReservationGuard` 在每次 claim 上执行的两条完整屏障**，
把注册握手的同步成本从热路径（每条消息）移到冷路径（订阅者注册/注销）。动态注册、代际回收、
最慢订阅者背压、TTL、借用期、递归隔离与变长回绕语义全部不变。

执行顺序是先判定、后实施：

1. **E1（约 0.5 天，机器约 1.5 小时）**：对冻结客户端做 4 字节补丁，只把这两条屏障指令换成普通
   store，指令长度和地址完全不变。这样可以在**零布局变化**下测出屏障的真实成本，
   绕开 P2 全部实验都被干扰的二进制布局问题。
2. **E1 达到预先登记的 GO 门槛**，才实施 P3-1 产品改动并做正式 A/B；**NO-GO** 则停止，不改产品。

立项时本方案没有已证实的收益，§2 和 §4 保留当时的机制推断与 E1 门槛。
后续实测见 [E1 报告](phase3-e1-fence-report.md) 与正式验收报告；吞吐达标不等于长尾验收通过。

## 1. 证据链

| # | 事实 | 来源 |
|---|---|---|
| 1 | 64 B 1P2S 受限于发布端：背压 0.00033%，消费者空 poll 20.4%（在等数据） | `diagnostic-20261019-195710` |
| 2 | 64 B 每条消息周期约 154 ns（6.489 Mmsg/s）；timed claim 186–209 ns（含计时开销），与整个周期同量级 | 延长基线、Phase 1.5 |
| 3 | 冻结客户端 Fifo `Tx::claim`（`0x231a0`，FifoMpsc/FifoMpmc 共用）的成功路径执行 **2 条 `xchg`**：`claim_active` 置 1 与清 0 的 seq_cst store。x86 上每条都是完整屏障 | 本次 objdump |
| 4 | Aeron 比较器的 `aeron_exclusive_publication_try_claim`、`aeron_buffer_claim_commit`、`aeron_image_poll` 均为 **0 条**加锁/屏障指令 | 本次 objdump |
| 5 | Salias 消费端 `Rx::try_recv_run`（Fifo）没有加锁指令；出现的 `xchg %ax,%ax` 是 NOP | 本次 objdump |
| 6 | 这两条屏障只服务注册 Dekker 握手（`registration_gate` 与 `claim_active`），而注册是冷路径。v3 benchmark 在发令前完成全部订阅注册 | `shared_hybrid_mpsc.hpp:399-438`，`channel.cpp:1115-1170` |
| 7 | 每次 claim 写 hot line（`local_sequence`、`reservation_*`），commit 写同一行的 `visible_producer_pos`，而两个消费者在轮询这一行 | `channel.cpp:131-140`，`shared_hybrid_mpsc.hpp:884-890,1135-1142,1291` |
| 8 | 本机 `membarrier` 可用：`GLOBAL_EXPEDITED` 注册成功，单次 571/629 ns（min/avg，n=2000） | 本次探针 |
| 9 | 二进制布局极度敏感（S.2.2、P2-C），任何重编译 A/B 都会混入布局效应 | P2 报告 |
| 10 | 4 字节补丁（`41 87` → `41 89`，`xchg` → `mov`）只改这 4 条指令：`cmp` 恰好 4 字节差异，反汇编只有这 4 行变化。补丁 SHA-256 为 `5efae0b41ed42f9a38d69997ed8a1fafc69d78ae69eea948c79abbdec0f433b6` | 本次验证，临时文件已删 |

## 2. 机制推断与可证伪预测

**推断（本机无 PMU，未经硬件计数器证实）**：没有屏障时，发布者写 hot line 和 ring 行
（消费者轮询会让这些行处于 Shared 状态，所以每次写都要 RFO）由 store buffer 异步完成。
每次 claim 开头的 `xchg` 迫使发布者等上一条 commit 的 `visible_producer_pos` RFO 等全部先前 store 完成，
于是**每条消息至少串行一次跨核 cache line 传输**。这与事实 1、2 一致：系统受限于发布端，
且 claim 耗时约等于整个消息周期。Aeron 热路径没有屏障（事实 4）。

记忆中的 “Guard 只有 14.79 ns” 来自无竞争 micro-benchmark，测的是 store buffer 为空时的屏障，
不覆盖这里的竞争场景，所以不能用来否定本推断。早期 “visible/reservation 分行只快 4 ns”
是在屏障仍存在、1M msg/s 的延迟场景下测的，同样不构成反证。

**预测（E1 用于证伪）**：

- P-a：64 B 1P2S 吞吐显著上升（门槛见 §4.4）。
- P-b：1M msg/s 的 attempt p50 变化很小，因为消息间隔内 store buffer 已排空。若 p50 也大幅下降，
  说明机制与上述推断不同，需要在报告中写明。
- P-c（只记录，不设门槛）：192 B 的高/低双模态减弱。

## 3. 撤回的旧版方案

本文件上一版的 “Aeron driver 移植” 方案作废：

- Aeron IPC 的 exclusive publication 与 image poll 都在客户端共享内存中完成，driver 不在消息热路径上。
- 让 driver 改写 `visible_producer_pos` 等于绕开 authoritative visible bound，属于已被反例否决的路线。
- “需追回约 900 ns” 是算术错误。实际差距是每条消息 154 ns 对 20 ns，约 134 ns。
- 文中的成功概率是编造的，没有依据。

## 4. E1：零布局判定实验

### 4.1 补丁客户端

新增 `tools/aeron_compare/make_fence_patch_client.py`（配测试），必须逐项校验：

1. 输入为冻结客户端，SHA-256 = `3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da`。
2. 文件偏移 `0x23259`、`0x23264`、`0x23283`、`0x232c5` 的字节都是 `0x87`，且前一字节为 `0x41`。
   此可执行段的 vaddr 等于文件偏移（LOAD 0x3000 → 0x3000）。全部改为 `0x89`。
3. 与源文件恰好 4 字节不同；`objdump -d` 差异只有这 4 条 `xchg` → `mov`。
4. 输出 SHA-256 必须为 `5efae0b41ed42f9a38d69997ed8a1fafc69d78ae69eea948c79abbdec0f433b6`。
5. 写出 `salias_ipc_compare.nofence-diag` 与 `patch-manifest.json`，内容包括偏移、原/新字节、两个 SHA。

**该客户端只用于诊断。** 它破坏了注册与 claim 之间的 SC 握手，只有在测量期间没有任何注册/注销
时结果才有效。执行前在 v3 源码中确认三点：订阅者在 `ready_and_wait_for_start` 之前完成注册；
测量期内没有注册或注销；消费者只 poll，不 arm 等待。任何一点不成立就停止并报告。

### 4.2 设计（预先登记）

| 项 | 设置 |
|---|---|
| 臂 | B = 冻结客户端；C = 补丁客户端 |
| 主实验 | 64 B，1P2S，1 MiB，50M 消息/轮，batch 1，poll-limit 64，CPU0 coordinator |
| 规模 | 每臂 6 组 × 20 轮 |
| 交错顺序 | `B0 C0 C1 B1 B2 C2 C3 B3 B4 C4 C5 B5`，每块一次 runner 调用：`--groups 6 --start-group g --end-group g+1 --allow-existing` |
| runner 参数 | `--interval-seconds 0 --max-consecutive-low 0`（64 B 没有高模式，默认准入规则会误中止） |
| 次实验 | 192 B，相同设计与参数，只记录描述性结果 |
| 延迟 | `run_latency_compare.py --engines fifo`，attempt 与 planned，64/256 B，1M msg/s，每臂 10 轮，B/C 逐轮交替 |

### 4.3 分析工具

新增 `tools/aeron_compare/compare_throughput_ab.py`（配测试），仅用标准库：

- 每臂的中位数、均值、CV、`>=10 Mmsg/s` 轮数；
- 中位数比值 C/B；
- 以**组中位数**为单位做 6 对 6 精确置换检验（924 种划分，双侧）；
- ABBA 相邻块对比：C 高于相邻 B 的块数；
- 组级 cluster bootstrap 给出中位数差的 95% 区间。

64 B 不使用 `>=15 Mmsg/s` 高模式率作为指标（P2-C 已证明该指标在 64 B 退化）。

### 4.4 判定规则（64 B，运行前固定，不得事后修改）

- **GO**：中位数比 ≥ 1.25，**且**置换检验 p < 0.01，**且** 6 块中至少 5 块 C 高于 B。
- **NO-GO**：中位数比 < 1.10，**或** p ≥ 0.05。
- **其他**：不确定，扩展到每臂 12 组一次后按同一规则判定，不做第三次。

192 B 与延迟只做描述性记录，不设门槛；如与 P-b、P-c 不符，在报告中写明。

## 5. P3-1 产品实现规格（仅 E1 GO 后执行）

### 5.1 必须保持的不变式

对任意注册操作 R 与任意 claim C，二者必居其一：C 读到 `gate != 0` 并返回 BackPressured；
或 R 读到 `claim_active == 1` 并等待 C 结束。C 用 release 清除 `claim_active` 后，R 必须能看到
C 写入的 reservation。

### 5.2 发布端（light side）

**进程级注册（关键）**：GLOBAL_EXPEDITED 要求发布进程本身注册为接收方。每个发布进程在创建首个 Publisher 时调用一次：

```cpp
// 全局状态（非 call_once，避免 fork 时死锁）
static std::atomic<int> g_membarrier_state{0};  // 0=未尝试 1=尝试中 2=成功 -1=失败

// 在 Publisher 构造时调用
int register_process_for_asymmetric_fence() {
    int state = g_membarrier_state.load(std::memory_order_acquire);
    if (state != 0) return state;
    
    int expected = 0;
    if (!g_membarrier_state.compare_exchange_strong(expected, 1, 
                                                     std::memory_order_acq_rel)) {
        // 另一个线程在注册中，等它完成
        while ((state = g_membarrier_state.load(std::memory_order_acquire)) == 1)
            ; // spin
        return state;
    }
    
    // 本线程执行注册
#ifdef __linux__
    if (membarrier(MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED, 0, 0) == 0) {
        g_membarrier_state.store(2, std::memory_order_release);
        return 2;
    }
#endif
    g_membarrier_state.store(-1, std::memory_order_release);
    return -1;
}
```

**Fork 约束**：fork 后子进程的 mm 副本保留父进程的 `membarrier_state`，因此已注册的父进程 fork 出的子进程继承注册状态。但 Publisher 对象本身不能跨 fork 共享，必须在子进程里重新构造。Benchmark 符合此模式（fork 后创建 Publisher）。

**Light path**（通道协议为 asymmetric 且本进程注册成功时）：

```cpp
active.store(1, std::memory_order_relaxed);
std::atomic_signal_fence(std::memory_order_seq_cst);  // 只阻止编译器重排
if (gate.load(std::memory_order_relaxed) != 0) { 
    active.store(0, std::memory_order_release);
    return BackPressured; 
}
// ... reservation 写入保持原样 ...
active.store(0, std::memory_order_release);           // 析构
```

**Fallback**：注册失败或非 Linux 时，该进程的全部 Publisher 退回现有 seq_cst 路径（两次 xchg），仍然安全。

`claim` 与 `claim_batch` 都要改（`shared_hybrid_mpsc.hpp:834`、`:932`）。

### 5.3 注册端（heavy side）

**注册端不需要注册进程自身**，直接调用 `membarrier()` 即可，因为这是让**已注册的发布进程**过屏障。

凡是 “设置 gate 后读取 `claim_active`” 的地方，都在两步之间插入 heavy fence：

```cpp
int heavy_fence() {
#ifdef __linux__
    // 优先 GLOBAL_EXPEDITED（需要发布端已注册，但调用端自己不需要注册）
    if (membarrier(MEMBARRIER_CMD_GLOBAL_EXPEDITED, 0, 0) == 0)
        return 0;
    // 退回 GLOBAL（慢，但不需要注册）
    if (membarrier(MEMBARRIER_CMD_GLOBAL, 0, 0) == 0)
        return 0;
#endif
    return -1;  // 不支持或失败
}
```

**已知位置**：
- `channel.cpp` 中 `acquire_consumer`、`release_consumer` 在 `acquire_registration_gate` 之后、
  `wait_for_claims` 之前（`:990`、`:1198` 附近）；
- `shared_hybrid_mpsc.hpp:262-281` 的 `try_arm_wait_notification`。它只在 sticky 激活前执行，属于冷路径。

执行者必须 grep `registration_gate` 和 `claim_active` 的全部使用点，补齐遗漏的位置。

**通道协议为 asymmetric 时**，`heavy_fence()` 失败就 **fail closed**：
- `acquire_consumer`/`release_consumer` 返回错误（如 `Error::Unavailable`）
- `try_arm_wait_notification` 返回 false，退回轮询

绝不能在没有屏障的情况下继续读 `claim_active`。

### 5.4 协议与模式选择

- 新增 `NamedControl::claim_fence_protocol`（1 字节），由创建端在 `ready=1` 之前写入，之后不可变。
  选择依据：Linux，且 `MEMBARRIER_CMD_QUERY` 返回支持 `GLOBAL_EXPEDITED`，且未设置
  `SALIAS_DISABLE_ASYMMETRIC_FENCE=1`。
- 通道级协议标识该通道是否**允许** asymmetric fence；发布端各进程独立决定是否成功注册并使用 light path。
  注册失败的发布进程退回 seq_cst，仍与通道兼容。
- 注册端在 asymmetric 通道上调用 `heavy_fence()` 失败时 fail closed（见 §5.3）。
- `kNamedVersion` 由 14 升到 15，阻止不会发 heavy fence 的 v14 注册端连接。字段放在现有填充区，
  `static_assert` 尺寸不变，hot line 不动。
- `SALIAS_DISABLE_ASYMMETRIC_FENCE=1` 作为运维开关保留，同时让正式 A/B 能用**同一二进制**跑两种模式。

### 5.5 不改的部分

reservation 写入内容、`visible_producer_pos` 的 release、背压计算、TTL、代际与死亡回收
（`wait_for_claims` 的陈旧 `claim_active` 清理）、消费端全部路径、帧格式、ring 布局。

### 5.6 测试

- 现有回归：Release CTest 21 项、ASan/UBSan/TSan、Aeron 工具回归 23 项、HFT summary 回归 36 项。
- 新增跨进程压力测试：
  - 一个 producer 进程（已注册）持续 claim/commit；另一进程循环 acquire/release consumer 10,000 次。
    每个新 consumer 从注册快照开始校验收到的序号连续、无缺、无重。asymmetric 与强制 seq_cst 两种协议各跑一遍，要求 0 失败。
  - Fork 场景：父进程创建通道，fork，子进程创建 Publisher 并发布；另一进程注册 consumer。校验子进程发布的消息正确送达。
- 故障注入（`SALIAS_TEST_HOOKS`）：
  - 让 `heavy_fence()` 失败，注册必须返回错误，且没有消息损坏。
  - 让发布端注册失败，该进程应退回 seq_cst，仍能正常发布。
- v14 控制段连接被拒绝。
- 边界：TSan 无法建模 `membarrier`，正确性主证据是压力测试。本机为 x86 TSO，ARM 上的行为**待验证**。
- 注册状态检查：验证 `GET_REGISTRATIONS` 在父子进程中的行为符合预期。

## 6. P3-1 正式验收

### 6.1 臂与构建

按 `phase2-execution-baseline.md` §4，以未修改的 v3 benchmark 源码链接候选产品库，构建候选客户端 C。

| 臂 | 二进制 | 模式 |
|---|---|---|
| B | 冻结客户端 `3f8fc695...` | 原实现 |
| C-seq | 候选 C | `SALIAS_DISABLE_ASYMMETRIC_FENCE=1` |
| C-asym | 候选 C | 默认 |

C-seq 与 C-asym 是同一个二进制，所以二者的差异只来自屏障，不含布局效应。
runner 需要新增 `--setenv KEY=VAL` 透传选项，记录到 `environment.json` 并配测试。

### 6.2 设计

- 64 B 与 192 B：3 臂 × 6 组 × 20 轮，轮换块序
  `B, C-seq, C-asym, C-asym, C-seq, B, ...`，并带 `--max-consecutive-low 0`。
- 延迟：64/256 B，1M msg/s，attempt 与 planned，每臂 10 轮交错。

### 6.3 门槛（预先登记）

| 门槛 | 条件 |
|---|---|
| G1 屏障效应 | 64 B：C-asym 对 C-seq 的中位数比 ≥ 1.25，组中位数置换检验 p < 0.01，至少 5/6 块方向一致 |
| G2 产品对外数字 | 64 B：C-asym 对 B 的中位数比 ≥ 1.20。未通过时不发布对外收益数字，单列布局效应 |
| G3 192 B 非劣 | C-asym 中位数 ≥ C-seq × 0.97，高模式率降幅不超过 10 pp |
| G4 延迟非劣 | worst-subscriber planned p50 不超过 C-seq + 10 ns；p99/p99.9 用 HFT 工具判定非劣；吞吐完成度不下降 |
| G5 正确性 | §5.6 全部通过 |
| G6 注册成本 | 报告 `acquire_consumer` 前后耗时（预计增加约 0.6 µs，只记录） |

G1 和 G5 都通过才合入。G2 未通过时可以合入，但对外只写机制收益，不写吞吐数字。

### 6.4 G4 长尾执行细则（2026-10-21，采集 HFT 数据前固定）

本节将 §6.3 的“HFT 工具判定非劣”明确为可执行规则，不改 G1–G3 或 p50 门槛。
沿用 HFT 工具非目标配置的 **5% 非劣界限**，不套用旧 publisher-tail 实验的
“两组 burst 必须改善 20%”目标：P3-1 并未预登记长尾改善目标。

- C-seq/C-asym 使用同一个、链接本轮产品库的 `acceptance.cpp` 二进制，只切换环境变量。
- 使用 HFT 既有 8 种配置（2P1S/2P2S、64/256 B、steady/burst）、原流量、采样、校验与计时起点。
- 每种配置先预热两臂各 2 秒；随后 **3 个独立 SAAS 组，每轮 30 秒**，正式采集共 96 轮。
- 每组每配置按订阅者分别合并原始直方图，再分别取最慢订阅者的 planned p99 与 p99.9；
  **两个分位数均须 C-asym ≤ C-seq × 1.05**，24 个组/配置必须全部满足。
  不能用跨组池化、最优轮次或短测中位数挽救失败组。
- 所有轮次必须原始输入完成、worker 正常、完整内容/序号校验通过；独立 50 µs 预算另报，
  不能替代相对非劣。保留失败轮次，不为追求通过重跑或改门槛。
- HFT 2P2S 需要本机全部 4 个 vCPU：subscriber CPU0/1、publisher CPU2/3；2P1S 为
  subscriber CPU0、publisher CPU1/2。与 1P1S 短测及吞吐的绑核不同，不拼接数字。
  无同时编译或其他基准负载；不采用 SCHED_FIFO，不改 sysctl。
- `doc/plans/runners/p3-p31-tail.py` 在第一轮前写入完整协议、二进制/源码哈希和环境覆盖；
  `tools/hft_acceptance/compare_p31_tail.py` 复核轮序、命令、SHA 与原始 HFT 校验结果。

G6 使用独立探针直接调用原 `acquire_consumer`，不改产品源码；注册端 CPU1，已注册且忙等
但不 claim 的发布进程 CPU2。B/S/A 每臂 6 轮，每轮预热 1000 次、测量 5000 次，
释放操作在计时区间外。该结果只描述空闲注册成本，不代表并发 claim 下的最坏注册耗时。

## 7. 条件后续（不在本计划默认范围）

- **E1 NO-GO**：停止 P3-1，记录负结果。下一步用同样的零布局方法，把补丁客户端里每次 claim 对 hot line
  的 reservation/`local_sequence` store 换成等长 NOP，测 hot line 竞争成本（E3）。E3 在 E1 后单独设计，同样不改产品。
- **P3-3 hot line 分行**：P3-1 通过之后，先用 E3 式零布局实验证明有收益再做。若采纳，并入同一个 v15 发布，避免两次协议升级。
- **P3-2 多线程进程的 `shared_ptr` 计数**：冻结二进制中 `try_claim` 复制 endpoint `shared_ptr`，
  多线程进程里走 `lock incl` 路径；benchmark 的 fork 子进程是单线程，测不到这部分成本。
  可用零布局 E2 量化：LD_PRELOAD 一个 shim，在 `pthread_atfork` 的子进程回调里创建一个休眠线程。
  修复会改变 claim 的生命周期契约，**必须由用户决定**，不默认实施。

## 8. 约束（继续有效）

- E1 补丁客户端不得出现在任何产品性能结论中。
- 不修改冻结客户端；不用 offer 替代 claim；不用 SCHED_FIFO；不调 RT 限流；不改 sysctl。
- 本方案不是 “直接把 seq_cst 改弱”。产品改动用 heavy fence 保持完整的 Dekker 不变式，
  这与已否决的 SkipRegistrationGuard 不同：后者需要固定订阅，删除了握手。
- 本机无 PMU，不写 cache-miss、HITM 或 IPC 结论。
- 不提交、不打 tag、不推送，除非用户明确要求；保留工作区现有改动。

## 9. Session 提示词

### 9.1 Session E1

```
你接手 Salias Phase 3 E1 判定实验。先读 doc/plans/phase3-final-optimizations.md（§1、§4、§8）
与 doc/plans/phase2-execution-baseline.md。

约束：不改 src/；不改冻结客户端；不提交、不打 tag、不推送；保留工作区现有改动。
补丁客户端只用于诊断。§4.4 的判定规则已固定，不得修改。

1. 按 §4.1 实现 tools/aeron_compare/make_fence_patch_client.py 及测试，生成补丁客户端到
   build/benchmarks/p3-e1-<时间戳>/，逐项校验 SHA、字节、cmp 差异与反汇编差异。
2. 用 git show v3.0.0:tools/aeron_compare/salias_ipc_compare.cpp 确认 §4.1 的三个前提，
   任何一项不成立就停止并报告。
3. 按 §4.3 实现 compare_throughput_ab.py 及测试。
4. 按 §4.2 的 ABBA 顺序跑 64 B，然后跑 192 B，最后跑延迟。每块用独立 runner 调用，
   带 --max-consecutive-low 0。完成后校验 SHA256SUMS、RESULT 行和交付计数。
5. 按 §4.4 给出 GO / NO-GO / 不确定结论，并对照 §2 的 P-b、P-c 预测。
6. 写 doc/plans/phase3-e1-fence-report.md，更新 doc/plans/session-state.json。

报告需包含：两个客户端的 SHA、每臂中位数/均值/CV、中位数比、置换检验 p、ABBA 块方向数、
bootstrap 区间、192 B 与延迟的描述性结果、完整性校验结论。
```

### 9.2 Session P3-1 实现（仅 E1 GO）

```
E1 已 GO（见 doc/plans/phase3-e1-fence-report.md）。按 doc/plans/phase3-final-optimizations.md §5
实现 P3-1，用户已批准修改 src/。

1. 新增 src/core/platform/asymmetric_fence.hpp：light/heavy fence、进程级一次性注册、能力查询；非 Linux 退回 seq_cst。
2. 修改 ClaimReservationGuard，以及 claim、claim_batch 的使用点；补全 §5.3 列出的全部 heavy fence 位置，
   并 grep registration_gate 与 claim_active 找出遗漏。
3. 按 §5.4 增加 claim_fence_protocol，kNamedVersion 升为 15，实现运维开关。
4. 按 §5.6 补齐测试，跑完全部回归与 sanitizer。
5. 更新 CHANGELOG 草稿与协议说明。不提交。

报告：改动文件列表、不变式论证、每个 heavy fence 位置、测试结果（含失败项原文）。
```

### 9.3 Session P3-1 验收

```
P3-1 实现已完成。按 doc/plans/phase3-final-optimizations.md §6 做正式验收。

1. 按 phase2-execution-baseline.md §4 构建候选客户端，记录全部构建输入与 SHA；
   先重建无改动基线，确认得到 3f8fc695...。
2. 给 high-frequency-stability-runner.py 增加 --setenv 透传，配测试，并记录到 environment.json。
3. 按 §6.2 跑 3 臂实验，按 §6.3 逐条判定 G1–G6，门槛不得修改。
4. 写 doc/plans/phase3-p31-acceptance-report.md，更新 session-state.json 与 performance-report.md 的结论段。不提交。
```

**§9.3 执行结论（2026-10-21）**：正式验收已结束，P3-1 整体 **FAIL**。96 个 HFT worker
均正常退出，但 S 臂 4/48 轮原始输入未完成，HFT 逐组 p99/p99.9 非劣仅 7/24 单元通过。
保留全部失败轮次，未改门槛、未为追求通过重跑；详见验收报告 §4、§9。

## 10. 时间估计

| 阶段 | 工作 | 机器时间 |
|---|---|---|
| E1 | 补丁、分析器、64/192 B ABBA、延迟 | 约 1.5 小时 |
| P3-1 实现 | 代码与测试 | 约 1 小时（回归） |
| P3-1 验收 | 3 臂 × 2 payload + 延迟 | 约 3 小时 |

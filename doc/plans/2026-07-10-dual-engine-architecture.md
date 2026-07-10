# salias 双模引擎架构重设计 — 执行方案

Date: 2026-07-10
Status: Implemented（代码迁移与本地验证完成；x86 真机性能验收待外部环境）
Decision owner: salias 负责人

> 本文是对 `doc/plans/2026-07-08-hybrid-ring-architecture.md`、`-throughput-optimization*.md`、
> `-cas-backoff.md`、`2026-07-09-优化建议.md` 的收敛结论。前述文档的 B1–B5 / cas-backoff /
> batch / min-head / 大页 / 控制块布局都是**代码级**优化，每项 <12%，补不上结构性缺口。
> 本文是**架构级**重设计。

---

## 0. 决定确认（回答"当前是不是全局全序"）

**是。当前三条通道全部是全局全序（global total order）——这正是它们都追不平 Aeron 的根因。**

| 通道 | 全序实现方式 | 每消息共享原子 |
|---|---|---|
| `MpscChannel` / `SharedMpscChannel` | 所有生产者 CAS 同一 `reserved_tail`，单环内按位置预留互不重叠区间，单消费者严格按位置读 | 1× 竞争 CAS（`mpsc.hpp:234`, `shared_mpsc.hpp:89`）|
| `SharedMpmcChannel` | 同一单环 + `reserved_tail`，扇出给 M 消费者，各自按同一位置顺序读 | 1× 竞争 CAS（`shared_mpmc.hpp:174`）|
| `HybridMpscChannel` / `SharedHybridMpscChannel` | 每生产者私有环，但 `global_seq.fetch_add(1)` 盖全局序号，消费者按 `expected_sequence` 递增归并 | 1× `fetch_add`（`hybrid_mpsc.hpp:317`, `shared_hybrid_mpsc.hpp:187`）|

三者之上都还压着 `wait_word.fetch_add`（每 commit 一次），而 `SpinPause::wake()` 是空函数
（`spin_pause.hpp:19`）——在忙轮询基准里这是**纯浪费**的跨核一致性流量。

**Aeron 两样都没有**：每个 `ExclusivePublication` 是独立 term-buffer 日志，生产者热路径零共享原子，
且**不提供**跨生产者全序，消费者多路复用 N 个 image。它用"放弃全序"换零共享原子。

---

## 1. 用户决定

**保留"最高性能的全局全序"引擎；另外实现 per-producer FIFO（Aeron 对等）引擎。** 两种语义都要，
但收敛到**一个 per-producer-ring 引擎**上，靠模式开关区分，删掉所有冗余单环实现。

### 为什么"最高性能全序"是 hybrid，而不是单环 mpsc/mpmc

| 全序设计 | 生产者同步 | 扩展性证据 | 结论 |
|---|---|---|---|
| 单环 mpsc/mpmc | 竞争 CAS + 失败重试风暴 | 文档实测 4P 聚合 = 1P 的 **0.51×**（负扩展，`cas-backoff.md`）| 最差 |
| **hybrid（+global_seq）** | `fetch_add`，永不失败、无重试 | fit-4CPU 实测 ≥ mpsc（11.83 vs 10.71，绑核）| **最高性能全序** |

hybrid 的骨架**去掉 `global_seq` 就直接变成 FIFO**。因此保留 hybrid = 同时拿到两种模式，
单环家族被严格支配、可全部删除。

---

## 2. 目标架构：单引擎 · 双模式 · 覆盖 single/fanout

一个 per-producer magic-ring 引擎，正交两维：

```
                    consumer 拓扑
                 ┌──────────────┬──────────────┐
                 │ single (1C)  │ fanout (MC)  │
   ┌─────────────┼──────────────┼──────────────┤
 排│ fifo        │ 最快 MPSC     │ Aeron 对等    │  ← 生产者热路径零共享原子
 序│ (无序列器)   │ (=1P 退化SPSC)│  MPMC 扇出    │
 模├─────────────┼──────────────┼──────────────┤
 式│ ordered     │ 最高性能全序   │ 全序扇出      │  ← 每消息 1× fetch_add
   │ (global_seq)│ MPSC          │              │
   └─────────────┴──────────────┴──────────────┘
```

### 2.1 生产者热路径（两模式共用私有环，零竞争）

```
claim(len):
  # 私有环容量检查（无原子；仅在不足时 acquire-load 本生产者的 consumer_pos）
  if !has_capacity(private_ring): refresh cached_consumer_pos; if still full -> BackPressured
  # 模式分叉：仅此一处
  if MODE == ordered:  seq = global_seq.fetch_add(1, relaxed)   # 唯一共享原子
  else /* fifo */:     seq = producer_local_counter++          # 零共享原子（真·Aeron 对等）
  write_uncommitted_header(private_ring, pos, len, seq)
  visible_producer_pos.store(pos+need, release)                # 仅本生产者写
```

`fifo` 模式下**每消息 0 个共享 RMW**——这是唯一重要的结果，也是当前 hybrid 缺的最后一步。

### 2.2 commit：把 wake 关在 `needs_wake()` 后

```
commit(claim):
  store_meta_release(private_ring, claim.start_pos, claim.meta | COMMITTED)
  if wait_.needs_wake():           # SpinPause::needs_wake() -> false，整条通知链消失
     wait_word.fetch_add(1, release); wait_.wake(&wait_word)
```

忙轮询（SpinPause）时不再有 `wait_word` XADD。仅 Futex 策略才付通知代价。

### 2.3 消费者

- **fifo / single**：轮转扫描 N 个私有环，每个环独立按位置读，**不重建全局序**（per-producer FIFO）。
- **fifo / fanout**：M 个消费者，每个持有对 N 个环的**独立游标集**，各自读全部环 → Aeron subscription 扇出语义。
- **ordered / single**：现有 hybrid 消费端（`expected_sequence_` 归并）保留为 ordered 模式。
- **ordered / fanout**：M 个 `consumer_seq`，生产者背压看 `min`。（后续阶段，非首发。）

---

## 3. 保留 / 删除 / 修改清单

> 删除项均已由工作流对抗式核验（每条残留引用要么落在另一个 DELETE 文件内，要么落在明确 MODIFY 的文件里）。
> **BLOCKED 项见 §3.4，解依赖前不得删。**

### 3.1 保留（引擎 + 严格依赖）

- 平台/环：`src/core/platform/{mapping.hpp,mapping.cpp,map_options.hpp,error.hpp}`、
  `src/core/ring/{magic_ring.hpp,magic_ring.cpp,atomic_cell.hpp,error.hpp}`
- Frame：`src/core/frame/{header.hpp,codec.hpp,sequence.hpp,error.hpp}`
- Flow：`src/core/flow/{producer.hpp,producer.cpp,consumer.hpp,consumer.cpp,position.hpp,error.hpp}`
- Wait：`src/core/wait/{wait_strategy.hpp,spin_pause.hpp,cpu_relax.hpp}`
- **引擎**：`src/core/channel/{hybrid_mpsc.hpp,shared_hybrid_mpsc.hpp,hybrid_control.hpp,error.hpp}`
- 公共 API：`src/salias/include/salias/{salias.hpp,channel.hpp,config.hpp,publisher.hpp,subscriber.hpp,message.hpp,error.hpp}`、`src/salias/src/channel.cpp`

### 3.2 删除（CONFIRMED-SAFE）

| 路径 | 原因 |
|---|---|
| `src/core/channel/{spsc.hpp,shared_spsc.hpp}` | 退化单生产者；`fifo/single` 且 N=1 即 SPSC |
| `src/core/channel/{mpsc.hpp,shared_mpsc.hpp}` | 单环竞争 CAS 全序，被 hybrid-ordered 严格支配 |
| `src/core/channel/shared_mpmc.hpp` | 单环竞争 CAS 扇出；改由 per-producer `fifo/fanout` 提供 |
| `src/core/channel/channel_config.hpp` | 仅服务被删单环通道 |
| `src/core/metrics/{counters,reader,layout,error}.*` | 孤立子系统，无通道依赖 |
| `test/metrics/counters_test.cpp` | 随 metrics 删 |
| `test/channel/{spsc_channel_test.cpp,mpsc_channel_test.cpp}` | 随单环通道删 |
| `bench/smoke.cpp` | 冗余基准 |
| `example/{mpmc_publisher.cpp,mpmc_subscriber.cpp}` | 第二组示例，与 mpsc 示例重复 |
| `src/core/platform/{futex.hpp,futex.cpp}`、`test/platform/futex_test.cpp` | **仅在确定 SpinPause-only 时删**；否则保留为 Futex 模式后端。见 §3.4 |

### 3.3 修改（关键项）

- `src/core/channel/hybrid_mpsc.hpp` / `shared_hybrid_mpsc.hpp`：
  引入模式参数（编译期 `enum Order{Fifo,Ordered}` 模板实参 或运行期 flag）。`Fifo` 分支用
  producer-local 计数器替代 `global_seq.fetch_add`；消费端 `Fifo` 分支独立读各环、不做全序归并。
  `wait_word` XADD 关在 `wait_.needs_wake()` 后。
- `src/core/channel/hybrid_control.hpp`：`global_seq` 改为**仅 ordered 模式使用**（fifo 不触碰）；
  折叠 per-producer-shm 命名控制块布局。
- `src/core/wait/{wait_strategy.hpp,spin_pause.hpp}`：`WaitStrategy` concept 增 `needs_wake()`；
  `SpinPause::needs_wake()` 返回 false。
- `src/core/platform/mapping.cpp`：ring 映射加 `MAP_POPULATE`（或建环后预热触碰），把页错误移出计时窗口，
  对齐 Aeron `sparse.file=false`。
- `src/salias/src/channel.cpp`：删 shared_spsc/mpsc/mpmc 的 include、`ChannelTraits` 分支与模板实例化；
  暴露 `{Fifo,Ordered} × {single,fanout}`；`Publisher`/`Subscriber` **持有** Tx/Rx 端点（不再每次调用重建），
  `poll` 保留 `cached_tail_` 跨批（消除工作流查出的端点重建 + 函数指针税）。
- `src/salias/include/salias/{config.hpp,channel.hpp}`：`enum class Mode` 收敛到
  `{FifoMpsc, FifoFanout, OrderedMpsc, OrderedFanout}`（命名可再定），删 Spsc/Mpsc/Mpmc 旧别名。
- CMake：`src/core/CMakeLists.txt` 去 metrics（及 futex）`.cpp`；`test/CMakeLists.txt` 删
  `salias_metrics_tests`、从 `salias_channel_tests` 去掉 spsc/mpsc 两行；`bench/CMakeLists.txt` 去
  `salias_bench_smoke`；`example/CMakeLists.txt` 去 mpmc 目标。
- `test/channel/hybrid_mpsc_channel_test.cpp`：拆两组断言——ordered 模式保留跨生产者全序断言；
  fifo 模式改为 per-producer FIFO 断言。
- `test/api/channel_api_test.cpp`、`bench/channel_stress.cpp`、
  `tools/aeron_compare/{salias_ipc_compare.cpp,run_release_compare.sh}`、
  `example/{mpsc_publisher.cpp,mpsc_subscriber.cpp,example_smoke.sh}`：收敛到新 Mode。

### 3.4 需先解依赖（BLOCKED — 不要在解依赖前删）

**根因：`test/wait/wait_strategy_test.cpp` / `salias_wait_tests` 目标被遗留在悬空状态**——它 include 了
四个待删 wait 头，但自身未列入任何 DELETE/MODIFY。解锁 = 先把该测试 + `salias_wait_tests`
（`test/CMakeLists.txt:83-99`）显式删除或改写。之后才能删：

1. `src/core/wait/busy_spin.hpp`（`wait_strategy_test.cpp:1`）
2. `src/core/wait/yielding.hpp`（`wait_strategy_test.cpp:6`）
3. `src/core/wait/futex_wait.hpp`（`wait_strategy_test.cpp:3`；也是通往 `futex.*` 的唯一桥，
   §3.2 的 futex 删除仅在本项解决后才干净）
4. `src/core/wait/cas_backoff.hpp`（`wait_strategy_test.cpp:2`；产品路径已无引用，但被此测试活引用）

**有序删除：`src/core/ring/cache_aligned.hpp`** — 被 KEEP 文件 `test/ring/magic_ring_test.cpp:2` 引用。
必须**先**移除该 include 并确认 `magic_ring_test` 运行期不依赖其中符号，**再**删头，否则破坏保留的
`salias_ring_tests`。

---

## 4. 迁移步骤（有序）

1. **wait concept 加 `needs_wake()`**，`SpinPause` 返回 false。（无行为变化，先落地。）
2. **hybrid 加模式**：`Fifo` 分支去 `global_seq`、消费端独立读环；`Ordered` 分支保留现状。
   commit 的 `wait_word` XADD 关在 `needs_wake()` 后。→ **TSan 验证 `Fifo` 下每消息 0 共享 RMW**（唯一关键验证）。
3. **per-producer FIFO fanout**：消费者 M 套游标读 N 环，覆盖 MPMC 扇出。
4. **公共 API 收窄 + 去端点重建税**：`channel.cpp`/`config.hpp`/`channel.hpp` 收敛到新 Mode；
   `Publisher`/`Subscriber` 持有端点、`poll` 跨批保 `cached_tail_`。
5. **IPC/命名层**：per-producer-shm 布局 + 控制块 + `MAP_POPULATE` prefault。
6. **执行 §3.2 删除**；按 §3.4 解 BLOCKED，最后删 `cache_aligned.hpp`。
7. **改写测试**：ordered/fifo 分别断言；`channel_api_test`/`channel_stress`/IPC 工具收敛。
8. **公平 harness 重测（三项同时成立才算数）**：
   - **busy-spin 消费端**：把 `salias_ipc_compare.cpp:333` 的 `poll + yield` 换忙轮询，对齐 Aeron `BusySpinIdleStrategy`。
   - **prefault**：`MAP_POPULATE`/预热，页错误移出计时窗口，对齐 Aeron `sparse.file=false`。
   - **对称核**：生产者/消费者绑核；Aeron 有独占 driver 核，对比时核数/绑定对称，或在报告注明 Aeron 多占 1 核。
9. **首要判据**：公平 **x86 真机**（非 ARM Colima VM）上 `fifo` 模式 batch=1 追平 Aeron；
   batch=16 领先作回归护栏（R12 已见 1.40×）。ARM VM 的"赢"不作验收依据。

---

## 5. 验收标准

- `fifo` 模式：TSan 证明每消息 0 共享 RMW；x86 真机 batch=1 ≥ Aeron。
- `ordered` 模式：跨生产者全序断言通过；性能 ≥ 旧单环 mpsc/mpmc（fetch_add 优于竞争 CAS）。
- `ctest` 全绿（删除后目标数下降属预期）；ASan/UBSan/TSan 绿。
- 仓库只剩一个 per-producer-ring 引擎 + 双模式；单环家族、metrics、冗余 bench/example 已清。

---

## 6. 诚实边界

- `ordered` 模式**数学上追不平 Aeron**：全局全序强制一个共享单调 rank + 串行化消费端归并点，Aeron 没有这个点。
  它的目标是"最高性能的全序"（≥ 旧单环），不是"追平 Aeron"。
- `fifo` 模式才是追平/超越 Aeron 的路径，代价是放弃跨生产者顺序（与 Aeron 同）。
- 这是**重设计**，不是调参。当前 hybrid 在 fit-4CPU 真机实测 11.83M、输给 Aeron，正因为它删了 CAS
  却保留了 `global_seq` XADD + 全序扫描。本方案删的就是那最后一个共享 per-message 原子。

---

## 7. 执行结果（2026-07-10）

- 已完成单引擎双模式、single/fanout、公共 API 收敛、持久端点、共享控制块、prefault 与旧实现删除。
- 已迁移测试、示例、benchmark、IPC 对比工具和架构文档。
- ARM Colima 中 ASan/UBSan 37/37、TSan 7/7、Release 39/39 测试通过。
- FIFO/ordered 的单消费者与双消费者 IPC 冒烟均完成全量交付校验。
- ARM Release 汇编检查用于确认 FIFO 模板无共享 RMW；ordered 模板只保留全局序列分配所需的 RMW。
- 最终的 `fifo batch=1 >= Aeron` 判据必须在公平配置的 x86 物理机执行；当前 ARM Colima 环境不满足该硬件验收条件。
- Tencent x86 KVM 以 2P/1C、固定绑核、独立 Aeron driver 实测：FIFO batch=1 中位数 13.980 Mmsg/s，
  Aeron 34.077 Mmsg/s，salias 仅为 41.0%，因此当前实现未通过性能验收。详细结果见
  `doc/benchmarks/tencent-aeron-comparison-2026-07-10.md`；x86 物理机复验仍保留为外部验收项。
- 同机 2P/2C MPMC 各运行 20 轮：固定四核预算下 FIFO batch=1 中位数为 Aeron 的 52.5%，自由调度
  交叉验证为 49.3%；FIFO batch=16 没有实质改善，fanout 性能仍未达到 Aeron。
- 四核超卖扩展矩阵覆盖 3P/2C、2P/3C、3P/3C 与 batch=1/16；salias 中位吞吐为 Aeron 的
  45.5%–69.1%，增加消费者会使两边显著退化，当前实现没有在更高进程数下反超。
- 相同 Tencent 四核环境完成 1/64 采样的饱和负载单向延迟比较。2P/2C 固定绑核与自由调度共 8 个 case
  中，salias 聚合 p50 为 3.85–8.06 ms、p99 为 9.90–14.94 ms；Aeron p50 为 53.8–174.1 us、
  p99 为 1.54–2.47 ms。超卖 FIFO 矩阵中 salias p99 为 14.29–28.31 ms，Aeron 为 1.80–5.44 ms。
- 延迟结果是持续饱和发布下包含排队与 KVM 调度的应用端到应用端延迟，不是空载 ping-pong；固定绑核与
  自由调度结论一致，当前 salias fanout 的中位和尾延迟均未达到 Aeron。原始数据与方法见
  `doc/benchmarks/tencent-aeron-comparison-2026-07-10.md`。

# 2 - salias 实施计划

> 配套分析文档：`1-aeron-analysis-and-optimizations.md`
> 定位：纯共享内存（同机 IPC / 进程内）高性能消息库，**C++20 实现，driverless，只做 Linux**。
> 不做网络传输。目标是在同机场景的延迟/吞吐/尾延迟上超越 Aeron IPC。
>
> **技术栈**：C++20 / CMake + vcpkg / 仅 Linux / 核心不用协程（详见 `1-` 第六节）。

---

## 0. 指导原则

- **先正确，后极致**：无锁 + 内存序易错，每阶段配套压力/竞态测试再谈优化。
- **约定优于配置**：默认参数开箱即用，进阶参数可调不必调。
- **可度量**：每个性能主张都要有基准（bench）背书，对比 Aeron IPC 官方数据/本机实测。
- **可移植但不牺牲主线**：主线 Linux（`memfd_create` + 双映射 + huge pages + NUMA）；macOS/Windows 作为兼容层，功能对齐、性能次级。
- 遵循仓库编码规范：小文件、immutable-first、显式错误处理、边界校验、80% 覆盖率。

---

## 1. 里程碑总览

| 阶段 | 名称 | 交付 | 退出标准 |
|------|------|------|----------|
| M0 | 骨架与基准 | crate 结构、CI、bench 框架、Aeron 对照基线 | 能跑通空 bench，拿到 Aeron IPC 本机数据作对照 |
| M1 | Magic Ring SPSC | 单生产单消费无锁环 + 双映射 | SPSC 正确性 + 延迟/吞吐达标，无分片 |
| M2 | 背压与 position 模型 | 64 位 position、claim/commit、批量 API | 背压语义正确，零拷贝 claim 路径打通 |
| M3 | 等待策略与可观测性 | 可插拔 wait strategy、共享内存计数器 | futex 空载近零 CPU；计数器可外部读取 |
| M4 | MPSC / 多订阅扇出 | 多生产者、独立订阅进度 | 多写多读正确，尾延迟可控 |
| M5 | 大消息 bulk channel | 独立大消息通道 | 大消息零重组，吞吐达标 |
| M6 | 加固与发布 | TSan/ASan/UBSan + libFuzzer + 长期稳态压测、文档 | 测试套件全绿，API 冻结 v0.1（仅 Linux） |

---

## 2. 阶段详解

### M0 — 骨架与基准（对照先行）
**目标**：先量出对手的数，别拍脑袋。
- 建立 CMake 工程：`core/`（无锁原语与 ring，静态库 `salias_core`）、`salias/`（公共 API 头+库）、`bench/`（基准，依赖 google-benchmark via vcpkg）、`test/`。
- 目录：遵循"多小文件"——`core/ring/`、`core/frame/`、`core/wait/`、`core/flow/`、`core/channel/`、`core/metrics/`、`core/platform/` 分模块（与 L0–L7 分层一一对应）。
- 编译选项：`-std=c++20 -O3 -flto -march=native`（release）；debug 附 `-fsanitize=address,undefined`；并发测试附 `-fsanitize=thread`。
- CI：`cmake --build` + `ctest` + google-benchmark + clang-tidy + cppcheck。
- **对照基线**：在本机跑 Aeron C++ `aeron-ipc` 的 `EmbeddedIpcThroughput` / ping-pong 延迟样例，记录 p50/p99/p99.9/吞吐，写入 `doc/benchmarks/aeron-baseline.md`。
- 交付：benchmark 能跑；基线数据入库。

### M1 — Magic Ring SPSC（核心引擎）
**目标**：无 term 轮转、无清零、无分片的单向环。
- 实现 **magic ring buffer**：Linux 用 `memfd_create` + `ftruncate` + 两次 `mmap`（`MAP_FIXED` 到相邻虚拟地址），使 `[base, base+size)` 与 `[base+size, base+2*size)` 指向同一物理页。
- 8 字节帧头（`len: u32` + `flags/seq: u32`），8 字节对齐。
- 生产者只写 `tail`、消费者只写 `head`，各自独占缓存行（`struct alignas(64)` + padding，或 `std::hardware_destructive_interference_size`）。
- 跨边界消息因双映射天然连续，**禁止分片路径**（超容量直接报错，留给 M5 的 bulk）。
- 测试：单线程写读、跨边界写读、满/空边界；TSan 下并发内存序检查。
- 退出：SPSC ping-pong 延迟 p50 优于 Aeron 基线；小消息（≤64B）吞吐优于基线。

### M2 — 背压与 position 模型
**目标**：保留 Aeron 的优雅背压，去掉 Driver 中介。
- 64 位单调 `position`（相对 ring 用 `& (size-1)` 取偏移）。
- **claim/commit** 零拷贝 API：`claim(len) -> WriteSlot`（在 ring 上原地写）→ `commit()`；对标 Aeron `tryClaim`。
- `offer(&[u8])` 便捷 API（已有 buffer 时）。
- 背压：生产者读消费者的 `head`（消费位置）判断可用空间，返回 `BackPressured`，**不经任何第三方轮询**。
- 批量：`claim_batch` / `read_batch` 减少每消息同步开销。
- 测试：满环背压、慢消费者、位置单调性、commit 顺序性。

### M3 — 等待策略与可观测性
**目标**：延迟与能耗可权衡；运维可观测。
- 可插拔 `WaitStrategy` 概念/抽象基类：`BusySpin` / `SpinPause`（`_mm_pause`）/ `Yielding`（`std::this_thread::yield`）/ `Backoff` / `Futex`（`FUTEX_WAIT`/`FUTEX_WAKE`，跨进程作用于共享内存上的 `std::atomic_ref<uint32_t>`）。
- 共享内存 **counters 区**：发布位置、消费位置、背压次数、错误数——布局固定、缓存行隔离，外部工具可只读 `mmap` 观测。
- 提供 `salias-top` 小工具（可选）读取计数器实时展示。
- 退出：Futex 策略下空载 CPU 近 0；busy-spin 下延迟不回退。

### M4 — MPSC / 多订阅扇出
**目标**：从点对点扩展到多写/多读。
- MPSC：多生产者用 CAS 抢 `tail` 预留区间，写完各自区间后按序 commit（gap 处理）。
- 多订阅：每个订阅者独立 `head`，共享同一 ring（广播语义）；生产者以最慢订阅者约束回收空间。
- 明确取舍：MPSC 抢占引入 CAS 竞争，提供 SPSC 快路与 MPSC 慢路两套实现，按 channel 类型选择。
- 测试：多线程压测、TSan 检查数据竞争、自建并发交错测试用例（穷举关键 interleaving）、独立进度回收、尾延迟统计。

### M5 — 大消息 bulk channel
**目标**：大消息零重组。
- 独立 bulk 通道：单帧连续写入（消息 ≤ ring 容量），消费端拿到连续切片，**无 FragmentAssembler**。
- 超大消息（> ring）策略：要么拒绝并提示扩容 ring，要么走"引用+独立大页段"（评估后决定，不默认引入分片）。
- 测试：MB 级消息延迟/吞吐、内存占用、与小消息通道隔离性。

### M6 — 加固与发布
**目标**：可发布的 v0.1（仅 Linux，无跨平台工作）。
- 加固：TSan 覆盖无锁并发、自建并发交错测试穷举关键 interleaving、libFuzzer 覆盖帧解析、ASan/UBSan 全量、长期稳态压测（数小时看尾延迟漂移与内存）。
- huge page / NUMA 运行时探测与自动降级。
- 文档：Doxygen API 文档、迁移指南（Aeron→salias 概念映射）、性能报告。
- 退出：CI 全绿；API 冻结；发布 `v0.1`。

---

## 3. 横切关注点

### 性能验收基线（对照 Aeron IPC，同机同核绑定）
| 指标 | 目标 |
|------|------|
| SPSC ping-pong p50 延迟 | ≤ Aeron IPC 的 70% |
| p99.9 延迟抖动 | 消除周期性尖峰，抖动幅度显著小于 Aeron |
| 小消息（16B）吞吐 | ≥ Aeron IPC 的 1.3× |
| Futex 空载 CPU | 近 0（Aeron busy-spin 满核） |
| 内存有效载荷利用率（8B 消息） | 8B 头 → 显著高于 Aeron 32B 头 |

> 所有数字以 M0 采集的本机 Aeron 基线为准，避免与官方不同硬件的数据混谈。

### 测试策略（对齐仓库 testing.md，80% 覆盖）
- **单元**：ring 偏移计算、帧编解码、position 运算、wait strategy。
- **并发正确性**：TSan 检查数据竞争；自建并发交错测试穷举 SPSC/MPSC 关键 interleaving；ASan/UBSan 查越界与 UB。
- **集成**：跨进程（fork）真实 `mmap` 共享，端到端收发。
- **性能回归**：google-benchmark 基准纳入 CI，阈值回归即失败。
- **模糊**：libFuzzer 打帧解析与边界。
- TDD：每个 ring/背压特性先写失败测试再实现。

### 风险登记
| 风险 | 影响 | 缓解 |
|------|------|------|
| 双映射平台差异大 | 移植成本 | Linux 主线先行，抽象 `Mapping` trait 隔离平台代码 |
| 无锁内存序错误 | 数据损坏/极难复现 | TSan + 自建并发交错测试 + 代码评审专项 + 最小化裸原子/裸内存面 |
| MPSC CAS 竞争退化 | 吞吐不达标 | 提供 SPSC 快路，MPSC 仅在必要时启用 |
| huge page/NUMA 环境不可用 | 性能回退 | 运行时探测，自动降级普通页并告警 |
| 放弃网络缩小适用面 | 定位受限 | 明确只在同机场景对标，不与网络能力比较 |

### 目录与工程规范
- 遵循 CLAUDE.md：immutable-first、文件 200–400 行、显式错误处理（`std::expected`/`Result` 风格或 `std::error_code`/异常分层）、边界校验。
- 裸内存/原子/`reinterpret_cast`/syscall 等危险操作集中在 `core/platform`、`core/ring`，每处附 `// SAFETY:` 注释说明前置条件；其余代码避免裸操作。
- 每个 PR 走 code-reviewer + cpp-reviewer；安全相关走 security-reviewer。

---

## 4. 近期可立即执行的第一步（M0 起步清单）
1. 建立 CMake 工程，拆 `core/`（`salias_core` 静态库）/ `salias/`（公共头+库）/ `bench/` / `test/`，vcpkg 接 google-benchmark、GTest。
2. 定义 `Mapping` 抽象（C++20 concept 或抽象类）与 Linux `memfd` 实现骨架（先能双映射并自检地址连续）。
3. 写第一个失败测试：向双映射 ring 写跨边界数据，读出连续 → 驱动 M1。
4. 拉起 Aeron C++ IPC 基线 bench，落 `doc/benchmarks/aeron-baseline.md`。
5. 配 CI：`cmake` + `ctest` + google-benchmark + clang-tidy + cppcheck；debug 带 ASan/UBSan，并发测试带 TSan。

> 完成 M1 即可得到"无 term 轮转、无清零、无分片"的核心引擎，这是相对 Aeron 尾延迟优势的最大来源。

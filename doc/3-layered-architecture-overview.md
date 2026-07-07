# 3 - salias 分层架构总览

> 配套：`1-aeron-analysis-and-optimizations.md`（为什么这么做）、`2-implementation-plan.md`（里程碑）。
> 本文定义 salias 的**分层架构**：每层职责、依赖方向、对应模块、以及每层的详细文档索引。
>
> **技术栈**：C++23 / CMake + vcpkg / 仅 Linux / 核心不用协程。下文"模块"指 `core/` 下的目录与 C++ namespace（如 `salias::platform`），"抽象"用 C++ concept 或抽象基类表达。

---

## 0. 设计约束（决定分层的原则）

1. **单向依赖**：上层依赖下层，**下层永不依赖上层**。任何一层可被单独测试（下层用真实实现，不需要 mock 上层）。
2. **热路径无抽象税**：跨层调用在热路径上必须能被内联/单态化，抽象只存在于源码组织，不存在于运行时。
3. **unsafe 下沉**：裸内存/内存序/平台 syscall 尽量集中在 L0–L1，越往上越 safe。
4. **无协程于热路径**：核心不引入 async runtime；async 仅作 L7 的可选适配层（feature flag，默认关）。见 `2-implementation-plan.md` 并发模型决策。
5. **约定优于配置**：每层暴露少量必要参数，默认值开箱即用。

---

## 1. 分层总图

```
┌─────────────────────────────────────────────────────────────┐
│ L7  Public API & Async 适配层   (salias)                      │
│     Channel 门面 / driverless 握手 / 可选 async 适配           │
├─────────────────────────────────────────────────────────────┤
│ L6  可观测性层                  (salias::metrics)              │
│     共享内存计数器布局 / 只读观测                              │
├─────────────────────────────────────────────────────────────┤
│ L5  通道层                      (salias::channel)              │
│     SPSC / MPSC / 多订阅广播 / bulk 大消息                     │
├─────────────────────────────────────────────────────────────┤
│ L4  等待策略层                  (salias::wait)                 │
│     spin / pause / yield / backoff / futex(跨进程)            │
├─────────────────────────────────────────────────────────────┤
│ L3  流控与 Position 层          (salias::flow)                 │
│     64 位 position / 背压 / claim-commit / 批量               │
├─────────────────────────────────────────────────────────────┤
│ L2  帧格式层                    (salias::frame)                │
│     8 字节头 / 编解码 / 对齐 / 无头定长模式                    │
├─────────────────────────────────────────────────────────────┤
│ L1  环与内存原语层              (salias::ring)                 │
│     magic ring / 原子 cell / 缓存行隔离                        │
├─────────────────────────────────────────────────────────────┤
│ L0  平台抽象层                  (salias::platform)             │
│     双映射 mmap / shm / hugepage / NUMA / futex syscall       │
└─────────────────────────────────────────────────────────────┘
                         ↓ 依赖操作系统
                            Linux
```

**依赖方向严格自上而下。** 例如 L5 通道可以调用 L4 等待、L3 流控、L2 帧、L1 环；但 L1 环绝不 `#include` 任何 L2+ 的头文件。namespace 与目录同构：`salias::ring` 对应 `core/ring/`。

---

## 2. 每层职责与详细文档索引

| 层 | 模块 | 一句话职责 | unsafe 密度 | 详细文档 |
|----|------|-----------|:----------:|----------|
| **L0** | `platform` | 把"双映射共享内存 / 大页 / NUMA / futex"抹平成跨平台 trait | 高 | `4-L0-platform.md` |
| **L1** | `ring` | 提供无 term 轮转、无清零、无分片的 magic ring 与缓存行隔离原语 | 高 | `5-L1-ring.md` |
| **L2** | `frame` | 8 字节头的帧编解码与对齐；可选无头定长模式 | 中 | `6-L2-frame.md` |
| **L3** | `flow` | 64 位单调 position、背压判定、claim/commit 零拷贝、批量 | 中 | `7-L3-flow.md` |
| **L4** | `wait` | 可插拔等待策略；跨进程 futex 唤醒 | 中 | `8-L4-wait.md` |
| **L5** | `channel` | 组合下层，产出 SPSC/MPSC/广播/bulk 四种通道 | 低 | `9-L5-channel.md` |
| **L6** | `metrics` | 共享内存计数器布局，外部工具只读观测 | 中 | `10-L6-metrics.md` |
| **L7** | `salias` | driverless 握手、门面 API、可选 async 适配 | 低 | `11-L7-api.md` |

> 文档编号从 `4-` 起，与已有的 `1-/2-/3-` 连续。

---

## 3. 关键数据结构的层归属（避免职责漂移）

| 数据结构 | 归属层 | 说明 |
|----------|--------|------|
| `Mapping`（双映射句柄） | L0 | 持有两段虚拟地址 + 底层 fd/section |
| `Futex`（shm 上的等待字） | L0 | 封装 `FUTEX_WAIT/WAKE` 及平台等价物 |
| `MagicRing` | L1 | 基于 `Mapping`，暴露连续 `&[u8]` 视图 |
| `CacheAligned<T>` | L1 | 缓存行隔离包装，用于 head/tail/limit |
| `Frame` / `FrameHeader` | L2 | 8 字节头编解码 |
| `Position`（u64 单调） | L3 | 发布/消费进度 |
| `Producer` / `Consumer` 位置逻辑 | L3 | 背压、claim/commit |
| `WaitStrategy`（concept/抽象基类） | L4 | spin/pause/yield/futex 实现 |
| `Spsc/Mpsc/Broadcast/Bulk Channel` | L5 | 组合层，面向内部 |
| `Counters` 区布局 | L6 | 固定布局、缓存行隔离 |
| `Channel`（门面）/ `Publisher`/`Subscriber` | L7 | 用户可见 API |

---

## 4. 层与里程碑(M0–M6)的映射

`2-implementation-plan.md` 的里程碑是"交付节奏"，本文的层是"代码结构"。两者关系：

| 里程碑 | 主要涉及层 |
|--------|-----------|
| M0 骨架与基准 | 搭 L0–L7 空骨架 + CI + Aeron 基线 |
| M1 Magic Ring SPSC | **L0 + L1**（+ 最小 L2 帧、L5 SPSC 打通端到端）|
| M2 背压与 position | **L3** + L2 完整 |
| M3 等待策略与可观测 | **L4 + L6** |
| M4 MPSC/多订阅 | **L5**（Mpsc/Broadcast）|
| M5 大消息 bulk | **L5**（Bulk）|
| M6 跨平台与加固 | 全层 TSan/ASan/UBSan + libFuzzer + 长期稳态压测 + **L7** 冻结（仅 Linux，无跨平台工作）|

**实现顺序 = 自底向上**：先 L0→L1 打通 magic ring（M1 拿到核心引擎），再往上叠。每层完成即可独立测试。

---

## 5. 跨层契约（接口冻结点）

为保证并行开发与稳定性，以下接口是**层间契约**，一旦确定尽量不改：

1. **L0→L1**：`Mapping` 暴露 `as_ptr() -> *mut u8`、`len()`、保证 `[ptr, ptr+len)` 与 `[ptr+len, ptr+2*len)` 同物理页。
2. **L1→L3**：`MagicRing` 暴露"给定偏移返回连续切片"，不关心谁在读写。
3. **L2↔L3**：帧编解码是纯函数（`encode(header, payload)`/`decode(&[u8])`），不持有状态。
4. **L3→L5**：`Producer::claim/commit`、`Consumer::read/advance` 是通道层唯一入口。
5. **L4→L5**：`WaitStrategy::wait(&AtomicU64, expected)` / `signal()` 稳定签名。
6. **L6**：计数器布局一旦发布即为 ABI，外部工具依赖它，**只能追加不能重排**。
7. **L7**：面向用户的 `Channel`/`Publisher`/`Subscriber` 是稳定 API，v0.1 冻结。

---

## 6. 测试分层策略

| 层 | 主要测试手段 |
|----|-------------|
| L0 | 跨进程 fork 验证同物理页；ASan/UBSan 查映射逻辑；失败回滚用例 |
| L1 | 跨边界读写连续性；TSan + 自建并发交错测试穷举 head/tail interleaving |
| L2 | 编解码往返（属性测试/快速检查风格）；对齐边界；libFuzzer 解析 |
| L3 | position 单调性、背压语义、claim/commit 顺序；TSan |
| L4 | 唤醒正确性、无丢失唤醒、空载 CPU 采样 |
| L5 | 跨进程端到端；MPSC 多写压测（TSan + 交错测试）；尾延迟统计 |
| L6 | 计数器可被独立进程只读读取；布局稳定性快照测试 |
| L7 | 握手鲁棒性、async 适配层（可选）集成、API 文档示例即测试 |

> 每层详细文档包含：该层的**数据结构定义、公开接口、unsafe 契约与 SAFETY 说明、平台差异、测试清单、验收标准**。

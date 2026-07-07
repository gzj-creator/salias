# 1 - Aeron 分析与优化方向（纯共享内存竞品 · salias）

> 目标：构建一个**只做进程内/进程间共享内存传输、不做网络通信**的高性能消息库，
> 在延迟（尤其 p99/p99.9 抖动）、吞吐、CPU 效率、易用性上全面超越 Aeron 的 IPC 路径。
> 本文先解剖 Aeron，再给出我们应重点优化的方向。

---

## 一、Aeron 是什么（聚焦与我们相关的部分）

Aeron 是一套高性能消息传输系统，支持三种介质（media）：

- **UDP unicast / multicast**（网络）—— 与本项目无关，本文不展开。
- **IPC**（同机共享内存）—— 我们的直接对标对象。

它的核心抽象：

| 概念 | 含义 |
|------|------|
| **Media Driver** | 独立进程（或嵌入式线程），负责建立/管理 channel、流控、心跳、计数器 |
| **Publication** | 发布端，向一个 stream 写消息 |
| **Subscription / Image** | 订阅端，从一个 stream 读消息 |
| **Log Buffer** | 每条 stream 对应的共享内存文件，承载真正的数据 |
| **Term** | Log Buffer 内的分段，采用 3 段轮转（term rotation） |
| **CnC file** | Command-and-Control 文件，客户端与 Driver 交换控制命令的共享内存 |
| **Counters file** | 位置计数器（发布位置、消费位置、publisher limit 等） |

### Aeron IPC 的数据通路

1. **建立阶段（控制面，走 Driver）**：客户端通过 CnC 文件向 Media Driver 发命令（ADD_PUBLICATION / ADD_SUBSCRIPTION）。Driver 的 Conductor 线程创建 Log Buffer 文件（`mmap`，通常在 `/dev/shm`），把文件名回传给客户端。
2. **传输阶段（数据面，不走 Driver）**：Publisher 直接把消息写入 Log Buffer 的当前 term；Subscriber 直接从 Log Buffer 读。**数据不经过 Driver 拷贝**，这是 Aeron 快的核心原因。
3. **流控/心跳（控制面，走 Driver）**：Driver 的 Conductor 循环周期性读取 Subscriber 的消费位置计数器，据此更新 **publisher limit** 计数器。Publisher 每次发布都要检查 publisher limit 决定是否背压。

关键点：**IPC 场景下数据零拷贝、旁路 Driver，但"流控更新"仍依赖 Driver 的 Conductor 轮询循环。**

### Log Buffer 结构（Aeron 的精华）

```
+-----------+-----------+-----------+------------------+
|  Term 0   |  Term 1   |  Term 2   |  Log Meta Data   |
+-----------+-----------+-----------+------------------+
```

- Term 长度必须是 2 的幂（IPC 默认 64 MB，范围 64 KB ~ 1 GB）。
- 位置 `position` 是 64 位，由 `term id` + `term offset` 计算得出，单调递增。
- 每条消息前有 **32 字节的 DataHeaderFlyweight**（frame length / version / flags / type / term offset / session id / stream id / term id）。
- 帧按 **32 字节对齐**（`FRAME_ALIGNMENT`）。
- 跨 term 边界时写 padding 帧，并轮转到下一个 term；被腾空的 term 需要清零（memset）以便复用。

---

## 二、Aeron 的优点（要保留/借鉴的）

1. **数据面零拷贝 + Driver 旁路**：IPC 时 publisher→subscriber 无中间拷贝。这是必须保留的底线。
2. **单调递增的 64 位 position 模型**：优雅地表达"已发布/已消费"进度，天然支持背压、回放、多订阅者独立进度。
3. **term 轮转 + 2 的幂掩码**：偏移计算用位运算（`offset & (term_len-1)`），无除法/取模。
4. **计数器（counters）体系**：所有位置、错误、指标都在共享内存里，运维/监控可零侵入观测。
5. **`tryClaim`/`offer` 双 API**：`tryClaim` 支持"直接在 log buffer 上原地构造消息"实现真零拷贝；`offer` 适合已有 buffer 的场景。
6. **成熟的背压语义**：publisher limit 让发布端在订阅端跟不上时明确得到 `BACK_PRESSURED`，而不是丢数据或无限缓冲。
7. **可观测性与生态**：Archive（持久化/回放）、Cluster（Raft）建立在同一 log 抽象上，设计一致性强。

---

## 三、Aeron 的缺点 / 我们的机会

### 1. Media Driver 是强制中介（部署与启动耦合）
- 即便是纯 IPC，也**必须先跑一个 Media Driver 进程**（或嵌入式 driver 线程），客户端要先与之握手。
- 带来：额外进程/线程、CnC 文件依赖、启动延迟、故障域扩大（driver 挂了全挂）。
- **机会**：纯共享内存场景根本不需要中介。可做 **brokerless / driverless** 的点对点 channel——建立通道就是"两端 `mmap` 同一个文件 + 一次轻量握手"。流控直接由两端的计数器完成，无需第三方轮询。

### 2. 流控依赖 Conductor 轮询（增加尾延迟与耦合）
- Publisher limit 由 Driver Conductor 循环（默认若干毫秒一轮，或 busy loop 占 CPU）更新，publisher 的"解背压"感知有滞后。
- **机会**：SPSC/直连场景下，publisher 直接读 subscriber 的消费位置（缓存行隔离的单个 64 位计数），**零中介、零滞后**。

### 3. 32 字节帧头 + 32 字节对齐（小消息开销大）
- 每条消息固定 32 字节头，且向上对齐到 32 字节。发 8 字节的消息，实际占用 ≥ 64 字节，**有效载荷利用率可低至 12.5%**。
- 头里的 `session id / stream id / term id` 在"单通道点对点"场景基本是冗余的。
- **机会**：精简到 **8 字节头**（length + flags/seq），对齐降到 **8 字节**。对固定长度消息可提供"无头帧"模式（header 由 channel 元数据统一描述）。

### 4. Term 轮转带来周期性抖动
- 跨 term 边界要写 padding、切换 term、并对被回收的 term **清零 64 MB**。清零虽可后台化，但仍是 p99.9 抖动来源，且占用内存带宽。
- **机会**：改用 **magic ring buffer（魔术环形缓冲）**——用 `mmap` 把同一段物理内存**连续映射两次**，逻辑上首尾相接。跨边界的消息在虚拟地址上是连续的，**无需 padding、无需分片、无需清零**，彻底消除 term 轮转类抖动。

### 5. 大消息分片 / 重组（Fragmentation）
- 超过一个 term 或 MTU 概念的消息被拆成多帧，订阅端用 `FragmentAssembler` 重组——**引入额外拷贝和延迟**。
- **机会**：magic ring buffer 下，只要消息 ≤ ring 容量即可**单帧连续写入**，消除分片/重组路径（大消息走单独的 bulk channel）。

### 6. 以 JVM 为主，GC 与预热抖动
- 主力实现是 Java（虽大量用堆外内存，仍有 GC/安全点/JIT 预热抖动）；C++ 客户端存在但生态更薄。
- **机会**：用 **C++23** 实现，无 GC、无安全点、无预热抖动；`std::atomic_ref` 直接对共享内存原子操作（不手写裸汇编）、`alignas` + `std::hardware_destructive_interference_size` 精确控制缓存行布局、`std::span` 表达连续视图、concepts 约束模板，`std::expected` 表达显式错误返回。低延迟中间件领域 C++ 是主流，Aeron 也有 C++ 客户端可对标复用。

### 7. False sharing 与缓存行管理不彻底
- 生产/消费位置若落在相邻缓存行，会产生跨核 false sharing。
- **机会**：head/tail/limit 各自独占缓存行（padding 到 64/128 字节），生产者只写 tail、消费者只写 head，**读写分离 + cache line 隔离**。

### 8. 单一等待策略、CPU 占用高
- 低延迟依赖 busy-spin，长期空转烧 CPU、发热、抢占其他线程。
- **机会**：可插拔 **wait strategy**：busy-spin / spin+pause / yield / backoff / **futex 阻塞唤醒**，按场景权衡延迟与能耗。

### 9. 配置复杂、上手成本高
- Aeron 配置项极多（term 长度、MTU、flow control、多种 channel URI 参数），初学者门槛高。
- **机会**：**约定优于配置**——默认参数开箱即用；单通道点对点、多播扇出、bulk 大消息三种预设模板。

### 10. 未充分利用大页 / NUMA
- 默认页 4 KB，大 buffer 下 TLB miss 明显；跨 NUMA 分配影响延迟。
- **机会**：可选 **huge pages（2 MB/1 GB）** 承载 ring；**NUMA 感知**分配，把 ring 绑到生产/消费者所在节点。

---

## 四、我们的差异化设计要点（salias 应做的优化汇总）

| 维度 | Aeron IPC | salias 目标 |
|------|-----------|-------------|
| 中介 | 必须 Media Driver | **Driverless**，两端直连 `mmap` |
| 流控 | Conductor 轮询更新 limit | 两端计数器直读，**零滞后** |
| 帧头 | 32 字节 | **8 字节**（可选无头定长模式） |
| 对齐 | 32 字节 | **8 字节** |
| 环形缓冲 | 3-term 轮转 + 清零 | **magic ring（双映射）**，无轮转/无清零 |
| 分片 | 需要，重组有拷贝 | 单帧连续，**免分片** |
| 语言/运行时 | JVM 为主 | **C++23**，无 GC 抖动 |
| False sharing | 部分处理 | **缓存行独占** head/tail/limit（`alignas` + `std::hardware_destructive_interference_size`）|
| 等待策略 | 主要 busy-spin | **可插拔**：spin/yield/futex |
| 内存页 | 默认 4 KB | **huge pages + NUMA 感知** |
| 配置 | 复杂 | **约定优于配置**，模板化 |
| 零拷贝 API | offer / tryClaim | **claim/commit + batch**（保留并增强） |

### 保留 Aeron 的三块基石
1. **数据面零拷贝**。
2. **64 位单调 position 背压模型**（多订阅者独立进度、可回放）。
3. **共享内存计数器可观测性**。

### 预期性能收益（定性）
- **延迟**：driverless + 无分片 + 小头 → 更低 p50，尤其**消除 term 清零/轮转导致的 p99.9 尖峰**。
- **吞吐**：小头 + 8 字节对齐 + 批量 API → 小消息有效带宽显著提升。
- **抖动**：Rust 无 GC + magic ring 无周期性清零 → 尾延迟更平。
- **CPU**：可插拔 futex 等待 → 空载时几乎零占用。

---

## 五、风险与边界（诚实标注）

- **magic ring 双映射**依赖 OS（Linux `memfd_create`+`mmap` 两次 / `MAP_FIXED`）。跨平台（macOS/Windows）需各自实现，Windows 用 `MapViewOfFileEx` 两次映射同一 section。
- **放弃网络**意味着放弃 Aeron 最大的适用面；我们只在**同机**场景声称"更强"，不与其网络能力比较。
- **多订阅者/多播**扇出、以及**持久化回放**（对标 Archive）属于第二阶段，先把点对点 SPSC/MPSC 打穿。
- **正确性门槛高**：无锁 + 内存序 + 双映射极易踩坑，必须有 TSan/ASan/UBSan + 自建并发交错测试 + 压力测试与形式化推理（见计划文档）。

> 详细里程碑、任务拆解、验收基线见 `2-implementation-plan.md`。

---

## 六、技术栈决策（已定）

| 项 | 选择 | 理由 |
|----|------|------|
| 语言 | **C++23** | 低延迟中间件/HFT/交易所领域主流；Aeron 有 C++ 客户端可对标复用；招聘与团队现成。关键武器：`std::atomic_ref`（直接对共享内存原子操作，无需手写裸原子）、`alignas` + `std::hardware_destructive_interference_size`（精确缓存行布局）、`std::span`（连续视图）、concepts（模板约束）、`std::expected`（显式错误返回）。 |
| 构建/依赖 | **CMake + vcpkg** | CMake 跨平台主力，vcpkg 管第三方依赖，IDE 友好，CI 成熟。 |
| 平台 | **只做 Linux** | 砍掉跨平台，L0 大幅瘦身：`memfd_create` + `ftruncate` + 双 `mmap`（`MAP_FIXED`）实现 magic ring、`MAP_HUGETLB` 大页、`mbind`/libnuma NUMA 绑定、`FUTEX_WAIT/WAKE` 跨进程等待。不做 macOS/Windows。 |
| 并发模型 | **核心不用协程** | 低延迟目标与协程让出相悖。等待走可插拔 wait strategy（spin/pause/yield/backoff/futex）。仅 L7 提供可选 async 适配层（默认关）。 |
| 正确性验证 | TSan/ASan/UBSan + 自建并发交错测试 | C++ 无 loom/miri 这类 Rust 专属工具；用 sanitizer + 压力测试 + 形式化推理补位。 |

> 性能上 C++ 与 Rust 峰值打平（同为 AOT、无 GC、同 LLVM 量级），选 C++ 的决定因素是生态同源与团队，不是性能。

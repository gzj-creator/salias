# Phase 3 E2：Aeron 风格非拥有型 claim 隔离计划

日期：2026-10-22，Asia/Singapore。
状态：**设计完成，未执行**；不改 `src/`，不提交、不打 tag、不推送。
前置证据：[P3-2 多线程发布进程报告](phase3-p32-multithread-report.md)、
[P3-1 尾部诊断报告](phase3-p31-tail-diagnosis-report.md)。

## 1. 背景与问题

P3-2 已经确认：同一二进制下，MT publisher 进程的吞吐显著低于 ST：

| payload | ST 中位数 | MT 中位数 | ST/MT | 方向 |
|---|---:|---:|---:|---:|
| 64 B | 8.849060 Mmsg/s | 6.223405 Mmsg/s | 1.422 | 6/6 组 |
| 256 B | 16.298050 Mmsg/s | 6.246215 Mmsg/s | 2.609 | 6/6 组 |

但该实验只证明“多线程 publisher 进程有成本”，没有拆开以下来源：

1. `try_claim()` 成功时复制 endpoint `shared_ptr`、claim 结束时释放引用造成的原子计数；
2. glibc 进入多线程状态后，其他运行时原子/慢路径；
3. 一个仍然存活的休眠 helper 线程对内核调度和内存管理的影响；
4. 通用 MPSC 1P 路径本身可能保留的协调成本。

本计划只解决前三个问题，不把 P3-2 的相关性直接当作产品根因，也不把
“删除引用计数”当作已批准的修复。

## 2. 可借鉴的 Aeron 约束

本仓库 Aeron 对比客户端的快路径是：

```cpp
aeron::concurrent::logbuffer::BufferClaim claim;
const auto position = publication->tryClaim(payload, claim);
claim.buffer().putInt64(claim.offset(), marker);
claim.commit();
```

关键不在“攒批”，而在于：

- `publication` 在热循环外长期存活；
- `BufferClaim` 是循环外复用的临时对象；
- 每条 claim 不复制 `Publication`/client 的共享所有权；
- 单条消息仍即时 commit，不等待凑批。

Salias 当前 `PublishClaim` 每条消息复制一次 `shared_ptr<PublisherEndpoint>`，commit 后
`reset()`。ST 进程由 libc 单线程快路径绕过原子计数；MT 进程则进入原子引用计数路径。
这是本计划要隔离的候选，不是已证明的唯一根因。

## 3. 目标与非目标

### 3.1 目标

- 量化非拥有型 claim 能否收回 P3-2 的 MT 吞吐损失。
- 区分“libc 多线程状态”与“仍存活的 helper 线程”的影响。
- 在不改变 wire protocol、不默认引入 batching 的前提下，为是否修改 claim
  生命周期契约提供证据。
- 为后续产品候选保留完整、可复核的 A/B 证据。

### 3.2 非目标

- 不重跑或重新判定 P3-1 G4。
- 不改变默认发布路径为批量发布。
- 不通过 sleep、固定窗口或“等凑够 N 条”来优化吞吐。
- 不在本阶段修改产品 `src/`。
- 不把诊断库、非拥有 claim 或 helper 线程带入产品性能结论。

## 4. 实验臂

诊断客户端继续基于未修改 v3.0.0 benchmark，链接诊断构建的产品库。所有轮次使用
A 屏障、1P、1 MiB ring、batch=1、poll-limit=64。

| 臂 | publisher 线程状态 | claim 所有权 | 用途 |
|---|---|---|---|
| ST | 无 helper，实际 1 线程 | 当前 owning `shared_ptr` | 单线程上限对照 |
| MT-O | 休眠 helper 存活，实际 2 线程 | 当前 owning `shared_ptr` | 复现 P3-2 MT 基线 |
| MT-X | helper 创建后立即退出，发布时实际 1 线程，但 libc 多线程标志为 0 | 当前 owning `shared_ptr` | 隔离 libc 永久多线程状态 |
| MT-B | 休眠 helper 存活，实际 2 线程 | 非拥有型 claim | 隔离逐条引用计数 |

`MT-X` 必须在发布前分别记录：

- `/proc/self/task` 实际线程数 = 1；
- `__libc_single_threaded` = 0；
- helper 已完成 join，不参与发布或轮询。

`MT-B` 的 diagnostic-only claim 仍要求 Publisher 在 claim 生命周期内存活。它只用于
测量，不是可发布的产品实现。

## 5. 非拥有型 claim 的诊断实现

在构建目录中由生成器复制当前产品源码并注入 diagnostic-only 变体，不直接修改
`src/`。变体只更换 `PublishClaim` 的保活字段：

```cpp
// 诊断变体示意：不拥有端点，契约由调用方保证。
PublisherEndpoint<M>* endpoint_;
```

保持不变的部分：

- `Publisher` 仍长期持有 `shared_ptr<PublisherEndpoint>`；
- `try_claim()` 仍返回同一 `flow::Claim`；
- `commit()` 仍调用同一底层提交逻辑；
- 不改变帧布局、内存序、协议版本或产品默认路径。

必须显式记录并测试新契约：claim 不得比 Publisher 活得更久；未提交 claim 的使用限制
与当前实现一致。诊断变体出现悬空引用属于未定义行为，不能作为产品候选直接发布。

如果只构建一个诊断变体，需要同时保留当前 owning 构建。两种二进制使用相同编译器、
相同优化参数和相同 benchmark 源码；必须承认重新链接可能带来布局差异，因此不能把
微小差异当成引用计数结论。

## 6. 矩阵与顺序

### 6.1 吞吐

| 项目 | 固定值 |
|---|---|
| 拓扑 | FIFO 1P2S |
| payload | 64 B、256 B |
| 消息 | 50,000,000/轮 |
| ring | 1 MiB |
| batch / poll-limit | 1 / 64 |
| 组 | 每配置 6 组 |
| 每组顺序 | ST、MT-O、MT-X、MT-B、MT-B、MT-X、MT-O、ST |
| 正式轮次 | 2 payload × 6 组 × 8 轮 = 96 |

每组包含每个臂两轮，前后对称排列；不因低吞吐停止、筛轮或重跑。

### 6.2 短延迟

| 项目 | 固定值 |
|---|---|
| 拓扑 | FIFO 1P1S |
| payload / 起点 | 64 B、256 B × attempt、planned |
| 消息 | 2,000,000/轮 |
| rate | 1,000,000 msg/s |
| 采样 | 每 16 条 |
| 组 | 每配置 6 组 |
| 每组顺序 | 同 §6.1 |
| 正式轮次 | 4 配置 × 6 组 × 8 轮 = 192 |

所有分位数按轮保留，不跨轮池化。尾部必须报告每轮 p99、p99.9 及范围；不以中位数
掩盖长尾。

### 6.3 预热与环境

- 每个配置每臂一次独立预热，不进入统计。
- 另有 4 臂功能 smoke，全部保留但不进入统计。
- coordinator CPU0；subscriber 从 CPU1 起；publisher 紧随其后。
- 每轮独立 systemd cgroup：AllowedCPUs=0-3、Nice=5、MemoryHigh=640 MiB、
  MemoryMax=704 MiB、swap=0。
- 每轮保存命令、完整环境、客户端/输入/诊断库 SHA、stdout/stderr/cgroup、退出码。

## 7. 判定规则

本轮是**诊断**，不是产品验收；不设“必须提升 X%”的发布门槛，但提前固定证据解释规则。

### 7.1 有效性

以下任一失败则本轮无效，不得用剩余数据勉强解释：

- 任一轮未完成、worker 失败、超时或 cgroup 有内存事件；
- ST/MT-O/MT-X/MT-B 的实际线程数或 libc 状态与上表不符；
- claim 所有权变体未通过身份、内容、序号和交付计数检查；
- ABBA 顺序、SHA、命令或环境不一致。

### 7.2 引用计数假设

以吞吐为主，按配置和组计算：

```text
recovered_fraction =
    (MT-B - MT-O) / (ST - MT-O)
```

同时报告中位数、均值、CV、组内比值和方向数。判定为“强候选”需同时满足：

- 64 B 与 256 B 的 MT-B 均优于 MT-O；
- 每个 payload 至少 5/6 组方向一致；
- 回收比例在中位数上至少 50%，且不是由单轮离群值造成。

若回收比例低于 25% 或方向不一致，则拒绝“逐条 `shared_ptr` 计数是主要成本”的假设，
转向 single-writer/MPSC 协调路径或调度诊断。

25%–50% 之间只记为“可能有贡献”，不得据此改产品。

### 7.3 libc 与 live helper 假设

- `ST vs MT-X`：估计仅保留 libc 多线程状态、无 live helper 时的成本；
- `MT-X vs MT-O`：估计 live helper 本身的成本；
- 若 `MT-X` 已接近 `MT-O`，live helper 不是主要差异来源；
- 若 `MT-X` 接近 `ST`，libc 多线程状态本身不足以解释 P3-2 的成本。

### 7.4 延迟约束

对每个配置、臂、轮分别记录 p50/p99/p99.9 和 max，并报告轮间范围。若 MT-B 的吞吐收益
伴随任一配置的 p99/p99.9 在所有组持续恶化，则不能把它视为低延迟路径候选。不得用
吞吐收益抵消尾部退化，也不得把不同分位数相减来做阶段归因。

## 8. Batching 边界

本计划**不包含**攒批发布，原因是固定窗口或整批 commit 会在低负载和非满载时增加等待、
延迟和 head-of-line blocking。Aeron 的 `BufferClaim` 快路径本身也是单条 claim/commit，
不是等待凑批。

如果未来需要批量路径，必须另立计划并满足：

- 默认低延迟路径仍为 batch=1；
- 批量只处理已经就绪的消息，不为凑批 sleep；
- 单独比较 batch=1/2/4/8 的吞吐、p50/p99/p99.9 和完成度；
- 增加低负载、突发和 2P2S 场景；
- 不用批量吞吐收益掩盖尾部退化。

## 9. 正确性与回归

诊断实现至少需要：

1. claim 移动、重复 commit、moved-from、未提交销毁、Publisher 先于 claim 销毁的契约测试；
2. ST/MT-O/MT-X/MT-B 线程和 libc 状态自检；
3. 1P2S 内容、身份、序号、交付计数完整；
4. Release CTest、相关 ASan/UBSan/TSan 回归；
5. 所有 Python 测试、JSON 校验、输入哈希和 `git diff --check`。

非拥有 claim 的 use-after-free 不属于可接受行为；测试必须明确区分“契约内使用”与
“契约外误用”，不能用 sanitizer 通过来证明所有权契约安全。

## 10. 产品化条件

只有在 §7.2 给出强候选且用户明确批准后，才讨论产品 API：

- **方案 A**：保留 `claim.commit()`，claim 持非拥有端点裸指针；源码改动小，但契约风险高；
- **方案 B**：改为 `publisher.commit(claim)` 的 Aeron 风格 API；所有权关系更清晰，但需
  API 迁移和 ABI 变更。

两种方案都不修改 wire protocol。不能在本阶段直接删除保活引用，也不能把诊断变体提交为
产品实现。

## 11. 交付物

- `tools/aeron_compare/build_e2_claim_probe.py`：从固定源码生成 owning/borrowed 诊断库和客户端；
- `doc/plans/runners/p3-e2-claim.py`：固定矩阵、ABBA、cgroup 和证据采集；
- `tools/aeron_compare/analyze_e2_claim.py`：独立身份复核与统计；
- `doc/plans/phase3-e2-claim-report.md`：结果、边界和产品建议；
- 对应测试与独立证据目录，不提交、不打 tag、不推送。

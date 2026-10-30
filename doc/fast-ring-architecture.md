# Salias 固定槽 Ring 与 Aeron IPC 架构分析

更新日期：2026-10-29，Asia/Singapore。

本文分析当前 `FastSpscRing`、`FastSpmcRing`、`FastMpscRing`、`FastMpmcRing` 候选实现与
Aeron 1.52.0 IPC 的差异。候选名称只有在正确性和性能门禁完成后才会去掉 `Fast` 前缀；
当前验收状态以 [Fast Ring 性能替换计划](plans/2026-10-29-fast-ring-performance-replacement.md)
为准。

## 1. 两条数据面

### Aeron IPC

Aeron 的数据面是 publication 到 subscription 的共享 log：

1. Media Driver 在控制面处理 `ADD_PUBLICATION` / `ADD_SUBSCRIPTION`，建立 log buffer、
   counters、publisher limit 和 CnC 通道。
2. Publisher 在数据面直接 `tryClaim` 当前 term 中的 frame，写 payload 后 publish。
3. Subscriber 直接轮询 log buffer 并推进独立消费位置。
4. Driver 周期观察消费计数器并发布 publisher limit；发布端据此执行背压。

它的核心优势是成熟的 log 抽象、跨机能力、动态订阅、term 轮转、流控和多语言生态。
纯 IPC 数据不经过 Driver，但控制面仍需要 Driver，发布窗口也受 Driver 流控更新节奏影响。

### Salias 固定槽 Ring

Salias 候选实现只覆盖同机、固定 payload、固定参与者的可靠 FIFO：

1. 每个 producer 独占一条 lane；多 producer 不在同一个 published 字段上执行 RMW。
2. 每个 slot 固定 stride。slot 内先写 payload，再对 8 B 完成序号做 release-store。
3. consumer 对当前 slot 的完成序号做 acquire-load；成功后才读取 payload。
4. consumer 对每条 lane 持有自己的 consumed 位置。fanout 时每个 subscriber 独立推进，
   生产者按最慢 subscriber 背压，不覆盖仍未释放的 slot。
5. SPSC 直接使用一条 lane；SPMC 在一条 lane 上维护多个 consumer cursor；MPSC 轮询多条
   producer lane；MPMC 组合两者，并为每条 lane/consumer 保存 cursor。

数据面没有 Driver、没有可变长 frame 扫描、没有 term 轮转和回收 term 清零。参与者必须在
流量开始前创建句柄，消费者按 producer lane 内部 FIFO 交付，不承诺跨 producer 全局顺序。

## 2. 内存协议

以 SPSC 为例，共享区由独立 cache line 的元数据、consumer cursor 和固定 slot 数组组成：

```text
identity / ready | consumer cursor 0 | consumer cursor 1 | slot 0 | slot 1 | ...
                                                         fixed stride <-------->
```

每个 slot 的布局为：

```text
8 B completion sequence | PayloadSize bytes | zero padding to 64 B stride
```

64 B payload 的 slot stride 为 128 B，256 B payload 的 stride 为 320 B。slot 序号本身承担
“本条已完整发布”的完成位，避免再维护每消息反弹的全局 published cache line。producer
只在 lane 满时读取 consumer cursor；consumer 可以处理若干条后通过 `flush_release()` 成批
发布进度。MPSC/MPMC 的 consumer 每条 lane 最多停留 64 条，避免单个高流量 producer 让
其他 lane 长期饥饿。

核心 happens-before 链为：

```text
consumer release(cursor = N)
    -> producer acquire(slowest >= N)
    -> overwrite slot N
    -> payload stores
    -> completion sequence release-store(N+1)
    -> consumer acquire-load(N+1)
    -> payload loads
```

未提交的 claim 不会写完成序号，因此 consumer 不可见。consumer 未 release 的 slot 不会被
producer 复用。消费者进程退出也不会推进 cursor，安全结果是明确背压，而不是静默覆盖。

## 3. 能力边界

| 维度 | Salias 固定槽 Ring | Aeron IPC |
|---|---|---|
| 部署 | 无 broker/driver，直接映射共享内存 | 需要 Media Driver 或嵌入式 Driver |
| payload | 编译期固定大小 | 可变长度 frame |
| 参与者 | 创建时固定 producer/consumer 数量 | 可动态增删 publication/subscription |
| 交付 | 每 producer lane FIFO，fanout 给所有 consumer | log stream 顺序，subscription 独立位置 |
| 背压 | 直接读取慢 consumer cursor | Counter + Driver 更新 publisher limit |
| 顺序 | 不提供跨 producer 全局顺序 | stream 内 publication/frame 顺序 |
| 持久化 | 不支持 | 可接 Archive 等 log 能力 |
| 网络 | 不支持 | UDP unicast/multicast 等 |
| 丢弃/TTL | 候选实现不提供 | 由上层应用语义决定 |

因此这些 ring 不能替换通用 `Channel` 的 Ordered、Latest、TTL、动态订阅和恢复语义。它们是
固定 schema、可靠、同机 IPC 热路径的专用类型。

## 4. 为什么当前实现能提高吞吐

主要收益来自减少每一步的固定成本，而不是单纯增加缓冲：

- **去除可变帧扫描**：consumer 直接检查下一固定 slot 的完成序号。
- **去除全局发布热点**：completion sequence 与 payload 同 slot，不产生每消息集中的
  published cache line invalidation。
- **按 producer 分 lane**：MPSC/MPMC 不共享 producer RMW，扩容主要增加独立 cache line。
- **按 consumer 分 cursor**：MPMC 消费者无需修改同一 head；生产者在慢路径读取最慢位置。
- **批末刷新进度**：consumer 每批只发布一次 cursor，减少与 producer 的 cache coherence
  往返。
- **限制 lane 批次**：64 条上限兼顾批处理收益和跨 producer 公平性。

这些优化与 Aeron 的 log 设计解决的问题不同。Aeron 为通用可变长消息和跨机传输保留了更多
元数据与控制面；Salias 固定槽路径将能力缩小到明确契约后，才能把这些成本从热路径移除。

## 5. 延迟含义

- **attempt p50/p99** 从 `try_claim` 前开始，包含背压等待、payload 写、发布和端到端投递，
  是判断应用发布路径卡顿的主要指标。
- **planned p50/p99** 从开环计划到达时间开始，额外包含 producer 进程的调度积压；它反映
  当前机器是否按时执行，不能替代 attempt 延迟。
- p99 使用固定对数桶直方图；每个案例先计算各 subscriber 分位数并取最差值，再对独立轮次
  取中位数。不会把不同轮次的直方图合并后伪造百分位。

共享 vCPU 的短测只能决定是否继续开发，不能作为独占核生产 SLA。正式结果、原始输出和
失败边界见 [2026-10-29 Fast Ring 对比](benchmarks/2026-10-29-fast-ring/README.md)。

### 5.1 当前开发结果说明什么

**是的，当前设计明显更偏吞吐。** 它把 Aeron 保留的可变 frame、动态参与者、Driver
流控和跨机能力换成固定的 lane/slot 布局，因此能在小 payload 下缩短发布与消费路径。
同时它没有放弃可靠背压，attempt p50/p99 也守住了门禁；但 MPMC p99.9 证明“吞吐优先”
仍会留下与调度和共享核相关的尾延迟问题，不能简单称为全面低延迟领先。

这里的性能数据直接来自 `Fast*Ring` 热路径；`FixedChannel` 的集成正确性已验证，但
`FixedSubscriber::poll()` 仍逐条发布 consumer cursor，尚未接入 ring 的 64 条批量
`release(false)` + `flush_release()`。因此这些数字是 ring 结果，不是完整产品 API 的 SLA。

1 轮预热、5 轮正式 ABBA 的开发矩阵中，8/8 单元的发布吞吐均至少达到 Aeron 的 1.10x，
attempt p50/p99 也均低于 Aeron。SPSC 64 B 对 Aeron 为 2.51x，MPSC 64 B 为 1.70x，
SPMC 64 B 为 5.02x，MPMC 64 B 为 5.00x；256 B 为 1.31x 至 4.34x。相对 v4.0.1
通用 FIFO，8/8 单元提升 1.48x 至 16.31x。这说明固定 schema 后消除 frame 扫描、
driver 流控反馈和全局发布热点，能直接换来吞吐和常见分位数收益。

Fast 矩阵全部 240 次正式性能轮已完成逐字节、producer 身份、每 producer 序号、顺序和
重复校验；Fast 与 FIFO 两套矩阵各发布 15.2 亿条、交付并校验 22.8 亿次。这还不是最终
签署：正式矩阵尚未达到 6 组 x 20 轮，且 4 vCPU 上的 MPMC driver 仍与 subscriber0 共核。
MPMC attempt p99.9 为 Fast **3.178 ms** 对 Aeron **1.671 ms**（64 B），
**1.589 ms** 对 **1.163 ms**
（256 B）；其余 6 个单元的 attempt p99.9 低于 Aeron。planned 中 SPMC 64 B 的
p99/p99.9 也未领先。因此当前实现首先是一个吞吐优先、同时守住 p50/p99 门禁的固定拓扑
快路径，不能声称所有延迟分位全面领先。

## 6. 继续超过 Aeron 的条件

固定槽路径继续优化时不应用牺牲正确性换取数字。优先级如下：

1. 保持 payload completion、cursor release 和背压不变量，先排除重复、覆盖和伪完成。
2. 用独占核或明确隔离的 vCPU 固定 producer/consumer，消除调度长尾后再判断 p99。
3. 对容量、lane batch 和 consumer flush batch 做单变量 ABBA，不能同时改多个热路径参数。
4. 与 Aeron 使用相同进程模型、CPU 预算、消息数量、采样率和 payload 写入范围。
5. 同时报告吞吐和 attempt p50/p99；只报告均值或最好一轮不构成性能证明。

针对当前结果，最值得做的是以下几项，而不是继续增加通用功能：

1. **先解决 MPMC 的 p99.9。** 在至少 5 个独占 vCPU 上把 driver、coordinator、2 个
   producer 和 2 个 consumer 分离，再复跑 p99/p99.9。若尾延迟随核隔离消失，产品部署
   应给出明确的隔离要求；若仍存在，再分析 lane 轮询和 cache migration。
2. **把 consumer release 批量化带到生产 API。** ring 已支持 `release(false)` 后
   `flush_release()`，但 `FixedSubscriber::poll()` 当前仍逐条发布 cursor。可按
   `max_messages` 在批末刷新，并用 ABBA 测试 8/16/32/64 条窗口，兼顾背压时延。
3. **提供批量 claim。** 同一 producer lane 可一次 claim 连续的多个固定 slot，每批只推进
   producer 本地位置并在最后一个 completion 前维持批量可见性。该设计必须保留按消息
   completion 或引入批次 completion 的崩溃安全证明，不能只靠放宽原子序来提数字。
4. **把最慢 cursor 读取限制到真实满环。** 继续使用 producer 本地缓存；对 SPMC/MPMC
   只在剩余空间不足时扫描 cursor，避免每批读取所有 consumer 的 cache line。
5. **按 payload 选择发布方式。** 64/256 B 继续普通 store；更大的固定 payload 才评估
   非临时写与预取，避免对小消息引入额外指令和弱化缓存局部性。
6. **减少跨进程共享元数据。** 控制段只在 create/connect 校验，数据面不要引入心跳、
   schema 转换或统计原子；诊断计数必须能编译期关闭。
7. **固定部署条件。** 使用 2 MiB 大页、`MAP_POPULATE`、关闭 swap、绑定 NUMA、同一
   NUMA 节点调度，并避免 Aeron driver 与数据面 worker 共享物理核。任何数字都必须带上
   这些条件和完整 SHA-256。

Aeron 的优势是通用 log、动态参与者、流控、网络和成熟生态。Salias 不应在通用功能数量上
追赶 Aeron；可赢的路线是把固定 schema、固定参与者的同机路径做到更短，同时明确它不适用的
场景。性能轮全文校验已经完成；只有在至少 5 个独占 vCPU（或独占裸机）上完成 6 组 x 20 轮
ABBA，才应去掉 `Fast` 前缀。

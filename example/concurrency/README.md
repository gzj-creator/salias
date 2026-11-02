# concurrency：固定槽 FastRing 并发拓扑

本目录使用 `FixedChannel`，底层按拓扑选择固定槽 `Fast*Ring`。它只支持编译期固定的 payload 大小和
参与者数量，不经过通用 Channel 的变长 frame、动态订阅、TTL 或 Latest 语义，适合追求最低热路径
开销的同机 IPC。
示例文件名直接使用完整的 `FixedTopology` 值，例如 `Spsc` 对应 `spsc.cpp`；不额外添加
`fast_` 前缀，因为 `Fast*Ring` 仍是候选实现名，而拓扑才是稳定的公共分类。

| 示例 | 底层 ring | producer | consumer | 场景 |
|---|---|---:|---:|---|
| `spsc.cpp` | `FastSpscRing` | 1 | 1 | 一条稳定数据流，如行情网关到策略引擎 |
| `mpsc.cpp` | `FastMpscRing` | 2 | 1 | 多路数据汇聚到一个消费者 |
| `spmc.cpp` | `FastSpmcRing` | 1 | 2 | 一份数据 fanout 给策略和监控 |
| `mpmc.cpp` | `FastMpmcRing` | 2 | 2 | 多路写入，策略和审计各自收全量 |

四个示例使用同一个 `Quote` 消息和 schema，在单进程内 `create()` 后直接创建端点，方便对照拓扑差异；
跨进程时创建者仍调用 `create(name)`，另一端改为 `connect(name)`。

共同约束：

- 容量单位是每个 producer lane 的 slot 数，必须是 2 的幂；示例使用 1024。
- 每个 producer index 只能由一个线程独占，每个 consumer index 也只能由一个线程独占。
- 流量开始后不能增加或删除参与者。
- 只保证同一 producer lane 内 FIFO，不提供跨 lane 全局顺序。
- 所有 consumer 都是 fanout，各自收到全量消息，不是竞争消费；最慢 consumer 决定背压。

## Ordered 和 Latest 为什么不在这个目录

`FixedChannel` 没有 Ordered 或 Latest 拓扑，固定槽 ring 只提供每 producer lane FIFO：

- `Spsc` 和 `Spmc` 只有一个 producer，lane FIFO 已经是该通道的全局顺序，不需要额外的 Ordered 模式。
- `Mpsc` 和 `Mpmc` 有多个 producer，固定槽快路径不建立跨 lane 全局顺序；确实需要全序时改用
  `simple/ordered_mpsc.cpp` 或 `simple/ordered_mpmc.cpp`。
- 需要保留最新窗口、允许丢旧值时改用 `simple/latest_mpmc.cpp` 的 `LatestMpmcChannel`；固定槽
  `FixedChannel` 不提供 Latest 变体。

## SPSC：单 producer、单 consumer

```bash
./build/release/example/salias_example_concurrency_spsc
```

同时演示 `send()` 拷贝路径和 `try_claim()` + `commit()` 零拷贝路径，是开销最低的拓扑。

## MPSC：多 producer、单 consumer

```bash
./build/release/example/salias_example_concurrency_mpsc
```

两个 producer 交替写入两个 lane；单 lane 内序号递增，lane 之间没有全局顺序。需要全局顺序时改用
`simple/ordered_mpsc.cpp` 的 Ordered Channel。

## SPMC：单 producer、多 consumer fanout

```bash
./build/release/example/salias_example_concurrency_spmc
```

两个 consumer 各自收到全部消息。多个 reader 分担消息（竞争消费）不是这个拓扑的语义。

## MPMC：多 producer、多 consumer fanout

```bash
./build/release/example/salias_example_concurrency_mpmc
```

策略端和审计端各自收到两个 lane 的全量消息，两个 consumer 的读取顺序可以不同。

# simple：通用 Channel 入门与语义示例

本目录使用通用 `Channel`，底层是每个 producer 一个 `MagicRing`。它支持变长 payload、动态订阅、
TTL、恢复和跨 producer 全局有序；代价是每帧需要变长 frame 扫描，热路径成本高于固定槽 `Fast*Ring`。
示例文件名直接使用完整的 `Mode` 值，例如 `FifoMpsc` 对应 `fifo_mpsc.cpp`。

| 示例 | 场景 | 关键语义 |
|---|---|---|
| `fifo_mpsc.cpp` | 多个 producer 写、单个 consumer 读 | FIFO 按提交顺序交付，不保证跨 producer 全局顺序 |
| `ordered_mpsc.cpp` | 多个 producer 的事件必须按全局顺序消费 | 分配全局序号，consumer 等待更早序号 |
| `fifo_mpsc_subscriber.cpp` + `fifo_mpsc_publisher.cpp` | 两个进程通过具名共享内存通信 | 一端 `create()`、另一端 `connect()`，支持变长文本 |
| `fifo_mpmc.cpp` | 一份消息同时送给多个订阅者 | 每个订阅者独立 cursor，fanout 不是竞争消费 |
| `ordered_mpmc.cpp` | 多 producer 全序，同时 fanout 给多个订阅者 | 共享全局序号，每个订阅者按相同顺序交付 |
| `latest_mpmc.cpp` | 读者只关心最新值，旧值可丢 | 有界保留窗口，默认 DropOldest 不拖住 producer |

## 为什么没有 LatestSpsc、LatestSpmc 或 OrderedSpsc

公开的 `Mode` 只有 `FifoMpsc`、`FifoMpmc`、`OrderedMpsc`、`OrderedMpmc` 和 `LatestMpmc`，
不存在 `LatestSpsc`、`LatestSpmc`、`OrderedSpsc` 或 `OrderedSpmc` 类型。`Mpsc`/`Mpmc` 表示能力上限，
创建时仍可把 `num_producers` 或 `num_consumers` 配成 1：

| 需要的能力 | 可用入口 | 说明 |
|---|---|---|
| 单 producer、单 consumer 的 Latest | `LatestMpmcChannel`，两个数量都配为 1 | Latest 只提供 Mpmc 模式，但仍可配置单 producer/consumer |
| 单 producer、多 consumer 的 Latest | `LatestMpmcChannel`，`num_producers=1` | 多个订阅者各自追赶最新窗口 |
| 单 producer、单 consumer 的 Ordered | 优先 `FifoMpsc`，`num_producers=1` | 单 producer 下 FIFO 已经是全序；`OrderedMpsc` 也可配置 1P1S，但会多付全局序号成本 |
| 单 producer、多 consumer 的 Ordered | 优先 `FifoMpmc`，`num_producers=1` | 单 producer 下 FIFO 已经是全序；确需 Ordered API 时用 `OrderedMpmc` |
| 多 producer 的 Ordered | `OrderedMpsc` 或 `OrderedMpmc` | 分别对应单 consumer 和多 consumer fanout |

所以示例只提供一个 `latest_mpmc.cpp`，但它也能覆盖 1P1S、1PNS 等“SPSC/SPMC 形状”的 Latest 用法；
Ordered 由 `ordered_mpsc.cpp` 和 `ordered_mpmc.cpp` 分别覆盖单 consumer 与多 consumer fanout，
单 producer 的 SPSC/SPMC 使用 FIFO 即可。

## FIFO MPSC：提交顺序不等于全局顺序

```bash
./build/release/example/salias_example_simple_fifo_mpsc
```

producer 0 先 claim 但暂不 commit，producer 1 随后提交，因此 consumer 先收到 producer 1 的消息。

```text
FIFO received ready producers: 2, 1
```

## Ordered MPSC：跨 producer 全局顺序

```bash
./build/release/example/salias_example_simple_ordered_mpsc
```

全局序号更晚的消息先 commit，consumer 仍会等待更早的序号，最终按 1、2 交付。

```text
Ordered received global sequence: 1, 2
```

事件溯源、审计日志、确定性回放选 Ordered；只需要单 producer 内 FIFO 时选 FIFO，吞吐更高。

## 跨进程 FIFO MPSC

先启动订阅端创建具名通道，再启动发布端连接：

```bash
./build/release/example/salias_example_simple_fifo_mpsc_subscriber --name demo-mpsc --count 3
./build/release/example/salias_example_simple_fifo_mpsc_publisher --name demo-mpsc --count 3 --message hello
```

两端 `--name` 必须一致。订阅端未就绪时发布端会重试连接，直到超时。

## MPMC：多个订阅者各收全量

```bash
./build/release/example/salias_example_simple_fifo_mpmc
```

两个 `subscriber()` 各自持有独立 cursor，策略端和审计端都收到全部消息。需要跨 producer 全局
顺序时把 `Mode::FifoMpmc` 换成 `Mode::OrderedMpmc`，订阅端代码不变。

## OrderedMpmc：全局全序加多订阅者 fanout

```bash
./build/release/example/salias_example_simple_ordered_mpmc
```

producer 1 的全局序号更晚但先 commit，两个订阅者仍会等待 producer 0 后按相同顺序交付。
多 producer 且必须全局有序时才用这个模式；单 producer 的 SPSC/SPMC 形状使用 FIFO 即可。

## Latest：只保留最新窗口

```bash
./build/release/example/salias_example_simple_latest_mpmc
```

示例发送 3000 条、读取最新窗口，输出保留条数、最后一条序号和淘汰计数。默认 `DropOldest`
订阅者会跳过已淘汰消息；需要每条都不能丢时改用 `SubscriptionPolicy::Reliable` 或 FIFO `Channel`。

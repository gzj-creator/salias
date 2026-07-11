# 最终性能报告与独立进程压测设计

## 目标

建立一套不在单个 benchmark 进程中 `fork()` worker 的最终压测体系。salias 与 Aeron
均使用独立 Publisher/Subscriber 可执行程序，由外部 coordinator 启动、同步和收集结果，
在 Tencent x86 机器上完成容量、MPSC、Fanout/MPMC 和 Ordered 性能矩阵，并只保留一份
最终性能报告作为 README 的性能真相源。

## 进程模型

### salias

- `salias_bench_subscriber`：subscriber 0 创建具名 Channel，其余 Subscriber 连接；每个程序
  独占一个 Subscriber endpoint。
- `salias_bench_publisher`：每个程序连接具名 Channel，并独占一个 Publisher endpoint。
- Publisher 与 Subscriber 通过协调目录中的 ready/start/result 文件完成进程间同步。
- worker 不创建子进程，不调用 `fork()`。

### Aeron

- `aeron_bench_subscriber`：每个程序创建独立 Aeron client 和 Subscription。
- `aeron_bench_publisher`：每个程序创建独立 Aeron client 和 ExclusivePublication。
- `aeronmd` 作为独立 media-driver 进程运行。
- 使用与 salias 相同的 ready/start/result 协调协议。

## 数据与统计

- payload 固定 64 B，前 8 B 编码 producer id 与本地 sequence。
- 每 Publisher 正式发布 2,000,000 条消息。
- 每个 Subscriber 校验每个 Producer 的消息数与本地 sequence 连续性。
- 每个 Publisher 记录 offer 次数、BackPressured 次数、连续重试最大值、起止单调时间。
- 每个 Subscriber 记录消费数、起止单调时间和采样延迟直方图。
- coordinator 以所有 worker 的最早开始时间和最晚结束时间计算端到端吞吐。
- 每组预热 3 轮、正式 20 轮，报告中使用中位数与 p10-p90。

## 测试矩阵

容量均为每 Producer Ring；Aeron 使用相同 term length：

- 1 MiB
- 4 MiB
- 64 MiB

场景：

| 拓扑 | salias | Aeron | Batch |
|---|---|---|---:|
| 2 Publisher / 1 Subscriber | FIFO MPSC | MPSC | 1 |
| 2 Publisher / 2 Subscriber | FIFO Fanout | MPMC fanout | 1 |
| 2 Publisher / 1 Subscriber | Ordered MPSC | 不做语义比例 | 1、8 |
| 2 Publisher / 2 Subscriber | Ordered Fanout | 不做语义比例 | 1、8 |

## 公平条件

- 相同机器、Release 编译、payload、消息数、容量、轮数和执行顺序。
- 2P1S：Subscriber CPU0，Publisher CPU1/2，Aeron driver CPU3。
- 2P2S：四个 worker 使用 CPU0-3；Aeron driver 与 CPU0 共享，并在报告中明确 4 vCPU 限制。
- salias FIFO 与 Aeron 使用等价的 per-publication FIFO / fanout 语义。
- Ordered 提供 Aeron 不具备的跨 Publisher 全局全序，只给出 salias 绝对性能。
- 容量、库和模式交错执行，减少固定运行顺序偏差。

## 最终文档

- `doc/performance-report.md`：唯一最终性能报告，包含机器、软件、编译、绑核、命令、结果和限制。
- `doc/benchmarks/final-performance-*.log`：最终原始日志。
- README 直接复制最终核心表格，并链接完整报告。
- 删除旧的性能对比 Markdown 与历史 benchmark 日志，避免多个相互冲突的结论入口。

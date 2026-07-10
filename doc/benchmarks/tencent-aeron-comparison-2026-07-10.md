# Tencent x86 salias 与 Aeron IPC 对比（2026-07-10）

## 环境

- 主机：Tencent Cloud `VM-8-5-ubuntu`
- 架构：x86_64，KVM 虚拟机
- CPU：Intel Xeon Platinum 8255C，4 vCPU，1 thread/core
- 系统：Ubuntu，Linux 6.8.0-101-generic
- 编译器：GCC 14.2.0，Release `-O3`/`NDEBUG`
- Aeron：1.52.0，独立 `aeronmd` media driver，`sparse.file=false`
- 消息：64 B；每生产者 1,000,000 条；2 producers / 1 consumer
- ring/term：4 MiB；salias normal pages
- 预热 1 轮，测量 5 轮；表格使用 5 轮发布吞吐中位数

## 核绑定与公平条件

- consumer：CPU 0
- producer 0/1：CPU 1/2
- Aeron media driver：CPU 3
- salias 消费端空 poll 忙轮询；Aeron 使用 `BusySpinIdleStrategy`
- salias 映射使用 `MAP_POPULATE`；Aeron term buffer 使用 `sparse.file=false`
- salias 使用 `try_claim`/`commit`；Aeron 使用 `ExclusivePublication::tryClaim`/`commit`
- poll limit 与 fragment limit 都是 64

Tencent 节点只有 4 vCPU，无法容纳计划中的 4P/1C 加 Aeron driver，因此本次使用 2P/1C。Aeron 额外占用
一个独立 driver 核；salias 不需要该核。该机器是 x86 KVM，不是计划要求的 x86 物理机。

## 结果

单位：Mmsg/s，数值为 5 轮中位数。

| salias 模式 | batch | salias | Aeron MPSC | salias / Aeron | 差距 |
|-------------|------:|-------:|-----------:|---------------:|-----:|
| FIFO | 1 | 13.980 | 34.077 | 41.0% | -59.0% |
| Ordered | 1 | 10.700 | 35.303 | 30.3% | -69.7% |
| FIFO | 16 | 14.547 | 34.910 | 41.7% | -58.3% |
| Ordered | 16 | 12.218 | 29.463 | 41.5% | -58.5% |

## 结论

- FIFO batch=1 没有达到 `>= Aeron` 验收线，只达到 Aeron 中位吞吐的约 41%。
- FIFO batch=16 相比 batch=1 只提升约 4%，仍明显落后 Aeron。
- Ordered 模式符合预期地更慢；batch=1 约为 Aeron 的 30%。
- 本次是真实多进程共享内存 IPC 与独立 Aeron media driver 对比，但硬件是 KVM 虚拟机，因此不能替代最终
  x86 物理机验收；同时当前结果已经足以否定“现实现已追平 Aeron”的判断。

## 2P/2C MPMC 扩展测试

由于实例只有 4 vCPU，扩展测试采用相同的四核总预算：

- salias：consumer 0/1 使用 CPU 0/1，producer 0/1 使用 CPU 2/3。
- Aeron：相同 worker 绑核，media driver 与 consumer 0 共享 CPU 0。
- 每生产者 2,000,000 条 64 B 消息；每条消息由两个消费者分别接收。
- 预热 3 轮，正式测量 20 轮。
- 表格为发布吞吐 Mmsg/s；括号内为 p10–p90。fanout 交付吞吐约为发布吞吐的两倍。

| 模式 | batch | salias median | Aeron median | salias / Aeron |
|------|------:|--------------:|-------------:|---------------:|
| FIFO | 1 | 11.037 (9.535–12.373) | 21.029 (14.068–24.636) | 52.5% |
| Ordered | 1 | 8.092 (7.324–9.429) | 22.742 (11.373–25.321) | 35.6% |
| FIFO | 16 | 11.136 (10.221–12.265) | 22.323 (11.897–24.976) | 49.9% |
| Ordered | 16 | 10.344 (8.320–11.598) | 23.079 (9.810–25.525) | 44.8% |

固定共享 CPU 会让 Aeron 结果出现较大波动，因此额外运行了一组不绑核、由 Linux 在四核上调度所有进程的
batch=1 交叉验证：

| 模式 | salias median | Aeron median | salias / Aeron |
|------|--------------:|-------------:|---------------:|
| FIFO | 12.275 (10.280–13.880) | 24.883 (18.680–28.145) | 49.3% |
| Ordered | 8.461 (7.120–9.634) | 23.064 (14.985–28.290) | 36.7% |

两种 CPU 分配方式的结论一致：FIFO fanout/MPMC 中位发布吞吐约为 Aeron 的一半；ordered 更慢。
FIFO batch=16 基本没有改善，说明当前 MPMC 瓶颈不在单次 batch API 调用开销。Ordered batch=16 有改善，
但仍不到 Aeron 中位吞吐的 45%。

## 超卖扩展性诊断

为继续增加生产者与消费者进程数，在相同 4 vCPU 上运行无绑核 FIFO MPMC 诊断矩阵。由于 worker 数量已经
超过 CPU 数量，以下数据只用于观察退化趋势，不作为独占核性能验收。每生产者 1,000,000 条消息，预热 2 轮，
正式测量 10 轮。

| 拓扑 | batch | salias median | Aeron median | salias / Aeron |
|------|------:|--------------:|-------------:|---------------:|
| 2P/2C | 1 | 12.275 | 24.883 | 49.3% |
| 2P/2C | 16 | 13.435 | 22.372 | 60.1% |
| 3P/2C | 1 | 11.533 | 19.155 | 60.2% |
| 3P/2C | 16 | 8.804 | 19.015 | 46.3% |
| 2P/3C | 1 | 8.439 | 16.516 | 51.1% |
| 2P/3C | 16 | 11.416 | 16.519 | 69.1% |
| 3P/3C | 1 | 8.442 | 16.194 | 52.1% |
| 3P/3C | 16 | 7.323 | 16.101 | 45.5% |

观察：

- batch=1 下从 2P/2C 增至 2P/3C，salias 中位发布吞吐下降约 31%，Aeron 下降约 34%；增加 fanout
  消费者是两边共同的主要成本。
- batch=1 下从 2P/2C 增至 3P/2C，salias 下降约 6%，Aeron 下降约 23%；超卖后 salias 的相对比例提高，
  但绝对吞吐仍低于 Aeron。
- batch=16 在 2P/2C、2P/3C 有提升，在 3P/2C、3P/3C 反而下降，说明它对调度和生产者数量敏感，
  不能作为稳定的扩展性解决方案。
- 所有超卖 case 中 salias 为 Aeron 中位发布吞吐的 45.5%–69.1%，没有出现追平或超过 Aeron 的场景。

## 采样单向延迟

延迟测试使用与吞吐测试相同的真实多进程 IPC 路径，并在独立运行中启用 `--latency-sample-rate 64`：

- 生产者每 64 条消息采样 1 条，在提交前把 `CLOCK_MONOTONIC` 时间戳写入 payload。
- 消费者在回调入口读取同一时钟，统计从应用发布端到应用消费端的单向延迟。
- 2P/2C 每生产者发送 1,000,000 条消息，每轮聚合 62,500 个 fanout 消费样本。
- 固定布局沿用 consumer CPU 0/1、producer CPU 2/3，Aeron driver 与 consumer 0 共享 CPU 0；自由调度布局不绑核。
- 每个 2P/2C case 预热后测量 10 轮。表中数值是各轮聚合 p50/p99 的中位数，而不是把多轮原始样本再次合并。
- 这些结果是在持续饱和发布下测得的排队延迟，不是空载 ping-pong 延迟；它包含共享内存、Aeron driver、排队和 KVM 调度成本。

单位：微秒。

| CPU 布局 | 模式 | batch | salias p50 | salias p99 | Aeron p50 | Aeron p99 |
|----------|------|------:|-----------:|-----------:|----------:|----------:|
| 固定绑核 | FIFO | 1 | 6520.8 | 13566.0 | 53.8 | 2162.7 |
| 固定绑核 | FIFO | 16 | 4685.8 | 10551.3 | 174.1 | 2039.8 |
| 固定绑核 | Ordered | 1 | 4849.7 | 14942.2 | 89.6 | 1589.2 |
| 固定绑核 | Ordered | 16 | 5832.7 | 12517.4 | 58.4 | 1540.1 |
| 自由调度 | FIFO | 1 | 4358.1 | 11993.1 | 166.9 | 2228.2 |
| 自由调度 | FIFO | 16 | 3850.2 | 9895.9 | 64.0 | 2113.5 |
| 自由调度 | Ordered | 1 | 8060.9 | 12845.1 | 80.1 | 2474.0 |
| 自由调度 | Ordered | 16 | 6127.6 | 11534.3 | 74.9 | 2211.8 |

观察：

- Aeron 的聚合 p50 通常为 53.8–174.1 微秒，p99 为 1.54–2.47 毫秒；salias 的聚合 p50 为
  3.85–8.06 毫秒，p99 为 9.90–14.94 毫秒。
- salias 的 fanout 消费者存在明显进度不对称，慢消费者会把同一采样消息的聚合分位数推高；原始日志中的
  每消费者 `LATENCY` 行可用于区分两个消费者。Aeron 也受 KVM 调度影响，但尾延迟明显更低。
- batch=16 能改善部分 salias case 的 p50/p99，但没有缩小到与 Aeron 相同的延迟量级。
- 固定绑核与自由调度的绝对值不同，但结论一致：当前 salias 在饱和 2P/2C fanout 下的 p50/p99 均落后 Aeron。

### 超卖 FIFO 延迟诊断

在相同 4 vCPU 上继续运行无绑核 3P/2C、2P/3C 和 3P/3C；每生产者发送 500,000 条消息，每个 case
预热后测量 5 轮。该矩阵只用于观察进程超卖时的排队和调度退化，不作为独占核延迟验收。

单位：微秒。

| 拓扑 | batch | salias p50 | salias p99 | Aeron p50 | Aeron p99 |
|------|------:|-----------:|-----------:|----------:|----------:|
| 3P/2C | 1 | 8650.8 | 28311.6 | 239.6 | 4784.1 |
| 3P/2C | 16 | 7536.6 | 20709.4 | 278.5 | 4784.1 |
| 2P/3C | 1 | 1228.8 | 14286.8 | 161.8 | 2359.3 |
| 2P/3C | 16 | 274.4 | 15466.5 | 184.3 | 1802.2 |
| 3P/3C | 1 | 2457.6 | 26214.4 | 458.8 | 4259.8 |
| 3P/3C | 16 | 2097.2 | 27787.3 | 421.9 | 5439.5 |

超卖时 salias p99 为 14.29–28.31 毫秒，Aeron p99 为 1.80–5.44 毫秒。2P/3C batch=16 的 salias
p50 降至 274.4 微秒，但 p99 仍为 15.47 毫秒，说明较低的中位数并未消除慢消费者和调度造成的长尾。

## 原始数据

- `benchmark-tencent-2026-07-10-batch1.log`
  - SHA-256: `4896a2a0697d722e540bfbacf2cef5efbf0e98a39ec2d7246d77db47263648f3`
- `benchmark-tencent-2026-07-10-batch16.log`
  - SHA-256: `7fa15b64ea0e6ed1af8a9b3e2039e611706123dc3ceb4d5c2f35400be3176fee`
- `benchmark-tencent-mpmc-2026-07-10-batch1-20r.log`
  - SHA-256: `41dc0807bbd57ac6091d996444ad883d026ad4103a90b700f62e59afb9d8befc`
- `benchmark-tencent-mpmc-2026-07-10-batch16-20r.log`
  - SHA-256: `facf89bab62b6a2676c2b9ff1d242973cd826f31ca1087277e3e5574af03628a`
- `benchmark-tencent-mpmc-2026-07-10-unpinned-batch1-20r.log`
  - SHA-256: `b777bfd5ad2793ea59368bc92202a463cdaa929b39e6bfc0e50ce02b5aa785a3`
- `benchmark-tencent-mpmc-2026-07-10-oversubscribed-scaling.log`
  - SHA-256: `b5672938932a8b470b2ead9cbb4e2785890b48273f03789728b3cebd963711c4`
- `benchmark-tencent-mpmc-2026-07-10-unpinned-2p2c-batch16-10r.log`
  - SHA-256: `1c43dcfa0cbc08901111ad5d20d31e97b59e46b67cef88364c23c14d8233d328`
- `benchmark-tencent-latency-2026-07-10-2p2c.log`
  - SHA-256: `9351a69ace6febc61b69cb60aac7f13674e74962b254045c2f48315089ccf6f6`
- `benchmark-tencent-latency-2026-07-10-oversubscribed.log`
  - SHA-256: `a1401e37009588105f554d3828a5a10df5027c025811b75bec6fb6791bfaa587`

吞吐远端运行目录：`/home/ubuntu/salias-dual-engine-bench/run-20260710-1315`。
延迟远端运行目录：`/home/ubuntu/salias-dual-engine-bench/run-20260710-latency`。

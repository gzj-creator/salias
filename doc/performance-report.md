# salias 最终独立进程性能报告

> 日期：2026-07-11。本文是项目唯一有效的 salias/Aeron 性能对比报告；历史阶段数据仅用于开发过程，
> 不再作为当前性能结论。

最新 Publisher/尾延迟结果见 [2026-10-18 性能总结](performance-summary-2026-10-18.md)：
候选正确性回归通过，但 50 µs p99.9 验收 0/8 配置通过，不能与本页历史 Aeron 吞吐对比混用。
后续 [三组相对收益验收](publisher-tail-relative-acceptance-2026-10-18.md) 也未通过，
该轮新产品候选已撤销，不计入已实现的性能收益。

## 结论摘要

在 Tencent 4 vCPU x86 KVM、64 B 消息、两个独立 Publisher 程序的最终测试中：

- 2 Publisher / 1 Subscriber：salias FIFO 在 1/4/64 MiB 容量下分别达到 Aeron 的
  109.1%、115.5%、176.3%。
- 2 Publisher / 2 Subscriber：salias FIFO Fanout 分别达到 Aeron MPMC 的
  192.0%、120.0%、160.2%。
- salias 的最佳 FIFO MPSC 中位发布吞吐为 44.867 Mmsg/s；最佳 FIFO Fanout 中位发布吞吐
  为 30.158 Mmsg/s，对应 60.316 Mmsg/s 交付吞吐。
- salias Ordered 提供 Aeron 不具备的跨 Publisher 全局全序。batch=8、64 MiB 时，2P1S
  达到 42.856 Mmsg/s，2P2S 达到 34.207 Mmsg/s 发布吞吐和 68.414 Mmsg/s 交付吞吐。
- salias 在 64 MiB 下背压中位数为 0；但大容量没有提高 FIFO 绝对吞吐，说明容量主要是
  突发与排队参数，不是越大越快。

## 机器配置

| 项目 | 配置 |
|---|---|
| 云厂商 | Tencent Cloud CVM |
| 主机名 | `VM-8-5-ubuntu` |
| 虚拟化 | KVM 全虚拟化 |
| CPU | Intel Xeon Platinum 8255C @ 2.50 GHz |
| vCPU | 4，1 socket，4 cores，1 thread/core |
| Cache | L1d 128 KiB，L1i 128 KiB，L2 16 MiB，L3 35.8 MiB |
| NUMA | 1 node，CPU 0-3 |
| 系统 | Ubuntu 24.04.4 LTS |
| Kernel | Linux 6.8.0-101-generic x86_64 |
| 编译器 | GCC 13.3.0 |
| CMake | 3.28.3 |
| salias | C++23，Release `-O3 -flto -march=native -DNDEBUG` |
| Aeron | 1.52.0，C++ client + 独立 `aeronmd` |

完整机器输出见 `benchmarks/final-performance-machine.txt`。

## 独立进程模型

本次没有在 benchmark 主程序中 `fork()` Publisher 或 Subscriber worker。每个角色都是由 shell
coordinator 独立启动的可执行程序：

```text
salias_final_publisher   × Publisher 数量
salias_final_subscriber  × Subscriber 数量

aeron_final_publisher    × Publisher 数量
aeron_final_subscriber   × Subscriber 数量
aeronmd                  × 1
```

salias Subscriber 0 创建具名 Channel，其他 Subscriber 和所有 Publisher 分别连接。Aeron 每个
Publisher/Subscriber 创建自己的 Aeron client；media driver 是额外独立进程。Coordinator 只通过
ready/start/result 文件同步，不参与消息收发。

## 公平条件

- 每个 Publisher 发布 2,000,000 条 64 B 消息。
- 2 个 Publisher；分别测试 1 个和 2 个 Subscriber。
- 容量/term length：1、4、64 MiB。
- 每组预热 3 次，正式测量 20 次；容量顺序轮转。
- salias FIFO batch=1 使用 `Publisher::offer()` 拷贝发布；Ordered batch=8 使用 `offer_batch()`。
  本轮没有测 `try_claim()` 零拷贝热路径。
- Aeron 使用 `ExclusivePublication::tryClaim()`。
- Subscriber poll/fragment limit 均为 64。
- 2P1S：Subscriber CPU0，Publisher CPU1/2，Aeron driver CPU3。
- 2P2S：Subscriber CPU0/1，Publisher CPU2/3；Aeron driver 与 CPU0 共享。机器只有 4 vCPU，
  因而 2P2S Aeron 存在 driver 共享核限制，结果不能替代更多物理核心上的最终验证。
- FIFO 对比采用等价的 per-publication FIFO 与 fanout 语义。
- Ordered 是 salias 扩展能力，不计算 salias/Aeron 公平比例。

吞吐使用所有 worker 的最早单调时钟开始值到最晚结束值计算，包含最慢 Publisher/Subscriber 的完成时间。
所有 480 个正式样本均完成消息数校验，`invalid=0`。

## Publisher 指标口径

这里的性能是独立进程 Publisher 到 Subscriber 的端到端结果，不是单次 `offer()` 的函数耗时。
常用 FIFO MPSC 对应 2P1S；2P2S 是 fanout 场景，每条发布消息交付给两个 Subscriber。

- **发布吞吐**：每秒成功发布的消息数，表中以 Mmsg/s 表示。
- **交付吞吐**：每秒成功消费的总消息数；fanout 按所有 Subscriber 的交付数累加。
- **有效载荷带宽**：按 64 B payload 换算，`消息数/秒 × 64 ÷ 1,048,576`，表中以 MiB/s 表示。
  交付带宽在 fanout 下包含每个 Subscriber 各自收到的 payload，是逻辑有效载荷统计，不是物理内存总线带宽。
- **背压率**：发布尝试中因容量限制而需要重试的比例；salias 记 `BackPressured`，Aeron 记可重试的
  `tryClaim()` 非成功。两者实现口径不完全相同，只适合分别观察趋势。
- **离散度与样本数**：吞吐列给出 20 次正式运行的中位数及 p10-p90；完整矩阵还列发布/交付带宽和
  中位背压率。`invalid=0` 表示本轮样本的发布数、交付数校验全部通过。

这轮 salias/Aeron 最终对比没有测单次 Publisher 调用延迟、p50/p99/p99.9、CPU 占用、峰值内存或分配量；
不能从吞吐反推出这些指标。当前代码另有一组 salias FIFO 采样单向时延结果，含 publication window 对照，
见 [当前 Publisher 时延报告](publisher-latency-2026-10-18.md)。另有包含发布前积压的
[30分钟合成IPC验收](hft-market-data-acceptance-2026-10-18.md)及
[尾延迟优化独立验收](tail-latency-acceptance-2026-10-18.md)；后者为性能仍待改进的候选补丁，
完整性回归通过但50µs p99.9未达标，不能混为本页Aeron对比结果。Benchmark worker 还会记录每个 Publisher
的尝试数和最长连续重试次数，但最终对比的归档汇总未包含这些逐 Publisher 原始计数。

## FIFO 与 Aeron 公平对比

单位为中位发布吞吐 Mmsg/s；范围为 p10-p90。

| 拓扑 | 容量 | salias FIFO | Aeron | salias/Aeron | salias 背压 | Aeron 背压 |
|---|---:|---:|---:|---:|---:|---:|
| 2P1S | 1 MiB | 44.867 (42.783-46.408) | 41.108 (36.112-45.335) | **109.1%** | 0.312% | 27.828% |
| 2P1S | 4 MiB | 44.485 (38.606-46.316) | 38.515 (34.980-42.101) | **115.5%** | 0.200% | 15.891% |
| 2P1S | 64 MiB | 38.806 (35.909-43.545) | 22.011 (20.672-22.617) | **176.3%** | 0.000% | 0.000% |
| 2P2S | 1 MiB | 29.206 (24.095-32.785) | 15.214 (9.253-18.579) | **192.0%** | 0.884% | 73.875% |
| 2P2S | 4 MiB | 30.158 (24.403-34.688) | 25.127 (21.480-27.882) | **120.0%** | 0.386% | 45.811% |
| 2P2S | 64 MiB | 28.423 (24.456-33.147) | 17.743 (15.694-18.699) | **160.2%** | 0.000% | 22.189% |

2P2S 中每条消息交付给两个 Subscriber，因此 salias 4 MiB 的 30.158 Mmsg/s 发布吞吐对应
60.316 Mmsg/s 交付吞吐；Aeron 对应 50.254 Mmsg/s。
按 64 B payload 换算，salias 该场景的发布/交付有效载荷带宽分别为 1,840.706 / 3,681.412 MiB/s；
各容量、拓扑及模式的带宽见 `benchmarks/final-performance-table.md`。

## salias Ordered 性能

Ordered 保证跨 Publisher 全局全序，Aeron IPC 没有直接等价模式。表格只报告 salias：

| 拓扑 | 容量 | Batch | 发布中位数 | p10-p90 | 交付中位数 | 背压中位数 |
|---|---:|---:|---:|---:|---:|---:|
| 2P1S | 1 MiB | 1 | 18.784 | 16.790-20.219 | 18.784 | 3.178% |
| 2P1S | 1 MiB | 8 | 34.890 | 33.155-36.644 | 34.890 | 12.616% |
| 2P1S | 4 MiB | 1 | 18.267 | 17.234-19.764 | 18.267 | 3.209% |
| 2P1S | 4 MiB | 8 | 34.607 | 33.024-38.625 | 34.607 | 12.342% |
| 2P1S | 64 MiB | 1 | 19.517 | 17.705-22.377 | 19.517 | 0.111% |
| 2P1S | 64 MiB | 8 | **42.856** | 40.049-43.809 | **42.856** | 0.000% |
| 2P2S | 1 MiB | 1 | 15.498 | 13.329-17.338 | 30.997 | 0.871% |
| 2P2S | 1 MiB | 8 | 28.691 | 23.395-31.117 | 57.382 | 14.184% |
| 2P2S | 4 MiB | 1 | 15.129 | 13.360-18.503 | 30.259 | 0.455% |
| 2P2S | 4 MiB | 8 | 28.208 | 23.135-32.046 | 56.415 | 12.876% |
| 2P2S | 64 MiB | 1 | 16.512 | 15.225-17.727 | 33.025 | 0.000% |
| 2P2S | 64 MiB | 8 | **34.207** | 26.649-35.737 | **68.414** | 0.000% |

## 容量结论

- salias FIFO 的最佳绝对结果出现在 1/4 MiB，而不是 64 MiB。64 MiB 消除了背压，但扩大了
  工作集，吞吐反而下降。因此 Ring 容量应按突发吸收与延迟目标选择。
- Aeron 对 term length 更敏感：2P1S 64 MiB 的绝对吞吐明显低于 1/4 MiB。扩大 term length
  不能被视为通用吞吐优化。
- Ordered batch=8 在 64 MiB 下受益明显，说明批量全局 sequence 分配与更大在途窗口可以共同
  降低 Ordered 的同步摊销；延迟敏感部署仍需使用 publication window 控制排队。

## 限制

- Tencent 节点是 4 vCPU KVM，不是独占物理机，p10-p90 仍包含宿主机调度波动。
- 2P2S Aeron media driver 与一个 Subscriber 共享 CPU；该限制已明确列入公平条件。
- 本轮最终对比矩阵聚焦吞吐、正确性与背压，没有做时钟采样延迟结论；当前 salias 代码的独立采样结果见
  `publisher-latency-2026-10-18.md`，它不是 Aeron 对照，也不能从本矩阵的吞吐推导。
- salias 和 Aeron 的背压计数含义不同：salias 统计 API 返回 BackPressured；Aeron 统计
  `tryClaim()` 非成功且可重试的次数。比例只用于各自容量趋势诊断。

## 原始数据

- `benchmarks/final-performance-summary.txt`：机器可读汇总，含发布/交付吞吐、64 B 有效载荷带宽及背压率。
- `benchmarks/final-performance-table.md`：完整 24 组自动生成汇总表。
- `benchmarks/final-performance-machine.txt`：机器配置原始输出。

原始逐轮 `final-performance-raw.log` 未随当前仓库归档；summary/table 是已提交的汇总数据，不能据此恢复
单轮时间或每个 Publisher 的背压尝试计数。

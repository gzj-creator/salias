# salias 最终独立进程性能报告

> 日期：2026-07-11。本文是项目唯一有效的 salias/Aeron 性能对比报告；历史阶段数据仅用于开发过程，
> 不再作为当前性能结论。

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
- salias FIFO batch=1；Aeron 使用 `ExclusivePublication::tryClaim()`。
- Subscriber poll/fragment limit 均为 64。
- 2P1S：Subscriber CPU0，Publisher CPU1/2，Aeron driver CPU3。
- 2P2S：Subscriber CPU0/1，Publisher CPU2/3；Aeron driver 与 CPU0 共享。机器只有 4 vCPU，
  因而 2P2S Aeron 存在 driver 共享核限制，结果不能替代更多物理核心上的最终验证。
- FIFO 对比采用等价的 per-publication FIFO 与 fanout 语义。
- Ordered 是 salias 扩展能力，不计算 salias/Aeron 公平比例。

吞吐使用所有 worker 的最早单调时钟开始值到最晚结束值计算，包含最慢 Publisher/Subscriber 的完成时间。
所有 480 个正式样本均完成消息数校验，`invalid=0`。

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
- 本轮最终矩阵聚焦吞吐、正确性与背压，没有做时钟采样延迟结论。延迟需要单独使用低采样率、
  受控 publication window 的独立报告，不能从吞吐矩阵推导。
- salias 和 Aeron 的背压计数含义不同：salias 统计 API 返回 BackPressured；Aeron 统计
  `tryClaim()` 非成功且可重试的次数。比例只用于各自容量趋势诊断。

## 原始数据

- `benchmarks/final-performance-raw.log`：480 个正式 RUN 样本。
- `benchmarks/final-performance-summary.txt`：机器可读汇总。
- `benchmarks/final-performance-table.md`：完整自动生成表格。
- `benchmarks/final-performance-machine.txt`：机器配置原始输出。

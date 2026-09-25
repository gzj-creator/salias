# 2026-09-25 消费热路径优化

本次保留的改动仅针对 FIFO 同环批量接收：将每帧反复读取、写回的消费者游标和序号
放入局部变量，在一个 run 结束时回写一次。公共 API、共享内存布局、生产者发布流程、
acquire/release 内存序和回调完成后的空间回收时机均保持原有契约。

在本机 ABBA 对照中，FIFO 预填充批量接收的每消息处理耗时降低 **14.0%～21.3%**；
4 生产者并发批量接收的两轮对照分别降低 **13.8%** 和 **10.5%**。这些数字只描述下述
负载和运行环境，不能当作跨进程延迟或生产环境的普遍提升。

## 瓶颈与依据

1. **批量内的重复状态访问。** 两个引擎的 try_recv_run 原先逐帧调用 consume，重复
   校验端点并通过 producer_id 访问游标容器。FIFO 已选中同一个环，消息序号连续，
   可以在局部推进 position、sequence、visible_bound，再统一写回端点本地状态。
   预填充基准排除了生产线程调度和发布成本，直接验证了这部分收益。
2. **稀疏环扫描。** 消费者最坏需要扫描所有生产者环。原版单条 FIFO 接收在 1P 时
   为 33.408 ns/msg，在 8P 但仅末环活跃时为 51.703 ns/msg。环数和活跃分布确实
   会影响扫描成本，但尝试用分支替换取模没有得到足够稳定的整体收益，最终未保留。
3. **全序模式的串行约束。** Ordered 发布需要全局 sequence.fetch_add；消费者
   必须找到下一全局序号，不能跳过未提交的 gap。轮流向不同环发布时，一个 run 常常
   只能收一条，因此无法摊薄批量开销。原版 8P dense 的批量接收为 30.157 ns/msg，
   FIFO 对应 13.351 ns/msg。这是负载下的路径差异；没有硬件计数器证据来量化
   原子指令或缓存争用分别占多少时间。
4. **背压和共享进度交互。** 并发结果受批次形成、进度可见时间和调度影响。Fanout
   还必须等待最慢消费者。单纯扩大批次或延迟回收不保证更快，必须同时检查背压和
   回调期间 payload 的有效性。

## 最终实现

- src/core/channel/shared_hybrid_mpsc.hpp：具名 IPC 使用的 FIFO 批量接收路径。
- src/core/channel/hybrid_mpsc.hpp：进程内引擎的同等优化。
- FIFO 使用编译期分支；进入 run 前的校验与可见水位 acquire 保留。
- 每帧仍校验帧头、序号和可见边界，仍保留下一帧头预取。
- run 完成后才更新本地游标；共享进度仍在回调完成后 flush。
- Ordered 保留原有逐帧推进逻辑；单条接收、轮询顺序和公平性保持原有逻辑。

完整生产代码差异见 [production.patch](benchmarks/2026-09-25/production.patch)。
基线/最终源码哈希见 [build-manifest.json](benchmarks/2026-09-25/build-manifest.json)。

## 测量方法

- 机器：VMware 虚拟机，12 vCPU，暴露型号 Intel Core i7-14700K，Linux x86_64。
- 编译器：GCC 14.2.0；Release，-O3、-flto、-march=native，前后参数相同。
- 基线使用修改前的生产代码和**与最终版本相同的基准源码**，已经逐字节核对。
- 使用 bench/compare.py 按 before / after / after / before 顺序串行运行，
  基准期间没有同时运行本任务的编译或测试。
- 每个进程先预热一次，再采样；汇总值是两个进程各自中位数的中位数。
  每消息耗时降幅为 1 - after / before，不能直接等同于吞吐增幅。
- 原始 JSON 保留命令、stdout、stderr、各进程的 median/min/max 和背压计数。
- 本机 perf 硬件事件不可用，perf_event_paranoid=4；未获取火焰图、IPC、
  cache-miss 等硬件画像，也没有修改系统安全配置。

### 接收端隔离基准

bench/receiver_scan.cpp 使用公共 API，64B payload，每生产者 1 MiB ring。
每轮先填充 4096 条/活跃生产者，填充不计时，再计时接收；共 32 轮，跨越物理回绕。
单条模式使用 try_recv/release，批量模式使用 poll(256)，同时校验内容和序号。
固定到逻辑 CPU 0，每进程采样 11 次。dense 表示所有环活跃，sparse 仅末环活跃。

| FIFO 批量场景 | 修改前 ns/msg | 修改后 ns/msg | 耗时降低 |
|---|---:|---:|---:|
| 1P dense | 14.790 | 11.640 | 21.3% |
| 3P dense | 12.997 | 11.181 | 14.0% |
| 8P dense | 13.351 | 11.176 | 16.3% |
| 3P sparse | 15.159 | 12.365 | 18.4% |
| 8P sparse | 16.400 | 12.945 | 21.1% |

单条模式不是本次优化目标；其余对照包括 1P FIFO 单条从 33.408 到 34.872 ns/msg
（慢 4.4%），8P sparse FIFO 单条从 51.703 到 52.072（慢 0.7%）。
Ordered 批量各场景变化为快 1.7%～4.3%，没有把这些小幅变化作为优化成果。
全部 20 个场景见 [receiver-final.json](benchmarks/2026-09-25/receiver-final.json)。

### 并发吞吐与回归对照

bench/hotpath_ab.cpp 使用同进程多线程、具名共享内存；64B payload，4 MiB/ring，
每次总计 2,000,000 条消息。消费者固定逻辑 CPU 0，生产者固定逻辑 CPU 1～4。
本轮全量基准每进程采样 9 次，独立 4P 复核采样 11 次。

| 场景 | 修改前 ns/msg | 修改后 ns/msg | 耗时降低 |
|---|---:|---:|---:|
| FIFO 4P poll，全量对照 | 67.235 | 57.956 | 13.8% |
| FIFO 4P poll，独立复核 | 56.203 | 50.317 | 10.5% |
| FIFO SPSC poll | 62.454 | 40.652 | 34.9% |
| Ordered SPSC poll | 31.538 | 31.569 | -0.1% |
| FIFO 同线程往返 | 40.224 | 40.642 | -1.0% |
| FIFO SPSC 单条接收 | 272.760 | 285.662 | -4.7% |
| FIFO 4P 单条接收 | 199.728 | 210.645 | -5.5% |
| Ordered 4P poll，全量对照 | 119.362 | 139.708 | -17.0% |
| Ordered 4P poll，独立复核 | 95.226 | 70.089 | 26.4% |

虚拟机多线程波动明显。Ordered 4P 同一基线在独立复核的两个进程中分别为
121.196 和 69.256 ns/msg，前后结论还会反转，不能据此宣称全序性能提升或已经
排除所有并发性能回退。最终 Ordered 保持原接收逻辑；SPSC 回退已消除。
FIFO 4P 两轮都观察到收益，但仍需在部署机器上复核。FIFO SPSC 的 34.9%
受到批次形成和跨核交互影响，仅列作本次观测，主要结论采用接收隔离结果与 4P 复核。

原始数据：[全量并发对照](benchmarks/2026-09-25/hotpath-final.json)、
[FIFO 4P 复核](benchmarks/2026-09-25/fifo-4p-confirm.json)、
[Ordered 4P 复核](benchmarks/2026-09-25/ordered-4p-confirm.json)。
这些指标是完成消息总数的平均处理时间，不是消息延迟分位数；本次没有重跑 Aeron
对比或独立进程性能验收，不替代原 performance-report.md 的负载与结论。

## 被否决的方案

- **跨环合并 Ordered 批次后统一 flush**：4P 对照从 69.381 到 74.686 ns/msg，
  慢 7.6%；虽然部分预填充场景更快，但不适合保留。
- **对 FIFO/Ordered 都缓存 run 游标**：接收隔离基准有收益，Ordered SPSC
  并发 poll 却从 31.133 到 56.040 ns/msg，慢约 80%。最终只对 FIFO 应用；
  恢复 Ordered 原路径后 SPSC 为 31.538 到 31.569 ns/msg。
- **环索引取模改为递增分支回绕**：整体收益不稳定，且部分单条路径更慢。
  最终恢复原取模逻辑，避免把理论上的指令节省当成实际性能收益。

上述候选数据分别归档为 rejected-cross-ring.json、rejected-all-mode-cursor.json
和 rejected-modulo-and-cursor.json，均在同一 benchmarks/2026-09-25 目录中；
候选实现不在最终生产代码中。

## 正确性验证

在最终生产代码上完成：

- Debug：65/65 通过。
- Release：65/65 通过。
- ASan + UBSan：65/65 通过。
- TSan：channel、API 两个测试可执行程序通过，包含新增并发场景。
- FIFO/Ordered 并发批次、回调生命周期、部分批次游标等 5 个重点用例，
  每个连续执行 20 次，全通过。

新增 11 个测试，覆盖 3 个生产者环、稀疏与公平扫描、批次上限、单条/批量混用、
回绕、Ordered gap、慢 fanout 消费者背压、回调期间 payload 不能回收，以及
3 个生产线程各发布 20,000 条消息的并发检查。已有提交可见性、容量、分配和安装
消费者契约也全部通过。两个 fanout 消费端在测试线程轮流 poll，生产者在独立线程。

日志位于 benchmarks/2026-09-25 下的 debug-tests.log、release-tests.log、
asan-ubsan-tests.log、tsan-tests.log 和 repeat-tests.log。TSan 的通过只覆盖本次
运行的测试，不能当作所有跨进程共享内存行为的完整证明。

## 复现

不需要 Google Benchmark 即可构建本次两个可执行程序：

~~~bash
cmake -S . -B build-perf -DCMAKE_BUILD_TYPE=Release -DSALIAS_BUILD_BENCHMARKS=ON -DSALIAS_BUILD_EXAMPLES=OFF
cmake --build build-perf -j 4 --target salias_receiver_scan salias_hotpath_ab
./build-perf/bench/salias_receiver_scan 11
./build-perf/bench/salias_hotpath_ab --case fifo_4p1c_poll --messages 2000000 --reps 11
~~~

对照时复制当前源码到另一个目录，在副本中反向应用 production.patch 恢复生产
基线，并保留相同基准源码；使用相同编译器、编译选项分别构建。已有构建目录不能
在两个源码目录之间混用。Linux 上选择允许使用的 CPU，再运行：

~~~bash
python3 bench/compare.py /path/to/before/salias_receiver_scan /path/to/after/salias_receiver_scan --cpu 0 --reps 11 --output /tmp/receiver-abba.json
python3 bench/compare.py /path/to/before/salias_hotpath_ab /path/to/after/salias_hotpath_ab --kind hotpath --case fifo_4p1c_poll --messages 2000000 --reps 11 --output /tmp/fifo-abba.json
~~~

hotpath 基准沿用原工具的固定 CPU 编号，调用方应确保逻辑 CPU 0～4 可用；
其现有 pin_cpu 不报告 affinity 失败。新 receiver 基准通过比较脚本设置 affinity，
设置失败会直接报错。复测时不要并行启动其他基准或编译任务。

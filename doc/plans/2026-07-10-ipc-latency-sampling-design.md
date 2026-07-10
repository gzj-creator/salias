# IPC 采样延迟设计

Date: 2026-07-10
Status: Implemented

## 目标

为 salias 与 Aeron 的真实多进程 IPC harness 增加同口径的单向端到端延迟统计，在满负载 MPMC 下输出
p50、p99、p99.9 和 max，同时尽量降低读时钟对吞吐的扰动。

## 测量口径

- 生产者在提交前把 `CLOCK_MONOTONIC` 纳秒时间戳写入 payload 前 8 字节。
- 每个生产者每 `N` 条采样一条，默认 `N=64`；未采样消息的时间戳写 0。
- 消费者收到采样消息后立即读取同一单调时钟，记录 `receive_ns - send_ns`。
- 多消费者 fanout 中每个消费者分别记录同一采样消息的延迟，最终既输出每消费者统计，也输出聚合统计。
- 延迟表示应用发布端到应用消费回调之间的一向 IPC 延迟，包含排队、共享内存、driver 和调度成本。

## 数据结构

两个 harness 共用 `tools/aeron_compare/latency_histogram.hpp`。直方图使用每个 2 的幂区间 64 个子桶的
对数布局：64 ns 以下逐纳秒分桶，之后保持约 1.6% 相对分辨率，并覆盖完整 `uint64_t` 纳秒范围。

每个消费者独占一行共享直方图，不需要跨进程原子递增。父进程在 `waitpid` 完成后合并所有消费者行，计算
p50/p99/p99.9。共享状态同时保存每消费者样本数和最大值。

## CLI 与输出

- 新增 `--latency-sample-rate N`；`0` 表示关闭，默认关闭以保持现有吞吐结果不变。
- 启用延迟采样时 payload 必须至少 16 字节：前 8 字节时间戳，后 8 字节保留 producer/sequence marker。
- 每个消费者输出一行 `LATENCY ... consumer=N samples=... p50_ns=... p99_ns=...`。
- `RESULT` 行追加聚合字段：`latency_sample_rate`、`latency_samples`、`latency_p50_ns`、
  `latency_p99_ns`、`latency_p999_ns`、`latency_max_ns`。

## 公平性

- salias 与 Aeron 使用同一采样率、时间源、payload 布局和直方图实现。
- 时钟只在采样消息上读取；未采样消息只多写一个 0 时间戳。
- 延迟模式单独重跑，不使用历史吞吐日志推导百分位。
- Tencent 四核继续使用 2P/2C 固定共享核与自由调度两种布局；更高进程数仅作为超卖诊断。

## 验证

- 单元测试覆盖桶边界、单点百分位、合并后百分位和宽范围延迟。
- salias 本地 smoke 验证样本数与 p50/p99 字段。
- Tencent 上 salias/Aeron 使用相同参数运行预热和多轮测量，归档原始日志与 SHA-256。

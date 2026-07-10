# 2 - salias 实现路线

> 当前架构决策以 `plans/2026-07-10-dual-engine-architecture.md` 为准。

## 1. 已完成基线

- Linux `memfd`/具名共享内存双映射 magic ring。
- 8 字节帧头、对齐、零拷贝 claim/commit 与 poll/release。
- 一个 per-producer-ring 引擎，支持 `fifo|ordered` 与 `single|fanout` 正交组合。
- 持久 `PublisherEndpoint` / `SubscriberEndpoint`，避免每次 API 调用重建端点。
- 固定共享控制块 ABI，支持最多 16 个生产者与 8 个消费者。
- 普通页、2 MiB、1 GiB 大页配置与映射 prefault。
- 示例、benchmark、IPC 对比工具以及 sanitizer 测试入口。

## 2. 当前公共模式

| 模式 | 语义 | 验收重点 |
|------|------|----------|
| `FifoMpsc` | 多生产者各自 FIFO，单消费者 | 生产者每消息 0 共享 RMW |
| `FifoFanout` | 多生产者各自 FIFO，多消费者扇出 | 独立游标与最慢消费者背压 |
| `OrderedMpsc` | 多生产者全局全序，单消费者 | 跨生产者序列严格递增 |
| `OrderedFanout` | 多生产者全局全序，多消费者扇出 | 每个消费者看到相同全序 |

## 3. 交付验证

每次架构或热路径变更按以下顺序验证：

1. `clang-format` 与 `git diff --check`。
2. Debug ASan/UBSan 全量 `ctest`。
3. Debug TSan 全量 `ctest`；AArch64 环境使用 `setarch aarch64 -R` 关闭地址随机化。
4. Release 构建测试、示例 smoke 与短 benchmark。
5. FIFO/ordered 的 single/fanout IPC smoke。
6. Release 汇编检查 FIFO 无共享 RMW、ordered 只保留全局序列分配。

## 4. 性能验收

公平 Aeron 对比必须同时满足：

- 消费端使用忙轮询，不在空 poll 时 `yield`。
- 映射已 prefault，首次缺页不进入计时窗口。
- salias 与 Aeron 的生产者、消费者及 driver 核占用和绑核方式对称，差异明确记录。
- 首要判据在 x86 物理机上以 batch=1 测量；batch=16 仅作回归护栏。

ARM Colima VM 可用于功能、sanitizer、工具链和相对回归验证，但不能替代 x86 物理机的最终性能验收。

## 5. 后续工作

- 在目标 x86 物理机执行公平 Aeron batch=1 验收并归档原始结果。
- 根据真实工作负载决定是否增加新的等待策略；在此之前保持 `SpinPause` 单策略，避免通知流量进入热路径。
- 共享控制块 ABI 变更时同步提升版本，并增加旧版本连接拒绝测试。

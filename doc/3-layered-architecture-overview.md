# 3 - salias 分层架构总览

> 配套：`2-implementation-plan.md` 与 `plans/2026-07-10-dual-engine-architecture.md`。

salias 是 Linux 上的 driverless 共享内存消息通道。当前架构只保留一个 per-producer magic-ring 引擎，
通过排序模式与消费者拓扑组合出四种公共通道。

---

## 1. 设计约束

1. 依赖只从上层指向下层。
2. 热路径使用模板单态化，不引入虚调用。
3. 裸内存、映射与原子操作集中在 core 层。
4. FIFO 模式生产者每消息不执行共享 RMW；ordered 模式只为全局序列执行一次共享 RMW。
5. 默认等待策略为 `SpinPause`，面向忙轮询和独占核场景。

---

## 2. 分层总图

```text
L7  salias 公共 API
    Channel / Publisher / Subscriber / Config / Message
                         ↓
L5  双模通道引擎
    per-producer rings / fifo|ordered / single|fanout / shared control block
                         ↓
L4  等待策略
    SpinPause / needs_wake()
                         ↓
L3  流控与端点
    claim / commit / poll / release / minimum-consumer backpressure
                         ↓
L2  帧格式
    8-byte header / alignment / sequence encoding
                         ↓
L1  环与原子单元
    magic ring / atomic cell
                         ↓
L0  Linux 平台
    memfd / named shm / double mmap / huge pages / MAP_POPULATE
```

仓库不再包含独立 SPSC/MPSC/MPMC 单环实现、metrics 子系统、阻塞等待后端、bulk 或 broadcast 通道。

---

## 3. 公共模式

| 模式 | 排序语义 | 消费者拓扑 | 生产者共享 RMW |
|------|----------|------------|----------------|
| `FifoMpsc` | 每生产者 FIFO | 单消费者 | 0 |
| `FifoFanout` | 每生产者 FIFO | 多消费者独立游标 | 0 |
| `OrderedMpsc` | 跨生产者全局全序 | 单消费者 | 每消息 1 次序列分配 |
| `OrderedFanout` | 跨生产者全局全序 | 多消费者独立游标 | 每消息 1 次序列分配 |

每个生产者拥有独立 ring 和可见位置。fanout 模式下，每个消费者持有独立的每生产者游标；生产者以所有消费者
的最小位置作为回收边界，慢消费者不会被快消费者覆盖。

---

## 4. 进程间布局

具名通道由固定控制块和 N 个 producer ring 组成。控制块保存 ABI 版本、模式、容量、生产者/消费者槽位、
每生产者发布位置、每消费者读取位置与序列游标。Publisher/Subscriber 创建后持有持久端点，不在每次
`offer`、`poll` 或 `try_recv` 时重建端点。

普通页与大页映射都在建立时 prefault；性能计时只覆盖消息发布与消费。

---

## 5. 验证边界

- 单元与 API 测试分别验证 FIFO、ordered、single、fanout、背压和跨进程端点槽位。
- ASan/UBSan、TSan 与 Release 测试必须全绿。
- Release 汇编检查 FIFO 发布模板无共享 RMW，ordered 模板只保留全局序列分配。
- 公平 Aeron 验收必须在 x86 物理机、batch=1、忙轮询、prefault、对称绑核条件下执行；ARM Colima 只用于
  功能和 sanitizer 回归，不作为最终性能验收依据。

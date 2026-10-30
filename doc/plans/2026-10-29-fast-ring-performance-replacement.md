# Fast Ring 性能替换计划

日期：2026-10-29（Asia/Singapore）

状态：**执行中，Phase 6 全文校验开发门禁通过，最终门禁未通过**

## 1. 目标

为固定大小、可靠 FIFO 的同机 IPC 增加专用快速环形缓冲实现，先以
`FastSpscRing`、`FastSpmcRing`、`FastMpscRing`、`FastMpmcRing` 作为可审计的
候选名称。候选只有在正确性、跨进程压力、吞吐和尾延迟全部通过验收后，才允许：

1. 将 `FastSpscRing` 重命名为 `SpscRing`；
2. 将 `FastSpmcRing` 重命名为 `SpmcRing`；
3. 将 `FastMpscRing` 重命名为 `MpscRing`；
4. 将 `FastMpmcRing` 重命名为 `MpmcRing`；
5. 用上述最终类型替换产品中对应拓扑的通用热路径。

当前仓库在本次计划开始时没有名为 `SpscRing`、`SpmcRing`、`MpscRing` 或
`MpmcRing` 的类型。因此“替换现有 SpscRing”在本仓库中落实为：候选通过验收后，
以不带 `Fast` 前缀的名称成为唯一产品类型，不保留两套同名实现，也不发布
`Fast*` 永久别名。

## 2. 范围与语义

首版 Fast Ring 只覆盖以下明确约束：

- 同机共享内存、跨进程。
- 固定 payload 大小。
- 每个 producer 内 FIFO；不提供跨 producer 全局顺序。
- 每个 subscriber 接收全部消息，不做竞争消费。
- 可靠背压；最慢 subscriber 阻止覆盖未读数据。
- 参与者数量在通道创建时固定；流量开始后不动态注册或注销。
- 不包含 TTL、Latest、DropOldest、schema 转换或阻塞唤醒策略。

通用 `Channel` 继续保留 Ordered、Latest、TTL、动态订阅和恢复语义。Fast Ring
成为固定大小可靠 FIFO 的独立快速路径，不能为了性能破坏通用路径的现有契约。

## 3. 设计方向

- 固定 slot 布局，避免变长帧扫描和旧 payload 冒充未来帧头。
- producer 写 payload 后以一次 release 发布写位置；consumer 以 acquire 观察该位置。
- producer/consumer 分别缓存对端位置，仅在空、满或批末读写共享位置。
- 每生产者独立 ring，避免跨 producer 的共享 RMW。
- SPMC/MPMC 为每个 consumer 保存独立游标；生产者只在慢路径读取最慢游标。
- 热字段按 cache line 隔离，payload slot 不携带原子对象。
- 提供零拷贝 claim/commit，以及便于测试和业务使用的拷贝接口。
- 首版直接使用外部提供的共享内存映射，之后接入具名通道工厂。

## 4. 执行清单

### Phase 0：基线、命名与验收口径

- [x] 创建并落盘本计划。
- [x] 盘点当前仓库，确认没有现有 `SpscRing`/`SpmcRing`/`MpscRing`/`MpmcRing` 类型。
- [x] 记录当前 v4.0.1 的 SPSC/MPSC/SPMC/MPMC 基线来源与目标机器限制。
- [x] 固化“当前通用 Channel 对照”和“Aeron 对照”两套公平比较口径。
- [x] 固化最终重命名门禁，后续不得因工期或单轮结果放宽。

### Phase 1：FastSpscRing

- [x] 定义固定 slot、完成位和对端位置缓存的内存协议。
- [x] 实现外部共享内存视图 `FastSpscRing<PayloadSize, Capacity>`。
- [x] 实现单 producer claim/commit 与单 consumer acquire/release。
- [x] 覆盖空、满、回绕、跨进程、最慢消费者背压和完整 payload 校验测试。
- [x] 建立与当前 FifoMpsc(1P1S) 及 Aeron SPSC 的同机 benchmark。
- [x] SPSC 初版性能达到计划的晋级阈值。

### Phase 2：FastSpmcRing

- [x] 实现每 consumer 独立游标和 fanout。
- [x] 覆盖慢订阅者背压、订阅者位置独立推进和全部消息完整交付。
- [x] 建立 1P2S benchmark，确认 fanout 成本与 SPSC 基线可比。
- [x] SPMC 初版性能达到计划的晋级阈值。

### Phase 3：FastMpscRing

- [x] 实现每 producer 独立 ring、单 consumer 多环轮询。
- [x] 覆盖 producer 间独立推进、环间饥饿边界和每环 FIFO。
- [x] 建立 2P1S benchmark，确认无跨 producer 共享 RMW。
- [x] MPSC 初版性能达到计划的晋级阈值。

### Phase 4：FastMpmcRing

- [x] 组合每 producer 独立 ring 与每 consumer 独立游标。
- [x] 覆盖 2P2S、两条独立 FIFO、fanout 和慢消费者背压。
- [x] 记录固定 CPU 预算、Aeron driver 占核和共享核边界。
- [x] MPMC 初版性能达到计划的晋级阈值。

### Phase 5：产品集成

- [x] 将 Fast Ring 接入具名共享内存创建、连接和生命周期管理。
- [x] 增加 schema-bound 固定消息 API；首版拒绝不支持的可变长度语义。
- [x] 在 benchmark client 中加入可用命令行选择的 Fast Ring 引擎。
- [x] 验证发布、交付、payload、序号和重复消息完整性。
- [x] 保持通用 Channel 的默认行为、ABI 版本检查和失败回退边界清晰。

### Phase 6：性能验收

- [ ] 在最终 6×20 冻结二进制矩阵中同时比较当前 v4.0.1 通用路径、Fast Ring 和 Aeron。
- [x] 覆盖 64 B、256 B 和 SPSC/MPSC/SPMC/MPMC 四个拓扑。
- [x] 每格完成至少 1 轮预热、5 轮正式开发测量（ABBA 配对，240 次正式轮）。
- [ ] 最终门禁使用 6 组 × 20 轮 ABBA。
- [x] 吞吐、attempt p50/p99/p99.9、planned p50/p99/p99.9 全部留存原始记录。
- [ ] 使用独立裸机或独占 vCPU 环境复验；共享 KVM 结果只作开发证据。
- [x] Fast Ring 相对当前通用 Channel 在每个拓扑/Payload 单元均有可复现提升（1.48x–16.31x）。
- [x] Fast Ring 相对 Aeron 在四个拓扑、两种 Payload 的吞吐中位数均达到至少 1.10x（1.31x–5.02x）。
- [x] Fast Ring 的 attempt p50 不高于 Aeron；p99 不高于 Aeron 的 1.10x（8/8）。
- [x] 1×5 开发矩阵的所有 240 次正式性能轮完整发布和交付，且 payload、producer 身份、每 producer 序号、顺序、重复检查通过。
- [ ] 最终 6×20 矩阵的所有性能轮完成同样的全文校验。

开发证据为 1 轮预热、5 轮正式 ABBA、每套 240 次正式测量；Fast 与 FIFO 两套矩阵的每次
正式性能轮均使用 `--verify full`，各发布 15.2 亿条、交付并全文校验 22.8 亿次、
采集 3000 万个延迟样本，开发门禁脚本 8/8 通过。另有 8/8 无计时完整正确性矩阵，
每个 producer 1000 万条并逐字节、序号、顺序和重复校验通过。详细结果和限制见
[Fast Ring 开发验收](../benchmarks/2026-10-29-fast-ring/README.md)。

最终门禁仍被两项事实阻塞：

1. 尚未执行 6 组 × 20 轮 ABBA；
2. 4 vCPU 主机上的 MPMC 2P2S 必须让 Aeron driver 与 subscriber0 共用 CPU0，最终
   矩阵至少需要 5 个可独占 vCPU（或独占裸机）。

冻结 runner 已支持 `--groups 6 --rounds 20 --verify full --require-dedicated-driver`；
当前 `environment.json` 记录 `max_application_workers=4`、
`dedicated_driver_available=false`，因此本机只能完成 1×5 开发矩阵。性能轮全文校验
已经完成，不再是 Phase 7 的阻塞项。

因此 Phase 7 不得开始。

### Phase 7：最终重命名与替换

- [ ] 先签署性能验收结论，再执行生产名称变更。
- [ ] `FastSpscRing` 全局重命名为 `SpscRing`，并替换对应 SPSC 热路径。
- [ ] `FastSpmcRing` 全局重命名为 `SpmcRing`，并替换对应 SPMC 热路径。
- [ ] `FastMpscRing` 全局重命名为 `MpscRing`，并替换对应 MPSC 热路径。
- [ ] `FastMpmcRing` 全局重命名为 `MpmcRing`，并替换对应 MPMC 热路径。
- [ ] 删除候选名称、临时别名和 benchmark-only 分支；git 历史保留迁移轨迹。
- [ ] 更新 README、架构文档、迁移说明、CHANGELOG 和性能报告。

### Phase 8：最终回归

- [ ] Release CTest 全绿。
- [ ] ASan/UBSan CTest 全绿。
- [ ] TSan CTest 全绿。
- [ ] 跨进程压力、故障恢复和旧通用通道回归全绿。
- [ ] 新最终名称的安装头文件和下游 consumer 编译测试通过。
- [ ] 冻结最终 benchmark 输入与 SHA-256，并能由文档命令复现。

## 5. 晋级与重命名门禁

### 5.1 CPU 预算记录

2026-10-29 的开发机是 4 vCPU KVM，cgroup 可用 CPU 为 0-3，Aeron 使用外部
SHARED/backoff media driver。冻结 runner 的 CPU 布局如下：

| 拓扑 | 应用 worker | Aeron driver / coordinator | 共享核边界 |
|---|---|---|---|
| SPSC 1P1S | CPU1-2 | CPU0 | 无应用 worker 与 driver 共享 |
| MPSC 2P1S | CPU1-3 | CPU0 | 无应用 worker 与 driver 共享 |
| SPMC 1P2S | CPU1-3 | CPU0 | 无应用 worker 与 driver 共享 |
| MPMC 2P2S | CPU0-3 | CPU0 | driver、coordinator 与 subscriber0 共享 CPU0 |

SPSC/MPSC/SPMC 的 worker 与 Aeron driver 不共享核；MPMC 在本机无法满足该条件。
因此本机结果可验证实现趋势和开发门禁，不能签署“独占 CPU”最终晋级门禁。

### 5.2 正确性门禁

- 任意 slot 只有在完整 payload 写入后才可被消费者观察。
- consumer 只能覆盖自己已释放的 slot；慢 subscriber 必须对 producer 形成背压。
- 回绕后旧 payload 不得被当成新消息。
- producer/consumer 崩溃或进程退出时，未释放 slot 不得被静默复用。
- 单元、跨进程、ASan/UBSan、TSan 和故障注入全部通过。

### 5.3 性能门禁

正式性能门禁必须同时满足：

1. 相对当前 v4.0.1 通用 FIFO 路径，每个拓扑/Payload 单元吞吐中位数提升至少 25%；
2. 相对 Aeron，每个拓扑/Payload 单元吞吐中位数至少为 1.10x；
3. attempt p50 不高于 Aeron；
4. attempt p99 不高于 Aeron 的 1.10x；
5. 全量输入完成且 payload、序号、重复检查通过；
6. 结果来自独占 CPU 或至少不共享应用 worker 与 Aeron driver 的机器。

共享 4 vCPU KVM 上的开发结果可以决定是否继续优化，但不能触发最终重命名。

## 6. 回滚策略

- 每个 Phase 独立提交，性能候选始终在 `Fast*` 名称下开发。
- 任一阶段未达到正确性或性能门禁时，保留实验报告和失败反例，不晋级产品名称。
- 新产品路径与通用 Channel 在同一进程内不得共享未版本化控制段。
- 若最终重命名后出现回归，回滚对应拓扑到上一稳定 tag，而不是删除通用语义。

## 7. 执行记录

| 日期 | Phase | 结果 | 证据 |
|---|---|---|---|
| 2026-10-29 | 0 | 计划落盘；确认仓库当前无四个 Ring 类型 | 本文件 |
| 2026-10-29 | 1 | FastSpscRing 初版与 7 个 Release 测试通过 | `src/core/ring/fast_spsc_ring.hpp`、`test/ring/fast_spsc_ring_test.cpp` |
| 2026-10-29 | 1 | SPSC 开发基准：64 B 197.46-199.55 Mmsg/s，256 B 37.85-42.89 Mmsg/s；对 v4.0.1 基线中位数分别约 22.2x/4.5x，对 Aeron 基线中位数约 5.4x/2.7x | `bench/fast_ring_compare.cpp`；共享 4 vCPU KVM，只作晋级开发证据 |
| 2026-10-29 | 2 | FastSpmcRing fanout、独立游标、慢消费者背压测试通过 | `src/core/ring/fast_spmc_ring.hpp`、`test/ring/fast_ring_topologies_test.cpp` |
| 2026-10-29 | 3 | FastMpscRing 双 producer lane、轮询和无环间饥饿测试通过 | `src/core/ring/fast_mpsc_ring.hpp`、`test/ring/fast_ring_topologies_test.cpp` |
| 2026-10-29 | 4 | FastMpmcRing 2P2S 与跨进程双 producer/双 consumer 测试通过 | `src/core/ring/fast_mpmc_ring.hpp`、`test/ring/fast_ring_topologies_test.cpp` |
| 2026-10-29 | 2-4 | 五轮开发吞吐中位数（64 B）SPSC 134.79、MPSC 118.23、SPMC 46.29、MPMC 75.26 Mmsg/s；较 v4.0.1 基线约 15.2x/3.5x/6.7x/4.5x，较 Aeron 历史中位数约 3.7x/2.7x/1.6x/6.3x | `bench/fast_ring_compare.cpp`；每 producer 1000 万条、完整 payload 写入、跨进程 marker/序号校验 |
| 2026-10-29 | 2-4 | 五轮开发吞吐中位数（256 B）SPSC 50.01、MPSC 46.83、SPMC 22.58、MPMC 26.69 Mmsg/s；较 v4.0.1 基线约 5.8x/3.1x/3.5x/2.2x，较 Aeron 历史中位数约 3.5x/3.0x/2.3x/3.7x | 同上；共享 4 vCPU KVM，仅作晋级开发证据 |
| 2026-10-29 | 6 | Fast 全文校验矩阵：1 轮预热 + 5 轮正式 ABBA 全部退出 0；240 次正式轮发布 15.2 亿条、交付并全文校验 22.8 亿次、采集 3000 万延迟样本 | `doc/benchmarks/2026-10-29-fast-ring/formal-full-fast-5-round/` |
| 2026-10-29 | 6 | 同协议 FIFO 全文校验矩阵完成；8/8 单元对 Aeron 吞吐 1.31x-5.02x，对 FIFO 1.48x-16.31x，attempt p50/p99 全部通过，门禁 8/8 | `doc/benchmarks/2026-10-29-fast-ring/formal-full-fifo-5-round/`、`formal-full-fast-5-round/verification-full-5-round.json` |
| 2026-10-29 | 6 | 8/8 拓扑/Payload 完整正确性矩阵通过；每个 producer 1000 万条，逐字节、producer 身份、每 producer 序号、顺序和重复校验 | `doc/benchmarks/2026-10-29-fast-ring/correctness-full-10m/` |
| 2026-10-29 | 6 | Release 22/22、ASan/UBSan 21/21、TSan 21/21 通过；安装后下游 consumer 编译并运行 `FixedChannel`；门禁脚本 5/5 通过 | `doc/benchmarks/2026-10-29-fast-ring/validation/` |
| 2026-10-29 | 6 | MPMC attempt p99.9 未全面领先（64 B 3.178/1.671 ms，256 B 1.589/1.163 ms）；最终验收仍缺 6 组 × 20 轮 ABBA 和 5+ 独占 vCPU（当前 MPMC driver 与 subscriber0 共核），Phase 7 阻塞 | 本计划 §5.3；不执行 `Fast*` 到最终名称的替换 |

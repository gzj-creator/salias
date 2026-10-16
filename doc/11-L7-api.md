# L7 公共 API

公共 API 暴露单一 per-producer-ring 引擎的四种排序/拓扑组合，并增加 `LatestFanout` 保留窗口模式：

```cpp
enum class Mode {
  FifoMpsc,
  FifoFanout,
  OrderedMpsc,
  OrderedFanout,
  LatestFanout,
};

struct Config {
  std::string name;
  Mode mode = Mode::FifoMpsc;
  std::size_t capacity = 1u << 20;
  std::uint32_t num_producers = 1;
  std::uint32_t num_consumers = 1;
  HugePage huge = HugePage::None;
  std::size_t publication_window = 0;
  std::uint32_t high_watermark_percent = 90;
};
```

`Channel<M>::create` 创建 owner，`connect` 连接已有通道。模板参数覆盖 `Config::mode`。
single 模式要求 `num_consumers == 1`；fanout 模式允许 1–8 个消费者；生产者上限为 16。
`capacity` 必须为 2 的幂和系统页大小的整数倍；显式大页配置还要求它是所选大页大小的整数倍。
`publication_window == 0` 表示满环。低延迟推荐 64KiB–256KiB：窗口把在途字节数收成窗口量级，
从而把排队延迟压到同一量级。128KiB 对照满 4MiB 环时，p99 从约 5.7ms 降到约 350us，吞吐约少一成。
窗口越小，生产者越常刷新消费者位置，需要按消息大小复测吞吐拐点。
当前公共协议只支持变长帧，不提供 fixed-size 配置模式。

`LatestFanout` 使用 FIFO 顺序和 fanout 拓扑。`salias::LatestFanoutChannel<Capacity,
HighWatermarkPercent>` 将每个 producer ring 的字节容量和保留比例固定在模板参数中，水位默认
为 90%；创建时模板参数覆盖 `Config::capacity` 与 `Config::high_watermark_percent`，连接时会
校验共享通道参数，不一致返回 `Error::BadConfig`。达到保留水位后，慢消费者会在下一次读取时
跳过已淘汰的旧消息。未 `release()` 的消息 payload 会被 pin；若生产者写入将覆盖借用内存，发布
暂时返回 `Error::BackPressured`，释放该消息后恢复。
`LatestFanout` 不支持非零 `publication_window`；该模式以保留水位而非消费者游标控制排队量。
订阅进程若在持有借用时退出，生产者会在需要复用对应物理区间时检查注册进程身份并回收失效 lease。
保留水位计算包含每帧 8 字节帧头和 8 字节对齐；单帧超过水位时，`offer`、`try_claim` 和
等长 `offer_batch` 返回 `Error::MessageTooLarge`，不会临时扩大窗口。等长批次可能只发布
水位能容纳的前缀，调用者须按返回条数续发；变长批次按逐帧语义返回部分成功条数。
因 pin 而失败的写入不会淘汰更多旧消息或消耗序列号。同一订阅端每个 producer ring 同时
最多借用一条消息；未 release 时该 ring 暂不返回新消息，其他 producer ring 不受影响。
`poll` 按最多 32 条的内部批次 pin payload，每批的最后一个回调返回后统一释放。

LatestFanout 还提供 `try_recv_checked()`，用于把丢旧从静默跳过变成可观测事件。若本地游标
已落在保留水位之前，第一次 checked 接收返回 `Error::Lagged`，同时把游标推进到保留窗口
起点且不借用消息；调用方重试后即可收到该窗口的第一条消息。`lagged_messages()` 统计
已被淘汰的消息数，`skipped_messages()` 统计主动跳过的保留消息数；
`skip_to_latest(producer_id)` 会将指定 producer ring 推进到最新已提交帧的起点，保留该帧
未读，返回淘汰与主动跳过的总数。持有该 ring 的单条或批量借用时跳读返回 `BackPressured`。
可靠 FIFO/Ordered 模式的 checked 接收只返回普通消息或空结果，不会生成 `Lagged`，也拒绝
`skip_to_latest()`。

```cpp
using QuoteChannel = salias::LatestFanoutChannel<1u << 20>;  // 默认保留水位为 90%
auto owner = QuoteChannel::create(config);
auto publisher = owner->publisher();
auto subscriber = owner->subscriber();
```

`create` 和 `connect` 会把动态内存分配失败转换为 `Error::OutOfMemory`。`publisher()` 和
`subscriber()` 需要分配端点共享状态，因此分配失败时仍可能抛出 `std::bad_alloc`。

```cpp
auto owner = salias::FifoFanoutChannel::create(config);
auto publisher = owner->publisher();
auto subscriber = owner->subscriber();
```

每次 `publisher()` 分配独占 producer slot；每次 fanout `subscriber()` 分配独立 consumer
slot。slot 耗尽后的端点操作返回 `BadConfig` 或无消息。

`Channel::connected_consumer_count()` 返回当前注册槽中仍存活的订阅者数，统计的是订阅槽而非
不同进程数或仍存活的 `Subscriber` C++ 对象。跨进程消费者须在连接后调用 `subscriber()` 注册；
`fork()` 继承的端点仍归属于注册它的原进程，不会自动产生新的订阅槽。进程退出后该接口会
检测到槽位对应进程已退出，但此阶段不会回收槽位或改变生产者背压；槽位注销与回收由后续能力提供。

`Publisher` 持有 Tx，`Subscriber` 持有 Rx。端点不会在 `offer`、`try_recv`、`poll`、`release`
调用中重建，因此缓存与游标跨调用保留。

`PublishClaim` 提供零拷贝写入：

```cpp
auto claim = publisher.try_claim(payload_size);
write(claim->payload());
claim->commit();
```

每个成功 claim 都必须提交。乱序 commit 不会把可见水位推进到空洞之后，补齐后才连续发布。
不支持放弃未提交 claim，也不支持发布进程异常退出后的自动恢复；这类故障需要重建通道。

`Subscriber::poll` 在回调返回后自动 release。回调必须 `noexcept`，消息 payload 不能在 release
后继续持有。

进程内有界队列可使用 `salias::Ringbuffer<T, Capacity, HighWatermarkPercent>`。默认
`HighWatermarkPercent = 90`，即最多保留 `floor(Capacity * 0.9)` 项（最少一项）；入队超出
高水位时淘汰最旧项、保留最新项。`push()` 返回本次是否发生淘汰，`dropped_count()` 提供
累计数。它是单线程容器，不改变 IPC Channel 的背压、借用消息或 fanout 语义。

# L7 公共 API

## 公开入口分层

公开 Channel API 只有一个通用入口、一个固定槽入口和一个 latest wrapper 主入口：

| 场景 | 推荐入口 | 数据面 |
|---|---|---|
| 动态、变长、Ordered、TTL 或 schema-bound 通用流 | `Channel<Mode, Schema>` | per-producer magic-ring |
| 固定 payload、固定参与者、可靠 FIFO 快速流 | `FixedChannel<Topology, Schema, Capacity, ...>` | 固定槽 Ring |
| 有界 latest fanout | `LatestMpmcChannel<Capacity, HighWatermarkPercent, Schema>` | latest 保留窗口 |

`Channel<Mode>` 与 `Channel<Mode, Schema>` 是同一通用 Channel 产品的 raw 和 fixed-message payload
模式，不是两套产品；两者不能互连。`FixedChannel` 是独立数据面，不经过通用 Channel 的变长 frame、
动态订阅、TTL 或 Latest 语义，也不能与通用 `Channel` 互相替代。

以下四个 raw 快捷入口已在 `v5.0.0` 标记为 **removed**，不再由头文件或 `import salias;` 导出：

| 已删除入口 | 规范入口 | 当前状态 |
|---|---|---|
| `salias::FifoMpscChannel` | `Channel<Mode::FifoMpsc>` | removed since `v5.0.0` |
| `salias::FifoMpmcChannel` | `Channel<Mode::FifoMpmc>` | 同上 |
| `salias::OrderedMpscChannel` | `Channel<Mode::OrderedMpsc>` | 同上 |
| `salias::OrderedMpmcChannel` | `Channel<Mode::OrderedMpmc>` | 同上 |

直接 `Channel<Mode::LatestMpmc>` 的 `create()`/`connect()` 已作为 **removed** 路径关闭。latest
fanout 的 raw 和 typed payload 都使用 `LatestMpmcChannel`：省略 `Schema` 时是 raw payload，
显式传入 `Schema` 时保持 schema ID、版本和 payload 大小校验。这样 latest 入口不会绕过静态容量与
水位检查。
`LatestByKey`、Priority 和进程内 `Ringbuffer` 已在 `v6.0.0` 与整个 `salias::extras` 边界一起
永久移除。九个规范 extras 类型、四个 extras header、`salias::extras` CMake target/component 和
`import salias.extras;` 均没有替代入口；应用需要自行迁移或重新设计对应场景。
完整迁移表见 [Channel API 迁移说明](migration/channel-api-convergence.md)。
API 冻结候选与逐文件 SHA-256 见 [Channel API v6 冻结候选](api/channel-api-freeze.md)。

公共 API 暴露单一 per-producer-ring 引擎的四种排序/拓扑组合，并增加 `LatestMpmc` 保留窗口模式：

```cpp
enum class Mode {
  FifoMpsc,
  FifoMpmc,
  OrderedMpsc,
  OrderedMpmc,
  LatestMpmc,
};

struct Config {
  std::string name;
  Mode mode = Mode::FifoMpsc;
  std::size_t capacity = 1u << 20;
  std::uint32_t num_producers = 1;
  std::uint32_t num_consumers = 1;
  HugePage huge = HugePage::None;
  std::size_t publication_window = 0;
  std::size_t publication_window_messages = 0;
  std::uint32_t high_watermark_percent = 90;
};
```

`Channel<M>::create` 创建 owner，`connect` 连接已有通道。模板参数覆盖 `Config::mode`。
single 模式要求 `num_consumers == 1`；fanout 模式允许 1–8 个消费者；生产者上限为 16。
`FifoMpmc` / `OrderedMpmc` 是广播语义，每个订阅者都会收到消息，不是竞争式消费。
`connect` 先确认控制段达到固定尺寸后再建立映射，随后等待 `ready == 1`；创建端
`shm_open` 与 `ftruncate` 之间的半初始化段会被继续等待，不会被当作可读控制块。
`capacity` 必须为 2 的幂和系统页大小的整数倍；显式大页配置还要求它是所选大页大小的整数倍。
`publication_window == 0` 表示满环。低延迟推荐 64KiB–256KiB：窗口把在途字节数收成窗口量级，
从而把排队延迟压到同一量级。128KiB 对照满 4MiB 环时，p99 从约 5.7ms 降到约 350us，吞吐约少一成。
窗口越小，生产者越常刷新消费者位置，需要按消息大小复测吞吐拐点。
默认的 `Channel<M>` 是变长 raw-payload 通道。`Channel<M, Schema>` 绑定固定布局消息，并在共享控制块中
记录显式 schema ID、版本和 payload 大小；两种通道不可互连。

`publication_window_messages == 0` 表示不启用消息数限制；正数限制每个 producer ring 中已预留、
但尚未被最慢可靠订阅者消费的消息数。达到窗口后返回 `Error::BackPressured`，释放或批量轮询
推进消费游标后即可恢复。该限制与字节窗口可同时启用，空 payload 也计为一条消息。`LatestMpmc`
将此值作为额外的最大保留帧数：默认 `DropOldest` 订阅者落后时淘汰最旧帧，`Reliable` 订阅者仍可
阻止淘汰并造成背压。LatestMpmc 下 `publication_window` 必须为 0，单条或批量发布受消息数和静态
字节水位两者中更严格者约束；等长批次可返回部分成功条数。

`LatestMpmc` 使用 FIFO 顺序和 fanout 拓扑。`salias::LatestMpmcChannel<Capacity,
HighWatermarkPercent, Schema>` 将每个 producer ring 的字节容量和保留比例固定在模板参数中，水位默认
为 90%；创建时模板参数覆盖 `Config::capacity` 与 `Config::high_watermark_percent`，连接时会
校验共享通道参数，不一致返回 `Error::BadConfig`。省略 `Schema` 时使用 raw payload；显式传入时，
连接还会校验 schema ID、版本和 payload 大小，不匹配返回 `Error::SchemaMismatch`。默认订阅策略为 `DropOldest`；创建
`Subscriber` 时也可显式选择 `SubscriptionPolicy::Reliable`。Reliable 订阅者的未读游标参与
保留边界，生产者不能跨过它淘汰消息，因此慢的 Reliable 订阅者会在高水位处施加背压；
DropOldest 订阅者则可独立跳过已淘汰消息，不会拖住其他消费者。其他 channel mode 只支持
Reliable 策略，传入 `DropOldest` 返回 `Error::BadConfig`。达到保留水位后，DropOldest 慢消费者
会在下一次读取时跳过已淘汰的旧消息。未 `release()` 的消息 payload 会被 pin；若生产者写入将覆盖借用内存，发布
暂时返回 `Error::BackPressured`，释放该消息后恢复。
`LatestMpmc` 不支持非零 `publication_window`；该模式以静态保留水位和可选的
`publication_window_messages` 限制排队量。
订阅进程若在持有借用时退出，生产者会在需要复用对应物理区间时检查注册进程身份并回收失效 lease。
保留水位计算包含每帧 8 字节帧头和 8 字节对齐；单帧超过水位时，`offer`、`try_claim` 和
等长 `offer_batch` 返回 `Error::MessageTooLarge`，不会临时扩大窗口。等长批次可能只发布
水位能容纳的前缀，调用者须按返回条数续发；变长批次按逐帧语义返回部分成功条数。
因 pin 而失败的写入不会淘汰更多旧消息或消耗序列号。同一订阅端每个 producer ring 同时
最多借用一条消息；未 release 时该 ring 暂不返回新消息，其他 producer ring 不受影响。
`poll` 按最多 32 条的内部批次 pin payload，每批的最后一个回调返回后统一释放。

LatestMpmc 的 DropOldest 订阅端还提供 `try_recv_checked()`，用于把丢旧从静默跳过变成可观测事件。若本地游标
已落在保留水位之前，第一次 checked 接收返回 `Error::Lagged`，同时把游标推进到保留窗口
起点且不借用消息；调用方重试后即可收到该窗口的第一条消息。`lagged_messages()` 统计
已被淘汰的消息数，`skipped_messages()` 统计主动跳过的保留消息数；
`skip_to_latest(producer_id)` 会将指定 producer ring 推进到最新已提交帧的起点，保留该帧
未读，返回淘汰与主动跳过的总数。持有该 ring 的单条或批量借用时跳读返回 `BackPressured`。
Reliable 订阅端的 checked 接收只返回普通消息或空结果，不会生成 `Lagged`，也拒绝
`skip_to_latest()`。

```cpp
// 默认保留水位为 90%；省略第三个模板参数即可使用 raw payload。
using QuoteChannel = salias::LatestMpmcChannel<1u << 20, 90, QuoteSchema>;
auto owner = QuoteChannel::create(config);
auto publisher = owner->publisher();
auto subscriber = owner->subscriber();
auto reliable = owner->subscriber(salias::SubscriptionPolicy::Reliable);
auto lossy = owner->subscriber(salias::SubscriptionPolicy::DropOldest);
```

`create` 和 `connect` 会把动态内存分配失败转换为 `Error::OutOfMemory`。`publisher()` 和
`subscriber()` 需要分配端点共享状态，因此分配失败时仍可能抛出 `std::bad_alloc`；也可用
`try_subscriber()` 将分配失败转换为 `Error::OutOfMemory`。

```cpp
auto owner = salias::Channel<salias::Mode::FifoMpmc>::create(config);
auto publisher = owner->publisher();
auto subscriber = owner->subscriber();
```

每次 `publisher()` 分配独占 producer slot；每个 `subscriber()` 注册一个 consumer slot。
`try_subscriber()` 在槽位耗尽时返回 `Error::NoSubscriberSlots`，OOM 时返回
`Error::OutOfMemory`；兼容工厂 `subscriber()` 在槽位耗尽时返回无效端点，其操作返回
`BadConfig` 或无消息。`Subscriber` 的副本共享同一注册，最后一个副本析构时才注销槽位。
注册期间会暂停新的 claim，并从稳定的 producer reservation 尾初始化游标，因此 late join
不会重放加入前的消息；旧 endpoint 的 generation 与复用后的槽位不匹配，不能读写新订阅者游标。

`Channel::connected_consumer_count()` 返回当前注册槽中仍存活的订阅者数，统计的是订阅槽而非
不同进程数或仍存活的 `Subscriber` C++ 对象。跨进程消费者须在连接后调用 `subscriber()` 注册；
`fork()` 继承的端点仍归属于注册它的原进程，不会自动产生新的订阅槽。进程退出后该接口会
排除死亡注册；后续订阅注册会回收死亡进程留下的槽位和 LatestMpmc lease。正常析构则立即
注销槽位；fork 子进程销毁继承的 `Subscriber` 副本不会注销父进程的注册。槽位回收不使旧
endpoint 复活，也不放宽可靠通道的数据保留语义。

`Publisher` 持有 Tx，`Subscriber` 持有 Rx。端点不会在 `offer`、`try_recv`、`poll`、`release`
调用中重建，因此缓存与游标跨调用保留。

固定布局的平凡可复制消息可绑定显式 schema，连接时完成兼容性检查：

```cpp
struct Quote {
  std::uint64_t symbol;
  std::int64_t price;
  std::uint32_t quantity;
};

using QuoteSchema = salias::Schema<Quote, 0x51554F5445000001, 1>;

using QuoteChannel = salias::Channel<salias::Mode::FifoMpsc, QuoteSchema>;
auto channel = QuoteChannel::create(config);
auto publisher = channel->publisher();
auto subscriber = channel->subscriber();
publisher.send(Quote{1001, 12345, 10});
auto quote = subscriber.try_recv_copy();
```

`Schema<T, Id, Version>` 只提供无运行时成本的显式声明；也可继续手写 `message_type`、`schema_id`
和 `version`。Schema 必须提供非零、应用管理的 `schema_id` 和 `version`；布局或字段语义变更时递增版本。连接端
比较 raw/typed 标记、schema ID、版本和 payload 大小，不匹配时返回 `Error::SchemaMismatch`。
连接成功后固定长度用于定位每帧 payload 和下一帧，不逐消息比较 header payload 长度。
值接口只接受或返回 `Schema::message_type`。`Publisher::send(const T&)` 复制一次对象表示到共享 ring；
`try_claim()` 仍返回长度固定为 `sizeof(T)` 的字节 span，调用方必须写入完整、有效的对象表示。
`Subscriber::try_recv_copy()` / `recv_copy()` 复制到本地并自动释放。对象表示包含 padding 字节；跨越不可信
边界或携带敏感数据时应使用显式序列化。LatestMpmc 的
DropOldest 订阅端非阻塞读取仍可能返回 `Error::Lagged`。此路径无逐消息堆分配，但 T 必须平凡可复制，
自身及成员不得包含进程内指针。schema 是兼容性声明，不是对恶意参与进程的认证机制。

未绑定 Schema 的 `Channel<M>` 保留变长 raw API。其 `Publisher::send<T>()` 和
`Subscriber::try_recv_copy<T>()` 可用于简单的固定值传输，但接收端仍逐消息校验 payload 大小，
不承担 schema 版本兼容性检查；长度不匹配返回 `Error::PayloadSizeMismatch` 并消费释放该消息。

原始 `try_recv()` / `recv()` 继续返回共享 ring 上的借用 `Message`；它们是最低延迟路径，使用完后
必须调用 `release()`。共享内存 payload 不能直接移动为消费者私有对象；即使单生产者单消费者配置
也只能在类型化接口中复制到本地 T 后脱离 ring 生命周期。

`PublishClaim` 提供零拷贝写入：

```cpp
auto claim = publisher.try_claim(payload_size);
if (!claim) {
  handle(claim.error());
}
write(claim->payload());
publisher.commit(std::move(*claim));
```

每个成功 claim 都必须由申请它的 Publisher 通过 `commit()` 显式提交；`PublishClaim`
只保存预留位置和发布端身份，不再保活发布端，因此 Publisher 必须覆盖 claim 的生命周期。
把 claim 交给其他 Publisher 会在调试构建中断言，在发布构建中拒绝且不发布。乱序 commit
不会把可见水位推进到空洞之后，补齐后才连续发布。
不支持放弃未提交 claim，也不支持发布进程异常退出后的自动恢复；这类故障需要重建通道。

## TTL 与批量消费

`offer_for(payload, ttl)`、`try_claim_for(size, ttl)` 和 `offer_batch_for(payloads, ttl)` 提供
按消息指定 TTL 的路径。TTL 必须非负，截止时间在 API 调用时采样，0 表示立即到期；默认不带 TTL 的
`offer`/`try_claim`/`offer_batch` 不受影响。批次中的帧共用一个截止值，TTL 扩展头的 8 字节会
计入 ring 容量、发布窗口和 LatestMpmc 保留水位。消息元数据使用同主机 `CLOCK_MONOTONIC`
纳秒时间，因此跨进程订阅者可比较该截止值，但通道不能跨系统启动周期持久化复用。
订阅端在交付前自动跳过过期帧；`Subscriber::expired_messages()` 返回本端点当前进程累计跳过数。
过期跳过会推进可靠游标并释放容量；若后面紧接着存活帧，游标只推进到该帧起点，借用期间仍受保护，
不会跨越或丢弃未过期帧。已借用的消息不会因后来到期而
失效，仍须按原契约调用 `release()`。过期清理由接收端 poll/recv 惰性触发，没有后台清理线程；
接收端停止轮询期间，尚未观察的过期帧仍可能参与背压。

`Subscriber::poll` 在回调返回后自动 release。回调必须 `noexcept`，消息 payload 不能在 release
后继续持有。

两种 `poll` 重载均使用调用独占的栈上描述符存储；空轮询不再先清零 32 个 `Message`。
只有 `fetch_batch` 已完整写入的前 `got` 个描述符可读，未填充槽不得访问。此实现不在
endpoint 中缓存描述符，不引入热路径 heap 分配，也不改变公开 `Message` 的默认零值、
32 条批处理边界或批末 flush 时机。递归调用有独立描述符，但不额外承诺递归 flush 后
外层 payload 仍有效；借用规则仍以上述回调/release 契约为准。

## 已移除 extras

`v6.0.0` 永久删除 `salias::extras::LatestByKeyChannel`、`salias::extras::LatestByKeyPublisher`、
`salias::extras::LatestByKeySubscriber`、`salias::extras::LatestByKeyMessage`、
`salias::extras::PriorityPublisher`、`salias::extras::PrioritySubscriber`、
`salias::extras::PriorityMessage`、`salias::extras::PriorityWaitPolicy` 和
`salias::extras::Ringbuffer`。专项头 `<salias/extras/latest_by_key_channel.hpp>`、
`<salias/extras/priority_channel.hpp>`、`<salias/extras/ringbuffer.hpp>`、umbrella
`<salias/extras/extras.hpp>`、旧转发头 `<salias/latest_by_key_channel.hpp>`、
`<salias/priority_channel.hpp>`、`<salias/ringbuffer.hpp>`、`salias::extras` CMake
target/component 和 `import salias.extras;` 同时删除。

`v5.0.0` 删除的旧顶层名称 `salias::LatestByKeyChannel`、`salias::LatestByKeyPublisher`、
`salias::LatestByKeySubscriber`、`salias::LatestByKeyMessage`、`salias::PriorityPublisher`、
`salias::PrioritySubscriber`、`salias::PriorityMessage`、`salias::PriorityWaitPolicy` 和
`salias::Ringbuffer`，以及旧转发头 `<salias/latest_by_key_channel.hpp>`、
`<salias/priority_channel.hpp>`、`<salias/ringbuffer.hpp>` 也继续保持删除状态。

这些能力没有替代 API，也不在本库中保留兼容实现。迁移与仓库外证据边界见
[Channel API 迁移说明](migration/channel-api-convergence.md)。

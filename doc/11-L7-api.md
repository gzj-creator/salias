# L7 公共 API

公共 API 只暴露单一 per-producer-ring 引擎的四种组合：

```cpp
enum class Mode {
  FifoMpsc,
  FifoFanout,
  OrderedMpsc,
  OrderedFanout,
};

struct Config {
  std::string name;
  Mode mode = Mode::FifoMpsc;
  std::size_t capacity = 1u << 20;
  std::uint32_t num_producers = 1;
  std::uint32_t num_consumers = 1;
  HugePage huge = HugePage::None;
  std::size_t publication_window = 0;
};
```

`Channel<M>::create` 创建 owner，`connect` 连接已有通道。模板参数覆盖 `Config::mode`。
single 模式要求 `num_consumers == 1`；fanout 模式允许 1–8 个消费者；生产者上限为 16。
`capacity` 必须为 2 的幂和系统页大小的整数倍；显式大页配置还要求它是所选大页大小的整数倍。
`publication_window == 0` 表示满环。低延迟推荐 64KiB–256KiB：窗口把在途字节数收成窗口量级，
从而把排队延迟压到同一量级。128KiB 对照满 4MiB 环时，p99 从约 5.7ms 降到约 350us，吞吐约少一成。
窗口越小，生产者越常刷新消费者位置，需要按消息大小复测吞吐拐点。
当前公共协议只支持变长帧，不提供 fixed-size 配置模式。

`create` 和 `connect` 会把动态内存分配失败转换为 `Error::OutOfMemory`。`publisher()` 和
`subscriber()` 需要分配端点共享状态，因此分配失败时仍可能抛出 `std::bad_alloc`。

```cpp
auto owner = salias::FifoFanoutChannel::create(config);
auto publisher = owner->publisher();
auto subscriber = owner->subscriber();
```

每次 `publisher()` 分配独占 producer slot；每次 fanout `subscriber()` 分配独立 consumer
slot。slot 耗尽后的端点操作返回 `BadConfig` 或无消息。

`Publisher` 持有 Tx，`Subscriber` 持有 Rx。端点不会在 `offer`、`try_recv`、`poll`、`release`
调用中重建，因此缓存与游标跨调用保留。

`PublishClaim` 提供零拷贝写入：

```cpp
auto claim = publisher.try_claim(payload_size);
write(claim->payload());
claim->commit();
```

`Subscriber::poll` 在回调返回后自动 release。回调必须 `noexcept`，消息 payload 不能在 release
后继续持有。

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
  bool fixed_size = false;
  std::size_t record_size = 0;
};
```

`Channel<M>::create` 创建 owner，`connect` 连接已有通道。模板参数覆盖 `Config::mode`。
single 模式要求 `num_consumers == 1`；fanout 模式允许 1–8 个消费者；生产者上限为 16。

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

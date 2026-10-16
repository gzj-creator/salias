# L5 Channel

L5 以一个 per-producer magic-ring 引擎承载 FIFO、Ordered 与 LatestFanout 策略：

- 每个 producer 独占一个 ring，生产者之间不竞争 ring tail。
- `Order::Fifo` 使用 producer-local sequence，不执行共享 RMW。
- `Order::Ordered` 使用 `global_sequence.fetch_add` 分配全局 rank。
- single consumer 持有一组 per-producer 游标。
- 普通 fanout 中每个 consumer 持有独立游标组；生产者背压使用所有消费者位置的最小值。
- `LatestFanout` 不以最慢消费者位置限制生产者，而是为每个 ring 维护保留头；达到
  配置水位时推进保留头，慢消费者在下次读取时跳到该位置。
- LatestFanout 与 `publication_window` 互斥；其保留水位已限定 ring 中可排队的数据量。
- LatestFanout 的消息视图是借用的：订阅者持有 `Message` 期间会 pin 对应 payload，若新写入
  将覆盖仍借用的字节则暂时回压；`release()` 或批量轮询完成后解除 pin。进程异常退出时，生产者
  在下一次覆盖该区间前按注册的 PID 与启动标识识别并清除失效 lease。
- LatestFanout 用独立的 `retention_intent` 原子字宣布拟淘汰位置，检查 lease 成功后才发布
  保留头；失败不淘汰消息，也不消耗序列号。消费者先公布 lease，再依次核对 intent 与保留头，
  防止在检查与覆盖之间借用旧帧；这条握手路径不使用共享自旋锁。
- LatestFanout 单帧（含帧头与对齐）不得超过保留水位；淘汰只跨完整已提交帧。新增 intent
  字段及丢旧计数后共享控制段版本为 v8，连接旧布局返回 `VersionMismatch`，须重建通道并重启两端。
- LatestFanout consumer 可显式使用 checked receive 观察 `Lagged`，并用 per-ring seek 跳到最新
  已提交帧；淘汰和主动跳过计数保存在共享 consumer cursor 中，同一端点的副本共享计数，
  独立订阅者之间互不影响。
- `SpinPause::needs_wake()` 为 false，commit 不更新通知 word。

本地实现位于 `hybrid_mpsc.hpp`，具名 IPC 实现位于 `shared_hybrid_mpsc.hpp`。公共 facade
暴露四种排序/拓扑组合，以及采用 FIFO 与 fanout 拓扑的 LatestFanout 保留窗口模式。

ordered consumer 只读取当前 `expected_sequence`，即使后续 rank 已提交也不会越过空洞。
FIFO consumer 轮转扫描各 producer ring，只维持每个 producer 自身的 FIFO 顺序。

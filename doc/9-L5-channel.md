# L5 Channel

L5 只有一个 per-producer magic-ring 引擎：

- 每个 producer 独占一个 ring，生产者之间不竞争 ring tail。
- `Order::Fifo` 使用 producer-local sequence，不执行共享 RMW。
- `Order::Ordered` 使用 `global_sequence.fetch_add` 分配全局 rank。
- single consumer 持有一组 per-producer 游标。
- fanout 中每个 consumer 持有独立游标组；生产者背压使用所有消费者位置的最小值。
- `SpinPause::needs_wake()` 为 false，commit 不更新通知 word。

本地实现位于 `hybrid_mpsc.hpp`，具名 IPC 实现位于 `shared_hybrid_mpsc.hpp`。公共 facade
将两种排序模式与两种消费者拓扑组合为四种 Mode。

ordered consumer 只读取当前 `expected_sequence`，即使后续 rank 已提交也不会越过空洞。
FIFO consumer 轮转扫描各 producer ring，只维持每个 producer 自身的 FIFO 顺序。

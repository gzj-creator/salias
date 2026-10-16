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
- `SpinPause::needs_wake()` 为 false，commit 不更新通知 word。

本地实现位于 `hybrid_mpsc.hpp`，具名 IPC 实现位于 `shared_hybrid_mpsc.hpp`。公共 facade
暴露四种排序/拓扑组合，以及采用 FIFO 与 fanout 拓扑的 LatestFanout 保留窗口模式。

ordered consumer 只读取当前 `expected_sequence`，即使后续 rank 已提交也不会越过空洞。
FIFO consumer 轮转扫描各 producer ring，只维持每个 producer 自身的 FIFO 顺序。

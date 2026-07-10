# 8 - L4 等待策略层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：为低延迟通道提供可内联的忙轮询等待策略，并显式标识提交路径是否需要唤醒。

**技术栈**：C++23。namespace `salias::wait`，目录 `core/wait/`。

---

## 1. 当前范围

L4 只保留 `SpinPause`：等待期间循环读取共享等待字，并在每轮调用架构对应的 CPU relax 指令。
当前双模引擎面向独占核、低延迟 IPC，不提供阻塞等待后端、运行时策略切换或退避策略族。

```cpp
template <class S>
concept WaitStrategy = requires(S strategy, std::uint32_t* word, std::uint32_t expected) {
  { strategy.wait(word, expected) } -> std::same_as<void>;
  { strategy.wake(word) } -> std::same_as<void>;
  { strategy.reset() } -> std::same_as<void>;
  { strategy.needs_wake() } -> std::convertible_to<bool>;
};
```

`needs_wake()` 是提交热路径的关键契约。`SpinPause::needs_wake()` 恒为 `false`，因此 `commit` 在编译期消除
等待字的 `fetch_add` 与 `wake()` 调用，FIFO 模式不会因此引入额外共享 RMW。

---

## 2. SpinPause

`SpinPause` 的行为：

- `wait`：当等待字仍等于期望值时持续忙轮询，并调用 `cpu_relax()`。
- `wake`：空操作，因为等待方始终在轮询。
- `reset`：空操作，无内部退避状态。
- `needs_wake`：返回 `false`。

`cpu_relax()` 在 x86 使用 pause 指令，在 AArch64 使用 yield 指令，降低紧循环对执行资源的争用；它不让线程
进入睡眠，也不保证调度器让出 CPU。

---

## 3. 与通道层的契约

生产者提交顺序为：

1. release-store 已提交帧元数据；
2. 仅当 `wait.needs_wake()` 为真时更新等待字并调用 `wake()`。

消费者在未发现可消费帧时调用 `wait()`，重新观察生产者可见位置或序列。FIFO 与 ordered 模式共用该契约，
但排序与背压语义属于 L5，不由等待策略决定。

---

## 4. 验证

- 单元测试确认 `SpinPause` 提交不会修改等待字。
- TSan 覆盖 FIFO、ordered、single 与 fanout 并发路径。
- Release 汇编检查 FIFO 发布模板不包含共享 RMW 指令；ordered 模板只保留全局序列分配所需的 RMW。
- IPC 性能 harness 使用忙轮询消费者，与 Aeron `BusySpinIdleStrategy` 对齐。

---

## 5. 验收标准

- `SpinPause::needs_wake()` 恒为 `false`，提交路径不产生通知流量。
- 等待策略接口可静态内联，无虚调用和运行时分派。
- sanitizer 与 IPC 冒烟测试覆盖所有四种公共通道模式。

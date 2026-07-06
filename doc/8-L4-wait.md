# 8 - L4 等待策略层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：把"消息还没来时怎么等"抽象成可插拔策略，在延迟与 CPU 能耗之间提供从 busy-spin 到 futex 阻塞的全谱选择；跨进程唤醒基于共享内存 futex。

**技术栈**：C++20。namespace `salias::wait`，目录 `core/wait/`。依赖 L0（`Futex`）。与 L5 契约：`WaitStrategy` 稳定签名。**不用协程**（见项目决策）。

---

## 1. 职责与边界

**做：** 定义 `WaitStrategy` 概念/抽象；实现 spin / spin+pause / yield / backoff / futex；解决跨进程无丢失唤醒。

**不做：** 不认识 position 语义（只接受一个"检查条件"回调或原子字 + 期望值）；不做背压判定（L3）；不管消息内容。

---

## 2. 设计：策略作用于"一个原子字 + 期望值"

L3 已经把"有没有新消息"归结为 position 比较。L4 只需等一个共享内存里的 32/64 位字变化。统一接口：

```cpp
namespace salias::wait {

// C++20 concept：任何等待策略都要能"等到字变化"并"唤醒等待者"。
template <class S>
concept WaitStrategy = requires(S s, std::uint32_t* word, std::uint32_t expected) {
  { s.wait(word, expected) } -> std::same_as<void>;  // 阻塞直到 *word != expected（或被唤醒）
  { s.wake(word) }           -> std::same_as<void>;  // 唤醒可能在等的对端
  { s.reset() }              -> std::same_as<void>;   // 重置内部退避计数
};

} // namespace salias::wait
```

> 用 concept 而非虚函数，热路径可单态化内联，零虚调用开销。需要运行时切换策略时，再用类型擦除包装（见 §6）。

---

## 3. 策略谱系

| 策略 | 机制 | 延迟 | 空载 CPU | 适用 |
|------|------|------|:-------:|------|
| `BusySpin` | 纯循环读字 | 最低（纳秒级唤醒） | 100% 一核 | 独占核、极致延迟、能耗不敏感 |
| `SpinPause` | 循环 + `_mm_pause()` | 极低 | ~100%（但对 SMT 兄弟核友好、省功耗） | 通用低延迟默认 |
| `Yielding` | 自旋 N 次后 `std::this_thread::yield()` | 低-中 | 中（让出给同核其他线程） | 线程数 > 核数、需共存 |
| `Backoff` | 指数退避：spin→pause→yield→短睡 | 中 | 低 | 突发流量、多通道复用少量核 |
| `Futex` | 自旋短暂后 `FUTEX_WAIT` 内核阻塞 | 中（唤醒有 syscall 成本） | **近 0** | 稀疏消息、省电、大量通道 |

```cpp
struct BusySpin {
  void wait(std::uint32_t* w, std::uint32_t exp) noexcept {
    while (std::atomic_ref<std::uint32_t>(*w).load(std::memory_order_acquire) == exp) { /*spin*/ }
  }
  void wake(std::uint32_t*) noexcept {}   // 对端在 spin，无需显式唤醒
  void reset() noexcept {}
};

struct SpinPause {
  void wait(std::uint32_t* w, std::uint32_t exp) noexcept {
    while (std::atomic_ref<std::uint32_t>(*w).load(std::memory_order_acquire) == exp)
      _mm_pause();   // 降功耗、让出流水线给 SMT 兄弟核
  }
  void wake(std::uint32_t*) noexcept {}
  void reset() noexcept {}
};

class Futex {
 public:
  void wait(std::uint32_t* w, std::uint32_t exp) noexcept {
    for (int i = 0; i < spin_budget_; ++i) {           // 先自旋一小段（避免慢消息也 syscall）
      if (std::atomic_ref<std::uint32_t>(*w).load(std::memory_order_acquire) != exp) return;
      _mm_pause();
    }
    platform::Futex(w).wait(exp);                       // 落内核阻塞
  }
  void wake(std::uint32_t* w) noexcept { platform::Futex(w).wake_one(); }
  void reset() noexcept {}
 private:
  int spin_budget_ = 1000;
};
```

---

## 4. 跨进程无丢失唤醒（最易错处）

问题：生产者发布后调 `wake`，但若消费者"检查到无消息"与"进入 FUTEX_WAIT"之间生产者刚好发布并 wake，唤醒可能丢失 → 消费者永久睡。

Linux futex 的设计正是为此：`FUTEX_WAIT(word, expected)` **原子地**"检查 `*word==expected` 才睡"。协议：

**消费者（等待方）：**
```
loop:
  pos = load_acquire(producer_pos)
  if pos != last_seen: 有新消息, 处理, return
  // 声明"我要睡了"：把 futex 字设为当前观察值
  futex_word = encode(pos)   // 或直接用 producer_pos 的低 32 位
  // FUTEX_WAIT 原子复检：若期间 producer_pos 已变，futex_word != expected，立即返回不睡
  platform::Futex(&futex_word).wait(expected = encode(pos))
  goto loop
```

**生产者（唤醒方）：**
```
store_release(producer_pos, new_pos)   // 先发布（release）
// 再唤醒：即便消费者还没进 WAIT，它进 WAIT 时会发现 word 已变而不睡
platform::Futex(&futex_word).wake_one()
```

关键：**先改字（release）后 wake**（生产者）；**先记录期望值后 WAIT（内核原子复检）**（消费者）。两侧顺序保证任何交错下都不丢唤醒。用 futex 字承载 position 低位，使"字变化"与"有新消息"等价。

可选优化：**waiter 标志**——生产者只在"有人真的在睡"时才发 `wake` syscall（省掉无谓 syscall）。用一个原子 `waiters` 计数：消费者进 WAIT 前 `++waiters`，醒后 `--waiters`；生产者 `if (load(waiters)>0) wake`。注意标志的 release/acquire 与 position 顺序，避免"标志漏看"再次引入丢失唤醒——需 store 标志后再复查 position（double-check）。

---

## 5. 空载 CPU 目标

- `Futex` 策略稳态无消息时 CPU 近 0（对比 Aeron busy-spin 满核）——这是 salias 在"大量稀疏通道"场景的能耗优势。
- `BusySpin`/`SpinPause` 保证极致延迟，代价是占核。默认策略 `SpinPause`（延迟与功耗折中），用户可按通道切 `Futex`。

---

## 6. 运行时可切换（类型擦除）

热路径用静态 concept 单态化；需要"配置决定策略"时，提供类型擦除包装：

```cpp
class AnyWaitStrategy {                    // 运行时多态，仅在非热路径/低频通道用
 public:
  template <WaitStrategy S> explicit AnyWaitStrategy(S s);
  void wait(std::uint32_t* w, std::uint32_t exp);
  void wake(std::uint32_t* w);
  void reset();
 private:
  struct Concept { /* 虚接口 */ };
  std::unique_ptr<Concept> impl_;
};
```

约定优于配置：默认给 `SpinPause`，不配置即可用；进阶用户按通道选 `Futex`/`BusySpin`。

---

## 7. 危险操作契约（SAFETY）

- futex 字必须位于 `MAP_SHARED` 共享内存、4 字节对齐（L0 保证）。
- `wait/wake` 内所有对 futex 字的访问走 `atomic_ref` 或 futex syscall，不用普通读写。
- 唤醒顺序契约（§4）必须严格遵守，任何偏离都可能丢唤醒——此处附最详尽的 `// SAFETY:` 与 happens-before 论证。

---

## 8. 测试清单

| 测试 | 方法 | 通过标准 |
|------|------|----------|
| 无丢失唤醒 | 生产/消费高频压测，随机延迟注入到"检查-WAIT"窗口 | 消费者永不永久阻塞（看门狗超时=失败） |
| 唤醒正确性 | 单发一条后消费者能醒 | 100% 醒来并消费 |
| 空载 CPU | Futex 策略静置 10s，采样 CPU | 近 0% |
| 延迟不回退 | BusySpin/SpinPause ping-pong | p50 不高于目标基线 |
| waiter 标志 | 开启标志优化，统计 wake syscall 次数 | 无等待者时 syscall≈0，且不丢唤醒 |
| 策略切换 | AnyWaitStrategy 运行时切 | 行为正确，热路径静态版无虚调用（反汇编确认） |

工具：TSan（并发）、`perf stat`（CPU/ syscall 计数）、注入延迟的确定性 harness。

---

## 9. 验收标准

- 所有策略在压测下**零丢失唤醒、零永久阻塞**（含对抗性延迟注入）。
- `Futex` 空载 CPU 近 0；`BusySpin`/`SpinPause` 延迟不劣于基线。
- 静态 concept 路径零虚调用开销（反汇编确认内联）。
- waiter 标志优化在无等待者时消除 wake syscall，且不破坏唤醒正确性。
- 默认策略开箱即用（`SpinPause`），无需配置。

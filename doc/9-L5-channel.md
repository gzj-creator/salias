# 9 - L5 通道层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：把下层（ring/frame/flow/wait）组装成四种成品通道——SPSC、MPSC、多订阅广播、bulk 大消息——面向 L7 提供统一收发语义。

**技术栈**：C++20。namespace `salias::channel`，目录 `core/channel/`。依赖 L1–L4。绝不依赖 L6/L7（L6 计数器由 L5 写入但通过接口注入，避免反向依赖）。

---

## 1. 职责与边界

**做：** 组合 `MagicRing` + `Producer/Consumer` + `WaitStrategy`，实现四种通道；处理多生产者仲裁、多订阅进度、大消息通道。

**不做：** 不做平台/内存分配（L0）、不做裸内存序（L1/L3）、不做 driverless 握手与用户 API（L7）。

---

## 2. 四种通道概览

| 通道 | 写者 | 读者 | 语义 | 主要用途 |
|------|:----:|:----:|------|----------|
| **SPSC** | 1 | 1 | 单向队列，最快 | 线程/进程一对一流水线 |
| **MPSC** | N | 1 | 多写汇聚 | 多生产者汇总到单消费者 |
| **Broadcast** | 1 | M | 每个订阅者独立读全量 | 行情扇出、事件总线 |
| **Bulk** | 1 | 1/M | 大消息（> ring 单帧上限）单帧连续 | 快照、批数据 |

---

## 3. SPSC（快路，基线）

直接封装 L3 的 `Producer`/`Consumer` + 一个 `WaitStrategy`。零仲裁、零 CAS，纯 release/acquire。

```cpp
template <wait::WaitStrategy W = wait::SpinPause>
class SpscChannel {
 public:
  static std::expected<SpscChannel, ChannelError>
  create(const ChannelConfig&);   // 内部建 MagicRing + 位置字

  class Tx {                        // 发送端
   public:
    std::expected<Claim, FlowError> claim(std::uint32_t len) noexcept;
    void commit(const Claim&) noexcept;              // 内含 wait::wake
    std::expected<void, FlowError> offer(std::span<const std::byte>) noexcept;
  };
  class Rx {                        // 接收端
   public:
    Message recv() noexcept;                          // 阻塞：poll + wait 直到有消息
    std::optional<Message> try_recv() noexcept;       // 非阻塞
    std::size_t recv_batch(std::span<Message>) noexcept;
    void release() noexcept;                          // advance head
  };
};
```
- `recv()` = `poll()`（L3）失败则 `wait()`（L4），醒后重试。`commit()` 发布后按策略 `wake()`。
- 这是性能基线，所有 bench 以它为准。

---

## 4. MPSC（慢路，多生产者仲裁）

多个生产者竞争同一 ring 的 tail。用 CAS 预留区间，各自写完后按序发布。

### 预留（claim）
```
loop:
  tail = load_acquire(producer_pos)
  need = frame_len(len)
  if 空间不足(据 cached head) -> 刷新 head; 仍不足 -> BackPressured
  if CAS(producer_pos, tail, tail+need) 成功:   // 抢到 [tail, tail+need)
      break
  // 失败则有其它生产者抢先，重试
return slot at tail
```

### 发布（commit）与 gap 问题
多生产者乱序完成写入。消费者必须**按 position 顺序**看到连续已提交的帧，不能越过尚未写完的空洞。方案（二选一，文档标注取舍）：

- **方案 A：每帧 COMMITTED 标志**。生产者写完 payload 后，用 `atomic_ref` release 写头的 `FLAG_COMMITTED`。消费者顺序扫描，遇到未 COMMITTED 的帧就停（等待）。简单，但消费者要读每帧头判标志。
- **方案 B：有序发布游标**。维护一个 `published_pos`，生产者写完后用 CAS 把它从"自己的 start"推进到"自己的 end"，只有当 `published_pos == 自己的 start` 时才能推进（否则等前面的先发布）。消费者只看 `published_pos`，无需逐帧查标志。吞吐更好，实现更绕。

默认采用 **方案 A**（正确性直观、易测），高吞吐场景提供方案 B 作为可选。

### 取舍说明
MPSC 的 CAS 抢占在高竞争下会退化（cache line 乒乓）。因此：
- SPSC 与 MPSC 是**两套实现**，用户按需选；不用"MPSC 兼容 SPSC"牺牲快路性能。
- 生产者数多时建议每生产者独立 SPSC + 消费者多路 poll（sharding），常比单 MPSC ring 更快——L7 文档给选型指引。

---

## 5. Broadcast（一写多读，独立进度）

一个生产者，M 个订阅者，每个订阅者读到**全量**消息、各自独立 head。

- 单 ring，生产者一份写入；每个订阅者维护自己的 consumer position。
- **空间回收受最慢订阅者约束**：生产者可覆盖的区间上界 = `min(所有订阅者 head)`。用一个"最小消费位置"缓存（周期刷新或订阅者 advance 时更新）。
- **慢订阅者掉队策略**（背压 vs 覆盖，二选一，配置决定）：
  - **可靠模式**：生产者背压等最慢订阅者（不丢），语义同 Aeron。
  - **有损模式**：生产者不等，覆盖最旧数据；掉队订阅者读时用 `seq`（L2 代次）检测到"被覆盖"，报 `Lagged` 并跳到最新（类 LMAX Disruptor 的 gating 可选放开）。
- 订阅者动态加入/退出：加入从当前 tail 开始（不重放历史）；退出把其 head 移出"最小值"计算。

```cpp
class BroadcastChannel {
 public:
  class Tx { /* 单写，wake 所有订阅者的 wait 字 */ };
  class Rx {
   public:
    std::expected<Message, RecvError> recv() noexcept;   // 可能返回 Lagged（有损模式）
    void release() noexcept;
  };
  Rx subscribe() noexcept;   // 动态订阅
};
```

---

## 6. Bulk（大消息通道）

消息 > 单帧上限（ring 容量的合理分数）时走此通道，**仍单帧连续、零重组**（对比 Aeron 的 FragmentAssembler）。

- 用**更大容量的独立 ring**（可配大页），消息 ≤ 该 ring 容量即单帧写入，消费者拿连续 span，无分片、无重组。
- 超过 bulk ring 容量的超大消息：默认**拒绝并提示扩容**；可选"引用 + 独立大页段"方案（把大 payload 放独立共享段，通道里只传引用/偏移），评估后再引入，不默认加分片。
- 与小消息通道**物理隔离**（不同 ring），避免大消息占满 ring 饿死小消息。

---

## 7. 通道配置（约定优于配置）

```cpp
struct ChannelConfig {
  std::size_t capacity = 1u << 20;         // 1 MiB 默认
  platform::HugePage huge = platform::HugePage::None;
  int numa_node = -1;
  bool fixed_size = false;                 // L2 无头定长模式
  std::size_t record_size = 0;             // fixed_size 时生效
  // 等待策略作为模板参数或 AnyWaitStrategy 注入
};
enum class ChannelError { Ok=0, BadConfig, PlatformFail, RingFail };
```
默认值开箱即用；三种预设模板：`p2p`（SPSC 小消息）、`fanout`（Broadcast）、`bulk`（大消息）。

---

## 8. 危险操作契约（SAFETY）

- MPSC 的 CAS 抢占后，`[tail, tail+need)` 归抢到者私有，其它生产者不得触碰——靠 CAS 独占 + 各写各区间保证。
- Broadcast 回收上界必须 = min(订阅者 head)，**任何时刻不得覆盖任一订阅者尚未读的区间**（可靠模式）；有损模式下覆盖必须让掉队者可检测（seq）。
- 通道生命周期 > 其 Tx/Rx；Tx/Rx 持有对通道内部 ring/位置的引用，不得悬垂。
- 危险仲裁逻辑集中在 `channel/mpsc.cpp`、`channel/broadcast.cpp`，附 `// SAFETY:`。

---

## 9. 测试清单

| 测试 | 方法 | 通过标准 |
|------|------|----------|
| SPSC 端到端 | 跨进程 fork，校验和 + 序号 | 全部有序、无丢、无重复、TSan 干净 |
| MPSC 多写 | N 生产者各发唯一序列 | 消费者收齐所有、无交叉损坏、按 position 有序可见 |
| MPSC 竞争交错 | 自建 harness 穷举提交顺序（含乱序完成） | 无空洞被越过、无未提交帧被读 |
| Broadcast 全量 | M 订阅者 | 每个都收全量、独立进度 |
| Broadcast 慢订阅 | 一个订阅者故意慢 | 可靠模式：生产者背压不丢；有损模式：慢者报 Lagged 并跳最新 |
| Bulk | MB 级消息 | 单帧连续、零重组、与小消息通道隔离 |
| 尾延迟 | 各通道 p50/p99/p99.9 | 达 `2-` 验收基线，MPSC 尾延迟可控 |

---

## 10. 验收标准

- 四种通道跨进程端到端正确（校验和/序号/有序性），TSan 零竞争。
- SPSC 达延迟/吞吐基线（对比 Aeron IPC）；MPSC 尾延迟可控且有 sharding 选型指引。
- Broadcast 可靠/有损两模式语义正确，慢订阅者可检测掉队。
- Bulk 大消息零重组、与小消息隔离。
- 三种预设模板开箱即用，默认配置无需调参即达标。

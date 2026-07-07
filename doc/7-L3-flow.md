# 7 - L3 流控与 Position 层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：用 64 位单调 position 表达"已发布/已消费"进度，实现零中介背压与 claim/commit 零拷贝写入，是 salias 保留 Aeron 优雅背压、去掉 Media Driver 的关键层。

**技术栈**：C++23。namespace `salias::flow`，目录 `core/flow/`。依赖 L1（`MagicRing`、原子原语）、L2（帧编解码）。绝不依赖 L4+。

层间契约（L3→L5）：`Producer::claim/commit`、`Consumer::read/advance` 是通道层唯一入口。

---

## 1. 职责与边界

**做：** 维护发布 position（tail）与消费 position（head）；背压判定（不经任何第三方轮询）；`claim/commit` 零拷贝写；`offer` 便捷写；批量 API。

**不做：** 不做等待/睡眠（返回"背压"由 L4 决定怎么等）；不认识多生产者仲裁（那是 L5 MPSC）；不做帧内容语义（L2 管编解码）。

---

## 2. Position 模型（保留 Aeron 精华，去掉中介）

- `position` 是 **64 位单调递增**计数，单位是字节（累计写入/读出量），永不回绕（64 位足够）。
- ring 内偏移 = `position & (cap-1)`（L1 掩码）。
- **可用空间** = `cap - (tail - head)`；**可读空间** = `tail - head`。
- Aeron 靠 Media Driver 的 Conductor 轮询更新 publisher limit；**salias 直连**：生产者直接 `load_acquire(head)` 算可用空间，零滞后、零第三方。

```cpp
namespace salias::flow {

// 位置字位于共享内存（L6 布局），此处持引用/指针，用 atomic_ref 访问。
struct Positions {
  std::uint64_t* producer;   // tail：生产者写、消费者读
  std::uint64_t* consumer;   // head：消费者写、生产者读
  std::size_t    cap;        // ring 容量（2 的幂）
};

enum class FlowError { Ok = 0, BackPressured, MessageTooLarge };

} // namespace salias::flow
```

---

## 3. Producer：claim / commit（零拷贝）

对标 Aeron `tryClaim`：在 ring 上**原地**拿到可写 span，用户直接构造消息，再 commit 发布。全程零拷贝。

```cpp
class Producer {
 public:
  Producer(ring::MagicRing& ring, Positions pos) noexcept;

  // 预留 payload_len 字节（含头与对齐）。成功返回可写 span（跳过头，指向 payload）。
  // 失败：空间不足 -> BackPressured；超容量 -> MessageTooLarge。
  std::expected<Claim, FlowError> claim(std::uint32_t payload_len) noexcept;

  // 提交上一次 claim：写入帧头 + store_release(tail)，使消费者可见。
  void commit(const Claim&) noexcept;

  // 便捷：已有 buffer 时一步 offer（内部 claim+memcpy+commit）。
  std::expected<void, FlowError> offer(std::span<const std::byte>) noexcept;

 private:
  ring::MagicRing* ring_;
  Positions pos_;
  std::uint64_t cached_head_ = 0;   // 缓存 head，减少跨核读（见下）
};

struct Claim {
  std::span<std::byte> payload;   // 用户就地写这里
  std::uint64_t start_pos;        // 本帧起始 position
  std::uint32_t payload_len;
  std::uint32_t meta;
};
```

### claim 流程
```
need = frame_len(payload_len)            // L2：头 + 对齐 payload
if need > cap  -> MessageTooLarge         // 不分片，交 L5 bulk
tail = *producer (本进程私有，单生产者可非原子读)
// 用缓存的 head 先判；不够再真读一次 head（减少跨核 acquire）
if cap - (tail - cached_head_) < need:
    cached_head_ = load_acquire(*consumer)
    if cap - (tail - cached_head_) < need -> BackPressured
slot = ring.slice_mut(tail + kHeaderSize, payload_len)   // 连续（双映射）
return Claim{ slot, tail, payload_len, meta_with(BEGIN|END, seq) }
```

### commit 流程
```
encode_header(ring.slice_mut(claim.start_pos, kHeaderSize),
              claim.payload_len, claim.meta | FLAG_COMMITTED)
store_release(*producer, claim.start_pos + frame_len(claim.payload_len))
// release 保证：payload 写 + 头写，都 happens-before 消费者的 acquire。
```

**cached_head 优化**：生产者大多数时候空间充足，无需每次跨核读 head（那会引发 cache line 争用）。只有本地缓存判定不足时才真 `acquire` 读一次，读到就更新缓存。这是 Disruptor/Aeron 都用的经典手法，显著降低跨核流量。

---

## 4. Consumer：read / advance

```cpp
class Consumer {
 public:
  Consumer(const ring::MagicRing& ring, Positions pos) noexcept;

  // 返回下一条可读消息（若有）。不移动 head。
  // 无新消息返回 std::nullopt（由 L4 决定等待）。
  std::optional<Message> poll() noexcept;

  // 消费完（一条或一批）后推进 head，释放空间给生产者。
  void advance(std::uint64_t new_head) noexcept;

  // 批量：一次拿回连续的多条消息，减少 head 更新次数。
  std::size_t poll_batch(std::span<Message> out) noexcept;

 private:
  const ring::MagicRing* ring_;
  Positions pos_;
  std::uint64_t cached_tail_ = 0;
};

struct Message {
  std::span<const std::byte> payload;
  std::uint64_t position;   // 本消息起始 position
  std::uint32_t meta;
};
```

### poll 流程
```
head = *consumer (本进程私有)
if head == cached_tail_:
    cached_tail_ = load_acquire(*producer)   // 跨核 acquire，配对生产者 release
    if head == cached_tail_ -> nullopt        // 确无新消息
hdr = decode_header(ring.slice(head, kHeaderSize))
if !(flags(hdr) & FLAG_PADDING):
    payload = ring.slice(head + kHeaderSize, hdr.len)   // 连续（双映射）
    return Message{ payload, head, hdr.meta }
else:
    head += frame_len(0); retry   // 跳过 padding
```

`advance`：`store_release(*consumer, new_head)`，让生产者的 `load_acquire(head)` 看到空间释放。

**批量消费**：`poll_batch` 连续解多条、只在最后 `advance` 一次——把 head 更新的跨核开销摊薄到一批，吞吐显著提升。

---

## 5. 背压语义（零中介的核心卖点）

| 状态 | 判定 | 返回 |
|------|------|------|
| 可写 | `cap - (tail-head) >= need` | `Claim` |
| 背压 | 空间不足 | `FlowError::BackPressured`（**不丢数据、不无限缓冲**） |
| 超限 | `need > cap` | `MessageTooLarge`（交 bulk） |

- 生产者拿到 `BackPressured` 后，**怎么等由 L4 决定**（spin/yield/futex）——L3 只报状态，不阻塞。这层解耦让"背压检测"与"等待策略"正交。
- 对比 Aeron：publisher limit 由 Conductor 周期更新，解背压有滞后；salias 生产者直接读 head，消费者一 `advance`，生产者下次 `claim` 立刻看到，**零滞后**。

---

## 6. 批量 API

- `claim_batch(count, each_len)`：一次预留多帧，减少多次空间判定的跨核读。
- `poll_batch`：见上。
- 批量的价值全在**摊薄跨核同步**：position 的 acquire/release 是跨核可见性点，批量把 N 次降到 1 次。

---

## 7. 危险操作契约（SAFETY）

- **position 访问**：生产者读自己的 tail、消费者读自己的 head 可非原子（单写者私有）；**读对方的位置必须 `atomic_ref` + acquire**，写自己的位置必须 `atomic_ref` + release。
- **claim 未 commit 的区间**：属于生产者私有，消费者的 head 不会越过未发布的 tail，故不会读到半成品——**前提是 commit 用 release 且消费者用 acquire**。
- **单生产者假设**：L3 的 `Producer` 是 SPSC 语义（tail 单写）。多生产者仲裁在 L5 用 CAS 实现，不在此层。
- 每处 `store_release`/`load_acquire` 附 `// SAFETY:` 说明它在 happens-before 链中的角色。

---

## 8. 测试清单

| 测试 | 方法 | 通过标准 |
|------|------|----------|
| position 单调 | 长跑发布，记录 tail 序列 | 严格单调递增，无回退 |
| 背压触发 | 慢消费者填满 ring | `claim` 返回 `BackPressured`，无越界写、无覆盖未消费数据 |
| 解背压零滞后 | 消费者 advance 后立即 claim | 同一轮即成功，无需外部轮询 |
| claim/commit 顺序 | payload 带校验和 + seq | 消费端校验和全对、无撕裂（TSan 干净） |
| 批量正确 | poll_batch vs 逐条 | 结果一致，head 更新次数下降 |
| 超限 | claim(cap+1) | `MessageTooLarge`，不分片 |
| 交错 | 自建 harness 穷举 tail/head interleaving（小容量） | 所有交错不变量成立 |

---

## 9. 验收标准

- SPSC 端到端 payload 校验和 100% 正确，TSan 零数据竞争。
- 背压语义正确：满时不覆盖未消费数据、不丢、不无限缓冲。
- 解背压零滞后（消费 advance 后生产者下一次 claim 即见空间）——写入性能报告对比 Aeron Conductor 轮询滞后。
- cached_head/cached_tail 优化使稳态跨核读次数显著低于"每次都读对方位置"（perf 计数验证）。
- 批量 API 在高吞吐下 head/tail 更新次数按批下降。

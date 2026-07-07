# 5 - L1 环与内存原语层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：在 L0 的双映射 `Mapping` 之上，提供*无 term 轮转、无清零、无分片*的 magic ring 与缓存行隔离原语，让上层把物理内存当作一条首尾相接的连续字节流读写。

**技术栈**：C++23。namespace `salias::ring`，目录 `core/ring/`。依赖 L0（`salias::platform::Mapping`），绝不 `#include` L2+。

---

## 1. 职责与边界

**做：**
- 封装 `MagicRing`：容量 2 的幂，任意逻辑偏移都能返回**连续**切片（跨尾回绕由 L0 双映射兜底）。
- 提供 `CacheAligned<T>`：把 head/tail/limit 各自放独立缓存行，消除 false sharing。
- 提供对共享内存 64 位位置的原子访问原语（`std::atomic_ref<uint64_t>` 封装），并规定内存序。

**不做：**
- 不认识"帧"（L2）、不认识"背压/claim"（L3）、不认识"谁是生产者/消费者"（L5）。
- 不做分片：消息 > 容量直接返回错误，bulk 由 L5 处理。
- 不管等待策略（L4）。

---

## 2. 为什么用 magic ring（相对 Aeron 的核心优势）

Aeron 用 3-term 轮转：跨 term 写 padding、切 term、并对回收的 term **清零 64MB**——这是 p99.9 抖动的大来源，且吃内存带宽。

salias 用双映射：同一段物理内存连续映射两次（L0 保证）。逻辑偏移 `pos` 落在 `[0, cap)`，物理访问用 `base + (pos & (cap-1))`。**即使读写跨越 `cap` 边界，因为第二段映射紧邻且是同一物理页，返回的指针 + 长度天然连续**——无需分两段拷贝、无需 padding、无需清零、无需轮转。

---

## 3. `MagicRing` 结构与接口

```cpp
namespace salias::ring {

enum class RingError { Ok = 0, NotPowerOfTwo, TooLarge, ZeroLen };

class MagicRing {
 public:
  // cap 必须 2 的幂；内部持有 L0 Mapping（其 len()==cap）。
  static std::expected<MagicRing, RingError>
  create(platform::Mapping mapping) noexcept;

  std::size_t capacity() const noexcept { return cap_; }
  std::size_t mask()     const noexcept { return cap_ - 1; }  // 2 的幂 -> 位掩码

  // 返回 [pos, pos+len) 对应的连续可读切片。
  // 前置：len <= cap。跨边界也连续（双映射）。
  // SAFETY: 调用方负责保证该区间已被生产者以 release 发布，且未被回收覆盖。
  std::span<const std::byte> slice(std::uint64_t pos, std::size_t len) const noexcept {
    return { base_ + (pos & mask()), len };
  }

  // 可写切片：用于生产者原地构造消息（配合 L3 claim/commit）。
  // SAFETY: 调用方保证该区间是自己预留的、未发布给消费者、不与消费者读区重叠。
  std::span<std::byte> slice_mut(std::uint64_t pos, std::size_t len) noexcept {
    return { base_ + (pos & mask()), len };
  }

 private:
  std::byte*  base_ = nullptr;  // = mapping_.as_ptr()
  std::size_t cap_  = 0;
  platform::Mapping mapping_;   // 持有以保证 base_ 存活
};

} // namespace salias::ring
```

不变量：
- `cap_` 是 2 的幂 —— 偏移用 `pos & (cap_-1)`，**无除法/取模**。
- `mapping_.len() == cap_`，且 `[base, base+cap)` 与 `[base+cap, base+2*cap)` 同物理页（L0 契约）。
- `slice`/`slice_mut` 对 `len <= cap` 的任意 `pos` 返回连续 span —— 这是 L1→L3 的核心契约。

**为什么跨边界不用拷贝**：设 `off = pos & mask()`，若 `off + len > cap`，普通环形缓冲要拆成 `[off, cap)` + `[0, len-(cap-off))` 两段。这里因为 `base+cap` 起是同一物理页的第二份映射，`base+off` 往后 `len` 字节在虚拟地址上连续有效，直接一个 span 覆盖，读写都不分段。

---

## 4. 缓存行隔离 `CacheAligned<T>`

```cpp
namespace salias::ring {

// 用硬件缓存行大小对齐，避免相邻变量 false sharing。
// C++17 起有 std::hardware_destructive_interference_size（通常 64；
// 某些平台预取相邻行，用 128 更稳）。
inline constexpr std::size_t kCacheLine =
    std::hardware_destructive_interference_size;  // 回退 64

template <class T>
struct alignas(kCacheLine) CacheAligned {
  T value;
  // 填充到整缓存行，杜绝与后续字段共享行。
  std::byte _pad[kCacheLine - (sizeof(T) % kCacheLine)];
};

} // namespace salias::ring
```

用法与读写分离原则：
- `producer_pos`（tail）、`consumer_pos`（head）、`limit` 各自 `CacheAligned`，分处独立缓存行。
- **生产者只写 tail、只读 head**；**消费者只写 head、只读 tail**。写者独占其缓存行，避免跨核 invalidate 风暴（false sharing）。
- 这些位置字位于**共享内存**（L0 Mapping 的元数据区或独立小映射），故用 `std::atomic_ref` 访问（见下）。

---

## 5. 共享内存上的原子位置

跨进程共享内存里的 64 位位置，用 `std::atomic_ref<std::uint64_t>` 包裹裸字段访问。**不能用普通 `std::atomic<uint64_t>` 成员**，因为该字段的存储位于 L0 映射出的共享内存、由布局（L6）固定，不是我们 new 出来的对象——`atomic_ref` 正是为"对已存在对象施加原子操作"设计。

```cpp
// 位置字定义在共享内存布局里（L6），此处仅示意访问方式。
inline std::uint64_t load_acquire(const std::uint64_t& cell) noexcept {
  return std::atomic_ref<const std::uint64_t>(cell).load(std::memory_order_acquire);
}
inline void store_release(std::uint64_t& cell, std::uint64_t v) noexcept {
  std::atomic_ref<std::uint64_t>(cell).store(v, std::memory_order_release);
}
```

### 内存序（happens-before 链，SPSC 为例）
- **生产者**：先把消息字节写入 `slice_mut`（普通写），**再** `store_release(tail, new_pos)`。release 保证"消息字节写"不会重排到 tail 发布之后。
- **消费者**：先 `load_acquire(tail)` 看到新 `pos`，**再**读 `slice(old..pos)`。acquire 与生产者的 release 配对，保证消费者能看到生产者写入的消息字节。
- **回收**：消费者 `store_release(head, consumed_pos)`；生产者 `load_acquire(head)` 判断可覆盖空间。
- `Relaxed` 仅用于不构成 happens-before 的统计计数（如背压计数），且这些归 L6。

> 关键认知：跨进程共享内存下，编译器/CPU 的重排与另一进程的可见性由**内存序**保证，C++ 类型系统对此无能为力——必须显式、正确地选 `acquire`/`release`。这是 L1 最容易出错、最需 TSan + 交错测试覆盖的地方。

---

## 6. 无分片保证

- L1 只接受 `len <= cap` 的消息，单帧连续写入。
- `len > cap`：`slice_mut` 的调用方（L3/L5）须在更上层拦截；L1 提供 `fits(len)` 辅助判断，超限返回 `RingError::TooLarge`，**绝不静默分片**。
- 大消息（> 单 ring 容量）的策略是 L5 的 bulk 通道职责，不污染 L1。

---

## 7. 危险操作契约（SAFETY）

- **别名**：同一物理页被 `base` 与 `base+cap` 两个虚拟地址别名。禁止长期持有会触发 UB 假设的引用；用裸指针 + span，读写位置字一律走 `atomic_ref`。
- **span 生命周期**：`slice`/`slice_mut` 返回的 span 有效期 = `MagicRing` 存活期，且**仅在对应 position 协议（release/acquire、未被回收）成立时内容有效**。L3 负责协议，L1 只保证地址连续。
- **对齐**：位置字 8 字节对齐；`slice_mut` 起始不保证对齐（payload 可任意），需对齐的定长模式由 L2 处理。
- **cap 校验**：`create` 断言 2 的幂且非 0，否则返回 `RingError`。

---

## 8. 测试清单

| 测试 | 方法 | 通过标准 |
|------|------|----------|
| 回绕连续 | 在 `pos = cap-4` 写 16 字节，`slice` 读回 | 16 字节连续且正确 |
| 满/空边界 | tail==head（空）、tail-head==cap（满） | 判定正确，无越界 |
| 2 的幂断言 | `create(cap=3)` | 返回 `NotPowerOfTwo` |
| 掩码正确 | 遍历 pos，比对 `pos & mask` 与 `pos % cap` | 全等 |
| SPSC 内存序 | 生产/消费线程压测，payload 带校验和 | TSan 无告警；无撕裂/错序 |
| 交错穷举 | 自建 harness 枚举 head/tail 关键 interleaving（小容量、少消息） | 所有交错下不变量成立 |

**工具与局限**：TSan 抓数据竞争、ASan/UBSan 抓越界。C++ 无 Rust `loom`/`miri`——用**自建并发交错测试**（受控线程 + 内存屏障桩，穷举小规模 interleaving）近似替代，并辅以长期压测统计撕裂率=0。原子逻辑可先在普通堆内存上单测（不需真 mmap），真双映射走集成测试。

---

## 9. 验收标准

- 任意 `pos`、`len <= cap` 下 `slice`/`slice_mut` 返回连续正确内存，含跨边界。
- 偏移计算零除法/取模（反汇编确认为位运算）。
- head/tail/limit 分处独立缓存行（`offsetof` 校验间距 >= kCacheLine）。
- SPSC 压测 + TSan 零数据竞争、零撕裂；交错测试所有 interleaving 不变量成立。
- `len > cap` 一律 `TooLarge`，无静默分片。
- 无 term 轮转/清零代码路径（对比 Aeron，消除该类抖动源）。

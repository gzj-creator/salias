# 10 - L6 可观测性层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：把所有位置、指标、错误计数放进**共享内存里的固定布局**，让外部工具零侵入、只读地观测运行中的通道——对标并借鉴 Aeron 的 counters 体系。

**技术栈**：C++20。namespace `salias::metrics`，目录 `core/metrics/`。被 L1/L3/L5 写入（通过接口注入，避免反向依赖），供 L7 与外部工具读取。

---

## 1. 职责与边界

**做：** 定义共享内存计数器区的固定二进制布局（ABI）；提供写入端（低开销 `atomic_ref` 递增）与只读观测端；保证布局稳定可被独立进程 `mmap` 解析。

**不做：** 不做 UI（`salias-top` 是可选工具，非本层）；不参与数据面收发；不定义业务指标（只提供通用计数槽 + 命名）。

---

## 2. 为什么放共享内存（借鉴 Aeron）

Aeron 把所有计数器放共享内存文件，运维工具（`AeronStat`）无需侵入应用即可读位置、错误、流控状态。salias 沿用：**指标即共享内存的一段固定布局**，任何进程只读 `mmap` 就能观测，零锁、零 IPC 往返、对被观测方零开销（只多几条 `atomic_ref` 递增）。

---

## 3. 计数器区布局（ABI，只增不改）

```cpp
namespace salias::metrics {

inline constexpr std::uint32_t kMagic   = 0x53414C31;  // "SAL1"
inline constexpr std::uint32_t kVersion = 1;

// 文件头：定长，位于共享内存起始。
struct alignas(64) MetaHeader {
  std::uint32_t magic;        // = kMagic，校验
  std::uint32_t version;      // = kVersion，兼容性判断
  std::uint32_t counter_count;
  std::uint32_t slot_stride;  // 每个 counter slot 字节数（= 64，缓存行）
  std::uint64_t created_unix_nanos;
  std::byte     _pad[64 - 24];
};

// 单个计数器 slot：独占缓存行，避免写入方之间 false sharing。
struct alignas(64) CounterSlot {
  std::uint64_t value;                 // atomic_ref 访问
  std::uint32_t type_id;               // 见 CounterType
  std::uint32_t owner_channel;         // 归属通道 id
  char          label[64 - 16];        // 人类可读名，如 "producer_pos"
};
static_assert(sizeof(CounterSlot) == 64);

enum class CounterType : std::uint32_t {
  ProducerPos = 1,   // 发布 position（也是 L3 的 tail 真身所在）
  ConsumerPos = 2,   // 消费 position
  BackPressureCount = 3,
  ErrorCount = 4,
  BytesPublished = 5,
  MessagesPublished = 6,
  LagBytes = 7,      // tail - min(consumer)
  WakeSyscalls = 8,  // futex wake 次数（能耗观测）
};

} // namespace salias::metrics
```

布局规则（ABI 稳定性）：
- **只能追加新 slot / 新 `CounterType`，不能重排或改动已有字段**——外部工具依赖固定 offset。
- `version` 递增表示不兼容变更；工具先校验 `magic`+`version`。
- 每个 slot 独占 64 字节缓存行：不同写入者（不同通道/线程）互不 false sharing。

---

## 4. position 的真身放这里

关键设计：L3 用的 `producer_pos`/`consumer_pos` **本身就存在计数器区的 `CounterSlot::value` 里**（`ProducerPos`/`ConsumerPos` 类型）。这样：
- 数据面正常读写 position（release/acquire）。
- 观测端同一块内存只读读到实时 position，**无需额外同步或复制**。

即"位置"与"指标"是同一份内存的两个视角——省掉指标聚合开销，也保证观测值绝对实时（就是数据面在用的值）。

---

## 5. 写入端（对被观测方近零开销）

```cpp
class Counters {                       // 由 L5 通道持有，写入自己的 slot
 public:
  static std::expected<Counters, MetricsError>
  create_in(std::span<std::byte> region, std::uint32_t count);   // 在 L0 映射的区上初始化布局

  // 热路径递增：Relaxed 足够（指标不构成 happens-before）。
  void incr(std::uint32_t slot, std::uint64_t by = 1) noexcept {
    std::atomic_ref<std::uint64_t>(slots_[slot].value)
        .fetch_add(by, std::memory_order_relaxed);
  }
  // position 型 slot 直接 store（release，由 L3 语义决定）。
  void set_release(std::uint32_t slot, std::uint64_t v) noexcept {
    std::atomic_ref<std::uint64_t>(slots_[slot].value)
        .store(v, std::memory_order_release);
  }
 private:
  MetaHeader*  hdr_;
  CounterSlot* slots_;
};
```
- 普通指标用 `Relaxed`（不参与数据可见性链），开销极小。
- position 型用 `release`（它同时是数据面同步点）。

---

## 6. 只读观测端

```cpp
class CountersReader {                 // 独立进程用，只读
 public:
  static std::expected<CountersReader, MetricsError>
  open(std::string_view shm_path);     // 只读 mmap 共享内存

  bool valid() const noexcept;         // 校验 magic/version
  std::uint64_t value(std::uint32_t slot) const noexcept {
    return std::atomic_ref<const std::uint64_t>(slots_[slot].value)
        .load(std::memory_order_acquire);
  }
  std::span<const CounterSlot> slots() const noexcept;
};
```
- 观测端**只读**，不写、不加锁，对被观测方零干扰。
- 读到的可能是瞬时值（无事务），但对监控足够；需要一致快照时可读两次比对 `producer_pos` 判断是否跨越。

可选工具 `salias-top`：周期读 `CountersReader`，展示每通道吞吐/滞后/背压/能耗（wake syscalls）——类 `AeronStat`。属工具，不属本层核心。

---

## 7. 危险操作契约（SAFETY）

- 计数器区位于 L0 `MAP_SHARED` 内存，多进程共享；所有访问走 `atomic_ref`。
- `create_in` 必须校验 region 足够容纳 `MetaHeader + count*64`，否则 `MetricsError::RegionTooSmall`。
- 布局 `static_assert` 锁定 slot=64 字节；任何字段变更需过"ABI 审查"（只增不改）。
- 观测端信任边界：读到的 `label`/值来自其它进程，工具侧要防越界解析（把它当**数据**，做长度/范围校验）。

---

## 8. 测试清单

| 测试 | 方法 | 通过标准 |
|------|------|----------|
| 跨进程只读 | 进程 A 写、进程 B `CountersReader` 读 | B 读到实时值 |
| 布局稳定 | 对 `MetaHeader`/`CounterSlot` 做快照测试 | offset/size 不变（回归即失败） |
| ABI 校验 | 篡改 magic/version | `valid()` 返回 false，不解析 |
| 写入开销 | 热路径 incr 的纳秒开销 | 可忽略（相对消息处理） |
| 无 false sharing | 多通道并发写各自 slot | slot 独占缓存行（`offsetof` 校验），无争用退化 |
| 越界防护 | fuzz 一个损坏的计数器文件给 reader | 不 crash、拒绝解析 |

---

## 9. 验收标准

- 独立进程可只读观测所有通道的 position/吞吐/滞后/背压/能耗，零侵入、零锁。
- 布局 ABI 稳定（快照测试守护），只增不改。
- 热路径指标写入开销可忽略；position 与指标共用同一内存无重复聚合。
- slot 缓存行独占，多写者无 false sharing。
- reader 对损坏输入健壮（fuzz 通过）。

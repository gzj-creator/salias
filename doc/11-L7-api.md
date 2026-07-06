# 11 - L7 公共 API 与 Async 适配层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：面向最终用户的门面——driverless 建立通道的握手、简洁的 `Publisher`/`Subscriber` API，以及**可选**的 async 适配层。这是 v0.1 冻结的稳定 ABI/API。

**技术栈**：C++20。namespace `salias`（顶层），目录 `salias/`。依赖 L5（通道）、L6（可观测）。核心不用协程；async 仅此层可选 feature。

---

## 1. 职责与边界

**做：** driverless 通道建立/连接握手；用户可见的 `Channel`/`Publisher`/`Subscriber`；错误与生命周期管理；可选 async 适配（默认关）。

**不做：** 不碰裸内存序（下沉在 L1/L3）、不做仲裁（L5）、不做网络（项目根本不做）。

---

## 2. Driverless 握手（相对 Aeron 的核心简化）

Aeron 必须先跑 Media Driver 进程，客户端经 CnC 文件与之握手才能建通道。salias **无 Driver**：建立通道 = 两端 `mmap` 同一个共享内存对象 + 一次轻量元数据握手。

### 握手协议
```
建立方（owner）：
  1. 按 name 创建共享内存对象（memfd + pin 到 /dev/shm/salias/<name>，或具名 shm_open）
  2. 写入 ChannelMeta（容量、模式、record_size、counters 偏移、协议版本）到区首
  3. store_release(ready_flag = 1)

连接方（peer）：
  1. 按同名打开共享内存对象（轮询/inotify 等待存在）
  2. load_acquire(ready_flag) == 1 后读 ChannelMeta，校验 magic/version/容量
  3. 映射 ring 与 counters，构造本端 Publisher/Subscriber
```

- **无第三方**：流控由两端 position 直读（L3），握手只做一次性元数据交换。
- `ChannelMeta` 布局固定（含 magic/version），与 L6 counters 一致遵循"只增不改"。
- 进程内（同进程多线程）场景：跳过 shm，直接共享同一 `MagicRing` 对象，握手退化为构造函数传递。

```cpp
namespace salias {

struct ChannelMeta {           // 共享内存区首，握手用
  std::uint32_t magic;         // "SAL1"
  std::uint32_t version;
  std::uint32_t mode;          // Spsc/Mpsc/Broadcast/Bulk
  std::uint32_t flags;         // fixed_size 等
  std::uint64_t capacity;
  std::uint64_t record_size;   // fixed_size 时
  std::uint64_t counters_off;  // counters 区相对偏移
  std::uint32_t ready;         // release/acquire 握手位
};

enum class Mode { Spsc, Mpsc, Broadcast, Bulk };

} // namespace salias
```

---

## 3. 用户 API（门面）

```cpp
namespace salias {

struct Config {                        // 约定优于配置：全有默认值
  std::string  name;                   // 通道名（进程间用；进程内可空）
  Mode         mode = Mode::Spsc;
  std::size_t  capacity = 1u << 20;    // 1 MiB
  HugePage     huge = HugePage::None;
  int          numa_node = -1;
  bool         fixed_size = false;
  std::size_t  record_size = 0;
  WaitKind     wait = WaitKind::SpinPause;   // 运行时选策略（内部 AnyWaitStrategy）
};

class Channel {
 public:
  // 建立（owner 端，创建共享内存）
  static std::expected<Channel, Error> create(const Config&);
  // 连接（peer 端，打开已存在的）
  static std::expected<Channel, Error> connect(std::string_view name);

  Publisher  publisher();     // 取发布端（SPSC/Bulk 唯一；MPSC 可多次取）
  Subscriber subscriber();    // 取订阅端（Broadcast 可多次取 = 多订阅）

  CountersReader counters() const;   // L6 只读观测句柄
};

class Publisher {
 public:
  // 零拷贝：拿 ring 上的可写区，就地构造，再 commit。
  std::expected<Claim, Error> try_claim(std::uint32_t len) noexcept;
  void commit(const Claim&) noexcept;
  // 便捷：已有 buffer。返回是否成功（背压时 false / Error）。
  std::expected<bool, Error> offer(std::span<const std::byte>) noexcept;
};

class Subscriber {
 public:
  std::optional<Message> try_recv() noexcept;    // 非阻塞
  Message recv() noexcept;                        // 阻塞（按 wait 策略）
  std::size_t poll(FunctionRef<void(Message)> handler, std::size_t max) noexcept;  // 批量回调
};

enum class Error { Ok=0, NotFound, VersionMismatch, BackPressured,
                   MessageTooLarge, Lagged, PlatformFail, BadConfig };

} // namespace salias
```

设计：
- `try_claim`/`commit` 是零拷贝主路（对标 Aeron `tryClaim`）；`offer` 是便捷路。
- `poll(handler, max)` 是 Aeron 风格的批量回调消费，摊薄 head 更新。
- 所有失败显式 `std::expected<_, Error>`，不抛异常做控制流。
- 三个预设工厂：`Channel::p2p(name)`、`Channel::fanout(name)`、`Channel::bulk(name)`——一行建通道。

---

## 4. 可选 Async 适配层（feature flag，默认关）

**动机**：核心是 busy-spin/futex（见项目决策，核心不用协程）。但用户的应用可能本身是 async 的（如 tokio 风格的 C++ 协程框架 / asio），希望 `co_await sub.recv()` 融入其事件循环，而不是独占线程 spin。

**方案**：薄适配层，把 futex 等待桥接到用户的 executor。**不引入自己的 runtime**。

```cpp
#ifdef SALIAS_ASYNC     // 编译期 feature，默认未定义
namespace salias::async {

// 返回一个 awaitable：底层仍是共享内存 + futex，
// 但"等待"通过把 futex fd（FUTEX_FD / eventfd 桥）注册到用户 executor 完成，
// 不阻塞线程、不 spin。
Awaitable<Message> recv(Subscriber&);

} // namespace salias::async
#endif
```

- 实现要点：用 `eventfd` + futex 唤醒桥接，或 io_uring 的 futex 等待（较新内核），把"消息就绪"变成用户 executor 可 poll 的事件源。
- **明确取舍**：async 路径延迟高于 busy-spin（多一次 executor 唤醒），仅为集成便利，不是低延迟路径。文档警示：追求极致延迟请用同步 `recv()` + 独占核。
- 默认 `SALIAS_ASYNC` 关闭，核心库零 async 依赖、零 runtime。

---

## 5. 生命周期与所有权

- `Channel` 拥有共享内存映射与 counters；`Publisher`/`Subscriber` 借用其内部，**不得比 `Channel` 活得久**（用引用/句柄，文档强约束；可选用 shared 内核对象引用计数防误用）。
- owner 端销毁通道：解除映射、`unlink` 共享内存对象；peer 端读到 `ready=0`/对象消失时得到 `NotFound`，优雅退出。
- 崩溃安全：owner 崩溃后共享内存对象残留，提供 `salias-clean` 工具或启动时按约定清理 `/dev/shm/salias/*`（带 pid 存活检查）。

---

## 6. 危险操作契约（SAFETY）

- 握手 `ready` 位必须 release（owner 写）/acquire（peer 读），保证 peer 看到 `ready=1` 时 `ChannelMeta` 全部字段已写完。
- peer 读到的 `ChannelMeta` 来自另一进程，是**不可信输入**：校验 magic/version/capacity 上下界，非法即 `VersionMismatch`/`BadConfig`，绝不据其分配巨额内存或越界。
- `Publisher`/`Subscriber` 借用 `Channel` 内部指针，生命周期由文档+可选引用计数守护，禁止悬垂。

---

## 7. 测试清单

| 测试 | 方法 | 通过标准 |
|------|------|----------|
| 进程间握手 | owner create + peer connect | 双方建立、元数据一致、收发正常 |
| 握手竞态 | peer 先于 owner 启动 | peer 正确等待 ready，不读到半成品 meta |
| 版本不匹配 | 篡改 version | `connect` 返回 `VersionMismatch`，不 crash |
| 不可信 meta | fuzz `ChannelMeta` | 全部拒绝、无越界/巨额分配 |
| 崩溃清理 | kill owner 后重启 | 残留 shm 被安全清理（pid 存活检查） |
| API 示例即测试 | 文档中每段示例编成测试 | 全部编译+通过 |
| async 适配（开 feature） | 在 asio/协程框架里 co_await recv | 正确收消息、不阻塞 executor |
| 零拷贝路径 | try_claim/commit | 无中间 memcpy（反汇编/perf 确认） |

---

## 8. 验收标准

- driverless 握手在进程间可靠建立，无需任何后台 Driver 进程。
- 握手竞态、版本不匹配、损坏 meta 全部安全处理（不可信输入零信任）。
- 用户 API 简洁：一行工厂建通道，零拷贝主路 + 便捷 offer + 批量 poll 齐备。
- 崩溃后共享内存可安全清理。
- async 适配层作为可选 feature 默认关闭，核心库无 runtime 依赖；开启后能融入用户 executor。
- v0.1 API 冻结：`Channel`/`Publisher`/`Subscriber`/`Error`/`Config` 稳定，后续只增不破坏。

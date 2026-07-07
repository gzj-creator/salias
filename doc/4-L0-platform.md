# 4 - L0 平台抽象层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：把"双映射共享内存 / 大页 / NUMA / 跨进程 futex"封装成 C++23 抽象，向上提供 `Mapping` 与 `Futex`。仅 Linux。

**技术栈**：C++23 / CMake + vcpkg / 仅 Linux。namespace `salias::platform`，目录 `core/platform/`。

---

## 1. 职责与边界

**做：**
- 用 `memfd_create` + 双 `mmap` 造出"同一物理页映射到两段相邻虚拟地址"的 `Mapping`（magic ring 的地基）。
- 可选大页（`MAP_HUGETLB`）、可选 NUMA 绑定（libnuma / `mbind`）。
- 封装跨进程 `Futex`（`FUTEX_WAIT`/`FUTEX_WAKE` 作用于共享内存上的 32 位字）。
- 把所有 syscall 失败转成显式 `PlatformError`（不抛裸 errno）。

**不做：**
- 不认识"帧""position""通道"——这些是 L2+ 的概念。L0 只交付连续内存与等待原语。
- 不做跨平台。macOS/Windows 明确不在范围内（见技术栈决策）。
- 不管谁读谁写、内存序如何——那是 L1/L3 的事。

---

## 2. 核心数据结构

```cpp
namespace salias::platform {

enum class PlatformError {
  Ok = 0,
  MemfdCreateFailed,
  FtruncateFailed,
  ReserveFailed,      // 预留连续地址失败
  MapFixedFailed,     // MAP_FIXED 双映射失败
  UnmapFailed,
  HugePageUnavailable,
  NumaUnavailable,
  FutexFailed,
  InvalidSize,        // 非 2 的幂 / 非页对齐 / 溢出
};

enum class HugePage { None, Size2MB, Size1GB };

struct MapOptions {
  std::size_t size;              // ring 逻辑容量，必须 2 的幂且页对齐
  HugePage huge = HugePage::None;
  int numa_node = -1;            // -1 表示不绑定
};

// 双映射句柄：RAII，持有 fd 与基址；析构负责按序 unmap + close。
// 不变量：[base, base+len) 与 [base+len, base+2*len) 指向同一物理页。
class Mapping {
 public:
  static std::expected<Mapping, PlatformError> create(const MapOptions&);

  std::byte*  as_ptr() const noexcept { return base_; }   // 契约方法
  std::size_t len()    const noexcept { return len_; }    // 逻辑容量（单段）

  Mapping(Mapping&&) noexcept;                 // 可移动
  Mapping& operator=(Mapping&&) noexcept;
  Mapping(const Mapping&) = delete;            // 不可拷贝（持有 fd）
  ~Mapping();                                  // 按序释放

 private:
  std::byte*  base_ = nullptr;  // 两段的起点，实际映射长度 2*len_
  std::size_t len_  = 0;
  int         fd_   = -1;       // memfd
};

// 跨进程等待字：不拥有内存，指向共享内存里的一个 32 位对齐字。
class Futex {
 public:
  explicit Futex(std::uint32_t* word) noexcept : word_(word) {}

  // 若 *word == expected 则睡眠，直到被 wake 或值变化；带可选超时。
  PlatformError wait(std::uint32_t expected,
                     std::optional<std::chrono::nanoseconds> timeout = std::nullopt) noexcept;
  int wake_one() noexcept;   // 返回被唤醒的等待者数
  int wake_all() noexcept;

 private:
  std::uint32_t* word_;  // 必须 4 字节对齐、位于 MAP_SHARED 内存
};

} // namespace salias::platform
```

设计要点：
- 可失败构造用 `std::expected`，**不用异常做控制流**——保持显式错误处理，与仓库规范一致。
- `Mapping` 不可拷贝、可移动，RAII 保证 fd 与映射不泄漏、不重复释放（immutable-first：句柄建立后不再改字段）。

---

## 3. 双映射实现（Linux）

目标：让 `[base, base+size)` 和 `[base+size, base+2*size)` 落在同一物理页。这样 ring 里任何"逻辑上跨越尾部回绕"的读写，在虚拟地址上都是连续的，免 padding、免分片、免清零。

### syscall 顺序

```
1. fd = memfd_create("salias-ring", MFD_CLOEXEC [| MFD_HUGETLB]);
      失败 -> PlatformError::MemfdCreateFailed
2. ftruncate(fd, size);                 // 物理后备设为一个 size
      失败 -> FtruncateFailed（回滚：close fd）
3. base = mmap(nullptr, 2*size,         // 预留 2*size 连续地址
               PROT_NONE,
               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      失败 -> ReserveFailed（回滚：close fd）
4. mmap(base,        size, PROT_READ|PROT_WRITE,
        MAP_SHARED | MAP_FIXED, fd, 0);         // 第一段
      失败 -> MapFixedFailed（回滚：munmap(base,2*size); close fd）
5. mmap(base+size,   size, PROT_READ|PROT_WRITE,
        MAP_SHARED | MAP_FIXED, fd, 0);         // 第二段，同 offset 0
      失败 -> MapFixedFailed（回滚：munmap(base,2*size); close fd）
6. 自检：base[0]=0x5A; 断言 (base+size)[0]==0x5A; 复位。
```

关键点：
- 先用 `PROT_NONE` 匿名映射**占坑**拿到 2*size 连续地址，再用 `MAP_FIXED` 覆盖——避免 `MAP_FIXED` 盲踩已用地址（经典 magic ring 做法）。
- 两段都映射到 **fd 的 offset 0**，物理页因此共享。
- fd 在两次映射后即可 `close`（映射持有引用），但本设计保留 fd 以便调试/统计，析构时再关。

### 析构顺序（RAII）
```
~Mapping(): munmap(base_, 2*len_);  // 一次解掉两段（地址连续）
            if (fd_ >= 0) close(fd_);
```
移动后源对象置 `base_=nullptr, fd_=-1`，析构做空操作，杜绝 double-free。

---

## 4. 大页支持

- 请求 2MB/1GB：`memfd_create` 加 `MFD_HUGETLB`（配 `MFD_HUGE_2MB`/`MFD_HUGE_1GB`），`mmap` 无需再加 flag（fd 已是 hugetlbfs 后备）。
- **探测**：读 `/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages` 及 free 计数，判断可用与是否够分。
- **降级**：请求大页但不可用 -> `HugePageUnavailable`，由 L7 决定"告警后降级普通页"还是"硬失败"。默认：告警 + 降级普通页（约定优于配置）。
- `size` 必须是所选页大小的整数倍，否则 `InvalidSize`。

---

## 5. NUMA 感知

- `numa_node >= 0` 时，映射后用 `mbind(base, 2*size, MPOL_BIND, nodemask, ...)` 把物理页绑到指定节点；或用 libnuma 的 `numa_tonode_memory`。
- 目的：把 ring 的物理页放到生产/消费线程所在 NUMA 节点，减少跨节点访问延迟。
- **探测**：`numa_available()`；不可用 -> `NumaUnavailable`，降级为不绑定并告警。
- 绑定发生在 `ftruncate` 之后、首次触碰之前（利用 first-touch 或显式 `mbind` 预置）。

---

## 6. 跨进程 Futex

Linux `futex(2)` 直接作用于**共享内存上的 32 位字**——两个进程 `mmap` 同一段 `MAP_SHARED` 内存，futex 字落在其中，即可跨进程等待/唤醒。

| 操作 | 语义 | syscall |
|------|------|---------|
| `wait(expected, timeout)` | 原子检查 `*word==expected` 才睡；否则立即返回。防丢失唤醒关键：检查与睡眠对内核原子 | `futex(word, FUTEX_WAIT[_BITSET], expected, timeout)` |
| `wake_one()` | 唤醒至多 1 个等待者 | `futex(word, FUTEX_WAKE, 1)` |
| `wake_all()` | 唤醒全部 | `futex(word, FUTEX_WAKE, INT_MAX)` |

- 跨进程用 `FUTEX_WAIT`/`FUTEX_WAKE`（非 `_PRIVATE`），因等待者/唤醒者在不同地址空间。
- 与 L4 配合：生产者 `commit` 后若检测到消费者可能在睡，调 `wake_one`；消费者空转到阈值后 `wait`。**避免丢失唤醒**靠"先改 position(release)，再检查 waiter 标志"与"先置 waiter 标志，再 `FUTEX_WAIT` 复检 position"的双向握手（详见 L4）。

---

## 7. 危险操作契约（SAFETY 注释规范）

L0 是裸操作最密集的层。每处 `mmap`/裸指针/`reinterpret_cast` 附 `// SAFETY:` 说明：

- **`Mapping::as_ptr()` 返回的指针**：有效期 = `Mapping` 生命周期；调用方不得在析构/移动后使用。L1 持有 `Mapping` 保证其存活。
- **双映射别名**：同一物理页有两个虚拟地址别名。对 futex 字与 position 用 `std::atomic_ref` 访问，不用普通读写，避免编译器假设无别名而错误缓存。
- **对齐**：`Futex::word_` 必须 4 字节对齐；`Mapping` 基址由 `mmap` 保证页对齐。构造时 `assert`。
- **释放顺序**：先 `munmap` 后 `close`；移动语义置空源，禁止 double-free。
- **size 校验**：必须 2 的幂 + 页（或大页）对齐 + `2*size` 不溢出 `size_t`，否则 `InvalidSize`，造映射前拦截。

---

## 8. 测试清单

| 测试 | 方法 | 通过标准 |
|------|------|----------|
| 同物理页 | 主进程写 `base[0]`，读 `base[len]` | 值相等；反向亦然 |
| 跨进程共享 | `fork` 后子进程写、父进程读同一 `Mapping`（`MAP_SHARED`） | 值一致 |
| 回绕连续性 | 在 `base+len-4` 处写 8 字节（跨边界） | 从第二段读回连续 8 字节 |
| 失败回滚 | 注入 `mmap` 失败（超大 size / 占满地址） | 无 fd 泄漏、无残留映射（`/proc/self/maps` 校验） |
| 大页降级 | 无大页环境请求 2MB | 返回 `HugePageUnavailable`，降级路径成功 |
| NUMA 降级 | 无 NUMA 环境请求绑定 | 返回 `NumaUnavailable`，降级成功 |
| futex 无丢失唤醒 | 生产/消费线程压测，消费者 `wait`、生产者 `wake_one` | 无永久阻塞（超时看门狗验证） |
| 泄漏 | ASan + 反复 create/destroy 10^6 次 | 无泄漏、maps 稳定 |

工具：GTest + ASan/UBSan（TSan 用于 futex 并发）。**注意**：这些依赖真实 syscall，必须在真机/CI 容器跑，不能靠纯逻辑单测替代。

---

## 9. 验收标准

- `Mapping::create` 在合法参数下 100% 成功并通过"同物理页/回绕连续"自检。
- 非法参数（非 2 的幂、溢出、未对齐）一律返回对应 `PlatformError`，不 crash。
- 反复 create/destroy 无 fd/地址泄漏（ASan + `/proc/self/maps` 快照一致）。
- futex 压测无丢失唤醒、无永久阻塞。
- 大页/NUMA 不可用时可靠降级并告警，不影响功能正确性。
- 全部危险操作有 `// SAFETY:` 注释，clang-tidy 无高危告警。

# 4 - L0 平台抽象层

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：把 Linux 双映射共享内存与大页映射封装成 C++23 RAII 抽象。

**技术栈**：C++23 / CMake / Linux。namespace `salias::platform`，目录 `core/platform/`。

---

## 1. 职责与边界

L0 负责：

- 用 `memfd_create`、`ftruncate` 与两次固定地址 `mmap` 创建 magic ring 的双映射。
- 打开具名共享内存后，在不同进程中建立相同的双映射视图。
- 支持普通页、2 MiB 与 1 GiB hugetlb 映射，并把失败转换为 `PlatformError`。
- 通过 RAII 管理 fd、预留地址和映射生命周期。
- 对最终数据映射使用 `MAP_POPULATE`，把首次缺页成本移出消息计时窗口。

L0 不认识帧、序列、生产者、消费者或通道模式，也不提供等待与通知 syscall。

---

## 2. 公共类型

```cpp
enum class PlatformError {
  Ok = 0,
  MemfdCreateFailed,
  FtruncateFailed,
  ReserveFailed,
  MapFixedFailed,
  UnmapFailed,
  HugePageUnavailable,
  NumaUnavailable,
  InvalidSize,
};

enum class HugePage { None, Size2MB, Size1GB };

struct MapOptions {
  std::size_t size;
  HugePage huge = HugePage::None;
  int numa_node = -1;
};
```

`Mapping` 不可拷贝、可移动。`as_ptr()` 返回的地址只在 `Mapping` 生命周期内有效，`len()` 返回单段逻辑容量；
实际连续虚拟地址长度为 `2 * len()`。

---

## 3. 双映射流程

1. 创建或打开后备 fd，并把长度设置为逻辑容量。
2. 用 `PROT_NONE` 预留两倍容量的连续虚拟地址。
3. 用 `MAP_SHARED | MAP_FIXED | MAP_POPULATE` 把 fd offset 0 映射到前半段。
4. 用相同 offset 把同一 fd 映射到后半段。
5. 任意跨逻辑尾部的帧都可通过一个连续 span 访问，无需 padding 或分片。

任一步失败都回滚已创建的 fd 与映射。移动后源对象清空，析构只释放仍持有的资源。

---

## 4. 大页与容量约束

- 普通页容量必须非零、是 2 的幂，并满足系统页对齐。
- 2 MiB/1 GiB 模式的容量必须按请求的大页大小对齐。
- 请求大页但系统未配置 hugetlb 池、权限不足或映射失败时返回 `HugePageUnavailable`，不静默降级。
- 是否回退普通页由调用方决定，避免基准和生产环境在不知情时改变内存语义。

---

## 5. 安全契约

- `Mapping::as_ptr()` 的裸指针不得越过 `2 * len()`，不得在对象移动或析构后使用。
- 两段虚拟地址别名同一物理页；跨线程共享状态必须由上层使用明确的原子与内存序访问。
- 映射容量与两倍长度的计算必须先做溢出检查。
- 固定地址映射只能覆盖本次预留的地址区间。

---

## 6. 验证

- 主进程验证两段地址互为别名，并验证跨尾连续读写。
- `fork` 子进程写入后，父进程通过共享映射读取同一数据。
- 非法尺寸与大页未配置场景返回稳定错误。
- ASan/UBSan 覆盖边界与资源生命周期；Release/TSan 构建覆盖真实 Linux syscall 路径。

---

## 7. 验收标准

- 合法普通页映射稳定创建并通过别名、跨尾和跨进程测试。
- 非法容量返回 `InvalidSize`，大页不可用返回 `HugePageUnavailable`。
- 重复创建、移动和销毁不泄漏 fd 或虚拟地址。
- 性能 harness 的计时阶段不再承担映射的首次缺页成本。

# 12 - 工程目录与文件划分

> 上层文档：`3-layered-architecture-overview.md`
> 一句话职责：把 L0–L7 分层映射到**目录结构、CMake 目标、文件粒度**，给出每个文件的职责，使任何人能照着建工程、照着找代码。

**技术栈**：C++23 / CMake + vcpkg / 仅 Linux。遵循仓库规范：小文件（200–400 行典型、800 上限）、高内聚低耦合、namespace 与目录同构。

---

## 1. 设计原则

1. **目录 = namespace = 层**：`core/ring/` ↔ `salias::ring` ↔ L1。找代码与改代码的路径一致。
2. **依赖方向 = 包含方向**：上层目录的头文件可 `#include` 下层；下层绝不 `#include` 上层。CI 用 include-linter 或 `clang-tidy` 的 `Includes` 检查守护。
3. **公私分离**：L7 的公共头放 `salias/include/salias/`（对外 ABI）；L0–L6 是私有实现，编译进 `salias_core` 静态库，不外泄头。
4. **header-only 优先于零开销**：纯函数/模板/概念（L2 编解码、L4 spin 策略、L1 缓存行包装）做成 header-only，热路径内联；有状态的实现（L0 syscall、L5 通道）才拆 `.cpp`。
5. **错误类型就近定义**：每层一个 `error.hpp`，避免跨层大杂烩。

---

## 2. 顶层目录树

```
salias/
├── CMakeLists.txt              # 顶层：project()、vcpkg toolchain、子目录、全局编译选项
├── CMakePresets.json           # debug(sanitizers)/release(O3 LTO) 预设
├── vcpkg.json                  # 依赖清单：gtest, benchmark, fmt
├── .clang-format               # 统一格式
├── .clang-tidy                 # 守护 include 方向、modernize、bugprone
├── .gitignore
├── README.md
│
├── doc/                        # 设计文档（本目录）
│   ├── 1-aeron-analysis-and-optimizations.md
│   ├── 2-implementation-plan.md
│   ├── 3-layered-architecture-overview.md
│   ├── 4-L0-platform.md … 11-L7-api.md
│   └── 12-project-layout.md     # ← 本文
│
├── core/                       # 私有实现库 salias_core（L0–L6）
├── salias/                     # 公共 API 库/头（L7）
├── tools/                      # 可执行工具（salias-top / salias-clean）
├── bench/                      # 性能基准（google-benchmark）
├── test/                       # 单元/集成测试（GTest）
└── cmake/                      # 复用脚本（sanitizer、warnings、include-lint）
```

---

## 3. `core/` —— L0–L6 私有实现（`salias_core` 静态库）

```
core/
├── CMakeLists.txt              # add_library(salias_core STATIC ...)
│
├── platform/                   # L0  · namespace salias::platform
│   ├── error.hpp                  # enum class PlatformError
│   ├── map_options.hpp            # MapOptions / HugePage / NumaNode
│   ├── mapping.hpp                # Mapping（双映射句柄，RAII）
│   ├── mapping.cpp                # memfd + ftruncate + 双 mmap(MAP_FIXED) + 自检
│   ├── huge_page.hpp / .cpp       # /sys/kernel/mm/hugepages 探测 + 降级
│   ├── numa.hpp / .cpp            # mbind/numa_available + 降级
│   └── futex.hpp / .cpp           # Futex（FUTEX_WAIT/WAKE 跨进程）
│
├── ring/                       # L1  · namespace salias::ring
│   ├── error.hpp                  # RingError
│   ├── cache_aligned.hpp          # CacheAligned<T>（header-only，alignas）
│   ├── atomic_cell.hpp            # load_acquire/store_release（atomic_ref 包装，header-only）
│   ├── magic_ring.hpp             # MagicRing（slice/slice_mut/capacity/mask）
│   └── magic_ring.cpp             # 仅 create 校验与持有 Mapping
│
├── frame/                      # L2  · namespace salias::frame（全 header-only 纯函数）
│   ├── error.hpp                  # FrameError
│   ├── header.hpp                 # FrameHeader / Flags / kHeaderSize / kFrameAlign
│   └── codec.hpp                  # encode_header / decode_header / frame_len / align_up
│
├── flow/                       # L3  · namespace salias::flow
│   ├── error.hpp                  # FlowError（BackPressured / MessageTooLarge）
│   ├── position.hpp               # Positions 布局指针结构（header-only）
│   ├── producer.hpp / .cpp        # Producer: claim/commit/offer + cached_head
│   └── consumer.hpp / .cpp        # Consumer: poll/poll_batch/advance + cached_tail
│
├── wait/                       # L4  · namespace salias::wait
│   ├── wait_strategy.hpp          # concept WaitStrategy（header-only）
│   ├── busy_spin.hpp              # header-only 策略
│   ├── spin_pause.hpp             # header-only（_mm_pause）
│   ├── yielding.hpp               # header-only（this_thread::yield）
│   ├── backoff.hpp                # header-only（指数退避）
│   ├── futex_wait.hpp / .cpp      # 自旋预算 + platform::Futex 落内核
│   └── any_wait.hpp / .cpp        # 类型擦除 AnyWaitStrategy（非热路径用）
│
├── channel/                    # L5  · namespace salias::channel
│   ├── error.hpp                  # ChannelError
│   ├── channel_config.hpp         # ChannelConfig + 预设工厂声明
│   ├── spsc.hpp / .cpp            # SpscChannel（快路）
│   ├── mpsc.hpp / .cpp            # MpscChannel（CAS 仲裁 + commit 顺序）
│   ├── broadcast.hpp / .cpp       # BroadcastChannel（多订阅 + 最慢者回收）
│   └── bulk.hpp / .cpp            # BulkChannel（大消息单帧）
│
└── metrics/                    # L6  · namespace salias::metrics
    ├── error.hpp                  # MetricsError
    ├── layout.hpp                 # MetaHeader / CounterSlot / CounterType（ABI，static_assert）
    ├── counters.hpp / .cpp        # Counters 写入端（incr/set_release）
    └── reader.hpp / .cpp          # CountersReader 只读观测端
```

### 文件粒度规则
- header-only：纯函数、模板、concept、策略小对象 → 无 `.cpp`。
- `.cpp` 只放：有状态的实现、syscall 封装、不可内联的非热路径代码。
- 每个文件 200–400 行；`magic_ring.cpp`、`spsc.cpp` 等若超 400 行优先拆（如 `mpsc_commit.cpp`）。

---

## 4. `salias/` —— L7 公共 API（用户可见头 + 库）

```
salias/
├── CMakeLists.txt              # add_library(salias ...)，link salias_core
│
├── include/salias/             # ★ 公共头（对外，安装/暴露给用户）
│   ├── salias.hpp                 # 总入口（聚合 include）
│   ├── config.hpp                 # Config / Mode / WaitKind
│   ├── error.hpp                  # 公共 Error 枚举（v0.1 冻结）
│   ├── message.hpp                # Message / Claim 视图类型
│   ├── channel.hpp                # Channel（create/connect/publisher/subscriber）
│   ├── publisher.hpp              # Publisher（try_claim/commit/offer）
│   └── subscriber.hpp             # Subscriber（try_recv/recv/poll）
│
├── src/
│   ├── handshake.hpp / .cpp       # ChannelMeta + ready 握手协议（L7 §2）
│   ├── channel.cpp                # Channel 实现，组合 L5 通道
│   ├── publisher.cpp
│   └── subscriber.cpp
│
└── async/                      # 可选 async 适配层（SALIAS_ASYNC feature flag，默认关）
    ├── CMakeLists.txt             # 条件 add_library，依赖用户 executor
    └── include/salias/async.hpp   # Awaitable recv（eventfd/io_uring 桥接 futex）
```

公共头规则：`include/salias/` 下的头**只能依赖标准库 + core 的稳定抽象**，不泄露 L0–L5 私有头；`v0.1` 冻结后只增不破坏。

---

## 5. `tools/` `bench/` `test/`

```
tools/
├── salias-top/                 # 周期读 metrics::CountersReader，打印吞吐/滞后/背压/能耗
│   └── main.cpp
└── salias-clean/               # 按 pid 存活检查清理 /dev/shm/salias/* 残留
    └── main.cpp

bench/                          # google-benchmark，对照 Aeron 基线（见 2-）
├── CMakeLists.txt
├── spsc_pingpong.cpp           # SPSC 往返延迟 p50/p99/p99.9
├── spsc_throughput.cpp         # 小/中消息吞吐
├── mpsc_throughput.cpp
├── bulk.cpp                    # 大消息
└── aeron_baseline/             # 拉取 aeron-ipc C++ 样例做同机对照（结果落 doc/benchmarks/）

test/                           # GTest，按层分目录，与 core/ 同构
├── CMakeLists.txt
├── platform/                   # 同物理页/回绕连续/fork 共享/降级/futex 唤醒
├── ring/                       # 回绕连续/满空边界/2 的幂/交错
├── frame/                      # 编解码往返/对齐/fuzz
├── flow/                       # position 单调/背压/批量/超限
├── wait/                       # 无丢失唤醒/空载 CPU/延迟
├── channel/                    # 四通道端到端/MPSC 竞争/Broadcast 慢订阅
├── metrics/                    # 跨进程只读/ABI 快照/fuzz
└── api/                        # 握手竞态/版本不匹配/不可信 meta/async

cmake/
├── sanitizers.cmake            # ASan/UBSan/TSan 开关
├── warnings.cmake              # -Wall -Wextra -Werror + 全局警告
└── include_lint.cmake          # 守护"下层不 include 上层"的脚本
```

---

## 6. CMake 目标与依赖

```
salias_core  (STATIC)  ← core/**                  私有，不安装头
salias       (STATIC/SHARED) ← include/salias/** + src/ + link: salias_core   公共，安装头
salias_top   (EXE) ← link: salias_core (只用 metrics::CountersReader)
salias_clean (EXE)
bench_*      (EXE) ← link: salias, google-benchmark
test_*       (EXE) ← link: salias_core (或 salias), GTest
```

- 全局编译：`-std=c++20`；release `-O3 -flto -march=native`；debug `-Og -g + sanitizers`。
- CI 矩阵：build=Debug(Sanitizers)/Release，并发测试单独跑 TSan。
- 依赖（`vcpkg.json`）：`gtest`、`benchmark`、`fmt`（可选日志/工具）。错误返回直接使用 C++23 `std::expected`。

---

## 7. 从层到文件的速查表

| 层 | 目录 | namespace | 关键文件 | 状态 |
|----|------|-----------|----------|------|
| L0 | `core/platform/` | `salias::platform` | `mapping.{hpp,cpp}`, `futex.{hpp,cpp}` | syscall 实现 |
| L1 | `core/ring/` | `salias::ring` | `magic_ring.{hpp,cpp}`, `cache_aligned.hpp`, `atomic_cell.hpp` | 多 header-only |
| L2 | `core/frame/` | `salias::frame` | `header.hpp`, `codec.hpp` | 全 header-only |
| L3 | `core/flow/` | `salias::flow` | `producer.{hpp,cpp}`, `consumer.{hpp,cpp}` | 有状态实现 |
| L4 | `core/wait/` | `salias::wait` | `wait_strategy.hpp`(concept), `futex_wait.{hpp,cpp}` | 策略 header-only |
| L5 | `core/channel/` | `salias::channel` | `spsc.{hpp,cpp}`, `mpsc.{hpp,cpp}`, `broadcast.{hpp,cpp}`, `bulk.{hpp,cpp}` | 有状态实现 |
| L6 | `core/metrics/` | `salias::metrics` | `layout.hpp`(ABI), `counters.{hpp,cpp}`, `reader.{hpp,cpp}` | ABI 锁定 |
| L7 | `salias/` | `salias` | `include/salias/channel.hpp` 等 + `src/handshake.{hpp,cpp}` | 公共 ABI 冻结 |

---

## 8. 依赖方向约束（include-lint 守护）

允许的 `#include` 方向（上 → 下）：

```
salias(src) ──▶ channel ──▶ flow ──▶ frame ──▶ ring ──▶ platform ──▶ <系统>
     │              │        │        │        │
     └─▶ metrics ◀──────────┴────────┴────────┘   (metrics 被多层写入，但经接口注入，不反向依赖)
     └─▶ wait ──▶ platform
```

- **硬规则**：`platform` 不得 include 任何 `salias::` 其它 namespace；`ring` 不得 include `frame` 及以上。
- `metrics/layout.hpp` 是纯数据布局，被 L5/L7 引用是允许的；但写入走注入的接口，不在数据面热路径里反向调用 channel。
- `cmake/include_lint.cmake` 在 CI 用正则/脚本扫 `#include` 违例，违例即失败。

---

## 9. 命名与编码约定

- 类型：`PascalCase`（`MagicRing`、`ChannelConfig`）。
- 函数/变量：`snake_case`。
- 常量：`kPascalCase`（`kHeaderSize`、`kCacheLine`）。
- enum class，不带 `ALL_CAPS`。
- 错误：每层 `enum class XError`，配合 `std::expected<T, XError>`。
- 裸内存/原子/reinterpret_cast 集中在 `platform/`、`ring/`，每处 `// SAFETY:` 注释。
- immutable-first：句柄类删除拷贝、只提供移动；配置对象按值传递、const& 访问。

# salias

salias 是一个 Linux-only 的 C++23 共享内存消息库，目标是服务同机进程间
IPC 和进程内线程间通信。它不做网络传输，也不依赖类似 Aeron Media Driver 的
常驻中介进程；核心数据面由两端直接访问同一段共享内存，通过 position 计数器
完成发布、消费和背压。

当前仓库已经实现了从平台映射、magic ring、帧编解码、SPSC flow、等待策略、
通道封装、metrics 布局到公共 `salias::Channel` API 的主线代码。部分设计文档
描述的是后续目标，README 会显式区分。

## 项目定位

salias 聚焦一个很窄的场景：同一台 Linux 主机上的高吞吐、低抖动消息传递。
它的核心取舍是：

- 只支持 Linux：依赖 `memfd_create`、`mmap`、`MAP_FIXED`、`shm_open`、
  `futex` 等 Linux/POSIX 共享内存能力。
- 不支持网络：不提供 UDP/TCP、跨主机路由、重传或集群能力。
- 不引入后台 Driver：具名通道通过共享内存控制块握手，数据面由两端直连。
- 优先小而可验证的无锁实现：核心状态以 64 位 position 和 cache-line 隔离的
  共享内存字段表达，失败用显式错误返回。

## 当前状态

已实现：

- L0 平台层：匿名 `memfd` 双映射、已有 fd 的双映射、RAII 释放、Linux futex。
- L1 ring 层：`MagicRing`、64 字节 cache-line 对齐包装、共享内存位置的
  `std::atomic_ref` acquire/release 访问。
- L2 frame 层：8 字节帧头、8 字节对齐、flags 和 generation/seq 编解码。
- L3 flow 层：SPSC `Producer::claim/commit`、`Consumer::poll/advance`、
  背压和 message-too-large 判定。
- L4 wait 层：`BusySpin`、`SpinPause`、`Yielding`、`FutexWait` 和静态
  `WaitStrategy` concept。
- L5 channel 层：进程内 `SpscChannel`、`MpscChannel`、`BroadcastChannel`、
  `BulkChannel`，以及用于具名共享内存的 `SharedSpscChannel`。
- L6 metrics 层：固定 ABI 的 counters 区布局、写入端 `Counters`、
  只读端 `CountersReader`。
- L7 公共 API：`Channel::create`、`Channel::connect`、`Publisher::offer`、
  `Subscriber::try_recv/recv/release`。进程内支持 SPSC/MPSC/Broadcast/Bulk；
  具名跨进程握手目前支持 SPSC。

尚未实现或尚未作为公共 API 暴露：

- 公共 `Publisher::try_claim/commit` 零拷贝 API。core/channel 内部已有
  `claim/commit`，公共 `Publisher` 当前只暴露 `offer`，会把用户 buffer 拷入 ring。
- `Config::fixed_size` / 无头定长帧模式。L2 有 `fixed_slot()` 计算，公共创建会拒绝
  fixed-size 配置。
- public `WaitKind::Futex` 配置。core 有 `FutexWait`，公共 `Channel::create` 当前只接受
  `SpinPause`。
- huge page 和 NUMA 绑定。配置类型存在，但 L0 当前对 `HugePage != None` 或
  `numa_node >= 0` 返回不可用错误。
- metrics 与公共 `Channel` 的自动集成、`salias-top` / `salias-clean` 工具。
- 原生单通道 MPMC fanout、Broadcast 有损模式、async 适配层、include-lint 规则 enforcement。

## Linux 依赖

构建和运行要求：

- Linux。顶层 `CMakeLists.txt` 在非 Linux 平台直接 `FATAL_ERROR`。
- 支持 `<expected>` 的 C++23 编译器，通常是 GCC 或 Clang。
- CMake 3.25+。
- Ninja。仓库的 CMake presets 使用 Ninja generator。
- vcpkg manifest 依赖：`gtest`、`benchmark`、`fmt`。
- TSan preset 运行测试时需要 `setarch`，测试配置会用它关闭地址随机化。

运行时依赖的内核接口：

- `memfd_create` + `ftruncate`：创建匿名共享内存后备。
- `mmap(PROT_NONE)` + 两次 `mmap(MAP_SHARED | MAP_FIXED)`：把同一 fd offset 0 映射到
  两段相邻虚拟地址。
- `shm_open` / `shm_unlink`：具名 SPSC 通道的控制块和 ring 共享内存对象。
- `futex(FUTEX_WAIT/FUTEX_WAKE)`：阻塞等待策略和跨进程唤醒。

## 快速开始

如果 vcpkg 没有通过环境或工具链文件自动接入，configure 时追加：

```bash
cmake --preset debug-asan-ubsan \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
```

Debug + ASan/UBSan：

```bash
cmake --preset debug-asan-ubsan
cmake --build --preset debug-asan-ubsan
ctest --preset debug-asan-ubsan
```

TSan：

```bash
cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan
```

Release + benchmarks：

```bash
cmake --preset release
cmake --build --preset release
ctest --test-dir build/release --output-on-failure
```

常用 benchmark：

```bash
./build/release/bench/salias_bench_smoke --benchmark_min_time=0.001
./build/release/bench/salias_bench_channel_stress
./build/release/bench/salias_bench_compare --scenario spsc --messages 1000000 --producers 1 --consumers 1 --payload 64
./build/release/bench/salias_bench_compare --scenario spmc --messages 500000 --producers 1 --consumers 2 --payload 64
./build/release/bench/salias_bench_compare --scenario mpmc --messages 200000 --producers 4 --consumers 2 --payload 64
```

与 Aeron C++ IPC 的 release 对照脚本：

```bash
bash tools/aeron_compare/run_release_compare.sh
```

该脚本会从 `third_party/aeron-1.52.0.tar.gz` 解包到 `/tmp` 构建 Aeron，启动外部
`aeronmd` media driver，并运行 salias/Aeron 的 SPSC、SPMC、组合 MPMC 对照。
`doc/benchmarks/aeron-baseline.md` 记录了一次 2026-07-06 的 Colima Linux VM
aarch64 单次运行结果；它是基线记录，不是最终性能承诺。该文档也说明了当前
salias MPMC 对照是“每个 producer 一个 Broadcast channel”的组合拓扑，尚不是原生
单通道 MPMC fanout。

## 最小 API 示例

```cpp
#include <array>
#include <cstddef>
#include <utility>

#include <salias/salias.hpp>

int main() {
  salias::Config config;
  config.mode = salias::Mode::Spsc;
  config.capacity = 1u << 20;

  auto created = salias::Channel::create(config);
  if (!created) {
    return 1;
  }

  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array payload{std::byte{0x41}, std::byte{0x42}, std::byte{0x43}};
  auto offered = publisher.offer(payload);
  if (!offered || !offered.value()) {
    return 2;
  }

  auto message = subscriber.try_recv();
  if (!message) {
    return 3;
  }

  subscriber.release(*message);
  return 0;
}
```

跨进程 SPSC 使用具名通道：

```cpp
salias::Config owner_config;
owner_config.name = "quotes";
owner_config.mode = salias::Mode::Spsc;
auto owner = salias::Channel::create(owner_config);

auto peer = salias::Channel::connect("quotes");
```

具名通道名只允许字母、数字、`-`、`_`、`.`，长度不超过 128。当前具名握手只支持
SPSC；具名 MPSC/Broadcast/Bulk 会返回 `BadConfig`。

## 目录结构

```text
.
├── CMakeLists.txt              # 顶层工程、Linux-only 检查、preset 选项入口
├── CMakePresets.json           # debug-asan-ubsan / tsan / release
├── vcpkg.json                  # gtest、benchmark、fmt
├── cmake/                      # warnings、sanitizers、include-lint 占位
├── doc/                        # 架构、分层、实现计划、benchmark 基线
├── src/core/                   # L0-L6 私有实现库 salias_core
│   ├── platform/               # mmap、memfd、futex、Result
│   ├── ring/                   # MagicRing、cache alignment、atomic cell
│   ├── frame/                  # 8 字节帧头和编解码
│   ├── flow/                   # SPSC producer/consumer position 协议
│   ├── wait/                   # spin/pause/yield/futex wait strategies
│   ├── channel/                # SPSC、MPSC、Broadcast、Bulk、SharedSPSC
│   └── metrics/                # counters ABI、writer、reader
├── src/salias/                 # L7 公共 API 库 salias
│   ├── include/salias/         # 用户头文件
│   └── src/channel.cpp         # API facade 和具名 SPSC 握手
├── test/                       # GTest，按模块分组
├── bench/                      # google-benchmark 和 salias/Aeron 对照入口
├── tools/aeron_compare/        # Aeron IPC 对照脚本和 C++ benchmark
└── third_party/                # Aeron tarball
```

## 分层架构

salias 的实现按 L0-L7 分层。依赖方向自上而下：上层可以使用下层，下层不反向依赖
上层。

| 层 | 模块 | 当前职责 | 实现状态 |
| --- | --- | --- | --- |
| L0 | `salias::platform` | Linux 双映射、shared fd 映射、futex、显式错误 | 已实现主路径；huge/NUMA 返回不可用 |
| L1 | `salias::ring` | magic ring、cache-line 对齐、共享内存 atomic cell | 已实现 |
| L2 | `salias::frame` | 8 字节帧头、8 字节对齐、flags/seq | 已实现；无头定长只到 helper |
| L3 | `salias::flow` | SPSC position、背压、claim/commit、poll/advance | 已实现 SPSC 基础路径 |
| L4 | `salias::wait` | 可插拔等待策略 concept 和实现 | core 已实现；公共配置只开放 SpinPause |
| L5 | `salias::channel` | SPSC/MPSC/Broadcast/Bulk 组合通道 | 进程内已实现；具名共享 SPSC 已实现 |
| L6 | `salias::metrics` | 共享内存 counters 布局、读写视图 | 独立模块已实现；尚未自动接入 Channel |
| L7 | `salias` | 用户 API、driverless 具名握手 | 公共 offer/recv 已实现；public claim 未实现 |

## 数据路径

### 进程内 SPSC

1. `Channel::create` 根据 `Config` 创建 core `SpscChannel`。
2. L0 用 `memfd_create` 创建 fd，`ftruncate` 到 ring 容量。
3. L0 预留 `2 * capacity` 的虚拟地址，再把同一 fd offset 0 映射到前后两段。
4. L1 `MagicRing` 持有 `Mapping`，对上暴露连续 `slice/slice_mut`。
5. Publisher `offer` 调用 L3 `Producer::claim`，检查可用空间并返回 payload span。
6. `offer` 把用户 buffer 拷到 payload span，`commit` 写 8 字节帧头，然后 release-store
   producer position。
7. Subscriber `try_recv` acquire-load producer position，解码帧头，返回指向 ring 的
   `Message::payload` span。
8. 用户处理完成后调用 `release`，consumer position release-store 前移，空间立刻可被
   producer 复用。

### 具名跨进程 SPSC

具名 SPSC 用两个 POSIX shm 对象：

- `/salias-<name>-ctl`：4 KiB 控制块，包含 magic/version/mode/capacity/ready、
  wait word、producer/consumer position。
- `/salias-<name>-ring`：ring 数据区，被 `Mapping::map_shared_fd` 双映射。

owner 端 `Channel::create` 创建并初始化控制块和 ring，最后对 `ready` 做 release-store。
peer 端 `Channel::connect` 会有限轮询等待控制块存在和 `ready == 1`，然后 acquire-load
ready，校验 magic/version/mode/capacity/record_size，再映射 ring。owner 析构时
`shm_unlink` 两个名字；已经映射的 peer 仍可继续持有内核对象引用。

### MPSC

MPSC 是进程内通道。多个 Tx 通过 `reserved_tail_` CAS 抢占互不重叠的 ring 区间。
每个 producer 先写未提交帧头和 payload，`commit` 时 release-store `FLAG_COMMITTED`。
单 consumer 按 position 顺序读取；如果遇到前序帧尚未 committed，即使后续帧已经提交也
不会越过该 gap。

### Broadcast

Broadcast 是单 producer、多 subscriber 的可靠扇出。producer 只写一份物理帧；
每个 subscriber 有独立 head。空间回收边界是所有 subscriber head 的最小值，因此慢
订阅者会让 producer 背压，不会被覆盖。当前未实现有损模式和 `Lagged` 返回。

### Bulk

Bulk 当前是基于 SPSC 的大消息通道包装。它保留“单帧必须放得进 ring”的约束，
不会分片，也不会重组。得益于双映射，跨 ring 尾部的大 payload 在虚拟地址上仍是连续
span。超过单帧容量返回 `MessageTooLarge`。

## 核心实现与原理

### 内存映射

`platform::Mapping::create` 的主流程：

1. 校验容量：非 0、2 的幂、页对齐、`2 * size` 不溢出、能放进 `off_t`。
2. `memfd_create("salias-ring", MFD_CLOEXEC)`。
3. `ftruncate(fd, size)`。
4. `mmap(nullptr, 2 * size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0)` 预留连续地址。
5. `mmap(base, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0)` 映射第一段。
6. `mmap(base + size, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0)` 映射第二段。
7. 对匿名 memfd 路径做自检：写 `base[0]`，检查 `base[size]` 是否同步变化。

`Mapping::map_shared_fd` 用同样的双映射技术包裹已有 shared fd，并复制 fd 所有权。
这条路径用于具名 shm ring。

### Magic ring

普通环形缓冲在跨尾部回绕时需要拆成两段拷贝，或者像 Aeron term 一样写 padding、
切换 term、清理旧 term。salias 的 `MagicRing` 依赖 L0 的双映射不变量：

```text
[base, base + capacity) 和 [base + capacity, base + 2 * capacity)
映射到同一份物理页
```

因此任意 `pos` 的 ring 偏移都可以写成：

```cpp
base + (pos & (capacity - 1))
```

只要 `len <= capacity`，即使 `offset + len > capacity`，从虚拟地址看仍是一段连续内存。
L1 只保证地址连续，不判断这段数据是否已经发布或是否可覆盖；这些协议属于 L3/L5。

### 帧编解码

标准帧头固定 8 字节：

```text
0..3: payload len, uint32_t
4..7: meta, uint32_t
```

`meta` 低 8 位是 flags：

- `FLAG_BEGIN`
- `FLAG_END`
- `FLAG_PADDING`
- `FLAG_COMMITTED`

高 24 位是 generation/seq，用于标识 ring 回绕代次。标准单帧消息使用
`BEGIN | END | COMMITTED`。帧占用长度为：

```cpp
kHeaderSize + align_up(payload_len)  // kHeaderSize = 8, align = 8
```

8 字节 payload 在 ring 中占 16 字节。对比 Aeron IPC 常见的 32 字节头和 32 字节对齐，
小消息元数据开销更低。当前实现固定同机小端字节序，不处理异构网络传输。

### 流控与 position

L3 用两个 64 位单调 position 表达进度：

- producer position：已发布到哪里，也就是 tail。
- consumer position：已消费到哪里，也就是 head。

可读字节数是 `tail - head`，可写空间是 `capacity - (tail - head)`。producer 直接读取
consumer position 判定背压，不需要第三方 Driver 轮询 publisher limit。

SPSC producer 的 `claim` 流程：

1. 计算 `need = frame_len(payload_len)`。
2. `need > capacity` 返回 `MessageTooLarge`。
3. 先用 cached head 判断空间；不足时 acquire-load consumer position 刷新缓存。
4. 仍不足返回 `BackPressured`。
5. 返回 ring 上的 payload span 和 claim 元数据。

`commit` 先写 payload 外部已经填好的内容和帧头，再 release-store producer position。
consumer acquire-load producer position 后再读帧头和 payload，形成 happens-before。
consumer `release`/`advance` 用 release-store 写 consumer position，producer 下次
acquire-load head 后即可复用空间。

### 等待策略

L4 把“没有消息时怎么等”抽象成静态 `WaitStrategy` concept：

```cpp
wait(word, expected)
wake(word)
reset()
```

已实现策略：

- `BusySpin`：纯自旋，最低延迟，空载占满核。
- `SpinPause`：自旋中执行 `_mm_pause()` 或 ARM `yield`，当前公共 API 默认策略。
- `Yielding`：短暂 pause 后 `std::this_thread::yield()`。
- `FutexWait`：先短自旋，再调用 `platform::Futex::wait`，`wake` 使用 `FUTEX_WAKE`。

SPSC、MPSC、Broadcast 在 commit 后递增 `wait_word_` 并调用 `wake`；recv 侧在没读到消息时
读取当前 `wait_word_`，再调用策略等待。futex 的关键正确性来自内核的原子复检：
`FUTEX_WAIT` 只有在内核观察到 `*word == expected` 时才睡眠；如果发布方已经先改变了
word，等待会立即返回而不是丢唤醒。

### 通道实现

`SpscChannel` 是最快路径：一个 producer、一个 consumer，无 CAS，只有 release/acquire
position 同步。

`MpscChannel` 支持多个 producer、一个 consumer。producer 用 CAS 递增 `reserved_tail_`
预留区间；commit 只发布自己的帧 `FLAG_COMMITTED`。consumer 顺序检查 commit flag，
防止越过未提交 gap。

`BroadcastChannel` 支持一个 producer、多个 subscriber。默认最大 subscriber 数由模板
参数 `MaxSubscribers` 决定，默认 8。可靠模式下 producer 以最慢 subscriber 的 head
为回收边界，空间不足时返回背压。

`BulkChannel` 是大消息单帧通道，内部复用 SPSC。它不做分片；消息必须能以
`8-byte header + aligned payload` 的形式放入一个 ring。

### 公共 API

公共 API 是 `src/salias/include/salias` 下的 facade：

- `Config`：`name`、`mode`、`capacity`、`fixed_size`、`record_size`、`wait`。
- `Channel::create`：创建进程内通道或具名 SPSC owner。
- `Channel::connect`：连接具名 SPSC。
- `Publisher::offer`：把用户 buffer 写入通道。
- `Subscriber::try_recv`：非阻塞读取。
- `Subscriber::recv`：按等待策略阻塞读取。
- `Subscriber::release`：释放消息占用的 ring 空间。

公共 `Result<T>` 保留项目自己的命名，但底层是 `std::expected<T, Error>`，不用异常做常规控制流。

### 指标模块

metrics 模块定义了共享内存 counters 区的固定 ABI：

- `MetaHeader`：64 字节，包含 magic、version、counter_count、slot_stride。
- `CounterSlot`：64 字节，每个 slot 独占 cache line，包含 value、type、owner、label。
- `CounterType`：producer position、consumer position、背压次数、错误数、消息数、字节数、
  lag、wake syscall 等类型。

`Counters::create_in` 可以在任意调用方提供的内存区初始化 counters，`CountersReader`
可以从内存 span 或只读文件路径打开并读取。当前模块已有跨进程测试，但还没有由公共
`Channel` 自动创建和暴露 counters。

## 测试覆盖

测试按模块组织：

- `test/platform`：双映射、共享 fd、futex。
- `test/ring`：跨边界连续切片、atomic cell、容量校验。
- `test/frame`：8 字节头、对齐、flags/seq。
- `test/flow`：SPSC claim/commit/poll/advance、背压释放、超限拒绝。
- `test/wait`：策略 concept 和 futex 唤醒。
- `test/channel`：SPSC、MPSC gap、Broadcast 可靠背压、Bulk 大消息。
- `test/metrics`：layout ABI、writer/reader、fork 共享、只读文件打开。
- `test/api`：公共 API、具名 SPSC 跨 fork、peer 先启动、版本/损坏 meta 拒绝。

`debug-asan-ubsan` preset 默认关闭 benchmark、打开测试和 ASan/UBSan。
`tsan` preset 打开 TSan，并通过 `setarch <arch> -R` 运行测试。
`release` preset 打开 benchmark、测试、`-O3`、LTO 和 `-march=native`。

## 设计目标与后续方向

这些方向来自 `doc/` 设计文档，但当前不要当作已完成能力：

- public zero-copy `try_claim/commit`，让用户直接在 ring 上构造消息，避免 `offer` 拷贝。
- fixed-size/no-header 模式，用 channel 元数据定义定长记录，消除每帧 8 字节头。
- public futex wait 配置、backoff/type-erased runtime wait strategy。
- huge page 和 NUMA 探测、绑定与降级策略。
- metrics 自动挂接到每个 Channel，并提供独立观测工具。
- 原生 MPMC fanout，而不是 benchmark 中的 Broadcast shard 组合拓扑。
- Broadcast 有损模式和 `Lagged` 检测。
- async 适配层。核心仍保持同步、无 runtime；async 只能是 L7 薄适配。
- include 方向 lint 的实际规则和 CI enforcement。

## 参考文档

- `doc/1-aeron-analysis-and-optimizations.md`：Aeron IPC 分析和 salias 设计取舍。
- `doc/2-implementation-plan.md`：M0-M6 里程碑和验收思路。
- `doc/3-layered-architecture-overview.md`：L0-L7 分层总览。
- `doc/4-L0-platform.md` 到 `doc/11-L7-api.md`：各层职责、接口、危险操作契约和测试清单。
- `doc/12-project-layout.md`：目录和 CMake target 设计。
- `doc/benchmarks/aeron-baseline.md`：一次 salias/Aeron release 对照记录和限制说明。

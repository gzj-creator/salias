# salias

`salias` 是支持 Linux 与 macOS 的 C++23 低延迟具名 IPC 消息通道。当前实现收敛为一个
per-producer magic-ring 引擎，并通过排序模式与消费者拓扑提供四种公共通道：

| Mode | 排序语义 | 消费者拓扑 |
|---|---|---|
| `FifoMpsc` | 每生产者 FIFO，不保证跨生产者顺序 | 单消费者 |
| `FifoFanout` | 每生产者 FIFO，不保证跨生产者顺序 | 多消费者 fanout |
| `OrderedMpsc` | 跨生产者全局全序 | 单消费者 |
| `OrderedFanout` | 跨生产者全局全序 | 多消费者 fanout |

FIFO 模式的生产者热路径不执行共享 RMW；ordered 模式通过单个全局 sequence
`fetch_add` 建立跨生产者全序。每个生产者拥有独立 ring，每个 fanout 消费者拥有独立的
per-producer 游标。

## 构建

要求 Linux 或 macOS、CMake 3.25+、支持 C++23 的 GCC/Clang。测试目标需要 GTest，
benchmark 目标还需要 Google Benchmark。预设使用 Ninja，也可直接选择其他 CMake generator。

```bash
cmake --preset debug-asan-ubsan
cmake --build --preset debug-asan-ubsan
ctest --preset debug-asan-ubsan
```

其他预设：

```bash
cmake --preset tsan
cmake --preset release
```

安装并供下游 CMake 工程使用：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
cmake --install build --prefix /path/to/salias
```

```cmake
find_package(salias CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE salias::salias)
```

配置下游工程时，将安装前缀加入 `CMAKE_PREFIX_PATH`。

## API

```cpp
#include <array>
#include <cstddef>
#include <utility>

#include <salias/salias.hpp>

int main() {
  salias::Config config;
  config.name = "quotes";
  config.capacity = 1u << 20;
  config.num_producers = 2;

  auto created = salias::FifoMpscChannel::create(config);
  if (!created) {
    return 1;
  }

  auto channel = std::move(created).value();
  auto publisher = channel.publisher();
  auto subscriber = channel.subscriber();

  std::array payload{std::byte{0x41}, std::byte{0x42}};
  if (!publisher.offer(payload)) {
    return 2;
  }

  auto message = subscriber.try_recv();
  if (!message) {
    return 3;
  }
  subscriber.release(*message);
}
```

连接已有通道：

```cpp
auto peer = salias::FifoMpscChannel::connect("quotes");
```

fanout 通道必须配置消费者 slot 数：

```cpp
salias::Config config;
config.name = "market-data";
config.num_producers = 4;
config.num_consumers = 2;
auto owner = salias::FifoFanoutChannel::create(config);
```

`Publisher` 和 `Subscriber` 持有底层 Tx/Rx 端点，因此生产者背压缓存、消费者 ring 游标和
`poll` 批次状态会跨 API 调用保留。

## Huge Pages

`Config::huge` 支持 `None`、`Size2MB` 和 `Size1GB`。Linux 上的显式大页请求使用
hugetlbfs，失败时返回 `PlatformFail`，不会降级。默认目录为 `/dev/hugepages`，可通过
`SALIAS_HUGETLBFS_DIR` 覆盖。macOS 支持普通页模式；显式 HugePage 和 NUMA 属于 Linux
专属能力，在 macOS 上会明确返回不可用。

普通页与大页 ring 在计时前使用 `MAP_POPULATE` prefault。控制块使用 POSIX shared memory。

## 工具

- `salias_bench_channel_stress`：FIFO/ordered 多生产者压力基准。
- `salias_ipc_compare`：`fifo` / `ordered` IPC 对比 harness。
- `tools/aeron_compare/run_release_compare.sh`：与 Aeron 的 release 对比脚本。
- `salias_mpsc_subscriber` / `salias_mpsc_publisher`：FIFO MPSC 示例。
- `salias_minimal_fifo` / `salias_minimal_ordered`：两个单文件最小模式示例，说明见 `example/README.md`。

## 架构动画

- [消息流交互动画](doc/salias-message-flow.html)：浏览器直接打开，查看 `Channel`、
  `Publisher` / `Subscriber`、底层 `Producer` / `Consumer`、per-producer `MagicRing`
  的关系，以及 FIFO、Ordered、MPSC、Fanout、release 与背压的完整流转。

## 最终性能

Tencent 4 vCPU x86 KVM，两个独立 Publisher 程序，每 Publisher 2,000,000 条 64B 消息；
每组预热 3 次、正式 20 次。Publisher/Subscriber 均为独立可执行程序，没有在 benchmark
程序内 `fork()` worker。完整机器配置、公平条件、Ordered 表格与原始数据见
[最终独立进程性能报告](doc/performance-report.md)。

| 拓扑 | 容量 | salias FIFO | Aeron | salias/Aeron | salias 背压 |
|---|---:|---:|---:|---:|---:|
| 2 Publisher / 1 Subscriber | 1 MiB | **44.867 M/s** | 41.108 M/s | **109.1%** | 0.312% |
| 2 Publisher / 1 Subscriber | 4 MiB | **44.485 M/s** | 38.515 M/s | **115.5%** | 0.200% |
| 2 Publisher / 1 Subscriber | 64 MiB | **38.806 M/s** | 22.011 M/s | **176.3%** | 0.000% |
| 2 Publisher / 2 Subscriber | 1 MiB | **29.206 M/s** | 15.214 M/s | **192.0%** | 0.884% |
| 2 Publisher / 2 Subscriber | 4 MiB | **30.158 M/s** | 25.127 M/s | **120.0%** | 0.386% |
| 2 Publisher / 2 Subscriber | 64 MiB | **28.423 M/s** | 17.743 M/s | **160.2%** | 0.000% |

2P2S 的每条消息交付两次；salias 4 MiB 对应 **60.316 Mmsg/s** 交付吞吐。Ordered 是
Aeron 不具备的跨 Publisher 全局全序能力，最佳结果为 2P1S batch=8、64 MiB 的
**42.856 Mmsg/s**，以及 2P2S 的 **34.207 Mmsg/s** 发布 / **68.414 Mmsg/s** 交付。

## 目录

```text
src/core/platform/   mmap、memfd、hugetlb、prefault
src/core/ring/       MagicRing、AtomicCell
src/core/frame/      frame header、codec、sequence
src/core/flow/       基础 producer/consumer
src/core/wait/       SpinPause 与 WaitStrategy concept
src/core/channel/    单一 hybrid per-producer-ring 引擎
src/salias/          四种公共 Mode 与具名 IPC facade
test/                platform/ring/frame/flow/channel/API 测试
bench/               stress 与 IPC compare 目标
example/             FIFO MPSC 示例
```

## 性能边界

- `fifo` 对齐 Aeron 的 per-publication FIFO 语义，是追平 Aeron 的性能路径。
- `ordered` 提供 Aeron 不提供的跨生产者全序，因此必须承担一个共享 rank 与归并点。
- Ring 容量按每个 Producer 独立配置。最终独立进程测试中，FIFO 的最佳绝对吞吐出现在
  1/4 MiB；64 MiB 虽消除背压，但扩大工作集后吞吐下降。容量主要用于突发吸收，不能替代
  Consumer 稳定吞吐能力或受控的 `publication_window`。
- ARM Colima VM 只用于功能回归；最终性能验收必须在公平绑核的 x86 真机完成。
- 当前唯一有效的性能结论见 `doc/performance-report.md`。

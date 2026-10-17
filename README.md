# salias

`salias` 是支持 Linux 与 macOS 的 C++23 低延迟具名 IPC 消息通道。当前实现收敛为一个
per-producer magic-ring 引擎，并通过排序模式与消费者拓扑提供四种基础通道，以及
面向低延迟 fanout 的 `LatestMpmc` 保留窗口策略：

| Mode | 排序语义 | 消费者拓扑 |
|---|---|---|
| `FifoMpsc` | 每生产者 FIFO，不保证跨生产者顺序 | 单消费者 |
| `FifoMpmc` | 每生产者 FIFO，不保证跨生产者顺序 | 多消费者 fanout |
| `OrderedMpsc` | 跨生产者全局全序 | 单消费者 |
| `OrderedMpmc` | 跨生产者全局全序 | 多消费者 fanout |
| `LatestMpmc` | 每个生产者内 FIFO；慢消费者跳过保留窗口外的旧消息 | 多消费者 fanout |

`FifoMpmc` 和 `OrderedMpmc` 会将每条消息广播给每个订阅者，不是竞争式消费队列。

FIFO 模式的生产者热路径不执行共享 RMW；ordered 模式通过单个全局 sequence
`fetch_add` 建立跨生产者全序。每个生产者拥有独立 ring，每个 fanout 消费者拥有独立的
per-producer 游标。

`LatestMpmcChannel<Capacity, HighWatermarkPercent>` 将每个 producer ring 的字节容量和
保留水位固定为模板参数，水位默认是容量的 90%。达到水位后淘汰最旧消息；每个订阅者独立
追赶保留窗口。`connect()` 会校验共享通道与模板参数一致。尚未 `release()` 的借用消息会
pin 住对应 payload；若写入会覆盖仍借用的字节，发布暂时返回 `BackPressured`，释放后即可继续。
若订阅进程在持有借用时退出，生产者会在需要复用该槽位时校验进程身份并清除失效 lease。
LatestMpmc 默认使用 `SubscriptionPolicy::DropOldest`；可为单个订阅者选择
`SubscriptionPolicy::Reliable`，让其未读游标参与保留边界并在慢读时对生产者施加背压。
其他模式仅支持 Reliable 策略。

`LatestByKeyChannel<Key, Value, Capacity, SchemaVersion>` 是独立的有界 latest-value IPC 表，按 key
保存最新值，重复 key 更新覆盖同一槽位；订阅者各自接收当前快照，未读期间的中间更新会合并，返回的是
本地拷贝而非共享内存借用。`Capacity` 表示最大不同 key 数，`SchemaVersion` 默认为 0；该默认值
仅使用编译器类型签名，不能可靠发现同名同尺寸类型的布局或语义变化。应用应在 key/value 布局或
语义变化时显式递增版本，使不兼容的进程连接被拒绝。满表时新 key 返回
`Error::CapacityExceeded`，已存在 key 仍可更新。Key 必须可平凡拷贝、具有唯一对象表示且不是指针；Value
必须可平凡拷贝且自身及其成员都不能包含进程内指针。不同 key 之间不承诺全局顺序，锁竞争通过
`try_recv_checked()` / `Publisher::offer()` 的 `Error::BackPressured` 报告；简化的
`try_recv()` 将错误折叠为空结果，`recv()` 返回 `Result<LatestByKeyMessage<...>>` 并只重试临时背压；
`conflated_updates()` 统计端点尚未读取前被合并掉的更新数。fork 前必须暂停所有使用该 channel 的
线程和操作；静置 fork 后子进程可使用继承端点，fork 时有线程持有内部锁不受支持。

独立头文件为 `<salias/latest_by_key_channel.hpp>`，对应端点和消息类型为
`LatestByKeyPublisher`、`LatestByKeySubscriber` 和 `LatestByKeyMessage`。

Pub/Sub 端点不提供内建 CPU 绑核接口，应用可在调用端点的线程上使用操作系统 affinity API。
当前内存映射实现不支持 NUMA 放置，指定 NUMA 节点会返回 `NumaUnavailable`。

`PriorityPublisher<Mode...>` 和 `PrioritySubscriber<Mode...>` 把多条独立 IPC 通道组合成按优先级
路由与读取的端点组，模板参数从高到低排列。发布操作只触及指定通道，因此某一路背压不会
占用其他通道的容量。订阅端以权重 `N, N-1, ..., 1` 做 work-conserving 加权轮询，保证持续
有数据的低优先级通道不会饥饿；各通道内部仍保持原顺序，通道之间不承诺全局顺序。返回消息
带有 priority 索引并借用原通道内存，必须通过组合订阅端 `release()`，或使用会自动释放的
`poll()`。例如两级 FIFO 通道可用 `PriorityPublisher<Mode::FifoMpsc, Mode::FifoMpsc>` 创建路由端。

## 构建

要求 Linux 或 macOS、CMake 3.25+、支持 C++23 的 GCC/Clang。测试目标需要 GTest，
压力 benchmark 目标还需要 Google Benchmark；hotpath_ab 与 receiver_scan 无此依赖。
预设使用 Ninja，也可直接选择其他 CMake generator。

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

## mcpp 与 C++23 模块

原生包身份为 `gzj-creator.salias`，版本 `2.1.0` 与 CMake 一致。`mcpp.toml`
编译公共库和必要的 core 实现，`src/salias.cppm` 提供 `import salias;`；默认构建不需要
GTest、Google Benchmark 或 Aeron。模块导出公共配置、枚举、错误和结果类型、Message、
Channel/Publisher/Subscriber/PublishClaim 模板、五种模式的端点别名、静态配置的
`LatestMpmcChannel` 和 `LatestByKeyChannel` 及其消息/发布/订阅端点，以及订阅交付策略
`SubscriptionPolicy`。应用专用别名由消费者维护。

安装 mcpp 和 LLVM 工具链后，在仓库中构建和验证：

```bash
mcpp build --offline --release --jobs 4
mcpp run test_mcpp_import --offline --release --locked --features mcpp-tests
python3 tools/check_mcpp.py
```

本次验证环境为 Linux x86_64、mcpp `2026.10.11.1`、LLVM `22.1.8`。
代码保留 Linux/macOS 平台实现；macOS 的 mcpp 构建尚未在本机验证。
`mcpp-tests` 是显式启用的测试 feature，默认不编译测试。
测试覆盖五种模式的模块与头文件混合消费、实际收发、claim/commit、非对齐 payload 的 offer_batch、
TTL 发送/过期跳过、模板 poll 和共享内存清理。`check_mcpp.py` 在两个临时消费者目录进行离线构建，
并在生成锁文件后复验 `--locked`。

等长 `offer_batch` 保留每条消息的原始 payload 长度，接收视图不包含 ring 对齐填充。
批量长度契约测试覆盖空消息、非对齐长度、回绕以及四种基础模式的跨进程收发和清理；
`LatestMpmc` 另覆盖跨进程慢消费者跳过旧消息、借用消息 pin 以及异常退出 lease 回收。
专项回归还覆盖 1%/50%/90%/100% 水位、完整帧边界、空消息、超限消息、部分批次、混合长度回绕、
序列号低位回绕、三生产者与单条/批量订阅者并发、批内 payload 保留及 `SIGKILL` 后的 lease 回收。
消息数窗口回归覆盖可靠模式的单条/批量背压、批量 poll 后恢复、fanout 慢订阅者边界、
LatestMpmc 的部分批次、可靠订阅者保护，以及变长空消息和物理回绕后的保留后缀。
共享通道协议当前为 v12；旧布局通道需重建，发布端和订阅端需一起升级。

`Publisher::offer_for`、`try_claim_for` 与 `offer_batch_for` 可为消息附加 TTL，截止时间在 API
调用时采样，因此 `try_claim_for` 返回后的 payload 填充时间也计入 TTL。截止值以同一主机
`CLOCK_MONOTONIC` 纳秒值写入扩展帧元数据；订阅端在交付前跳过过期帧，
并通过 `expired_messages()` 统计本端观察到的过期消息。未过期帧仍遵循原有 Reliable/DropOldest
策略，借用中的消息即使随后过期也必须先 release。TTL 批次共享同一截止值；TTL 头开销计入容量与水位。
poll/recv 跳过过期前缀时立即推进游标，但只推进到下一条存活借用帧的起点；过期清理由接收端惰性
触发，接收端停止轮询时没有后台线程替它回收队列容量。

LatestMpmc 可使用 `Subscriber::try_recv_checked()` 显式识别丢旧：首次观察到保留窗口已经
越过本地游标时返回 `Error::Lagged`，随后重试从保留窗口读取。`lagged_messages()` 和
`skipped_messages()` 分别统计被淘汰和主动跳过的消息；`skip_to_latest(producer_id)` 跳过
指定 producer ring 的当前保留帧到最新已提交帧，但保留最新帧未读。Reliable 订阅者不会产生
`Lagged`，也不支持丢旧跳读。

LatestMpmc 专项压测分别测量即时消费、停滞消费者持续丢旧和一快一停滞消费者的并发扇出，
每种场景比较 50% 与默认 90% 水位；停滞场景输出保留/丢弃计数，并发场景输出接收/丢弃/背压重试计数，
同时检查消息序列。
消息数窗口压力基准让 DropOldest 订阅者停滞，在持续发布期间只保留最新 64 条消息，并校验
丢弃计数与保留后缀。它用于比较按消息数约束时的发布热路径，不构成长期性能基线。

```bash
cmake --preset release
cmake --build --preset release
./build/release/bench/salias_bench_channel_stress \
  --benchmark_filter='BM_latest_' --benchmark_min_time=0.1s --benchmark_repetitions=3
./build/release/bench/salias_bench_channel_stress \
  --benchmark_filter='BM_latest_by_key_roundtrip_64b' --benchmark_min_time=0.1s --benchmark_repetitions=3
```

短跑用于正确性与吞吐冒烟，不代替长期压力测试。正式性能基线需同时使用 Release 构建的
Google Benchmark 依赖，并固定 CPU、负载和测试环境。

按共享 [gts-mcpp-dependencies skill](.codex/skills/gts-mcpp-dependencies/SKILL.md)，
将源码包准备在 `/opt/mcpp-deps/salias/`，消费者使用固定绝对路径：

```toml
[dependencies]
"gzj-creator.salias" = { path = "/opt/mcpp-deps/salias" }
```

本机 `/opt/mcpp-deps` 指向 `/data/disk1/projects`；其他机器也可通过软链接提供同一共享路径。
共享源码修改后消费者重建即可生效，不需要生成 gts-quote 模块封装。
头文件消费者需要 `src` 和 `src/salias/include` 两个包含目录；模块消费者使用：

```cpp
import std;
import salias;

auto channel = salias::FifoMpscChannel::connect("quotes");
```

模块入口在全局模块片段包含现有头文件，并重新导出原声明；公共类型仍属于头文件 ABI。
测试中的头文件编译单元与模块编译单元链接同一个库，传递 `salias::Config` 并实际收发消息。
CMake 的头文件 API 和安装目标继续保留，CMake 当前不安装 mcpp BMI。

这里完成的是本地共享源码包消费。`namespace` 和版本声明不代表索引发布。
`MCPP_HOME` 控制 mcpp 工具和注册表存储，不能代替清单中的固定源码路径；
编译产物和 BMI 由 mcpp 根据工具链及选项生成，共享源码不保证所有消费者复用同一产物，
也不能跨不兼容的编译器或标准库直接复用 BMI。

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
auto owner = salias::FifoMpmcChannel::create(config);
```

`Publisher` 和 `Subscriber` 持有底层 Tx/Rx 端点，因此生产者背压缓存、消费者 ring 游标和
`poll` 批次状态会跨 API 调用保留。

## Huge Pages

`Config::huge` 支持 `None`、`Size2MB` 和 `Size1GB`。Linux 上的显式大页请求使用
hugetlbfs，失败时返回 `PlatformFail`，不会降级。默认目录为 `/dev/hugepages`，可通过
`SALIAS_HUGETLBFS_DIR` 覆盖。macOS 支持普通页模式；显式 HugePage 属于 Linux 专属能力。
NUMA 放置当前尚未实现，Linux 与 macOS 请求都会明确返回 `NumaUnavailable`。

普通页与大页 ring 在计时前使用 `MAP_POPULATE` prefault。控制块使用 POSIX shared memory。

## 工具

- salias_receiver_scan：预填充后单独计时接收，覆盖 FIFO/Ordered、稀疏环与批量路径。
- salias_hotpath_ab、bench/compare.py：热路径基准与保留原始结果的 ABBA 对照脚本。
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

2026-09-25 的 FIFO 消费热路径改动、对照数据与回归测试见
[本地优化验证报告](doc/performance-optimization-2026-09-25.md)。该报告仅验证本次改动，
测量环境和指标与以下独立进程验收不同。

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
src/salias/          五种公共 Mode 与具名 IPC facade
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

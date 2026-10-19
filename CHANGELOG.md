# Changelog

## Maintenance Notes

This file records user-facing and engineering-significant changes before they
are released.

- Keep pending local commits under `## [Unreleased]`.
- When creating a release tag, move relevant entries into a version section
  formatted as `## [vX.Y.Z] - YYYY-MM-DD`.
- Group changes by `Added`, `Changed`, `Fixed`, `Docs`, or similar sections.
- Prefer concise behavioral summaries over mechanical file lists.
- Use major for breaking or large changes, minor for new capabilities, and patch
  for fixes; update this file before committing and collect changes since the
  previous tag when releasing.

## [Unreleased]

### Changed

- `Subscriber::poll` 的两种重载改用调用独占的栈上描述符存储，移除空轮询时对 32 个 `Message` 的无用清零；保留公开默认值、批处理边界、借用与 flush 时机，不引入端点共享缓冲或堆分配。
- 补充空批次、递归轮询、默认初始化和变长帧回绕的回归测试，以及覆盖五种模式和两种 poll 重载的独立 ASan/UBSan 契约测试；文档明确未填充描述符不可读取。

### Added

- Salias/Aeron 对比工具增加开环 planned/attempt 计时、受限资源串行运行器和逐轮汇总；补充真实 perf/火焰图、阶段探针、调度关联及隔离构建的布局/轮询设计原型，不将不安全实验接入产品。
- Publisher 尾延迟工具增加独立诊断开关、逐 worker 资源快照、冻结二进制构建和三组串行 ABBA 入口；正式汇总拒绝诊断、故障注入、配置不匹配及被篡改的原始文件，逐 publisher 合并桶并分别执行 20% 收益 / 5% 退化门禁。
- 新增合成行情 IPC 验收工具：按原计划到达时钟评估 2P1S/2P2S、64/256 B 稳态与突发负载，逐消息核验序号和 payload，记录背压、输入完成性及每个订阅者时延；提供交错 A/B、环境采集和原始桶汇总脚本。
- 新增 `Channel<Mode, Schema>` 固定布局通道：以应用声明的非零 schema ID、显式版本和 `message_type` 绑定类型；连接时拒绝 raw/typed 混连及 schema 不匹配，typed publisher 提供定长 claim、typed subscriber 只返回本地复制值。
- 新增 `Publisher::send<T>()` / `send_for<T>()` 与 `Subscriber::try_recv_copy<T>()` / `recv_copy<T>()`，为固定布局的平凡可复制类型提供无需逐消息堆分配的类型化发送和本地复制接收；不兼容长度返回 `PayloadSizeMismatch` 并释放消息。
- 新增 `PriorityPublisher<Mode...>` / `PrioritySubscriber<Mode...>`，将独立 IPC 通道按模板顺序组合；发布按优先级路由且各通道背压隔离，订阅端以 N..1 加权轮询避免低优先级饥饿，并标记消息来源通道。
- 新增静态容量的 `LatestByKeyChannel<Key, Value, Capacity, SchemaVersion = 0>` IPC latest-value 表，跨进程按 key 合并重复更新；订阅者获取独立的当前值快照，值以本地拷贝返回，满表的新 key 明确返回 `CapacityExceeded`。
- 为 IPC 消息增加 per-message TTL：`offer_for`、`try_claim_for`、`offer_batch_for` 将非负时长编码为同主机 `CLOCK_MONOTONIC` 截止值；订阅端在 poll/recv 时跳过过期帧并及时释放过期前缀，未过期帧和已借用 payload 仍受 Reliable/DropOldest 原契约保护。
- 新增 `Config::publication_window_messages`，按每个 producer ring 限制在途/保留消息数；可靠模式达到窗口后背压，LatestMpmc 的 DropOldest 订阅可淘汰最旧帧，Reliable 订阅仍保护未读帧。该窗口可与字节窗口并用，LatestMpmc 仍要求字节窗口为 0。
- 为 `LatestMpmc` 增加按订阅者选择 `Reliable` 或 `DropOldest` 的交付策略；可靠游标限制保留头推进，慢的可靠订阅者会触发背压而不会丢失未读消息，丢旧订阅者仍可独立跳过过期窗口。其他模式拒绝 `DropOldest` 策略，共享控制段版本升至 v10。
- 为 IPC 订阅者增加可复用注册槽：最后一个 `Subscriber` 副本销毁时注销，后续注册回收进程崩溃遗留槽位；代际令牌隔离旧端点，late join 从稳定的 reservation 尾开始。新增 `Channel::try_subscriber()` 与 `Error::NoSubscriberSlots`，共享控制段版本升至 v9。
- 新增编译期定容量的 `salias::Ringbuffer<T, Capacity, HighWatermarkPercent>`，默认以 90% 高水位丢弃最旧元素并保留最新数据；明确该类型为进程内单线程容器，不改变 IPC channel 的背压语义。
- 新增 `Channel::connected_consumer_count()`，按共享注册槽记录的 PID 与进程启动标识统计仍存活的消费者，并在跨进程消费者退出后将其排除；该诊断能力不回收槽位，也不改变背压语义。
- 新增 `LatestMpmc` IPC 通道策略及对应 Publisher / Subscriber 端点；`LatestMpmcChannel<Capacity, HighWatermarkPercent>` 在编译期固定 ring 容量与保留水位（默认 90%），慢消费者可跳过保留窗口外的消息，连接时校验模板配置，并在借用 payload 时防止覆盖、在订阅进程退出后回收失效 lease。共享控制段版本升至 v8，旧布局通道不能与新版本交叉连接。
- 新增 6 项 LatestMpmc 专项压测，覆盖 50%/90% 水位下的即时消费、停滞持续丢旧和并发扇出，校验保留后缀与消息顺序，并输出接收、丢弃和背压重试计数；并发场景按实际经过时间计量，README 补充复跑命令。
- `Subscriber::try_recv_checked()` 在 LatestMpmc 观测到消费者进度被淘汰时返回 `Error::Lagged`，并推进到可读保留窗口；新增 `skip_to_latest()`、`lagged_messages()` 和 `skipped_messages()`，支持显式跳到指定 producer ring 的最新已提交帧，同时保留缺口计数。

### Fixed

- LatestByKey 使用内核自动释放的跨进程文件锁和双缓冲提交字；进程在更新中被杀不会暴露半写入的值，也不会永久占住通道锁。直接指针 key/value 在编译期拒绝，避免把进程地址当作共享数据。
- LatestMpmc 单帧与等长批次严格受保留水位限制，单帧超限返回 `MessageTooLarge`，不再扩大窗口；空消息避免向空指针执行零长度拷贝，超出 32 位长度的输入不再截断。
- 将拟淘汰位置 `retention_intent` 与实际保留头分离：lease 检查成功后才淘汰旧帧，受 pin 阻止的单条或批量写入不再额外丢消息或消耗序列号；消费者在公布 lease 后核对 intent 和保留头快照，无需共享自旋锁。
- 淘汰前校验帧的已提交标志与长度边界，慢消费者跳读时同步本地和共享进度；修正 claim 注释，明确未提交空洞的可见性与不支持放弃 claim 的真实契约。
- 修复静态 LatestMpmc 包装器复制配置时泄漏 `bad_alloc`，以及共享引擎/订阅端初始化错误的 `noexcept` 导致分配失败直接终止进程的问题；将创建阶段的 reserve 纳入回滚范围，通过移动清理名称避免 OOM 路径遗留共享内存。

### Changed

- Reliable FIFO 发布慢路径先检查字节/消息窗口游标，仅对真正阻塞的消费者进行进程存活探测；保留注册代际复核、死亡回收、最慢消费者背压和批量安全前缀，不改变共享布局或 Latest 路径。该实现作为性能仍待改进的候选记录，不宣称全部配置尾延迟改善。
- 历史独立进程性能汇总增加 64 B payload 的发布/交付 MiB/s，区分消息吞吐、广播交付及逻辑有效载荷带宽。验收矩阵默认将临时原始输出放到 Git 忽略的 `build/benchmarks/`。
- 具名共享控制块协议升级至 v13，固定 schema payload 长度用于接收帧定位；raw 通道仍逐消息解析长度，schema-bound 路径不再做逐条 payload 大小相等比较。旧协议须重建，所有进程须同步升级。
- 帧头增加可选 8 字节 TTL 扩展元数据，TTL 开销计入容量、水位和批次步进；共享通道协议升至 v12，旧通道须重建并同步升级两端。
- 共享控制段协议升级至 v11，新增消息数窗口的 producer reservation、retention 和 consumer 进度字段；旧布局通道需重建并同时升级发布端与订阅端。
- 共享控制段协议升级为 v8，以支持独立淘汰意图字、LatestMpmc 丢旧/主动跳过计数；旧布局返回 `VersionMismatch`，需重建通道并同时升级发布端与订阅端。
- 公共多消费者模式统一命名为 `FifoMpmc`、`OrderedMpmc` 和 `LatestMpmc`，按 key 合并最新值的 channel、端点、消息及内部 storage 统一命名为 `LatestByKey*` / `latest_by_key_*`；`Mode` 数值保持 0..4，既有共享内存布局不变，下游源码需更新旧 API 标识符。
- `LatestByKeyChannel` 增加应用管理的 `SchemaVersion` 模板参数以显式拒绝不兼容的数据模式；阻塞 `recv()` 返回带错误的 `Result`，仅重试临时背压，并明确 fork 前静置线程的约束。

### Tests

- 新增 Reliable 探测 fixture 共 80 实例（78 通过、2 项单帧参数下的批量测试预期跳过），覆盖健康游标免探测、阻塞者探测、PID/注册复用、死亡回收、双窗口和借用期保护；全量 Release 20/20、定向 ASan/UBSan 与 TSan 各 3/3 通过。验收汇总回归共 12 项，改用独立合成输入，不依赖已清理的压测文件。
- 记录 2026-10-18 候选总计 30 分钟、计划 20.7 亿条的八配置验收：已发布消息完整交付，但截止时有 1 条计划输入未发送，50 µs p99.9 为 0/8 配置通过；交错 A/B 收益不一致，64 B 双订阅突发 p99.9 增加约 96.4%，不作为上线性能通过结论。
- 增加 schema create/connect 匹配与 ID/版本/raw 混连拒绝、五种模式固定长度解析、schema-sized claim、fanout 独立复制和 fork 跨进程 roundtrip；mcpp module/header 测试覆盖 typed connect。新增 raw-size-check 与 schema-bound fixed-parser 的单消费者/双消费者 benchmark；本机一次 0.1 秒冒烟中 raw/schema 分别为 124/126 ns (64B)、127/137 ns (256B)、202/209 ns (1KiB)，fanout 为 183/203 ns，未显示整体加速；Google Benchmark 库为 DEBUG 构建，结果不作为性能结论。
- 增加 typed send/copy receive 的类型约束、长度错误释放、LatestMpmc lag、TTL、五种模式 fanout、模块/头文件 roundtrip 与 1,000 次零堆分配契约测试；新增 64B/256B/1KiB typed-vs-borrowed roundtrip 和 64B 两订阅者 fanout benchmark。Release CTest 167/167、mcpp 混合消费通过；本机短基准中位约为 typed/raw 122/98 ns (64B)、134/105 ns (256B)、209/152 ns (1KiB)，两消费者 fanout typed/raw 195/155 ns。Google Benchmark 依赖是 DEBUG 构建，这些数据仅作冒烟参考。
- 新增优先级通道测试，覆盖 3:2:1 加权轮询、流内顺序、背压隔离、LatestMpmc Lagged 隔离、borrow/release 与 poll 自动释放、跨进程发布、无效和移动后端点；增加 64B weighted-roundtrip 基准及 mcpp 模块/头文件混合 IPC roundtrip。Release/ASan/UBSan 全量、优先级 TSan 专项与 mcpp roundtrip 均通过，结果记录在能力计划中；三次 0.5 秒基准中位 0.122 us，Google Benchmark 依赖为 DEBUG 构建，该数据仅供冒烟参考。
- 新增 LatestByKey 测试，覆盖重复 key 合并、独立及复制订阅游标、hash 冲突、满容量、模板/类型不匹配、事件日志溢出、版本与事件序列耗尽、跨进程写入、多线程一致性、锁竞争和写进程 `SIGKILL`；增加 64B 发布/接收往返压力基准及 mcpp 模块/头文件 roundtrip，并验证显式 schema 版本。
- 新增 TTL 回归，覆盖零/负/正时长、零拷贝 claim、均匀和变长部分批次、过期前缀释放同时保留后续借用帧、借用跨过期点仍受保护、LatestMpmc retention/loss、Ordered 跨 producer 序列、物理回绕和跨进程单调时钟。Release 142/142、ASan/UBSan 141/141 通过；TSan API suite 1/1 通过，完整 TSan 另有 4 个小型契约程序因运行时 `unexpected memory mapping` 启动失败。mcpp module/header TTL IPC 测试通过。TTL expiration 短基准 3 次中位数 0.259 us/迭代；Google Benchmark 依赖是 DEBUG 构建，结果仅作冒烟参考。
- 新增消息数窗口测试，覆盖 Ordered 多 producer 独立上限、字节/消息窗口独立生效、fanout 最慢消费者、部分批次、poll 后恢复、connect 后限制、LatestMpmc 可靠读者背压、空消息/变长帧与物理回绕；新增停滞订阅者保留 64 条消息的压测校验。Release 134/134、ASan/UBSan 133/133、TSan API suite 和 mcpp module/header IPC 测试通过；短基准 3 次复跑通过，Google Benchmark 依赖为 DEBUG 构建，结果仅作冒烟参考。
- 新增 20 个 LatestMpmc 回归测试，覆盖最小/默认/全容量水位、空消息和精确帧边界、超限与部分批次、混合长度回绕、序列号低位回绕、未提交空洞、非法帧头/lease 边界、借用与批量回调生命期、独立 producer/consumer 和 `SIGKILL` 后的失效 lease 回收。
- Linux x86_64 上 ASan/UBSan 全量 99/99、Release 全量 100/100（含 benchmark smoke）通过；三生产者与单条/批量双订阅者并发回绕在 ASan/UBSan、Release 下各复跑 50 轮，TSan 下复跑 10 轮。
- 分配契约测试逐点注入五模式及静态 LatestMpmc 的创建、连接和订阅端分配失败，验证 typed error/异常传播与同名通道重建。静态包装器 OOM 测试修复前失败、修复后通过；五模式 mcpp 模块/头文件混合消费复验通过。macOS 和长期压力运行未在本次验证。
- 6 项专项短压测各重复 3 次，保留后缀及并发顺序校验通过；当前 Google Benchmark 依赖提示以 DEBUG 构建，结果不作为正式性能基线。

### Docs

- 补齐 2026-10-19 Salias/Aeron 延迟对比、设计诊断、实现与后续候选报告；仅收录逐轮数据、8 组直接引用的真实 profile 及精简核验记录，明确约 100 ns 差距尚未完整归因、六个候选未晋升，不构成长时尾延迟 SLA 验收。
- 补齐 2026-10-18 三组 ABBA、96 个正式阶段的相对收益失败报告；被否决的产品补丁不发布，48 分钟测量不等于通过性能验收，原始大文件和临时日志仅保留于本地 build 目录。
- 汇总 Publisher 时延、基线/候选长测、交错 A/B、安全审查及使用边界；保留最终表、精简标量结果与来源哈希，将本日压测目录从 283 个文件、27.4 MB 精简至 4 个文件、约 0.29 MB。删除原始桶及临时日志后不能重新计算历史百分位，文档明确该限制。
- 说明显式 wire schema、连接时检查、固定帧步进、`SchemaMismatch` 与 v13 不兼容边界，并区分 raw 泛型复制 API 和 schema-bound API。
- 补充 per-message TTL API、同主机单调时钟域、接收时惰性过期、借用期约束和 v12 协议兼容说明，并增加 TTL drain 压测入口。
- 补充消息数窗口的配置单位、可靠/丢旧模式语义、批量部分成功行为、协议版本和压力基准说明。
- 补充完整帧水位计算、失败写入不淘汰、每环借用限制、批量回调 pin 和协议兼容要求。
- 补充 checked 接收、LatestMpmc 主动跳读、缺口计数和可靠模式不丢失的 API 语义。
- 补充 LatestByKey schema 版本、接收错误、fork 约束及 Pub/Sub 绑核和 NUMA 能力边界说明。

## [v2.1.0] - 2026-10-11

### Added

- 原生 mcpp 包 `gzj-creator.salias` 和 `import salias;` 模块入口，默认编译公共库及必要的 core 实现，不要求 GTest、Google Benchmark 或 Aeron。
- 提供显式启用的模块/头文件混合消费测试，以及两个独立消费者目录的共享绝对路径、离线与 locked 构建验证。

### Fixed

- 修复进程内和跨进程的等长批量发布丢失原始 payload 长度的问题：`BatchClaim` 保存 `payload_len`，commit 帧头写原始长度，ring 步进保留对齐后的 `frame_len`。例如 4 字节批量消息接收长度保持 4 字节，不再包含 4 字节填充。
- 新增批量长度契约测试，覆盖 24 种长度（含空消息、非对齐和 800 字节）、部分批次、反复回绕、FIFO/Ordered、四种跨进程通道和双消费者 fanout，逐条核对长度与内容并验证共享内存清理。

### Changed

- CMake、vcpkg 和 mcpp 包版本统一为 `2.1.0`，版本一致性检查纳入 mcpp 清单。使用 `BatchClaim` 的源码和模块消费者需要重新编译；共享帧头和控制段布局保持不变。
- 对齐 salias/Aeron 最终基准的 64B payload、`try_claim`/`tryClaim` 零拷贝发布与空转/背压 `yield` 策略，新增可选 `--aligned` 模式，减少测试策略差异对 MPSC/扇出结论的干扰。
- 优化进程内与跨进程 MPSC 热路径：用连续已提交水位统一发布帧头与 payload，为乱序 commit 保留空洞保护，并缓存 ring 几何信息以减少原子操作、取模和重复共享状态读取。
- 扩展 FIFO/Ordered 批量接收与轮询逻辑，在三生产者、稀疏 ring、跨 ring 空洞、部分批次和 fanout 背压下保持顺序、公平性与 payload 生命期。
- 新增 `salias_hotpath_ab`、`salias_receiver_scan` 及 ABBA 对照脚本，并保留 2026-09-25 的热路径、扫描、多生产者与被拒绝方案原始结果。

### Tests

- 新增混合策略独立交付、可靠订阅者高水位背压、single/batch 借用期间防淘汰、精确 lag、策略非法值及不支持模式拒绝测试；mcpp 模块导出并验证订阅策略 API，增加策略 roundtrip benchmark。
- 新增订阅槽位耗尽与复用、末副本注销、分配失败回滚、late join 边界、旧 generation endpoint 隔离、正常退出与 SIGKILL 回收、fork 子进程析构隔离，以及发布期间并发注册/注销覆盖。
- 修复前批量长度测试失败，修复后通过；release CTest 全量、ASan/UBSan 批量长度测试、mcpp 混合消费和外部消费者、gts-quote 契约与跨进程背压/清理验证通过。macOS mcpp 构建尚未实测。
- 新增 commit 可见性契约测试，覆盖同一 producer 连续 claim 后乱序 commit，验证 consumer 不会越过未提交空洞且共享可见位置不会回退。
- 新增 FIFO 三 ring 公平扫描、Ordered 跨 ring 批量、fanout 慢消费者保留、并发回绕背压与 SPSC 本地游标回归测试。

### Docs

- 说明固定 `/opt/mcpp-deps/salias` 路径消费、模块导出边界、源码与 BMI 缓存关系、版本和平台验证范围；共享 `.codex` 使用指向 gts-ai 的链接。
- 更新 L7 API、优化复盘与低延迟窗口建议，新增 2026-09-25 性能优化验证报告和可复核的基准原始记录。

## [v2.0.0] - 2026-07-12

### Changed

- 移除未实现但曾公开暴露的 `Config::fixed_size` 与 `Config::record_size`，明确当前公共协议只支持变长帧；控制段以保留槽位维持内部布局尺寸。
- `Channel::publisher()` 与 `Channel::subscriber()` 不再错误声明为 `noexcept`，端点状态分配失败可正常传播 `std::bad_alloc`，避免触发 `std::terminate`。
- 公共配置与 L7 API 文档明确 ring 容量必须同时满足 2 的幂、系统页对齐，以及显式大页大小对齐要求。

### Fixed

- 新增 `Error::OutOfMemory`，`Channel::create()` 与 `Channel::connect()` 捕获动态内存分配失败并通过 `std::expected` 返回 typed error。
- 创建通道过程中发生分配失败时回收已创建的控制段与 ring 名称，避免 OOM 路径遗留共享内存资源。
- CMake 与 vcpkg 包版本统一为 `2.0.0`，新增 CMake/vcpkg/精确 Git tag 一致性检查，修复安装包版本长期停留在 `0.0.1` 的问题。

### Tests

- 新增不依赖 GTest 的配置表面、容量约束和分配失败契约测试，并保留安装消费者测试覆盖。

### Docs

- 更新 L7 公共 API 文档，并新增 API 契约对齐设计与实施计划。

## [v1.3.0] - 2026-07-12

### Added

- CMake 安装导出能力：顶层新增 `install` 规则导出 `saliasTargets`（`salias::`
  命名空间）、安装 salias 与 core 公共头，并通过 `CMakePackageConfigHelpers`
  生成 `saliasConfig.cmake` / `saliasConfigVersion.cmake`（`SameMajorVersion`
  兼容）。下游工程可通过 `find_package(salias CONFIG REQUIRED)` +
  `target_link_libraries(... salias::salias)` 直接消费。`salias_core` 与
  `salias` 补 `salias::` ALIAS 目标，`salias` 增加 `INSTALL_INTERFACE` 头路径。
- 安装消费冒烟测试：新增 `cmake/saliasConfig.cmake.in` 包配置模板、
  `cmake/install_consumer_test.cmake` 安装并配置/构建下游工程的脚本，以及
  `test/install/` 最小 `find_package` 消费示例，统一挂到 CTest
  `salias_install_consumer`。
- 测试新增 `LongChannelNameSupportsCreateAndConnect`（最长 128 字符通道名创建 /
  连接 / 收发）与 macOS 专属 `ExplicitHugePagesAreUnavailableOnMacOS`。

### Changed

- `channel.cpp` 对公共通道名引入 FNV-1a 64 位哈希编码（`channel_name_hash` +
  `encoded_channel_name`），控制段与 ring 的 POSIX shm 名称统一改用固定 16 位
  hex 键（`/salias-<16hex>-c`、`/salias-<16hex>-r-<id>`），Linux 与 macOS 共用
  同一资源寻址，规避 macOS 对 POSIX shm 名称的短限制。公共 API、字符集与最大长度
  不变。
- README 由 "Linux-only" 改为 "支持 Linux 与 macOS"，补充 CMake 安装与
  `find_package` 消费说明。

### Fixed

- `create_named_state` 在非 Linux 平台（macOS）对显式 `HugePage` 请求提前返回
  `PlatformFail`，不再依赖后端隐式失败或触碰 Linux 专属 hugetlbfs 路径。

### Docs

- 新增 `docs/plans/2026-07-11-macos-install-support-design.md` 与
  `2026-07-11-macos-install-support.md` 设计与实现计划。

## [v1.2.0] - 2026-07-11

### Added

- Added independent-process salias and Aeron benchmark workers for Publisher and
  Subscriber roles, coordinated externally without forking benchmark workers.
  The final matrix covers 2P1S/2P2S, 1/4/64 MiB capacities, FIFO fairness,
  Ordered batch 1/8, message validation, and backpressure statistics.
- Added a standalone interactive HTML animation explaining Channel,
  Publisher/Subscriber, Producer/Consumer, per-producer MagicRing, ordering,
  fanout, release, and backpressure message flow.

### Changed

- Replaced historical performance comparison documents and logs with one final
  independent-process report, machine configuration, 480 validated raw samples,
  generated summaries, and the core salias/Aeron table embedded in README.
- Updated the optimization retrospective to treat the final independent-process
  report as the sole current performance truth source.

### Chore

- Added `*.log` to `.gitignore` so benchmark and runtime logs are no longer
  tracked by version control.

### Removed

- Removed superseded Tencent/Aeron comparison reports and intermediate benchmark
  logs that used earlier harnesses or conflicting measurement conditions.

## [v1.1.0] - 2026-07-11

### Added

- macOS platform support: the magic-ring double-mapping backend now builds and
  runs on macOS alongside Linux. macOS backs the ring with an immediately-unlinked
  `mkstemp` temp file where Linux uses `memfd_create`; `MAP_POPULATE` is applied
  only where the platform defines it (via a `kPopulateFlag` fallback in
  `mapping.cpp` and `channel.cpp`), and Linux-only headers (`linux/memfd.h`,
  `sys/syscall.h`) are guarded by `__linux__`. Explicit huge-page backends remain
  Linux-only (rejected on macOS).
- Channel publication flow-control window: a new `Config::publication_window`
  (bytes, `0` = full ring) caps how far a producer may lead its consumer at
  `min(ring_capacity, window)` in-flight bytes, letting steady-state queueing
  latency be driven down from "full ring" to "window size" without changing the
  ring geometry or wraparound. Plumbed through `hybrid_control.hpp`,
  `hybrid_mpsc.hpp`, and `shared_hybrid_mpsc.hpp` via a new
  `effective_capacity()` gate, and validated at named-channel create/connect
  (a non-zero window must fall in `[page, capacity]`).
- Consumer batch-receive hot path on both hybrid engines: a new
  `try_recv_run()` drains a single ring in a tight loop, while `consume()` /
  `flush_progress()` collapse per-message shared release-stores to a per-batch
  per-ring pair, and a cached `visible_producer_pos` drops cross-core
  acquire-loads from per-message to per-batch per-ring.
- Inlined L7 `Subscriber::poll` handler: the template path now batches into a
  32-entry buffer and runs the handler in-header, removing the per-message
  `.so` indirect call and flushing progress once per batch.
- `--publication-window` option on the `salias_ipc_compare` harness.
- Eight GTest cases covering window-bounded inflight, zero-window full-ring
  regression, batched flush reclaiming space, cached visible-position staging,
  and same-ring run draining / truncation for FIFO and Ordered.
- Two minimal end-to-end demos (`example/minimal_fifo.cpp`,
  `example/minimal_ordered.cpp`) plus `example/README.md` showing the shortest
  path to FIFO vs Ordered MPSC semantics; wired into the build and test graph
  via `example/CMakeLists.txt`.

### Changed

- Relaxed the top-level build from Linux-only to Linux + macOS; added a
  `BUILD_TESTING` option, defaulted `SALIAS_BUILD_BENCHMARKS` to OFF, and made
  Google Benchmark / GTest optional (`find_package ... QUIET`) with graceful
  skip messages. `salias_ipc_compare` is now gated to Linux-only.
- Refactored `SharedHybridMpscChannel` sequence-number acquisition from an
  immediately-invoked lambda to a plain if/else on both the single-frame and
  batch claim paths (portability cleanup; behavior unchanged).
- Named IPC control block now carries `publication_window`; `kNamedVersion`
  raised `3 -> 4` (binary-incompatible protocol bump).

### Docs

- Added `doc/13-optimization-retrospective.md`: a full retrospective of the
  optimization timeline (per-producer architecture → shared-state dedup →
  consumer hot path → frame-header micro-opts) with final recommended configs
  and measurements; referenced from `README.md` and the Tencent comparison doc.
- Appended the final 64-bit frame-header publish optimization re-verification to
  the Tencent 4 vCPU Aeron comparison (FIFO batch=1 lifted to 115.0% of Aeron;
  Ordered batch=8 at 93.4%), with the matching batch1/8/16 × 20-round raw logs.
- Updated the Tencent 4 vCPU Aeron comparison with flow-control-window and
  Phase A re-verification results (FIFO throughput lifted to ~85% of Aeron; a
  128 KiB window cuts FIFO p50 from 5.37 ms to 147 µs), plus the matching raw
  benchmark logs.
- Added Chinese Doxygen header docs and inline rationale comments across the
  entire C++ tree (src/test/bench/tools/example): file-level
  `@file/@brief/@details` with layering and thread/process model, per-function
  `@param/@return/@note`, and inline notes on memory ordering, flow control,
  sequence wraparound, and cache-line layout. Code unchanged (verified by a
  comment-stripped skeleton diff against the pre-change baseline).

## [v1.0.0] - 2026-07-10

### Added

- Redesigned the channel layer onto a per-producer hybrid magic-ring engine
  (`hybrid_control.hpp`, `hybrid_mpsc.hpp`, `shared_hybrid_mpsc.hpp`) where each
  producer owns a private ring and the consumer merges by `global_seq`, replacing
  the single-ring CAS MPSC/MPMC designs.
- Exposed four public channel modes through `salias::Mode`
  (`FifoMpsc` / `FifoFanout` / `OrderedMpsc` / `OrderedFanout`): FIFO modes keep
  the producer hot path free of shared read-modify-write, ordered modes establish
  cross-producer global total order via a single `global_seq.fetch_add`.
- Added `salias::HugePage` (`None` / `Size2MB` / `Size1GB`) on the public
  `Config`, mapping to `memfd_create(MFD_HUGETLB)` in-process and
  hugetlbfs-backed named IPC rings.
- Added runnable MPSC/MPMC publisher/subscriber examples under `example/` with a
  CTest smoke script.
- Added sampled p50/p99 one-way latency reporting to the salias and Aeron IPC
  comparison tools, backed by a header-only logarithmic histogram helper
  (`latency_histogram.hpp`) and unit tests.
- Added frame sequence split/encode helpers (`frame/sequence.hpp`) carrying the
  24-bit in-header sequence plus side high bits.
- Added design and implementation plan docs under `doc/plans/` for the hybrid
  ring, dual-engine redesign, huge page support, named huge IPC, throughput /
  CAS-backoff optimization, example pub/sub, and IPC latency sampling, plus a
  Tencent CVM Aeron comparison report.
- Initialized the Linux-only C++20 CMake project structure for salias.
- Added the documented layered source layout under `src/`, covering platform,
  ring, frame, flow, wait strategy, channel, metrics, and public API modules.
- Implemented L0 double mapping and futex primitives, plus focused tests.
- Added initial SPSC ring/channel flow, metrics counters, public API wrappers,
  benchmark smoke target, sanitizer presets, and GTest coverage.
- Added L5 MPSC, reliable Broadcast, and Bulk channel implementations with
  focused GTest coverage for committed-gap ordering, independent subscribers,
  slow-subscriber backpressure, and single-frame large messages.
- Extended the in-process L7 `Channel` facade to create SPSC, MPSC, Broadcast,
  and Bulk modes through the public `Config::mode` setting.
- Added named SPSC driverless create/connect over POSIX shared memory, including
  peer-first ready waiting and fork-based end-to-end API coverage.
- Added L7 named connect rejection coverage for version mismatches and
  untrusted metadata before mapping peer-controlled ring capacity.
- Added a channel stress benchmark covering SPSC, MPSC, Broadcast, and Bulk
  in-process workloads.
- Added release comparison runners and an Aeron C++ IPC baseline report for
  SPSC, SPMC, and all-to-all MPMC pub/sub workloads.
- Added L0 support for double mapping an existing shared-memory file descriptor.
- Added read-only `CountersReader::open(path)` support for external metrics
  observers over mmap-backed counter files.
- Added the project design documentation set under `doc/`.
- Added public `Subscriber::poll` batch consumption and public
  `Publisher::try_claim` / `PublishClaim::commit` zero-copy publishing APIs.
- Added an SPMC/MPMC hot-path design and implementation plan under `doc/plans/`.
- Added named cross-process MPSC create/connect support over POSIX shared memory.
- Added named cross-process MPMC fanout support where every subscriber receives
  every publisher's messages over one shared ring.
- Added a true multi-process salias IPC vs Aeron IPC comparison harness for
  MPSC and MPMC workloads.

### Changed

- Rewrote the public `salias` layer (`channel.cpp`, `channel.hpp`, `config.hpp`,
  `message.hpp`, `publisher.hpp`, `subscriber.hpp`) onto the four-mode
  per-producer engine.
- Rewrote `README.md` to describe the converged per-producer engine and the four
  modes, trimming the stale per-layer implementation status.
- Migrated the build and public/core result types to C++23 `std::expected`,
  removing the custom platform result wrapper and the `tl-expected` dependency.
- Expanded README coverage for the current API surface, Linux runtime
  dependencies, benchmark entrypoints, Aeron comparison limits, and known gaps.
- Localized and clarified API, core, benchmark, test, and platform comments so
  ownership, synchronization, and failure contracts are easier to audit.
- Updated `salias_bench_compare` to use batch polling with an explicit
  `--poll-limit` option for fairer comparison with Aeron's fragment limit.
- Updated the Aeron comparison runner to use forked producer/subscriber
  endpoints for both salias named IPC and Aeron IPC.

### Removed

- Removed the now-redundant channel implementations (`broadcast`, `bulk`, `mpsc`,
  `spsc`, `shared_mpmc`, `shared_mpsc`, `shared_spsc`, `channel_config`), the L6
  metrics module (`counters` / `layout` / `reader` / `error`), the platform futex
  primitive, and the `futex_wait` / `busy_spin` / `yielding` wait strategies,
  consolidating the wait layer onto `SpinPause` plus the hybrid engine.
- Removed the obsolete `channel_compare` and `smoke` benchmarks and the
  broadcast / bulk / mpsc / spsc / metrics / futex / wait-strategy unit tests,
  superseded by the hybrid channel and latency-histogram tests.

### Fixed

- Fixed ring capacity checks after position wrap so recreated publisher state
  cannot bypass backpressure through unsigned underflow.

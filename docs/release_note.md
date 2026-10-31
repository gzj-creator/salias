# Release Notes

本文件按时间顺序记录每次发版，每个版本节与同名的 git tag 对齐。tag 注解内容以此处对应版本节为唯一真源。

## v1.2.0 - 2026-07-11

- **版本级别**：次版本（minor）
- **Git 提交消息**：chore: 发版 v1.2.0
- **git tag**：v1.2.0
- **自上次 tag（v1.1.0）以来的变更摘要**：
  - **新增**：独立进程的 salias / Aeron 基准 worker（Publisher / Subscriber），由外部协调而非 fork；最终矩阵覆盖 2P1S/2P2S、1/4/64 MiB 容量、FIFO 公平、Ordered batch 1/8、消息校验与背压统计。
  - **新增**：独立交互式 HTML 动画，直观说明 Channel、Publisher/Subscriber、Producer/Consumer、per-producer MagicRing、顺序、fanout、release 与背压的消息流。
  - **变更**：用一份独立进程最终性能报告替代历史对比文档与日志，包含机器配置、480 份校验过的原始样本、生成的摘要，以及内嵌在 README 的核心 salias/Aeron 对照表。
  - **变更**：更新优化复盘，将独立进程最终报告作为当前唯一的性能真相来源。
  - **杂项**：将 `*.log` 加入 `.gitignore`，基准与运行时日志不再纳入版本库。
  - **移除**：移除被替代的 Tencent/Aeron 对比报告与采用旧测试框架或冲突测量条件的中间基准日志。

## v1.3.0 - 2026-07-12

- **版本级别**：次版本（minor）
- **Git 提交消息**：feat: 新增 CMake 安装导出能力并适配 macOS 共享内存命名
- **git tag**：v1.3.0
- **自上次 tag（v1.2.0）以来的变更摘要**：
  - **新增**：CMake 安装导出能力——顶层 `install` 规则导出 `saliasTargets`（`salias::` 命名空间）并安装 salias 与 core 公共头，通过 `CMakePackageConfigHelpers` 生成 `saliasConfig.cmake` / `saliasConfigVersion.cmake`（`SameMajorVersion` 兼容）；下游可用 `find_package(salias CONFIG REQUIRED)` + `target_link_libraries(... salias::salias)` 直接消费。`salias_core` / `salias` 补 `salias::` ALIAS 目标，`salias` 补 `INSTALL_INTERFACE` 头路径。
  - **新增**：安装消费冒烟测试 `salias_install_consumer`（`cmake/saliasConfig.cmake.in` + `cmake/install_consumer_test.cmake` + `test/install/` 最小下游工程）。
  - **新增**：最长通道名（128 字符）创建 / 连接 / 收发测试，以及 macOS 专属的显式 HugePage 不可用测试。
  - **变更**：控制段与 ring 的 POSIX shm 名称改为对公共通道名做 FNV-1a 64 位哈希编码后的固定 16 位 hex 键（`/salias-<16hex>-c`、`/salias-<16hex>-r-<id>`），Linux 与 macOS 共用同一资源寻址，解决 macOS shm 名称长度限制导致 `shm_open` 失败的问题；公共 API 不变。
  - **修复**：非 Linux 平台（macOS）显式 `HugePage` 请求在创建期即返回 `PlatformFail`，不再触碰 Linux 专属 hugetlbfs 路径。
  - **文档**：README 由 "Linux-only" 改为 "支持 Linux 与 macOS"，补充 CMake 安装与 `find_package` 消费说明；新增 macOS 安装支持设计与实现计划。

## v2.0.0 - 2026-07-12

- **版本级别**：主版本（major）
- **Git 提交消息**：fix: 对齐公共 API 契约与错误边界
- **git tag**：v2.0.0
- **自上次 tag（v1.3.0）以来的变更摘要**：
  - **破坏性变更**：移除未实现但曾公开暴露的 `Config::fixed_size` 与 `Config::record_size`，明确当前公共协议只支持变长帧；控制段使用保留槽位维持内部布局尺寸。
  - **错误边界**：新增 `Error::OutOfMemory`，`Channel::create()` 与 `Channel::connect()` 捕获 `std::bad_alloc` 并通过 `std::expected` 返回 typed error；OOM 回滚路径清理已创建的控制段与 ring 名称。
  - **异常契约**：移除 `Channel::publisher()` 与 `Channel::subscriber()` 的错误 `noexcept`，端点状态分配失败不再导致 `std::terminate`。
  - **配置契约**：公共注释和 L7 文档明确 ring 容量必须满足 2 的幂、系统页对齐，以及显式大页大小对齐要求。
  - **版本治理**：CMake 与 vcpkg 包版本统一为 `2.0.0`，新增 CMake/vcpkg/精确 Git tag 一致性测试，修复安装包版本仍为 `0.0.1` 的问题。
  - **测试与文档**：新增配置表面、容量约束、分配失败契约测试，以及 API 契约对齐设计和实施计划。

## v2.1.0 - 2026-10-11

- **版本级别**：次版本（minor），新增原生 mcpp 能力并包含批量长度修复。
- **Git 提交消息**：feat: 支持原生 mcpp 并修复批量消息长度
- **git tag**：v2.1.0
- **自上次 tag（v2.0.0）以来的变更摘要**：
  - **新增**：固定包身份 `gzj-creator.salias` 的原生 mcpp 清单和 `import salias;` 入口，导出公共类型、端点模板与四种模式别名；默认消费库不依赖测试或基准框架。
  - **修复**：进程内与跨进程批量 claim 保存原始 `payload_len`，commit 用它写入帧头，对齐 `frame_len` 继续控制 ring 步进；4 字节批量消息接收长度保持 4 字节，填充不进入 payload 视图。
  - **变更**：累计 MPSC 发布与接收热路径优化，连续已提交水位防止乱序 commit 越过空洞；FIFO/Ordered 批量扫描保持跨 ring 顺序、公平性和慢 fanout 消费者的 payload 生命期。
  - **基准**：对齐 salias/Aeron claim 路径、空转与背压策略，增加 aligned 模式、hotpath/receiver-scan 工具、ABBA 对照脚本以及 2026-09-25 原始结果和分析。
  - **测试**：新增 24 种 payload 长度、部分批次、回绕、四种跨进程模式和双消费者 fanout 的长度/内容/清理回归；保留 commit 可见性、跨 ring 空洞、公平扫描与并发背压测试。
  - **验证**：Linux x86_64 上 release CTest 69 项全部通过；批量长度 ASan/UBSan、四种模式 mcpp 模块/头文件混合消费、两个外部消费者的 offline/locked 构建、CMake 安装消费及 gts-quote 回归通过。
  - **兼容性**：CMake、vcpkg 与 mcpp 版本统一为 `2.1.0`；`BatchClaim` 新增原始长度字段，相关源码及 BMI 需重新编译；共享帧头和控制段布局保持不变。
  - **文档与范围**：使用 `/opt/mcpp-deps/salias` 本地共享路径消费，说明模块/头文件 ABI 和缓存边界；macOS mcpp 尚未实测，此 tag 不代表包索引发布。

## v3.0.0 - 2026-10-19

- **版本级别**：主版本（major），收束 v2.1.0 以来的公共 API 与共享协议不兼容变更。
- **Git 提交消息**：chore: 发布 v3.0.0 并汇总通道能力与延迟诊断
- **git tag**：v3.0.0
- **自上次 tag（v2.1.0）以来的变更摘要**：
  - **兼容性**：多消费者模式及类型统一使用 `FifoMpmc`、`OrderedMpmc`、`LatestMpmc` 和 `LatestByKey*`；下游须迁移旧标识符并重编译库、头文件及 mcpp BMI。`PrioritySubscriber::recv()` 新增等待策略参数，保存旧成员函数指针签名的代码须迁移。具名共享通道最终协议为 v14，LatestByKey 内部布局为 v2；旧通道须清理重建，同一通道各进程同步升级，不能混用新旧协议。
  - **通道能力**：新增进程内定容量 Ringbuffer、LatestMpmc 保留水位与可靠/丢旧独立订阅策略、checked 接收、显式跳到最新值及缺口计数；字节和消息数发布窗口协同约束背压，借用中的 payload 继续受保护。
  - **生命周期与 TTL**：可复用订阅注册槽、代际隔离、存活消费者计数、死亡进程回收和 `try_subscriber()`；支持基于同主机单调时钟的逐消息 TTL、过期跳过及安全释放前缀。
  - **类型与优先级**：增加平凡可复制类型的 send/copy-receive、显式 schema ID/版本的固定布局通道、拒绝 raw/typed 混连；提供独立背压的优先级端点、加权轮询及 Yield/Spin/Adaptive 等待，Linux 使用一次性通知票据与 futex。
  - **按 key 合并**：LatestByKey 以静态容量提供独立复制快照和版本检查；Linux 使用共享 robust mutex，macOS 使用进程退出可释放的文件锁，结合双缓冲提交与恢复校验避免更新进程异常退出后永久锁死或暴露半写值。
  - **正确性与性能实现**：修复 latest 保留水位、失败淘汰、长度和分配失败边界；Reliable 发布减少不必要的存活探测。`Subscriber::poll` 使用调用独占的未初始化描述符存储，避免空轮询 2 KiB 清零，同时保留默认值、重入、TTL、批量与借用契约，不弱化原子同步。
  - **诊断与取舍**：保留开环 planned/attempt 时延对比、真实 perf/火焰图、阶段与调度关联、设计原型及三组 ABBA 审计工具。Publisher 新候选未通过相对门禁，六个后续轮询候选未晋升；不安全帧头原型、默认 FIFO 调度及临时快速路径草稿均不作为产品优化发布。
  - **验证**：发布前 Linux Release 串行构建及 CTest 21/21 通过，包含安装消费者和 API/独立 poll 存储契约；HFT Python 36/36、Aeron Python 23/23 通过，HFT 独立 harness self-test、两端比较器构建、Shell 语法与版本元数据校验通过。历史报告中的 sanitizer 和候选验证有独立范围，不等同于本次全部重跑。
  - **证据与边界**：仅提交精简逐轮数据、必要真实火焰图、哈希与核验记录；生成源码副本、构建产物、perf.data、批量日志和误导性临时实验保留本地。共享 VM 无有效硬件 PMU；约 100 ns 典型延迟差仍未完整归因，不宣称已解决尾延迟或通过长时 SLA，本次不重跑 48 分钟 ABBA/30 分钟长测，也不声明新增 macOS 或 mcpp 实测。
  - **版本治理**：CMake、vcpkg、mcpp 与 README 统一为 3.0.0；注解 tag 直接复用本版本节，不代表包索引发布。

## v4.0.0 - 2026-10-22

- **版本级别**：主版本（major），移除 `PublishClaim::commit()` 并升级具名共享通道协议，属于不兼容 API/ABI 与协议变更。
- **Git 提交消息**：feat: 发布 v4.0.0 零拷贝 claim API 与非对称屏障
- **git tag**：v4.0.0
- **自上次 tag（v3.0.0）以来的变更摘要**：
  - **破坏性变更**：`PublishClaim` 不再持有 `shared_ptr<PublisherEndpoint>`，仅保存预留位置和非拥有发布端身份；`claim.commit()` 已移除，统一改为 `publisher.commit(std::move(claim))`。Publisher 提交时校验归属，跨 Publisher 提交在调试构建断言、发布构建拒绝；claim 不保活 Publisher，同一 Publisher 的副本仍可提交，未提交 claim 在调试构建析构时断言。schema-bound claim 使用 typed Publisher 的 `commit()`；下游必须修改调用点并重新编译库、头文件和 mcpp BMI。
  - **发布路径优化**：具名共享通道协议从 v14 升至 v15，在既有填充区新增 `claim_fence_protocol`，控制段尺寸与热字段偏移不变，旧通道返回 `VersionMismatch` 需重建。协议启用后，`Tx::claim` / `claim_batch` 只保留编译器屏障，完整屏障移到订阅者注册、注销和首轮阻塞接收的通知激活路径，通过 `membarrier(GLOBAL_EXPEDITED)` 送达到已注册发布进程；注册握手、动态订阅、代际回收、最慢订阅者背压、TTL 与借用期语义保持不变。注册失败或平台不支持时退回原 seq_cst 路径，`SALIAS_DISABLE_ASYMMETRIC_FENCE=1` 可让新建通道显式回退；heavy fence 失败时 fail closed，不在缺失屏障时读写 `claim_active`。
  - **正确性验证**：新增 claim API v4 契约测试，覆盖 raw/schema-bound 提交、移动后提交、同一 Publisher 副本提交、跨 Publisher 拒绝和未提交可见性；新增非对称屏障的跨进程压力、fork、故障注入与协议边界测试。Release CTest 22/22、ASan/UBSan CTest 21/21 通过；`tools/aeron_compare` 196 项通过、6 项跳过，`tools/hft_acceptance` 62 项通过，claim API v4 分析器 11 项通过。
  - **正式性能测量**：完成两次完整三臂产品 A/B，B=v3.0.0、S=v4 顺序屏障、A=v4 默认屏障，分别隔离 claim API、屏障和组合效果。两次均为 64/256 B、FIFO 1P2S、1 MiB ring、batch=1、poll-limit=64，每格 6 组 × 20 轮 × 5000 万条，延迟为 1P1S、100 万条/s、attempt/planned 各 10 轮 × 2 秒。第一次 64 B S/B 为 1.258804（+25.88%）、256 B 为 1.281718（+28.17%），64 B 判 GO；第二次 64 B 为 1.238393（+23.84%）、256 B 为 1.290653（+29.07%），64 B 因低于预注册 1.25 门槛判 INCONCLUSIVE。两次精确置换检验均为 p=0.002165、6/6 组同向。
  - **发布决定**：用户确认两次压测均有效，只是未达到理论收益值，并明确批准发布 v4.0.0。约 24%–26% 的 64 B claim API 吞吐收益在两次运行中方向稳定，但 GO 未复现；不得合并两次运行、改用组中位数比或挑选有利运行来改写判定，也不得把 256 B 的描述性收益套用 64 B 门槛。
  - **延迟与范围边界**：第二次 64 B planned p99.9 为 12351/12799 ns（候选/基线），候选轮间范围 7743–2064383 ns；256 B A/S attempt p99/p99.9 为 1247/501 ns 与 9471/4607 ns，约 2.49x/2.06x。短测仍见毫秒级尖峰，本轮未重跑 HFT 长尾门禁，因此本次发布不宣称延迟非劣、长尾已解决、生产 SLA 或 v4.0.0 整体性能验收通过。
  - **证据与文档**：两次运行的 Markdown 摘要、分析 JSON、实验 JSON 与 SHA-256 清单收录于 `doc/benchmarks/2026-10-22/`；完整 12 MiB 工作目录保留本地 `build/`。新增 claim API 迁移指南，更新 L7 API、最终性能报告、A/B 工具说明和接续状态；CMake、vcpkg、mcpp 与 README 版本统一为 4.0.0。

## v4.0.1 - 2026-10-25

- **版本级别**：小版本（patch），修复连接初始化竞态，无新增公共接口或 wire 变更。
- **Git 提交消息**：fix: 发布 v4.0.1 并修复共享控制段初始化竞态
- **git tag**：v4.0.1
- **自上次 tag（v4.0.0）以来的变更摘要**：
  - **连接安全**：`ControlMapping::open()` 在 mmap 前查询并校验共享控制段长度，未初始化到固定大小时关闭 fd、返回可重试的 `NotFound`；`fstat()` 失败返回 `PlatformFail`，避免零长度控制段上的原子访问触发 `SIGBUS`。
  - **回归与兼容性**：新增 `ConnectDoesNotMapZeroSizedControlSegment` 测试；claim API、消息布局与共享通道 v15 协议不变。v4 下游无需修改调用点或重建协议通道，但应重编译、部署库和 mcpp BMI 才能获得修复。
  - **历史性能证据**：归档已有第三次 v4.0.0 独立三臂 A/B 的 Markdown、分析 JSON 和实验清单，并同步性能报告与迁移计划。64 B S/B=1.245886，仍为 `INCONCLUSIVE`，不合并运行或改写预注册门槛；本次发版不宣称已重跑 v4.0.1 压测、HFT 长尾门禁或满足生产 SLA。
  - **版本元数据**：CMake、mcpp、vcpkg 和 README 当前包版本统一为 4.0.1。
  - **本版验证**：Release CMake 构建与 CTest 22/22 通过，零长度控制段新增用例单独复跑通过；mcpp 离线 Release（jobs=1）构建及模块导入测试通过。原始输出保留于本机 `/data/disk1/quote/reports/salias-v4.0.1-20261025-01/`，本次未重跑长时性能实验。

## v4.1.0 - 2026-10-30

- **版本级别**：次版本（minor），新增显式 Schema 声明辅助能力并冻结 Channel API 规范入口；不删除公开符号，不修改共享内存 wire protocol。
- **Git 提交消息**：feat: 发布 v4.1.0 并收敛 Channel API
- **git tag**：v4.1.0
- **自上次 tag（v4.0.1）以来的变更摘要**：
  - **规范入口**：README 快速开始收敛为 `Channel<Mode, Schema>`、`FixedChannel` 和 `LatestMpmcChannel` 三个核心入口；raw 与 typed 明示为同一通用 Channel 的 payload 模式，`FixedChannel` 保持独立固定槽数据面。
  - **兼容 API**：`FifoMpscChannel`、`FifoMpmcChannel`、`OrderedMpscChannel`、`OrderedMpmcChannel` 继续导出并标记 `[[deprecated]]`，提供同型兼容测试和 codemod 迁移表；直接 `Channel<Mode::LatestMpmc>` 标为 compat，typed latest 的替代契约留待 Phase 2。四个别名本版不删除，最早在 5.0 主版本处理。
  - **新增辅助模板**：`Schema<T, Id, Version>` 用显式非零 schema ID 和版本绑定 fixed-layout 消息；该类型无数据成员、无运行时路径，字段布局或语义变化仍必须由应用递增版本。
  - **固定槽通道**：新增 schema-bound `FixedChannel` 及 `FastSpscRing`、`FastSpmcRing`、`FastMpscRing`、`FastMpmcRing` 候选实现，覆盖固定参与者、可靠背压、独立 producer lane、独立 consumer cursor 和四拓扑跨进程验证；`FixedSubscriber::poll()` 在批末发布 consumer cursor，单个 SPSC 64 B 饱和吞吐开发对照由 8.20 提升到 40.03 Mmsg/s，不宣称延迟改善。
  - **API 基线**：新增机器可读 `channel-api-manifest.json`、仓库内使用矩阵和迁移说明，逐 symbol 标记 core/compat/extras/removed 状态、代码位置和替代入口；新增文档门禁防止快速开始重新引入兼容或 extras 入口。
  - **下游证据**：同工作区四个 salias 下游仓库快照均通过固定绝对 path 依赖并仍引用 raw 别名；未检索到 `LatestByKey`、Priority 或 Ringbuffer 引用。四个仓库均启用 `-Werror`，采用 4.1 前必须先完成别名迁移或显式调整弃用告警策略。由于缺少完整外部用户清单、发布包消费遥测、迁移成本确认和负责人签署，Phase 0 外部门禁仍为阻塞，不能把本快照当作全局零使用证明。
  - **保留边界**：`LatestByKey`、Priority、Ringbuffer 仍作为 extras 候选完整导出；`Channel` 与 `FixedChannel` 数据面未合并；共享通道版本、slot 布局、completion protocol 和 `Fast*Ring` 名称均未改变。
  - **验证**：Release 构建与 CTest 25/25、ASan/UBSan CTest 24/24 通过，其中安装消费者、别名兼容、Schema roundtrip 和文档检查通过；mcpp 离线 Release 构建、`test_mcpp_import --locked` 及两个独立 shared-path 消费者通过。本轮为模板/文档/测试层收敛，未改热路径，因此未执行收敛前后性能 ABBA。
  - **后续门禁**：Phase 0 仍需完整外部证据与负责人签署；Phase 2-5 未执行。Fast Ring 最终重命名仍由既有 6×20 ABBA、独占 CPU 和 p99.9 门禁控制，本版本不打主版本 tag。

## v5.0.0 - 2026-10-31

- **版本级别**：主版本（major），删除重复源码入口并完成 Channel API 收敛；不修改共享内存 wire protocol，不升级通道版本。
- **Git 提交消息**：refactor: 发布 v5.0.0 并完成 Channel API 收敛
- **git tag**：v5.0.0
- **自上次 tag（v4.1.0）以来的变更摘要**：
  - **破坏性源码变更**：删除 `FifoMpscChannel`、`FifoMpmcChannel`、`OrderedMpscChannel`、`OrderedMpmcChannel` 四个 raw 快捷别名，统一迁移到 `Channel<Mode>`；未修改 wire protocol，通道版本保持 15。
  - **Latest 唯一入口**：raw/typed latest fanout 统一使用 `LatestMpmcChannel<Capacity, HighWatermarkPercent[, Schema]>`；直接 `Channel<Mode::LatestMpmc>` 的 `create()`/`connect()` 已关闭，底层数据面继续由 wrapper 使用。
  - **extras 边界**：LatestByKey、Priority 和 Ringbuffer 的规范能力完整保留在 `salias::extras`、`<salias/extras/*.hpp>` 与 `import salias.extras;`；九个顶层 extras 别名和三个旧转发头已删除，核心安装、umbrella、module 和库不依赖 extras。
  - **保留边界**：通用 `Channel` 与 `FixedChannel` 继续使用独立数据面，未合并；`LatestByKey`、Priority、Ringbuffer 未删除；`Fast*Ring` 最终重命名仍由独立性能计划控制。
  - **迁移方式**：`-Werror` 工程应先在 `v4.1.0` 清除弃用告警，再按迁移表和 codemod 切换到规范入口并重新编译。源码删除不要求重建既有共享通道，但不能把本版本解释为仓库外使用量或迁移成本的证据。
  - **验证**：Release CTest 26/26、ASan/UBSan CTest 25/25、TSan CTest 25/25 通过；core-only/core+extras 安装消费、mcpp header/import、两个独立 shared-path consumer 全部通过。
  - **API 与性能证据**：API manifest 冻结为 30 core、0 compat、9 extras、14 removed，核心/extras module 分别导出 29/9 个符号，16 个 removed 契约执行真实编译失败检查；5 组 ABBA 共 80 次正式运行验证 5.2 亿条消息，`v4.1.0` 与当前树的 benchmark 二进制逐字节相同。冻结基线状态为 `post_phase4_released`，对应 tag `v5.0.0`。
  - **审计缺口**：按负责人决定，未采集仓库外源码消费方、发布包遥测和迁移成本；Phase 0 对应复选框继续保持未完成，不伪造外部证据或全局零使用结论。

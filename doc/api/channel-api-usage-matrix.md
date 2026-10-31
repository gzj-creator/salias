# Channel API 仓库内使用矩阵

日期：2026-10-31（Asia/Shanghai）

本文记录仓库内对 Channel 产品入口及外围能力的引用，只覆盖 salias 自身的源码、测试、工具、
安装包和 module。仓库外消费者不在本次审计范围内，也不据此推断其使用量或迁移成本。

## 1. 审计方法与边界

审计范围包括 `src/`、`test/`、`bench/`、`example/`、CMake 安装规则、`src/salias.cppm` 和
`mcpp.toml`。检索使用符号名和 header 路径全量搜索，不只看 README 或单个示例。当前工作区没有
适用的 `AGENTS.md`。历史 benchmark 原始日志和旧发布记录保留为历史证据，不作为当前 API 入口。

## 2. 使用矩阵

| 能力与当前入口 | 测试 | benchmark | 示例 | 安装消费 | mcpp/module | 当前处置 |
|---|---|---|---|---|---|---|
| 规范通用流 `Channel<Mode[, Schema]>` | `test/api/*`、`test/channel/*`、`test/mcpp/*` 的规范路径 | `bench/hotpath_ab.cpp`、`receiver_scan.cpp`、`channel_stress.cpp`、`fixed_channel_poll_compare.cpp` | `example/minimal_*`、`example/mpsc_*` | 通过 `salias::salias` 安装 | `salias.hpp` 与 `import salias;` 导出 | **core**；唯一通用主入口 |
| 四个 raw 快捷入口 `FifoMpscChannel`、`FifoMpmcChannel`、`OrderedMpscChannel`、`OrderedMpmcChannel` | `tools/check_channel_api.py` 逐个真实编译失败锁定不可见 | 无；使用 `Channel<Mode>` | 无；使用 `Channel<Mode>` | 安装消费不使用 | `src/salias.cppm` 导出集合与 manifest 精确一致 | **removed since v5.0.0** |
| 显式 `Schema<T, Id, Version>` 辅助模板 | `test/api/schema_helper_test.cpp` 及 typed Channel/latest 测试 | 通过 typed Channel 参数间接使用 | 暂无 | 经伞头可见 | `test/mcpp/typed_schema.hpp`、`import.cpp`、`src/salias.cppm` | **core**；无运行时成本，版本保持显式 |
| 固定快速流 `FixedChannel`、`FixedPublisher`、`FixedSubscriber`、`FixedPublishClaim` | `test/api/fixed_channel_test.cpp` 及固定 Ring 测试 | `bench/fixed_channel_poll_compare.cpp` | 暂无 | `test/install/main.cpp` 创建并收发 | `salias.hpp` 与 `import salias;` 导出 | **core**；独立数据面，不与通用 Channel 合并 |
| 规范 latest fanout `LatestMpmcChannel<Capacity, Watermark[, Schema]>` | `test/api/latest_subscription_test.cpp`、`channel_api_test.cpp`、`allocation_contract_test.cpp` | `bench/channel_stress.cpp` | 暂无 | `test/install/main.cpp` 创建并收发 typed latest | `test/mcpp/import.cpp`、`header_probe.cpp`、`src/salias.cppm` | **core**；raw/typed 共用的唯一最新窗口入口 |
| 直接 `Channel<Mode::LatestMpmc>` | `test/api/channel_api_test.cpp` 用编译期 concept 断言工厂不可调用 | 无；使用 `LatestMpmcChannel` | 无；使用 `LatestMpmcChannel` | 未使用 | `header_probe.cpp` 通过 wrapper 分派 | **removed since v5.0.0** |
| `salias::extras::LatestByKeyChannel`、`LatestByKeyPublisher`、`LatestByKeySubscriber`、`LatestByKeyMessage` | 仅 `test/api/removed_api_contract_test.cpp` 与 `tools/check_channel_api.py` 负向断言 | 无 | 无 | 安装检查反向断言不存在 | 核心 module 不导出；`salias.extras` module 已删除 | **removed since v6.0.0，无替代入口** |
| `salias::extras::PriorityPublisher`、`PrioritySubscriber`、`PriorityMessage`、`PriorityWaitPolicy` | 仅 removed 契约与 checker 负向断言 | 无 | 无 | 安装检查反向断言不存在 | 核心 module 不导出；`salias.extras` module 已删除 | **removed since v6.0.0，无替代入口** |
| `salias::extras::Ringbuffer` | 仅 removed 契约与 checker 负向断言 | 无 | 无 | 安装检查反向断言不存在 | 核心 module 不导出；`salias.extras` module 已删除 | **removed since v6.0.0，无替代入口** |
| extras headers、旧转发头、CMake target/component 与 module | `removed_api_contract_test.cpp` 锁定 header 不可见；checker 对每个 header 执行真实编译失败 | 无 | 无 | 安装检查反向断言 headers/archive/fragment 不存在 | `src/salias_extras.cppm` 删除；`import salias.extras;` 不可用 | **removed**；`<salias/extras/*.hpp>`、`<salias/latest_by_key_channel.hpp>`、`<salias/priority_channel.hpp>`、`<salias/ringbuffer.hpp>`、`salias::extras` 均不保留 |

## 3. 关键缺口与边界

- 仓库内正向 extras 使用已清零；剩余命中只允许出现在 removed 契约、安装反向检查、checker 负向样例和
  历史文档/benchmark 证据中。
- `LatestMpmcChannel` 同时支持 raw 和 typed payload；第三个模板参数省略时是 raw，显式传入时
  保留 schema ID、版本与 payload 大小校验。直接 `Channel<Mode::LatestMpmc>` 工厂保持关闭。
- 通用 `Channel` 与 `FixedChannel` 继续使用独立数据面；Fast*Ring 最终重命名不在本次范围。
- 仓库外使用证据没有采集，继续标记为阻塞；不据此声称零使用或迁移成本。v6 extras 删除依据维护成本
  决策，不声称已完成外部兼容迁移。

## 4. 证据命令

```bash
rg -n 'FifoMpscChannel|FifoMpmcChannel|OrderedMpscChannel|OrderedMpmcChannel' src test bench example
rg -n 'LatestMpmcChannel|Channel<.*LatestMpmc' src test bench example
rg -n 'LatestByKey|PriorityPublisher|PrioritySubscriber|PriorityMessage|PriorityWaitPolicy|Ringbuffer' src test bench example tools
rg -n 'salias::extras|salias/extras/|import salias.extras|salias_extras' src test bench example tools cmake CMakeLists.txt mcpp.toml
rg -n 'using ::salias::' src/salias.cppm
python3 tools/check_channel_api.py --compiler /usr/bin/c++
```

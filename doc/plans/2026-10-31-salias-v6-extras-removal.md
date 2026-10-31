# Salias v6 extras 维护收敛计划

日期：2026-10-31（Asia/Shanghai）

状态：**Phase 2 已完成；Phase 3 待执行**

## 1. 目标

在 `v5.0.0` 的 Channel API 收敛基础上，永久移除 `salias::extras` 及其三种能力
`LatestByKey`、Priority 和进程内 `Ringbuffer`，降低公开 API、安装组件、module、测试和
benchmark 的长期维护成本。本次不保留兼容入口，也不虚构仓库外使用或迁移证据。

完成后应满足：

1. 核心公开入口只有 `Channel<Mode, Schema>`、`FixedChannel` 和 `LatestMpmcChannel`。
2. 源码中不再存在 `salias::extras`、`salias_extras` target/archive、extras install component、
   `salias.extras` module 或 extras umbrella/specialized header。
3. 九类规范 extras 符号标记为 `removed`，且没有替代入口；只保留历史迁移说明。
4. 仓库内 test、benchmark、example、install consumer、mcpp consumer 不再正向使用 extras；
   专门契约测试反向锁定 extras 头、类型和 module/target 均不可见。
5. 通用 `Channel` 与 `FixedChannel` 数据面仍独立，共享内存 wire protocol 仍为 v15。
6. `Fast*Ring` 不在本计划中最终重命名；`Schema<T, Id, Version>` 继续作为零运行时成本的显式
   schema 写法保留。

## 2. 非目标与兼容策略

- 不提供 LatestByKey、Priority 或 Ringbuffer 的替代 API；它们从 `v6.0.0` 起永久移除。
- 不保留 `salias::extras` 兼容 namespace、头文件、CMake target/component 或 C++23 module。
- 不删除或重命名 `FastSpscRing`、`FastMpscRing`、`FastSpmcRing`、`FastMpmcRing`。
- 不合并通用 `Channel` 与 `FixedChannel`，不修改共享通道布局、控制段版本或动态订阅/背压语义。
- 不扫描或下载仓库外下游仓库；仓库外使用、迁移成本和采用情况继续标记为未获取证据。
- `doc/api/channel-api-downstream-evidence.md`、`channel-api-phase0-evidence.json` 和
  `channel-api-phase0-signoff.md` 的既有 Phase 0 未完成状态保持原样，不纳入本次提交。

## 3. 基线

- 基线 tag：`v5.0.0`。
- 基线公开面：30 core、0 compat、9 extras、14 removed；core module 29 个符号、extras module
  9 个符号。
- 目标公开面：30 core、0 compat、0 extras、23 removed；core module 29 个符号、extras module
  0 个符号。
- wire protocol：`src/salias/src/channel.cpp` 中的 `kNamedVersion = 15`，本计划不得改动。

## 4. Phase 1：删除公开实现、构建/安装/module 边界与仓库内正向使用

- [x] 盘点 extras 源码、header、CMake target/component、module、mcpp、test、benchmark、example 和工具引用。
- [x] 删除 extras 实现、umbrella/specialized headers、独立 module 与静态库 target。
- [x] 清理 core 中仅供 Priority 使用的私有 adaptive wait bridge 和仅供 LatestByKey 使用的错误码名称。
- [x] 删除 extras install component、package fragment、header/archive 安装与 `mcpp.toml` 源。
- [x] 删除 extras 测试、安装消费者和 benchmark，迁移仍保留的测试/benchmark/mcpp/install consumer 到核心入口。
- [x] 重写 removed 契约，反向锁定旧头、旧命名空间和 extras 安装/module 边界不存在。
- [x] 更新安装消费检查，只安装 `core` component，并断言 extras 头、archive、CMake fragment 均不存在。
- [x] 执行 Release 构建、相关测试、安装消费与 mcpp/header/import 验证，并记录实际命令/结果。
- [x] 更新本阶段执行记录与证据并独立提交，维护 `CHANGELOG.md`，不打 tag。

## 5. Phase 2：README、API 文档、迁移表、manifest 与检查工具

- [x] README 快速开始只保留 `Channel<Mode, Schema>`、`FixedChannel`、`LatestMpmcChannel` 三个核心入口。
- [x] L7/L1 API 文档删除 extras 主路径，并明确九类规范 extras 符号与 extras module/component 于 v6 永久移除。
- [x] 使用矩阵将 extras 正向引用清零，九个规范符号和 three-party 边界统一标为 removed。
- [x] manifest 将九个 canonical extras symbol 改为 removed、无替代入口，清空 extras module exports/install surfaces。
- [x] 迁移表记录全部 extras 类型、头和 module 的移除，明确“无替代入口”及需要重新设计的应用场景。
- [x] `tools/check_channel_api.py` 增加 extras header/symbol/module/target 负向断言，并更新计数、版本与冻结 tag。
- [x] 将 CMake、vcpkg、mcpp manifest 版本统一到 `6.0.0`，更新版本相关文档。
- [x] 执行 `git diff --check` 与 API 文档检查，更新本阶段执行记录与证据并独立提交，不打 tag。

## 6. Phase 3：完整验证、冻结与 v6 发布

- [ ] Release、ASan/UBSan、TSan 全量测试通过，并记录实际测试数。
- [ ] CMake 安装消费、mcpp core-only build/run、header/module consumer 和独立 shared-path consumer 通过。
- [ ] 验证 wire protocol 仍为 v15、通用 Channel/FixedChannel 数据面未合并、Fast*Ring 名称未变。
- [ ] 用二进制 symbol 差异和短时 channel benchmark 覆盖 include/link 边界变化风险；不修改或重测热路径算法。
- [ ] 重新生成并校验 API freeze，绑定 `v6.0.0`。
- [ ] 更新 `CHANGELOG.md` 与 `docs/release_note.md`，提交 release，创建并推送 `v6.0.0` tag，完成远端回验。
- [ ] 若不满足发布门禁，保持本 Phase 未完成并记录阻塞，不创建 tag。

## 7. 执行记录与证据

### 2026-10-31 初始化

- 已确认当前 `HEAD` 为 `9c0def3`（`v5.0.0`），工作树仅保留三个既有 Phase 0 文件：
  `doc/api/channel-api-downstream-evidence.md`（已修改）以及两个未跟踪证据文件。
- 仓库内没有 `salias` 适用的 `AGENTS.md`；构建入口为 CMake presets、CMake install consumer 和
  `mcpp.toml`/`tools/check_mcpp.py`。
- Phase 1-3 尚未执行，所有复选框保持未完成。

### 2026-10-31 Phase 1

- 全仓检索确认 extras 正向引用只剩源码、测试、benchmark、安装消费者和 mcpp/module 边界；历史
  `doc/benchmarks/**`、历史计划和旧 release note 保持原样。
- 删除 `LatestByKey`、Priority、`Ringbuffer` 的实现/header、`salias_extras` target/archive、
  `extras` install component、`saliasExtrasTargets.cmake` 和 `src/salias_extras.cppm`。
- 删除仅供 extras 使用的 `CapacityExceeded`、`SequenceExhausted` 名称，并保留枚举空洞 10/11，
  避免改变剩余错误码数值；删除 Priority 专用的 `SubscriberAdaptiveWaitAccess` 私有桥。
- `test/api/removed_api_contract_test.cpp` 改为反向断言 extras umbrella、三个专项头、三个旧转发头
  均不可见；安装消费者只安装 `core`，并断言 extras 头、archive、CMake fragment 和符号均不存在。
- `test/mcpp/import.cpp` 不再 `import salias.extras;`；header probe、allocation 测试、channel
  stress benchmark 和 aeron 构建工具同步移除 extras 正向使用。
- 实际验证：
  - `git diff --check`：通过，无 whitespace 错误。
  - `cmake --preset release`：通过。
  - `cmake --build --preset release -j 4`：通过，重建 50 个目标。
  - `ctest --test-dir build/release --output-on-failure -j 2 -E '^salias_channel_api_documentation_check$'`：
    22/22 通过；文档/manifest checker 因 Phase 2 尚未更新而暂不纳入本阶段门禁。
  - `mcpp build --offline --release --jobs 4`：通过。
  - `mcpp run test_mcpp_core_import --offline --release --locked --features mcpp-tests`：通过。
  - `mcpp run test_mcpp_import --offline --release --locked --features mcpp-tests`：通过。
  - `python3 tools/check_mcpp.py`：两套独立 shared-path consumer 的 offline/locked 构建与运行通过。

Phase 1 证据：Release 非文档 CTest 22/22；安装消费者反向检查通过；mcpp 与两个 shared-path
consumer 通过。`wire protocol` 文本仍为 `kNamedVersion = 15`，本阶段未修改热路径算法。

### 2026-10-31 Phase 2

- README 快速开始只保留 `Channel<Mode, Schema>`、`FixedChannel` 和 `LatestMpmcChannel`；removed 区
  补齐四个 v5 raw 别名、九个规范/旧顶层 extras 类型、全部 extras/specialized/旧转发/内部头、
  CMake target/component 和 `import salias.extras;`，并记录 2026-10-30 v5 批准与 2026-10-31 v6 批准。
- L7 API 与 L1 Ring 文档删除 extras 主路径；迁移表将九个规范 extras 类型标记为无替代入口，并说明
  需要重新设计相应场景，不能把通用 Channel 的背压、动态订阅或 latest 语义当作等价替代。
- 使用矩阵已清零仓库内 extras 正向使用；extras 只出现在 removed 契约、checker 负向样例、安装
  反向检查和历史迁移文档中。仓库外源码使用仍按负责人决定不扫描、不猜测，证据状态继续为 blocked。
- manifest 更新为 30 core、0 compat、0 extras、23 removed；core module 29、extras module 0。九个
  canonical extras symbol 和九个旧顶层 extras name 均为 removed 且没有替代入口。
- `tools/check_channel_api.py` 不再读取已删除的 extras umbrella/module；新增每个 extras 类型与
  header 的真实编译失败、core module 无 extras import、build/install 无 extras target/component、
  extras archive/fragment/header 不存在等负向契约。负向编译检查共 30 项：22 个旧类型名 + 8 个头。
- `test/api/removed_api_contract_test.cpp` 增加内部头
  `<salias/detail/latest_by_key_storage.hpp>` 的 `__has_include` 反向断言。
- CMake、vcpkg、mcpp 版本统一为 `6.0.0`；`Schema<T, Id, Version>` 仍是零运行时成本的显式版本
  辅助模板，不引入默认版本或隐式推断，本阶段只更新文档/manifest 记录，不修改实现。
- API freeze 候选为 47 个文件，聚合 SHA-256
  `42feb20252d6c16eb6e451c0b2e32f8258087ca50aaa854810afc7b59ea19bba`，state 为
  `v6_extras_removed_candidate`，目标 tag 为 `v6.0.0`；Phase 2 未创建 tag，Phase 3 发布前必须重新生成。
- 实际验证：
  - `git diff --check`：通过。
  - `python3 tools/check_channel_api.py --compiler /usr/bin/c++ --write-freeze`：通过；30 core、
    0 compat、0 extras、23 removed，core module 29、extras module 0，负向编译检查 30，freeze 47 files。
  - `python3 tools/check_channel_api.py --compiler /usr/bin/c++`：通过，freeze SHA 与文件集合一致。
  - `cmake --preset release`、`cmake --build --preset release -j 4`：通过。
  - `ctest --test-dir build/release --output-on-failure -j 2`：23/23 通过；包含 version consistency、
    removed API 契约、安装消费和 `salias_channel_api_documentation_check`。
  - `mcpp build --offline --release --jobs 4`：通过，显示 `salias v6.0.0`。
  - `mcpp run test_mcpp_core_import ...` 与 `mcpp run test_mcpp_import ...`：通过，覆盖 core module
    和 5 种模式 module/header 混合消费。
  - `python3 tools/check_mcpp.py`：两套独立 shared-path consumer 的 offline/locked 构建与运行通过。
- 代码检查确认 `wire protocol` 仍为 `kNamedVersion = 15`，`Channel` / `FixedChannel` 数据面未合并，
  `Fast*Ring` 名称未变；本阶段没有修改热路径或共享内存布局，因此未重复执行性能基准。

Phase 2 证据：Release CTest 23/23、安装消费者、mcpp core/module/header 消费和两套 shared-path
consumer 均通过；API checker 与 47 文件 freeze candidate 一致。外部证据缺口仍是 Phase 0 的
blocked 状态，不因仓库内验证而关闭。

### 下一门禁：Phase 3

Phase 3 尚未执行。正式发布前必须重新跑 Release、ASan/UBSan、TSan 全量；重新验证安装、mcpp、
shared-path consumer、wire v15、`Channel`/`FixedChannel` 数据面分离和 `Fast*Ring` 名称；执行与
include/link 风险匹配的二进制 symbol 差异和短时 channel benchmark；从最终发布树重新生成 freeze，
更新 `CHANGELOG.md`/`docs/release_note.md`，创建并推送 `v6.0.0` tag。未满足这些门禁前不得声称
v6 已发布，也不得把 candidate 冻结标记为 released。

## 8. 验证命令基线

```bash
git diff --check
cmake --preset release
cmake --build --preset release -j 4
ctest --test-dir build/release --output-on-failure -j 2
cmake --preset debug-asan-ubsan
cmake --build --preset debug-asan-ubsan -j 4
ctest --preset debug-asan-ubsan --output-on-failure -j 2
cmake --preset tsan
cmake --build --preset tsan -j 4
ctest --preset tsan --output-on-failure -j 2
mcpp build --offline --release --jobs 4
mcpp run test_mcpp_core_import --offline --release --locked --features mcpp-tests
mcpp run test_mcpp_import --offline --release --locked --features mcpp-tests
python3 tools/check_mcpp.py
python3 tools/check_channel_api.py --compiler /usr/bin/c++ --write-freeze
python3 tools/check_channel_api.py --compiler /usr/bin/c++
```

测试数量需按删除后的实际结果记录，不沿用 `v5.0.0` 的 26/25/25。

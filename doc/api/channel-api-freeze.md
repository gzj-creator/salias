# Channel API 冻结候选

日期：2026-10-31（Asia/Shanghai）

本文件记录 Phase 4 源码删除后的 API 兼容基线候选。它不是 `5.0.0` 发布声明，也没有对应 tag；
正式发布仍需负责人签署 Phase 5 并决定版本与 tag。

机器可读冻结记录为 [channel-api-freeze.json](channel-api-freeze.json)，当前状态是
`post_phase4_freeze_candidate_unreleased`。冻结基于 `c013a9214dc759751a92682237bb87e5f23ccabf`，
覆盖 53 个文件，聚合 SHA-256 为
`76dc56ee15686706688015fd89925d60e189340433021bd04150273046df2f83`。

## 冻结范围

- 逐 symbol 的 [公开 API 清单](channel-api-manifest.json)：30 core、0 compat、9 extras、14 removed。
- `src/salias/include/salias/**/*.hpp` 与安装到 `salias/core` 的 `src/core/**/*.hpp`。
- 核心与 extras 的 C++23 module interface；导出数分别为 29 和 9。
- CMake target、install component、package config 和 `mcpp.toml` 导出面。
- 当前共享内存 wire protocol 版本 15；本冻结不修改协议。

## 校验与更新

`tools/check_channel_api.py` 在 manifest/module/文档检查之后校验冻结文件集合、逐文件 SHA-256、
聚合 digest、API/module 计数和 wire version。该检查已是
`salias_channel_api_documentation_check` CTest 的一部分。

只有经过评审的公开 API 或安装/module 边界变更才允许重新生成：

```bash
python3 tools/check_channel_api.py --compiler /usr/bin/c++ --write-freeze
python3 tools/check_channel_api.py --compiler /usr/bin/c++
```

重新生成前必须确认 manifest 状态、迁移表、removed 契约和安装/module 消费均已同步；不能把
无关的头文件整理或未审阅的导出变化静默纳入基线。

## 发布边界

- `release_tag` 保持 `null`，不得用本冻结文件宣称 `5.0.0` 已发布。
- Phase 0 的仓库外证据与签署复选框继续保持未完成。
- Phase 5 的正式 release note、负责人签署和主版本 tag 决定仍未完成。

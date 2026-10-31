# Channel API 冻结基线

日期：2026-10-31（Asia/Shanghai）

本文件记录 `v6.0.0` 的 Channel API 兼容基线。它对应同名 tag，是后续兼容比较的冻结真源。

机器可读冻结记录为 [channel-api-freeze.json](channel-api-freeze.json)，当前状态是
`v6_extras_removed_released`，发布 tag 为 `v6.0.0`。API 语义基线来自 Phase 2 提交
`4e4789a7c890f8c42d1c418a0eb043d8d1ab3912` 及 Phase 3 发布态更新，文件哈希覆盖 47 个文件，
聚合 SHA-256 为 `baeb48e6110ccfff008cc2fc367d94f8735ad60a91922e8746071899d9a32a20`。

## 冻结范围

- 逐 symbol 的 [公开 API 清单](channel-api-manifest.json)：30 core、0 compat、0 extras、23 removed。
- `src/salias/include/salias/**/*.hpp` 与安装到 `salias/core` 的 `src/core/**/*.hpp`。
- 核心 C++23 module interface；导出 29 个符号，extras module 导出为 0 且接口文件已删除。
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

- `release_tag` 固定为 `v6.0.0`；tag、release note 和冻结文件指向同一发布提交。
- Phase 0 的仓库外使用证据仍未采集，对应审计复选框继续保持未完成，不能用本冻结基线推断外部使用情况。
- Release、ASan/UBSan、TSan、安装、mcpp、shared-path、二进制与短时 benchmark 发布门禁均已完成；
  仓库外使用证据仍是独立阻塞项，不影响本次由负责人批准的源码删除。
- 后续任何公开 API 或安装/module 边界变化都必须另立版本并重新评审、重新冻结。

# Phase 1 诊断产物清单

执行日期：2026-10-19（Asia/Singapore）；产品基线：v3.0.0 / `c5e6aa7`。
本轮沿用 Session 1/2，从 Session 3 顺序完成至 Session 6；执行起点为 `55e301a`，
该历史提交不代表本轮已经完成。本次只做一个本地普通提交，不创建版本号、tag 或远端推送；
不打包原有工作区性能文档归档修改。文件身份和实际结果以本清单、本轮报告及冻结证据为准。

## 入库范围

- `doc/plans/phase1-64b-throughput-diagnosis.md`：原始计划；已入库，本轮不改写。
- `doc/plans/session-handoff-prompts.md`：跨 session 执行提示词及本轮进度。
- `doc/plans/phase1-diagnosis-report.md`：实际测量、未通过门禁及下一步。
- `doc/plans/session3-diagnostic-results.md`、`session4-frame-layout-results.md`：逐 session 验收与溯源。
- `doc/plans/phase1-artifacts.md`、`doc/plans/session-state.json`：证据与接续信息。
- `doc/plans/runners/baseline-extended-runner.py`：已入库、未改；`diagnostic-runner.py`：冻结执行文件与二进制。
- `tools/aeron_compare/salias_ipc_compare.cpp`：沿用 Session 2 默认关闭的 off/counters/timed 插桩，纳入相关对齐断言与共享态注释；未改产品 `src/`。
- `tools/aeron_compare/parse_diagnostic.py`、`analyze_frame_layout.py`：分模式汇总、整数计数合并、严格原始证据及布局校验。
- `tools/aeron_compare/test_phase1_diagnostics.py`、`test_parse_diagnostic.py`、`test_analyze_frame_layout.py`：三模式/零消息冒烟及解析、布局回归。
- `CHANGELOG.md`：仅暂存本轮 Diagnostics 新条目，保留执行前已有的 Docs 修改在工作区。

## 本地保留，不入库

### 长测：`build/benchmarks/baseline-extended-20261019-183822/`

| 文件 | SHA-256 |
|---|---|
| `summary.json` | `e2ff362662982e6090931562fa1519648653f5dddf8fd6753dc5d09ea366d238` |
| `environment.json` | `1849cbd06c3177e6d917c66a015927262dbfb4d190b7cfc7b6d20c80de324139` |
| `results.jsonl` | `3fa34682bfb2c7b4ab88b141a39badb578842dde191efeaa6a9e7b314cff3a0e` |
| `frame-layout.json` | `371912784795f28652726ea2d4e8b582002de151eb9115ea79f344d757eb19ab` |
| `executed-runner.py` | `ed9631707c1e01e75b8457d1bfcfebeaf7d77e6aaf0dcda7e50668789c006e6b` |
| `salias_ipc_compare.uninstrumented` | `3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da` |

包含 72 个 stdout、72 个 stderr、Aeron driver 日志；100M 吞吐、2M 延迟。
执行时 runner 尚未增加 `--salias-client` 参数；其余实验参数相同。

### 本轮诊断与布局分析：`build/benchmarks/diagnostic-20261019-195710/`

| 文件 | SHA-256 |
|---|---|
| `diagnostic_summary.json` | `bd75dbff722dda02efa71301c5f0a1e4030cb16c849aa239119f18221756c8e9` |
| `frame-layout.json` | `63a8504a07803dfbbf32df14bd4dbe1b6901e1c94b6852afd110582996930f1c` |
| `environment.json` | `5767c54747293d5c350b4b14a0066f4cff64c1947629395435a6330f6ae6be06` |
| `results.jsonl` | `ec35d10e8091f0871d767cb7ad7d6e92fdedae53cf2cd969fa21c32ef6099a45` |
| `executed-runner.py` | `8579bfee59d23bc64c4141001067fc36c5d82754aeda76f2021e68c8e4f6803c` |
| `imported-baseline-runner.py` | `e1cec537881422e0bcd66f7eeaa04c547583175a8b4dc5323b73dd11d6cec393` |
| `executed-parser.py` | `a0da2a1ac942bd32629d9344380a48bc9e504dd4a95647a1434537eb11b98ce4` |
| `executed-frame-layout-analyzer.py` | `708b35cf7065bcb159d10965f5cae8eac15b7b34c9e29c88a62bf461330defca` |
| `salias_ipc_compare.instrumented`、`salias_ipc_compare.control`（相同二进制） | `d007c62a2c827c25bfe0d98c847e57f90b307dcb39a586b993629600fa83bba5` |
| `salias_ipc_compare.instrumented.cpp` | `b110de1fe2e0faaab2c6e4534cff2259b29ec29b6008f0d6dd07ec7ca4c0d790` |

36 个 stdout / 36 个 stderr：64/256 B × off/counters/timed，每组 1 轮预热及 5 轮正式，
每轮发布 10M、交付 20M；无 OOM、timeout 或内存门禁事件。只测 Salias 1P2S 吞吐，
不测延迟、不混入其他运行。off 与 counters/timed 使用同一二进制，非历史未插桩对照。
布局 JSON 使用保留的 Session 1 五轮正式长测，未覆盖基线目录内历史 `frame-layout.json`。
runner 日志、工具快照及相对路径 `SHA256SUMS` 一并本地保留，不入库。

H1/交接 H2/H3 阈值均未触发；原计划 H2（帧非对齐）也未确认为根因。
逐条计时扰动超过 5%，Phase 2B 仅是完成低扰动复核后的有条件隔离实验，未批准产品优化。

### 历史诊断（不纳入本轮汇总）：`build/benchmarks/diagnostic-20261019-185329/`

| 文件 | SHA-256 |
|---|---|
| `diagnostic_summary.json` | `3db535f9653e7345c275e65e01f1d8d32d523b83a2da541fb82c67356674296c` |
| `environment.json` | `11364dec0fd8eb150da12db24f09a23a3f847d1a402fc1976aed400e2dca3b8f` |
| `results.jsonl` | `4028080f2de449f4b42427f43a911aa88d7e572b283693edbb295f467f16d199` |
| `executed-runner.py` | `4cfef5324da1412da5aa99aa131a6f84d0b903102f1181f22c94f3a26144923c` |
| `imported-baseline-runner.py` | `e1cec537881422e0bcd66f7eeaa04c547583175a8b4dc5323b73dd11d6cec393` |
| `salias_ipc_compare.instrumented` | `d007c62a2c827c25bfe0d98c847e57f90b307dcb39a586b993629600fa83bba5` |
| `salias_ipc_compare.instrumented.cpp` | `970abe03fc9d374bc003d49bd5991b218cf971b5f23f9139321d5c7073cb96b5` |

包含 36 个 stdout、36 个 stderr，off/counters/timed 各 64/256 B，一轮预热及五轮正式。
冻结源码、二进制和 runner 均与 `environment.json` 的执行时 SHA-256 一致。
`runner.log`、解析工具快照及 `SHA256SUMS` 一并保留。

### 历史探索诊断（不可混入本轮组）

- `build/benchmarks/diagnostic-20261019-184518/`：共享计数实现，36 轮；完整保留源码、二进制、输出与汇总。
- `build/benchmarks/diagnostic-20261019-184933/`：本地计数第一次实现，36 轮；完整保留源码、二进制、输出与汇总。
- 历史执行为兼容既有 scheduler 变体生成器移动局部变量声明后，重新编译并完整重跑，以 185329 为当时最终结果；本轮使用独立的 195710 结果。
- 所有实验均独立顺序运行，无 OOM/timeout/内存门禁事件；不丢弃异常吞吐轮次，不拼接不同实现数据。

## 可复现命令

从仓库根目录执行。原提示词遗漏了 `SALIAS_RESOURCE_CAPPED=1`，这里显式设置。
基线使用保留的未插桩二进制，避免随后重新编译的比较器代码布局影响原始基线。

```bash
BASE="$PWD/build/benchmarks/baseline-extended-20261019-183822"
OUT="$PWD/build/benchmarks/baseline-extended-$(date +%Y%m%d-%H%M%S)"
systemd-run --user --wait --pipe --collect \
  -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  --setenv=SALIAS_RESOURCE_CAPPED=1 \
  python3 doc/plans/runners/baseline-extended-runner.py \
  --salias-client "$BASE/salias_ipc_compare.uninstrumented" --output "$OUT"

DIAG="$PWD/build/benchmarks/diagnostic-20261019-195710"
OUT="$PWD/build/benchmarks/diagnostic-$(date +%Y%m%d-%H%M%S)"
systemd-run --user --wait --pipe --collect \
  -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  --setenv=SALIAS_RESOURCE_CAPPED=1 \
  python3 doc/plans/runners/diagnostic-runner.py --output "$OUT" \
  --salias-client "$DIAG/salias_ipc_compare.instrumented" \
  --control-client "$DIAG/salias_ipc_compare.control"
python3 tools/aeron_compare/parse_diagnostic.py "$OUT"
python3 tools/aeron_compare/analyze_frame_layout.py \
  --baseline "$BASE/summary.json" --output "$OUT/frame-layout.json"

SALIAS_PHASE1_CLIENT="$PWD/build/release/bench/salias_ipc_compare" \
  python3 -m unittest discover -s tools/aeron_compare -p 'test_*.py'
sha256sum -c "$BASE/SHA256SUMS"
(cd "$DIAG" && sha256sum -c SHA256SUMS)
```

历史基线的 `SHA256SUMS` 使用仓库根目录相对路径，须在仓库根目录校验；
本轮诊断的清单使用诊断目录相对路径，须先切换至该目录。

## Session 6 验收与提交边界

- 52 项 Python 回归（包含真实 IPC 冒烟）、3 个 frame/ring/platform CTest 目标通过。
- 72 份基线及 36 份本轮诊断 stdout 的 SHA-256 / RESULT 全部一致；本轮两个汇总重算完全相同，未改写冻结输入。
- 本轮 86 个本地文件的相对路径校验清单通过；六个布局源文件与 `c5e6aa7` 一致。
- runner、解析/分析工具及三份诊断测试具有执行权限；入库仅文本源码、报告和状态，不含 `build/`、二进制或产品 `src/`。
- 仅纳入相关 Session 2 对齐/映射断言和冒烟补强；其余预先存在的性能文档与归档变更保持未暂存。
- 提交身份由 `git log -1 --format='%H%n%s' -- doc/plans/phase1-artifacts.md` 查询；不将提交自身 SHA 写入提交内容。

计数器只支持单发布者、batch1、无 pacing/延迟采样；consumer lag 未采样，无 debug API。
实际构建命令为 `cmake --build build/release --target salias_ipc_compare --parallel 1`。
未采用提示词中不对应真实 API 的 `tx.claim` / `subscriber.poll(handler, limit)`，
也未采用会改变重试次数的额外 yield 循环。

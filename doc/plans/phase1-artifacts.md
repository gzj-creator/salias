# Phase 1 诊断产物清单

执行日期：2026-10-19（Asia/Singapore）；基线：v3.0.0 / `c5e6aa7`。
本次只做本地普通提交，不创建版本号、tag 或远端推送；不打包原有工作区文档归档修改。

## 入库范围

- `doc/plans/phase1-64b-throughput-diagnosis.md`：原始计划及执行状态。
- `doc/plans/session-handoff-prompts.md`：跨 session 执行提示词。
- `doc/plans/phase1-diagnosis-report.md`：实际测量、未通过门禁及下一步。
- `doc/plans/phase1-artifacts.md`、`doc/plans/session-state.json`：证据与接续信息。
- `doc/plans/runners/baseline-extended-runner.py`、`diagnostic-runner.py`：长测与诊断运行器。
- `tools/aeron_compare/salias_ipc_compare.cpp`：默认关闭的 off/counters/timed 插桩；未改产品 `src/`。
- `tools/aeron_compare/parse_diagnostic.py`、`analyze_frame_layout.py`、`test_phase1_diagnostics.py`：验证、分析与回归。
- `CHANGELOG.md`：仅暂存本次 Diagnostics 条目，保留执行前已有的 Docs 修改在工作区。

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

### 最终诊断：`build/benchmarks/diagnostic-20261019-185329/`

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

### 探索诊断（不可混入最终组）

- `build/benchmarks/diagnostic-20261019-184518/`：共享计数实现，36 轮；完整保留源码、二进制、输出与汇总。
- `build/benchmarks/diagnostic-20261019-184933/`：本地计数第一次实现，36 轮；完整保留源码、二进制、输出与汇总。
- 最后为兼容既有 scheduler 变体生成器移动局部变量声明后，重新编译并完整重跑，使用 185329 作为最终结果。
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

DIAG="$PWD/build/benchmarks/diagnostic-20261019-185329"
OUT="$PWD/build/benchmarks/diagnostic-$(date +%Y%m%d-%H%M%S)"
systemd-run --user --wait --pipe --collect \
  -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  --setenv=SALIAS_RESOURCE_CAPPED=1 \
  python3 doc/plans/runners/diagnostic-runner.py --output "$OUT" \
  --salias-client "$DIAG/salias_ipc_compare.instrumented" \
  --control-client "$BASE/salias_ipc_compare.uninstrumented"
python3 tools/aeron_compare/parse_diagnostic.py "$OUT"
python3 tools/aeron_compare/analyze_frame_layout.py --baseline "$BASE/summary.json"

SALIAS_PHASE1_CLIENT="$PWD/build/release/bench/salias_ipc_compare" \
  python3 -m unittest discover -s tools/aeron_compare -p 'test_*.py'
sha256sum -c "$BASE/SHA256SUMS"
sha256sum -c "$DIAG/SHA256SUMS"
```

计数器只支持单发布者、batch1、无 pacing/延迟采样；consumer lag 未采样，无 debug API。
实际构建命令为 `cmake --build build/release --target salias_ipc_compare --parallel 1`。
未采用提示词中不对应真实 API 的 `tx.claim` / `subscriber.poll(handler, limit)`，
也未采用会改变重试次数的额外 yield 循环。

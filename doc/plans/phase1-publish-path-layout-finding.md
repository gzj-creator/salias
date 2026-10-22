# Phase 1 - Publish Path and Benchmark Layout Finding

**Date**: 2026-10-20  
**Status**: diagnostic evidence only; no product default changed

## 1. Conclusion

The next step should not be a product-level `claim` to `offer` default change.
The experiments found three separate effects that must remain separate:

1. The frozen P0 and current product library are identical for the exercised
   publish path: a clean build of tag `v3.0.0` reproduces the frozen binary
   SHA-256 exactly.
2. The current benchmark binary has a large, repeatable difference between
   its zero-copy `claim` path and diagnostic `offer` path.
3. Benchmark compilation-unit layout or state can change the result, so a
   difference measured across different binaries is not a product A/B result.

This is evidence for a benchmark-layout investigation and a same-binary
mechanism test. It is not evidence that `offer` is a better product API.

## 2. Reproduced Identity

A detached worktree at tag `v3.0.0` (`c5e6aa7`) was configured with Release,
benchmarks enabled, examples disabled and no tests, then built with the same
GCC 14.2 toolchain and Ninja. Its benchmark hash is bit-for-bit equal to the
frozen P0 executable:

```text
3f8fc6957ce56483d7051dd6625f44fd8f755d57a66d04e1e5efa48e65b9c7da
```

The current benchmark is different:

```text
bf18d5a89ff347adc4c414495a3bdbd7116c5a3620c690ed6cca3049cc334186
```

The product sources under `src/core` and `src/salias` are unchanged between
`c5e6aa7` and current `HEAD`. The relevant source difference is the benchmark
diagnostic implementation. Therefore product-library changes cannot explain
the observed benchmark separation.

## 3. Same-Binary Probes

All probes use one current executable, 192 B payload, 1 MiB ring, 5M messages
per round, 1P2S, batch 1, CPU0 coordinator and CPU1-3 workers. Each probe is
2 groups x 10 rounds. The high-mode threshold is 15 Mmsg/s.

| Probe | High mode | Median | Result directory |
|---|---:|---:|---|
| current, claim, before layout changes | 0/20 | 5.005 | `build/benchmarks/publish-path-current-claim-probe-20261020-1732/` |
| current, offer, before layout changes | 13/20 | 16.645 | `build/benchmarks/publish-path-current-offer-probe-20261020-1733/` |
| current, claim, restored L0 loop shape | 0/20 | 5.034 | `build/benchmarks/diagnostic-off-layout-probe-20261020-1757/` |
| current, claim, restored branch structure | 0/20 | 4.746 | `build/benchmarks/claim-layout-restored-probe-20261020-1804/` |
| current, claim, counters diagnostic | 0/20 | 4.685 | `build/benchmarks/claim-counters-probe-20261020-1810/` |
| current, offer, after layout changes | 16/20 | 17.565 | `build/benchmarks/offer-after-layout-probe-20261020-1815/` |

The same-binary `claim`/`offer` gap repeated after restoring the default loop
shape. Counter instrumentation did not create high mode, so a simple "counter
instruction is missing" explanation is insufficient.

The diagnostic `offer` path writes a local buffer and then copies it into the
ring. It therefore has different semantics from zero-copy `claim`; the result
must not be used to replace the product default.

## 4. Binary Identity Controls

These probes were run in one continuous window:

| Binary / local benchmark | High mode | Median | Result directory |
|---|---:|---:|---|
| frozen/clean `v3.0.0` claim | 13/20 | 16.307 | `build/benchmarks/v3-clean-repro-claim-probe-20261020-1739/` |
| current library + original benchmark source | 9/20 | 12.328 | `build/benchmarks/current-products-old-benchmark-probe-20261020-1748/` |
| current benchmark claim | 0/20 | 5.034 | `build/benchmarks/diagnostic-off-layout-probe-20261020-1757/` |
| current benchmark offer | 16/20 | 17.565 | `build/benchmarks/offer-after-layout-probe-20261020-1815/` |
| frozen/clean `v3.0.0` claim rerun | 15/20 | 16.291 | `build/benchmarks/v3-after-current-probe-20261020-1816/` |

The v3 result reproduced at the end of the window, and offer also reproduced.
The result is not explained by a one-time host state transition.

The "current library + original benchmark source" link used current product
archives but the resulting executable hash was exactly the v3 hash. LTO had no
material product code to re-emit, so this is not a valid independent product
comparison. It confirms only that the product archive is not the source of the
benchmark source-level difference.

## 5. What Was Rejected

- Do not claim a product improvement from the offer probe.
- Do not claim that removing `shared_ptr` ownership was neutral or beneficial.
  The earlier lightweight candidate produced no performance evidence and was
  reverted.
- Do not compare different L levels or binaries as a single-variable A/B.
- Do not treat the benchmark binary's 0/20 claim result as a product regression
  without first separating benchmark layout from API behavior.

## 6. Next Investigation

The next credible experiment is to make the benchmark's hot path independent
of its diagnostic feature set. The current source now routes the L0 claim path
through the original static method shape while retaining the diagnostic
instances. The next run must use a separately archived executable with a
recorded hash and one fixed source revision.

If the current L0 still stays at 0/20, stop treating benchmark layout as the
main product hypothesis. Move to a controlled investigation of the claim
wrap's generated code and runtime interaction, then test a candidate through
the existing 218+ round relative A/B protocol. If that cannot produce a
same-binary improvement, use the frozen v3 executable as the stability
reference and document the benchmark limitation rather than changing the
product default.

## 7. Verification

The benchmark target builds successfully after the source restoration. The
focused Python suite passes:

```text
107 tests passed, 3 skipped
```

The runner now records diagnostic capability and publish-path capability in
`environment.json` and only emits optional arguments for clients that declare
them. The experiment reports in this document remain diagnostic probes, not
acceptance results.

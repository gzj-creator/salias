# Resource-Limited IPC Latency Comparison

Results and limitations: [2026-10-19 report](../../doc/aeron-latency-comparison-2026-10-19.md).
This harness is Linux/cgroup-v2 specific and uses the user systemd manager.
It deliberately leaves CPU0 out of the benchmark. Do not use the older
`run_release_compare.sh` for this constrained host: it rebuilds with `nproc`,
defaults to larger topologies and includes huge-page configurations.

## Measurement

Both comparison binaries accept:

- `--rate N`: per-publisher open-loop messages/second. Zero, the default, keeps
  the previous unpaced saturation behavior. A positive rate must not exceed 1e9.
- `--latency-origin claim`: existing default, timestamp after successful claim.
- `--latency-origin attempt`: timestamp before the first claim attempt, including retries.
- `--latency-origin planned`: original scheduled arrival timestamp; requires a positive rate.

Paced and non-default-origin Salias runs require batch size 1. The paced start is
10 ms after the synchronized start signal; schedules never reset when late.
The matrix uses total rates, divides them across publishers and samples 1/16.
It checks delivery/sample counts, not message content, duplicates or ordering.
The existing benchmark writes only the first 16 payload bytes. Logical payload
sizes must not be interpreted as full-payload construction or validation tests.

Each group has one discarded pre-run and five independent measured runs, with
engine order rotated. Workers and channels are recreated on every run. The
summary reports **medians/ranges of run quantiles**, never pooled percentiles.
No histogram buckets are emitted, so pooled percentiles cannot be reconstructed.

## Bounded Build

Run from the repository root. Choose a new build/output directory. At least
1 GiB MemAvailable is recommended before building; compiler failure due to a
cgroup limit must not be addressed by unbounded parallel builds.

```bash
OUT="$PWD/build/benchmarks/aeron-latency-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT/deps"
systemd-run --user --wait --pipe \
  -p 'CPUAffinity=2 3' -p Nice=10 \
  -p MemoryHigh=600M -p MemoryMax=768M -p MemorySwapMax=0 \
  -p TasksMax=32 -p RuntimeMaxSec=300s -p KillMode=control-group \
  /usr/bin/env ROOT="$PWD" OUT="$OUT" /bin/bash -c '
    set -euo pipefail
    cd "$ROOT"
    tar -xzf third_party/aeron-1.52.0.tar.gz -C "$OUT/deps"
    src="$OUT/deps/aeron-1.52.0"
    bld="$OUT/deps/aeron-build"
    # Compatibility-only change in an extracted copy, not in the source archive.
    sed -i \
      -e "s/cmake_minimum_required(VERSION 3.30 FATAL_ERROR)/cmake_minimum_required(VERSION 3.28 FATAL_ERROR)/" \
      -e "s/cmake_policy(VERSION 3.30)/cmake_policy(VERSION 3.28)/" \
      "$src/CMakeLists.txt"
    cmake -S "$src" -B "$bld" -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DAERON_TESTS=OFF -DAERON_UNIT_TESTS=OFF -DAERON_SYSTEM_TESTS=OFF \
      -DAERON_BUILD_DOCUMENTATION=OFF -DBUILD_AERON_ARCHIVE_API=OFF \
      -DAERON_BUILD_SAMPLES=OFF -DBUILD_AERON_DRIVER=ON -DAERON_INSTALL_TARGETS=OFF
    cmake --build "$bld" --target aeronmd aeron_static -j1
    cmake --preset release
    cmake --build --preset release --target salias_ipc_compare salias_latency_histogram_tests -j1
    c++ -std=c++17 -O3 -DNDEBUG -march=native -flto=1 -DDISABLE_BOUNDS_CHECKS \
      tools/aeron_compare/aeron_ipc_compare.cpp \
      -I"$src/aeron-client/src/main/cpp_wrapper" -I"$src/aeron-client/src/main/c" \
      "$bld/lib/libaeron_static.a" -pthread -ldl -lrt -lm -o "$OUT/aeron_ipc_compare"
    build/release/test/salias_latency_histogram_tests
  '
```

Aeron 1.52.0 allocates about 512 MiB for its four native internal command
queues before other driver overhead. A 512 MiB total cgroup is insufficient.
The tested 704 MiB runtime limit accommodates the unmodified driver. It is not
necessary to modify Aeron queue sizes or allow the benchmark to use swap.

## Bounded Runs

These commands reuse `OUT` from the build. Keep the runs sequential, not in
background shells. The runner verifies the actual memory/swap cgroup limits,
refuses CPU0 affinity and requires the explicit resource-capped marker.

```bash
systemd-run --user --wait --pipe \
  -p 'CPUAffinity=1 2 3' -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  -p TasksMax=48 -p RuntimeMaxSec=900s -p KillMode=control-group -p OOMPolicy=stop \
  /usr/bin/env SALIAS_RESOURCE_CAPPED=1 \
  python3 "$PWD/tools/aeron_compare/run_latency_compare.py" \
  --output "$OUT/planned-formal" \
  --aeron-build "$OUT/deps/aeron-build" --aeron-client "$OUT/aeron_ipc_compare" \
  --origin planned --seconds 2 --rounds 5 \
  --rates 100000 1000000 --payloads 64 256 --producers 1 2

systemd-run --user --wait --pipe \
  -p 'CPUAffinity=1 2 3' -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  -p TasksMax=48 -p RuntimeMaxSec=600s -p KillMode=control-group -p OOMPolicy=stop \
  /usr/bin/env SALIAS_RESOURCE_CAPPED=1 \
  python3 "$PWD/tools/aeron_compare/run_latency_compare.py" \
  --output "$OUT/attempt-formal" \
  --aeron-build "$OUT/deps/aeron-build" --aeron-client "$OUT/aeron_ipc_compare" \
  --origin attempt --seconds 3 --rounds 5 \
  --rates 100000 1000000 --payloads 64 256 --producers 1

python3 tools/aeron_compare/summarize_latency.py \
  "$OUT/planned-formal/results.jsonl" "$OUT/attempt-formal/results.jsonl" \
  --json-output "$OUT/summary.json"
python3 tools/aeron_compare/test_latency_compare.py
```

The runner refuses to append to an existing results file. Each next case needs
at least 1 GiB MemAvailable; it stops if the polled value falls below 600 MiB.
The cgroup is the hard cap; these sampled host-memory checks are supplementary.
Cases have individual timeouts, a minimum 0.25 s cooldown, and process-group
cleanup. SIGTERM/SIGHUP trigger cleanup; service teardown kills the entire
cgroup if necessary. Results under `build/` are intentionally Git-ignored.

CPU1 hosts the subscriber, CPU2 publisher0, CPU3 publisher1 when present.
The Aeron SHARED/backoff driver is always pinned to CPU3: independent in 1P1S,
but **shared with publisher1 in 2P1S**. Do not interpret that second topology as
an equally provisioned, non-oversubscribed Aeron latency baseline. No 2P2S or
throughput-saturation matrix is run on this four-vCPU shared host.

## Design Diagnostics

Results, rejected candidates, profiling limits and tail correlation:
[2026-10-19 design diagnosis](../../doc/aeron-design-diagnosis-2026-10-19.md).
The original diagnosis left product sources and atomic ordering unchanged.
The follow-up [implementation report](../../doc/aeron-design-implementation-2026-10-19.md)
uses call-local uninitialized descriptor storage in both product poll overloads;
it does not use the recursive-unsafe endpoint scratch. Source-level variants are
generated under `build/` and must not be used as installed-compatible binaries.
In particular, the split-cursor prototype changes field offsets even though
the control segment size does not change; frame64 changes the wire stride.
The scratch prototype is not recursive-poll safe and is not a release candidate.

Build after the bounded Release build and Aeron setup described earlier:

```bash
DIAG="$PWD/build/benchmarks/design-diagnosis-$(date +%Y%m%d-%H%M%S)"
systemd-run --user --wait --pipe -p "WorkingDirectory=$PWD" \
  -p 'CPUAffinity=2 3' -p Nice=10 \
  -p MemoryHigh=600M -p MemoryMax=768M -p MemorySwapMax=0 \
  -p TasksMax=32 -p RuntimeMaxSec=300s \
  python3 tools/aeron_compare/build_design_variants.py --output "$DIAG" \
  --variants baseline split frame64 lazy pause yield watchdog256 single chunk1 scratch ready
```

Run a compact matrix in a fresh output directory, never alongside builds:

```bash
systemd-run --user --wait --pipe -p "WorkingDirectory=$PWD" \
  -p 'CPUAffinity=1 2 3' -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 \
  -p TasksMax=48 -p RuntimeMaxSec=600s -p KillMode=control-group \
  env SALIAS_RESOURCE_CAPPED=1 python3 tools/aeron_compare/diagnose_design.py \
  --output "$DIAG/timing" --baseline "$DIAG/bin/baseline" \
  --aeron-build "$OUT/deps/aeron-build" --aeron-client "$OUT/aeron_ipc_compare" \
  --variant split="$DIAG/bin/split" --variant scratch="$DIAG/bin/scratch" \
  --cases baseline split scratch aeron --seconds 2 --rounds 5
```

The runner holds an exclusive per-user lock, validates the cgroup and CPU mask,
and checks delivery/sample counts. It records actual commands, stdout hashes,
binary hashes, resource counters and every round, including unfavorable runs.
The lock prevents overlapping copies of this runner, not unrelated host jobs
or a concurrent compiler; operators must keep those out of measurement windows.
Do not add option dispatch to the hot receive loop: an early diagnostic version
changed LTO inlining and inflated baseline p50 by about 100 ns. Those results
were discarded and the normal comparison binary restored unchanged.

`--profile` runs real `sudo -n perf record -e cpu-clock` at 199 Hz with DWARF
stacks in a separate output directory. It needs existing profiling permission;
the tools do not change perf sysctls. `cycles`/HITM/cache-miss counters may not
exist inside a VM. Never use profiled quantiles as uninstrumented timing results.
The driver itself is not included in client call-stack flame graphs.

```bash
git clone --depth 1 https://github.com/brendangregg/FlameGraph.git "$DIAG/FlameGraph"
python3 tools/aeron_compare/render_profiles.py \
  --directory "$DIAG/profiles" --flamegraph "$DIAG/FlameGraph"
```

For matched-message stages, build `build_stage_probe.py` with `--output`,
`--fixed-source "$DIAG/fixed_loop_compare.cpp"`, `--aeron-source` and
`--aeron-build`. It creates `salias` and `aeron` binaries and emits `STAGE`
histograms. They reject payload <32 B, disabled sampling and origins other than
`attempt`; use `--origins attempt` with the runner. The additional
timestamps perturb the path; quantiles from different stages are not additive.

For scheduler correlation, build `build_tail_probe.py --output "$DIAG/tail"`
in the same bounded build service. Use the generated `tail/trace` as
`--baseline`, `--cases baseline --trace --origins planned` in a separate
bounded diagnostic run. It records top-32 samples >=10 us and real switch
events with `--clockid mono`. Then run:

```bash
python3 tools/aeron_compare/analyze_switches.py --directory "$DIAG/trace"
python3 -m unittest discover -s tools/aeron_compare -p 'test*.py'
```

`export_design_evidence.py` preserves the dated investigation's named datasets,
validates counts/memory events and exports compact JSON/SVG evidence. It is not
a generic merger for arbitrary matrices. Historical early runs and overlapping
experiments remain under `build/` for audit but are excluded from acceptance.

### Follow-up Implementation

The release keeps compact summaries, per-run JSONL, referenced profiles and
verification records, as specified in the [evidence scope](../../doc/benchmarks/2026-10-19/README.md).
Exporter output is a larger local audit bundle, not a manifest to add wholesale
to Git. Generated source snapshots, bulk stdout/stderr and full GTest dumps stay local.

`baseline` now builds current product code. `eager` restores the old 32-element
initialized array in a generated source copy. Keep the pre-change binary and
object/include snapshot when comparing a historical baseline; never rebuild it
from changed headers and call it unchanged.

`frame-checked` builds a header-first protocol prototype that still checks the
visible prefix. `frame-acquire` skips that bound and **fails the variable-length
stale-payload regression**; it requires `--allow-unsafe-frame-prototype`, must not
be installed or mixed with normal peers, and is not a performance candidate.
Both prototypes remain outside product `src/`.

`build_worker_scheduler.py` accepts `--output`, `--baseline-source`,
`--baseline-object`, `--baseline-include`, `--aeron-source` and `--aeron-build`.
It builds `baseline`, `stack`, and `aeron` scheduler binaries; `--tail` builds
just the first two with tail timestamps. With `diagnose_design.py`, add
`--fifo-priority 1 --compare-normal` to interleave normal/FIFO worker runs of
the same binaries. Controllers return to SCHED_OTHER before forking, and Aeron
conductor threads remain normal. Runs are limited to two seconds with FIFO,
priority 1-10, existing RT throttling enabled, bounded cgroup and whole-group
cleanup. The tool checks a binary marker but this is not a security boundary.

**Do not enable FIFO as a default tuning recipe.** Here, 950ms/1s RT runtime
produced repeatable ~50ms off-CPU windows and ~47.7ms planned p99.9. No sysctl,
IRQ affinity, host isolation or system-wide scheduler settings were changed.
Scheduler binaries changed LTO layout and Salias p50; compare scheduler modes
within those binaries, not against the unmodified timing binary.

`export_implementation_evidence.py` verifies counts, stdout hashes and zero
memory-pressure events, then preserves the follow-up datasets, SVG profiles,
switch analysis and per-run stdout/stderr. Do not pool distinct load, scheduler,
instrumentation or unsafe-protocol populations.

### Poll-path Follow-up

The accepted call-local descriptor-storage change was first saved in commit
`f9ea5c2`. The [poll-path follow-up report](../../doc/aeron-poll-path-followup-2026-10-19.md)
records six further candidates, two confirmation matrices, real profiles and
the decision not to promote any new default optimization.

`build_poll_path_variants.py --output DIR --variants ...` generates isolated
copies for `control`, `deferred-progress`, `branch-scan`, `combined`,
`cursor-pack`, `cached-registration`, and `layout-cached`. It leaves product
sources and shared-memory protocols unchanged. Run only inside the bounded
build service, with no concurrent measurement. `--verify-api-only` reuses
existing variant objects and current Release public-API test objects, links
each implementation separately, and preserves GTest JSON and input hashes.

`export_poll_path_evidence.py --source DIR --output EVIDENCE` validates counts,
stdout/result equality, output and executable hashes, and memory events before
exporting independent-run summaries, raw outputs, source snapshots, profiles
and API verification records. Warmups and profiles never enter the timing
population. Preserve unfavorable rounds; a promising screen is not acceptance.

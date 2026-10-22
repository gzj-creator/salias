# Synthetic HFT IPC Acceptance

This harness tests the real salias named FIFO channel in forked processes. It is
not an exchange feed decoder, order-book validator, NIC benchmark, or production
tick-to-trade acceptance test. No product code is changed.

See the [historical HFT report](../../doc/archive/performance/hft-market-data-acceptance-2026-10-18.md)
for that run's status, methodology, results, limitations, exact commands, and
the provisional 50 us percentile criteria.

Current consolidated results and optimization handoff:
[final performance report](../../doc/performance-report.md).
Historical HFT-only summary: [2026-10-18](../../doc/archive/performance/performance-summary-2026-10-18.md).
The later three-group relative-gain experiment is documented in
[the relative-gain report](../../doc/archive/performance/publisher-tail-relative-acceptance-2026-10-18.md); its product candidate was
rejected and removed. That experiment's complete local tree is now preserved in
`build/benchmark-cleanup-20261019/publisher-tail-20261018.tar.gz`; restore it before
auditing the original `build/benchmarks/publisher-tail-20261018/` paths. Archive
hashes and restoration details are in section 8.2 of the final report.
Historical temporary raw results were removed after verification; compact scalar
results and final Markdown tables remain in `doc/benchmarks/2026-10-18/`.
They do not contain histogram buckets and cannot regenerate historical quantiles.
Regression tests use small synthetic fixtures, not benchmark files under `doc/`.
The matrix defaults to timestamped `build/benchmarks/hft-acceptance/` output
(Git-ignored); keep new raw JSONL, traces and build logs out of documentation.

## Build and Run

```bash
bash tools/hft_acceptance/build.sh
build/release/tools/hft_acceptance/acceptance --self-test
bash tools/hft_acceptance/capture_environment.sh > /tmp/hft-environment.txt
# Eight phases x 225 seconds = 1800 measurement seconds, plus setup and drain.
bash tools/hft_acceptance/run_matrix.sh /tmp/hft-acceptance 225
python3 tools/hft_acceptance/summarize.py /tmp/hft-acceptance
python3 tools/hft_acceptance/test_summary.py
```

Run phases sequentially, without competing benchmark loads. CPU0 is subscriber0;
CPU1 is subscriber1 in 2P2S. Publisher CPUs are 1/2 in 2P1S and 2/3 in 2P2S.
The parent's progress collector runs without dedicated affinity. Pinning workers
does not reserve or isolate CPUs from the kernel or background services.

For a single phase:

```bash
build/release/tools/hft_acceptance/acceptance \
  --subscribers 2 --payload 256 --seconds 225 --pattern burst \
  --rate 1000000 --peak-rate 4000000 --burst-ms 100 --cpu-base 0
```

`--rate` and `--peak-rate` are total across two publishers, must be positive
multiples of 2000, and are configurable synthetic assumptions. The burst pattern
offers 4M/s for the first 100ms of every second, 1M/s for the rest: **1.3M/s average**.
The schedule is deterministic and synchronized across publishers. It is not a
claim about a production exchange's arrival distribution.

## Measurement and Correctness

- `from_first_attempt`: immediately before the **first** `try_claim`, through
  claim retries, payload write, commit and IPC delivery, to the subscriber after
  copying the 48-byte header and before sequence/body validation.
- `from_planned_arrival`: the same endpoint, starting at the original open-loop
  arrival schedule. Catch-up never resets that schedule. This captures publisher
  scheduling lag and unsent backlog which first-attempt latency alone misses.
- `first_attempt_to_commit`: publisher-side sampled post-commit clock reading;
  `planned_to_first_attempt`: producer pacing lag. Intentional waiting before a
  future scheduled arrival is not counted as latency.
- Sampling uses a deterministic mixed sequence hash with mask 63 (approximately
  1/64), avoiding a fixed periodic phase. The actual sample count is reported.
  A maximum is a **sampled maximum**, not a bound on all messages.
- Histograms reuse the existing bounded logarithmic histogram (3776 buckets,
  64 sub-buckets per power of two, approximately <=1.56% bucket width). Quantiles
  are bucket upper bounds. Exact sampled `>50000ns` counters decide provisional
  p99/p99.9 budget pass/fail, so bucket rounding cannot flip a borderline result.
- The **primary acceptance** is planned-arrival latency for the **worst subscriber
  per phase**, with every subscriber required to pass. Worker health, integrity
  and completion of the original input load are mandatory. First-attempt latency
  is auxiliary. Matrix exit=0 is not a latency acceptance verdict.
- Subscribers merge raw buckets for aggregate percentiles; no percentile-of-
  percentiles. The report also retains each subscriber so pooling cannot conceal
  a slower subscriber. Cross-configuration pooling is descriptive only.
- Every delivery checks publisher identity, sequence, header checksum, scheduled
  timestamp, sampling marker and every remaining 64-bit payload word. A bounded
  bitmap tracks exact duplicates/unique sequences; a high-water mark tracks gaps
  and reordering. Memory is bounded by the planned message count (roughly
  36.6MB/subscriber for a 225s burst phase), not by run-time arrival drift.
- Published-message integrity and completion of the original offered load are
  **separate** results. An unsent arrival is not called a transport sequence gap.
  At the fixed measurement cutoff, no new attempts begin. An already successful
  claim may finish its commit just beyond the cutoff. Unsent arrivals remain
  explicitly reported, rather than being silently dropped or drained as sends.
  Subscribers drain published frames for at most five additional seconds.
- Publisher retry episodes report duration from the first observed failed
  `try_claim` to success/abort, including retry work. They exclude the first
  failed call's own execution; the primary first-attempt latency includes it.
  Sums across publishers are not extra wall-clock time.
- Once-per-second progress uses independent atomic snapshots; publisher counters
  publish every 256 sends, so snapshot backlog is approximate and can undercount
  recent sends by up to 255/publisher. Internal observed high-water marks are
  also observations, not proven instantaneous maxima. Use final sent/unique
  counters for exact completion and loss checks.
- `unreceived_at_measurement_cutoff_observed` is taken on the subscriber's first
  loop after the deadline, after that poll. It can include a small drain amount;
  it is **not** an atomic system-wide snapshot at exactly the deadline.
- Fatal worker errors propagate a stop flag. Each phase has bounded startup,
  drain and process-reap deadlines; the matrix records the failure and continues.
  Signal cleanup kills/reaps children and lets the owning channel remove its
  named mappings. Child parent-death signaling guards against orphan workers.

## Checks

`--self-test` tests histogram boundaries/merging/50us rank logic, the complete
inverse schedule for two seconds of both patterns, sequence gap/duplicate/reorder
fault detection, 100000 clock pairs, and 10000 cross-process clock handshakes.
An optional `--stall-subscriber-ms 200` short run forces reliable backpressure
without modifying the product or substituting DropOldest/latest semantics.

Only synthetic IPC integrity is covered. Per-publisher FIFO order is not global
exchange order. Business acceptance still needs real recorded feeds, exchange
sequence domains, gap recovery, duplicates, snapshots and order-book invariants.
Neither p99.99 nor the all-message maximum latency is measured.

## Publisher Tail Optimization A/B

`--diagnostics` enables additional sampled clocks around successful/failed
`try_claim` calls and `commit`. It is off by default; diagnostic runs are rejected
by formal matrix and A/B summaries. Retry samples are conditioned on the sampled
message, not independent draws from all failed calls; zero failed-call samples
do not mean zero backpressure. Never subtract independent percentiles to attribute
component latency. `--stall-subscriber-ms` is also excluded from formal acceptance.

Each worker records resource deltas with `getrusage` immediately after the start
barrier and before exit: user/system CPU time, voluntary/involuntary context
switches, and minor/major faults. These snapshots include the synchronized 100ms
start lead and subscriber drain, not just the nominal measurement interval.
They are diagnostic observations, not proof of a particular scheduling cause.

Build the same harness against separately frozen baseline/candidate headers and
libraries. Both must use matching Release compiler/flags. Do not compare historical
reports against a new candidate as if they were an interleaved control.

```bash
bash tools/hft_acceptance/build_tail_variant.sh BASELINE_SOURCE OUTPUT_BUILD_BASELINE
bash tools/hft_acceptance/build_tail_variant.sh CANDIDATE_SOURCE OUTPUT_BUILD_CANDIDATE
# Warm all configurations, then three independent ABBA groups; 48 minutes of
# formal measurement, plus startup/drain/warm-up. Never compile concurrently.
bash tools/hft_acceptance/run_tail_suite.sh OUTPUT BASELINE_BINARY CANDIDATE_BINARY
python3 tools/hft_acceptance/summarize_tail_ab.py OUTPUT --suite
# Run this independent 30-minute candidate matrix only if relative gates pass.
bash tools/hft_acceptance/run_matrix.sh OUTPUT_LONG 225 CANDIDATE_BINARY
python3 tools/hft_acceptance/summarize.py OUTPUT_LONG
python3 -m unittest discover -s tools/hft_acceptance -p 'test_*.py'
```

Every ABBA phase lasts 30 seconds; each version has two runs/configuration/group.
Each subscriber's raw buckets and each publisher's metrics are merged separately.
For **each of all three groups**, worst-subscriber planned-arrival p99.9 must
improve by at least 20% in both 2P2S burst payload configurations; the other six
configurations must not regress by more than 5%. All original input must complete,
and all workers/deliveries must pass integrity. Preserve failed/incomplete runs.
The independent long matrix must also pass integrity and input completion.
Relative improvement and the provisional 50us budget are separate verdicts.
Script exit zero means orchestration completed, not performance acceptance.

Outputs include raw JSONL/histograms, commands, binary/source hashes, per-group
`ab-summary.json`/Markdown, `/proc/stat` snapshots and `relative-acceptance.json`.
The suite auditor re-reads raw files and checks saved summaries, file hashes,
ABBA order and group ownership before producing `relative-acceptance.md`.
Keep these artifacts under `build/benchmarks/`; do not remove them after reporting.
Run serially on unchanged binaries with no overlapping diagnostic or build load.
If relative gates fail, skip the conditional long matrix and do not retain an
unaccepted product patch as a claimed optimization.

## P3-1 Fence Non-Inferiority

The P3-1 gate is a separate experiment, not the historical publisher-tail
improvement target above. It uses **one frozen candidate binary** for C-seq
(`SALIAS_DISABLE_ASYMMETRIC_FENCE=1`) and C-asym (switch removed). The rule is
fixed before collection in [plan section 6.4](../../doc/plans/phase3-final-optimizations.md):
three independent SAAS groups, all eight existing HFT configurations, 30 seconds
per run. Both worst-subscriber planned p99 and p99.9 must be at most C-seq * 1.05
in every group/configuration. Integrity and original-input completion are
mandatory; the independent 50 us budget is not a relative-pass substitute.

```bash
# First build and self-test a candidate harness linked to the intended frozen
# product libraries. Do not compile while collecting formal HFT data.
systemd-run --user --wait --pipe --collect \
  -p WorkingDirectory="$PWD" -p AllowedCPUs=0-3 -p Nice=5 \
  -p MemoryHigh=640M -p MemoryMax=704M -p MemorySwapMax=0 -p RuntimeMaxSec=3900 \
  --setenv=SALIAS_RESOURCE_CAPPED=1 --setenv="PATH=$PATH" \
  taskset -c 0-3 python3 doc/plans/runners/p3-p31-tail.py \
  --client /absolute/path/to/frozen/hft-candidate --output /new/output/directory
python3 tools/hft_acceptance/compare_p31_tail.py /new/output/directory
```

The runner records its protocol before warm-up, then commands, exact environment
overrides, input/binary hashes, raw stdout/stderr hashes and exit codes. The
auditor re-reads the existing HFT raw-bucket validator and rejects missing runs,
changed inputs, unexpected order, mismatched identities and invalid completion.
Exit zero means execution and auditing completed, **not** that G4 passed.
On this four-vCPU host 2P2S needs CPUs 0-3; do not reuse the short latency runner's
CPUs 1-3 restriction. Explicit PATH forwarding also makes the environment
capture's `rg` available inside the transient service.

The 2026-10-21 formal run completed all 96 workers with exit zero, but G4 **failed**:
4/48 S runs did not complete the original input, and only 7/24 group/configuration
cells met both p99 and p99.9 non-inferiority. The candidate 50 us p99.9 budget passed
in 2/24 cells. Keep those failures rather than rerunning or repooling to seek a pass.
See the [P3-1 acceptance report](../../doc/plans/phase3-p31-acceptance-report.md).

The separate `registration_cost.cpp` probe includes the selected revision's
`channel.cpp` to time the real `acquire_consumer` without modifying product
sources. `doc/plans/runners/p3-p31-registration.py` builds baseline/candidate
probes and retains 90,000 raw registration timings. These idle-registration
measurements do not bound registration under concurrent claims.

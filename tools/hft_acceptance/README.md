# Synthetic HFT IPC Acceptance

This harness tests the real salias named FIFO channel in forked processes. It is
not an exchange feed decoder, order-book validator, NIC benchmark, or production
tick-to-trade acceptance test. No product code is changed.

See `doc/hft-market-data-acceptance-2026-10-18.md` for status, methodology, results,
limitations, exact commands, and the provisional 50 us percentile criteria.

Latest consolidated results: `doc/performance-summary-2026-10-18.md`.
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

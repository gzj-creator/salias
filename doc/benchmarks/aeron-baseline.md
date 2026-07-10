# salias vs Aeron C++ IPC Baseline

最新 Tencent x86 对比见 `tencent-aeron-comparison-2026-07-10.md`。该次 2P/1C、固定绑核、真实多进程
IPC 测试中，FIFO batch=1 中位吞吐为 Aeron 的约 41%，未达到追平验收线。

Date: 2026-07-06

Environment:
- Host path: `/Users/gongzhijie/Desktop/projects/git/salias`
- Runtime: Colima Linux VM, aarch64
- Compiler: GCC 13.3.0
- salias build: `release` preset, `-O3`, LTO, `-march=native`
- Aeron: `third_party/aeron-1.52.0.tar.gz`
- Aeron build: C/C++ release build in `/tmp`, tests/docs/archive disabled
- Aeron transport: external C media driver, `aeron:ipc`

Method:
- Payload: 64 bytes
- Timing window: all producers/consumers start together; stop when all consumers receive expected messages
- SPSC: 1 producer, 1 consumer
- SPMC: 1 producer, 2 consumers, each consumer receives every message
- MPMC: 4 producers, 2 consumers, all-to-all pub/sub; every consumer receives every producer's messages
- Aeron producers use `ExclusivePublication::tryClaim`
- salias currently uses public `Publisher::offer`, so salias includes one user-buffer-to-ring copy
- This salias MPMC baseline used a composed in-process topology: one Broadcast channel per producer, all consumers subscribe to all producer channels. Named cross-process MPMC fanout now exists, but this historical run did not use it.

Command:

```bash
bash tools/aeron_compare/run_release_compare.sh
```

Note: this was the command shape at the time of the 2026-07-06 run. The current
script now runs the cross-process MPSC/MPMC workload recorded below.

Raw output:

```text
RESULT library=salias scenario=spsc producers=1 consumers=1 payload=64 published=1000000 delivered=1000000 seconds=0.0252318 publish_msg_per_sec=3.96326e+07 delivery_msg_per_sec=3.96326e+07 delivery_mib_per_sec=2418.98
RESULT library=aeron-cpp scenario=spsc producers=1 consumers=1 payload=64 published=1000000 delivered=1000000 seconds=0.0537834 publish_msg_per_sec=1.85931e+07 delivery_msg_per_sec=1.85931e+07 delivery_mib_per_sec=1134.83
RESULT library=salias scenario=spmc producers=1 consumers=2 payload=64 published=500000 delivered=1000000 seconds=0.0470091 publish_msg_per_sec=1.06362e+07 delivery_msg_per_sec=2.12725e+07 delivery_mib_per_sec=1298.37
RESULT library=aeron-cpp scenario=spmc producers=1 consumers=2 payload=64 published=500000 delivered=1000000 seconds=0.0604662 publish_msg_per_sec=8.26908e+06 delivery_msg_per_sec=1.65382e+07 delivery_mib_per_sec=1009.41
RESULT library=salias scenario=mpmc producers=4 consumers=2 payload=64 published=800000 delivered=1600000 seconds=0.0332193 publish_msg_per_sec=2.40824e+07 delivery_msg_per_sec=4.81647e+07 delivery_mib_per_sec=2939.74
RESULT library=aeron-cpp scenario=mpmc producers=4 consumers=2 payload=64 published=800000 delivered=1600000 seconds=0.0526286 publish_msg_per_sec=1.52008e+07 delivery_msg_per_sec=3.04017e+07 delivery_mib_per_sec=1855.57
```

Summary:

| Scenario | salias publish msg/s | Aeron publish msg/s | salias/Aeron | salias delivery msg/s | Aeron delivery msg/s | salias/Aeron |
|---|---:|---:|---:|---:|---:|---:|
| SPSC | 39.63M | 18.59M | 2.13x | 39.63M | 18.59M | 2.13x |
| SPMC | 10.64M | 8.27M | 1.29x | 21.27M | 16.54M | 1.29x |
| MPMC all-to-all | 24.08M | 15.20M | 1.58x | 48.16M | 30.40M | 1.58x |

Interpretation:
- salias is ahead in this single release run for SPSC, SPMC, and composed all-to-all MPMC.
- This is a VM single-run baseline, not a final performance claim. Earlier runs in the same environment varied materially, so the next benchmark milestone should add repetitions, CPU pinning, warmup, and percentile latency.
- The SPMC/MPMC comparison is not yet a final product claim: at the time of this run,
  salias lacked named multi-process MPMC fanout and the public zero-copy `try_claim`
  API, so those paths were not equivalent to Aeron's optimized IPC publication path.
  A follow-up run must use the named salias IPC path and multiple measured rounds.

## Cross-Process MPSC/MPMC Run

Date: 2026-07-08

Method:
- Runtime: same Colima Linux VM, aarch64 environment.
- Payload: 64 bytes.
- salias transport: named POSIX shared memory control/ring objects, no driver.
- Aeron transport: external C media driver, `aeron:ipc`.
- Endpoints: forked producer and subscriber OS processes for both salias and Aeron.
- Publish path: salias `Publisher::try_claim` / `PublishClaim::commit`; Aeron
  `ExclusivePublication::tryClaim`.
- Consume path: salias `Subscriber::poll(..., poll_limit=64)`; Aeron
  `Subscription::poll(..., fragment_limit=64)`.
- Warmup: 1 round per scenario.
- Measured rounds: 5.
- MPSC: 4 producers, 1 consumer, 200,000 messages per producer.
- MPMC: 4 producers, 2 consumers, 200,000 messages per producer; every consumer
  receives every producer's messages.
- Current script salias page matrix: `SALIAS_PAGES="normal huge2m huge1g"` by
  default. `RESULT library=salias-ipc` rows include `page=<value>` and
  `capacity=<bytes>`. Aeron remains one IPC baseline per scenario.
- Huge page rows require a mounted hugetlbfs directory and reserved huge pages.
  The default hugetlbfs directory is `/dev/hugepages`; set
  `SALIAS_HUGETLBFS_DIR` to override it. If a huge page run cannot create its
  ring, the script prints `SKIP library=salias-ipc scenario=<scenario>
  page=<page> ... reason=status_<code>` instead of silently falling back to
  normal pages.

Command:

```bash
bash tools/aeron_compare/run_release_compare.sh
```

Measured raw output from the 2026-07-08 normal-page run captured before the
`page=` output field was added:

```text
RESULT library=salias-ipc scenario=mpsc producers=4 consumers=1 payload=64 poll_limit=64 published=800000 delivered=800000 seconds=0.0263676 publish_msg_per_sec=3.03403e+07 delivery_msg_per_sec=3.03403e+07 delivery_mib_per_sec=1851.83
RESULT library=aeron-cpp scenario=mpsc producers=4 consumers=1 payload=64 published=800000 delivered=800000 seconds=0.0185926 publish_msg_per_sec=4.3028e+07 delivery_msg_per_sec=4.3028e+07 delivery_mib_per_sec=2626.22
RESULT library=salias-ipc scenario=mpsc producers=4 consumers=1 payload=64 poll_limit=64 published=800000 delivered=800000 seconds=0.0286151 publish_msg_per_sec=2.79573e+07 delivery_msg_per_sec=2.79573e+07 delivery_mib_per_sec=1706.38
RESULT library=aeron-cpp scenario=mpsc producers=4 consumers=1 payload=64 published=800000 delivered=800000 seconds=0.0161963 publish_msg_per_sec=4.9394e+07 delivery_msg_per_sec=4.9394e+07 delivery_mib_per_sec=3014.77
RESULT library=salias-ipc scenario=mpsc producers=4 consumers=1 payload=64 poll_limit=64 published=800000 delivered=800000 seconds=0.0357375 publish_msg_per_sec=2.23854e+07 delivery_msg_per_sec=2.23854e+07 delivery_mib_per_sec=1366.3
RESULT library=aeron-cpp scenario=mpsc producers=4 consumers=1 payload=64 published=800000 delivered=800000 seconds=0.0205295 publish_msg_per_sec=3.89684e+07 delivery_msg_per_sec=3.89684e+07 delivery_mib_per_sec=2378.44
RESULT library=salias-ipc scenario=mpsc producers=4 consumers=1 payload=64 poll_limit=64 published=800000 delivered=800000 seconds=0.0417838 publish_msg_per_sec=1.91462e+07 delivery_msg_per_sec=1.91462e+07 delivery_mib_per_sec=1168.59
RESULT library=aeron-cpp scenario=mpsc producers=4 consumers=1 payload=64 published=800000 delivered=800000 seconds=0.0496329 publish_msg_per_sec=1.61183e+07 delivery_msg_per_sec=1.61183e+07 delivery_mib_per_sec=983.785
RESULT library=salias-ipc scenario=mpsc producers=4 consumers=1 payload=64 poll_limit=64 published=800000 delivered=800000 seconds=0.0300699 publish_msg_per_sec=2.66047e+07 delivery_msg_per_sec=2.66047e+07 delivery_mib_per_sec=1623.82
RESULT library=aeron-cpp scenario=mpsc producers=4 consumers=1 payload=64 published=800000 delivered=800000 seconds=0.0188545 publish_msg_per_sec=4.24302e+07 delivery_msg_per_sec=4.24302e+07 delivery_mib_per_sec=2589.74
RESULT library=salias-ipc scenario=mpmc producers=4 consumers=2 payload=64 poll_limit=64 published=800000 delivered=1600000 seconds=0.0378944 publish_msg_per_sec=2.11113e+07 delivery_msg_per_sec=4.22226e+07 delivery_mib_per_sec=2577.06
RESULT library=aeron-cpp scenario=mpmc producers=4 consumers=2 payload=64 published=800000 delivered=1600000 seconds=0.0453614 publish_msg_per_sec=1.76361e+07 delivery_msg_per_sec=3.52722e+07 delivery_mib_per_sec=2152.85
RESULT library=salias-ipc scenario=mpmc producers=4 consumers=2 payload=64 poll_limit=64 published=800000 delivered=1600000 seconds=0.0255875 publish_msg_per_sec=3.12652e+07 delivery_msg_per_sec=6.25305e+07 delivery_mib_per_sec=3816.56
RESULT library=aeron-cpp scenario=mpmc producers=4 consumers=2 payload=64 published=800000 delivered=1600000 seconds=0.0811495 publish_msg_per_sec=9.85834e+06 delivery_msg_per_sec=1.97167e+07 delivery_mib_per_sec=1203.41
RESULT library=salias-ipc scenario=mpmc producers=4 consumers=2 payload=64 poll_limit=64 published=800000 delivered=1600000 seconds=0.0305628 publish_msg_per_sec=2.61756e+07 delivery_msg_per_sec=5.23512e+07 delivery_mib_per_sec=3195.26
RESULT library=aeron-cpp scenario=mpmc producers=4 consumers=2 payload=64 published=800000 delivered=1600000 seconds=0.036785 publish_msg_per_sec=2.1748e+07 delivery_msg_per_sec=4.3496e+07 delivery_mib_per_sec=2654.78
RESULT library=salias-ipc scenario=mpmc producers=4 consumers=2 payload=64 poll_limit=64 published=800000 delivered=1600000 seconds=0.0313088 publish_msg_per_sec=2.5552e+07 delivery_msg_per_sec=5.11039e+07 delivery_mib_per_sec=3119.13
RESULT library=aeron-cpp scenario=mpmc producers=4 consumers=2 payload=64 published=800000 delivered=1600000 seconds=0.150166 publish_msg_per_sec=5.32744e+06 delivery_msg_per_sec=1.06549e+07 delivery_mib_per_sec=650.322
RESULT library=salias-ipc scenario=mpmc producers=4 consumers=2 payload=64 poll_limit=64 published=800000 delivered=1600000 seconds=0.0447808 publish_msg_per_sec=1.78648e+07 delivery_msg_per_sec=3.57296e+07 delivery_mib_per_sec=2180.76
RESULT library=aeron-cpp scenario=mpmc producers=4 consumers=2 payload=64 published=800000 delivered=1600000 seconds=0.0224311 publish_msg_per_sec=3.56647e+07 delivery_msg_per_sec=7.13294e+07 delivery_mib_per_sec=4353.6
```

Summary across measured rounds:

| Scenario | Metric | salias-ipc | Aeron IPC | salias/Aeron |
|---|---|---:|---:|---:|
| MPSC 4P/1C | Avg publish msg/s | 25.29M | 37.99M | 0.67x |
| MPSC 4P/1C | Median publish msg/s | 26.60M | 42.43M | 0.63x |
| MPMC 4P/2C | Avg publish msg/s | 24.39M | 18.05M | 1.35x |
| MPMC 4P/2C | Median publish msg/s | 25.55M | 17.64M | 1.45x |
| MPMC 4P/2C | Avg delivery msg/s | 48.79M | 36.09M | 1.35x |
| MPMC 4P/2C | Median delivery msg/s | 51.10M | 35.27M | 1.45x |

Interpretation:
- In this 5-round run, Aeron IPC is faster for MPSC 4P/1C.
- In this 5-round run, salias named IPC is faster for MPMC 4P/2C fanout on
  average and median delivery throughput, but both sides still show VM scheduling
  variance.
- These numbers should not be generalized without CPU pinning, longer timing
  windows, and latency percentile collection.

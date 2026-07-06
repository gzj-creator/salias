# salias vs Aeron C++ IPC Baseline

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
- salias MPMC is a composed topology: one Broadcast channel per producer, all consumers subscribe to all producer channels. salias does not yet have one native MPMC fanout channel.

Command:

```bash
bash tools/aeron_compare/run_release_compare.sh
```

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
- The SPMC/MPMC comparison is not yet a final product claim: salias currently lacks a native multi-producer multi-consumer fanout channel and the public zero-copy `try_claim` API is not implemented, so those paths are not equivalent to Aeron's optimized IPC publication path.
